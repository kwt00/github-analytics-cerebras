const express = require('express');
const fetch = require('node-fetch');
const { google } = require('googleapis');
const moment = require('moment-timezone');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const DISCORD_API_BASE = "https://discord.com/api/v10";
const TOKEN = process.env.TOKEN;
const GUILD_ID = process.env.GUILD_ID;
const CHANNEL_ID = process.env.CHANNEL_ID;
const SHEET_ID = process.env.SHEET_ID;

let GOOGLE_CREDENTIALS;
try {
    GOOGLE_CREDENTIALS = process.env.GOOGLE_CREDENTIALS ?
        JSON.parse(process.env.GOOGLE_CREDENTIALS) : null;
} catch (error) {
    throw new Error(`Invalid GOOGLE_CREDENTIALS: ${error.message}`);
}

const LOCAL_TIMEZONE = "America/Los_Angeles";
const AUDIT_LOG_ACTIONS = {
    MEMBER_ADD: 1,
    MEMBER_REMOVE: 20,
    MESSAGE_DELETE: 72,
    MEMBER_UPDATE: 24,
    MEMBER_ROLE_UPDATE: 25
};

const HEADERS = {
    'Authorization': TOKEN.startsWith('Bot ') ? TOKEN : `Bot ${TOKEN}`,
    'User-Agent': 'DiscordBot (discord-analytics-bot, 1.0.0)',
    'Content-Type': 'application/json',
    'X-RateLimit-Precision': 'millisecond',
    'Intent': 'GUILD_MEMBERS'
};


async function getMemberLeaves(startDate, endDate) {
    const leaves = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_REMOVE, startDate);
    return leaves.filter(entry => {
        const leaveDate = moment(entry.created_at);
        return leaveDate.isSameOrAfter(startDate) && leaveDate.isSameOrBefore(endDate);
    });
}



function adjustToLocalTime(utcTimeStr) {
    const utcTime = moment.utc(utcTimeStr);
    if (!utcTime.isValid()) {
        return moment.tz(utcTimeStr, LOCAL_TIMEZONE);
    }
    return utcTime.tz(LOCAL_TIMEZONE);
}

function parseDateRange(weekRange) {
    const [startStr, endStr] = weekRange.split(' - ').map(s => s.trim());

    const startDate = moment.tz(startStr, "MMM DD YYYY", LOCAL_TIMEZONE)
        .startOf('day');
    const endDate = moment.tz(endStr, "MMM DD YYYY", LOCAL_TIMEZONE)
        .endOf('day');

    return { startDate, endDate };
}


async function makeDiscordRequest(endpoint, method = "GET", ignore403 = false) {
    const url = `${DISCORD_API_BASE}${endpoint}`;

    while (true) {
        try {
            const response = await fetch(url, { method, headers: HEADERS });

            if (response.status === 429) {
                const data = await response.json();
                const retryAfter = data.retry_after;
                console.log(`Rate limited. Waiting ${retryAfter} seconds...`);
                await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                continue;
            }

            if (response.status === 403 && ignore403) {
                return null;
            }

            if (!response.ok) {
                throw new Error(`Discord API Error: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`Request successful: ${url}`);
            return data;
        } catch (error) {
            console.log(`Request failed: ${url}`, { error: error.message });
            throw error;
        }
    }
}

async function getAuditLogs(actionType, before = null, limit = 100) {
    let endpoint = `/guilds/${GUILD_ID}/audit-logs?action_type=${actionType}&limit=${limit}`;
    if (before) {
        endpoint += `&before=${before}`;
    }
    return await makeDiscordRequest(endpoint);
}

async function getAllAuditLogs(actionType, startDate) {
    const logs = [];
    let lastId = null;
    let rateLimitDelay = 100;
    let batchCount = 0;

    while (true) {
        try {
            batchCount++;
            const batch = await getAuditLogs(actionType, lastId);

            if (!batch || !batch.audit_log_entries.length) break;


            const relevantEntries = batch.audit_log_entries.filter(entry => {
                const entryTime = moment(entry.created_at);
                return entryTime.isSameOrAfter(startDate);
            });

            if (relevantEntries.length < batch.audit_log_entries.length) break;

            logs.push(...relevantEntries);
            lastId = batch.audit_log_entries[batch.audit_log_entries.length - 1].id;

            await new Promise(resolve => setTimeout(resolve, rateLimitDelay));
            if (batchCount % 5 === 0) rateLimitDelay += 50;

        } catch (error) {
            if (error.message.includes('rate limited')) {
                rateLimitDelay *= 2;
                continue;
            }
            throw error;
        }
    }

    return logs;
}


async function getGuildInfo() {
    return await makeDiscordRequest(`/guilds/${GUILD_ID}?with_counts=true`);
}

async function getHistoricalMemberCount(endDate) {
    const currentInfo = await getGuildInfo();
    const currentCount = currentInfo.approximate_member_count;

    const joinLogs = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_ADD, endDate);
    const leaveLogs = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_REMOVE, endDate);

    let historicalCount = currentCount;

    joinLogs.forEach(entry => {
        const joinDate = moment(entry.created_at);
        if (joinDate.isAfter(endDate)) {
            historicalCount--;
        }
    });

    leaveLogs.forEach(entry => {
        const leaveDate = moment(entry.created_at);
        if (leaveDate.isAfter(endDate)) {
            historicalCount++;
        }
    });

    return historicalCount;
}

async function getTotalMembers(endDate) {

    try {
        const allMembers = await getAllGuildMembers();
        const leaveAuditLogs = await getMemberLeaves(startDate, endDate);
        const leftMemberIds = new Set(leaveAuditLogs.map(log => log.target_id));


        const count = allMembers.filter(member => {
            if (!member.joined_at) return false;

            const joinedAt = adjustToLocalTime(member.joined_at);
            const wasInServer = joinedAt.isSameOrBefore(endDate);

            const leftBeforeEndDate = leftMemberIds.has(member.user.id) &&
                leaveAuditLogs.find(log =>
                    log.target_id === member.user.id &&
                    moment(log.created_at).isSameOrBefore(endDate)
                );

            const shouldCount = wasInServer && !leftBeforeEndDate;

            return shouldCount;
        }).length;

        return count;

    } catch (error) {
        console.log('Error getting total members:', error);
        throw error;
    }
}

async function getAllGuildMembers() {
    let allMembers = [];
    let after = '0';

    while (true) {
        const endpoint = `/guilds/${GUILD_ID}/members?limit=1000${after !== '0' ? `&after=${after}` : ''}`;
        const batch = await makeDiscordRequest(endpoint);

        if (!batch || batch.length === 0) break;

        allMembers = allMembers.concat(batch);

        if (batch.length < 1000) break;

        after = batch[batch.length - 1].user.id;
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    return allMembers;
}

async function getNewMembers(startDate, endDate) {

    try {
        const members = await getAllGuildMembers();

        const newMembers = members.filter(member => {
            if (!member.joined_at) return false;
            const joinedAt = adjustToLocalTime(member.joined_at);
            const isInRange = joinedAt.isSameOrAfter(startDate) && joinedAt.isSameOrBefore(endDate);
            if (isInRange) {
            }
            return isInRange;
        });

        return newMembers.length;
    } catch (error) {
        console.log('Error getting new members:', error);
        throw error;
    }
}

async function getAllChannelMessages(channelId, startDate, endDate) {
    const messages = [];
    let lastId = null;
    let batchCount = 0;
    let rateLimitDelay = 100;

    const deletedMessageLogs = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MESSAGE_DELETE, startDate);
    const deletedMessages = new Map();

    deletedMessageLogs.forEach(log => {
        if (log.options && log.options.channel_id === channelId) {
            deletedMessages.set(log.target_id, {
                deleteDate: moment(log.created_at),
                originalContent: log.changes?.find(c => c.key === 'content')?.old || ''
            });
        }
    });

    while (true) {
        try {
            const endpoint = `/channels/${channelId}/messages?limit=100${lastId ? `&before=${lastId}` : ''}`;
            const batch = await makeDiscordRequest(endpoint, "GET", true);

            if (!batch || batch.length === 0) break;

            batchCount++;

            let reachedEnd = false;
            for (const msg of batch) {
                const msgTime = adjustToLocalTime(msg.timestamp);
                if (msgTime.isBefore(startDate)) {
                    reachedEnd = true;
                    break;
                }
                if (msgTime.isSameOrBefore(endDate)) {
                    if (deletedMessages.has(msg.id)) {
                        const deleteInfo = deletedMessages.get(msg.id);
                        msg.deleted = true;
                        msg.deleteDate = deleteInfo.deleteDate;
                        msg.originalContent = deleteInfo.originalContent;
                    }
                    messages.push(msg);
                }
            }

            if (reachedEnd) break;
            lastId = batch[batch.length - 1].id;

            await new Promise(resolve => setTimeout(resolve, rateLimitDelay));
            if (batchCount % 10 === 0) rateLimitDelay += 50;

        } catch (error) {
            if (error.message.includes('rate limited')) {
                rateLimitDelay *= 2;
                console.log(`Increased rate limit delay to ${rateLimitDelay}ms`);
                continue;
            }
            console.log(`Error fetching messages for channel ${channelId}`, { error: error.message });
            break;
        }
    }

    return messages;
}


async function getReactions(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const textChannels = channels.filter(channel => channel.type === 0);
    let totalReactions = 0;

    for (const channel of textChannels) {
        const messages = await getAllChannelMessages(channel.id, startDate, endDate);
        let channelReactions = 0;
        messages.forEach(msg => {
            if (msg.reactions && !msg.deleted) {
                channelReactions += msg.reactions.reduce((sum, reaction) => sum + reaction.count, 0);
            }
        });
        totalReactions += channelReactions;
    }

    return totalReactions;
}

async function getProjectLinks(startDate, endDate) {
    const messages = await getAllChannelMessages(CHANNEL_ID, startDate, endDate);
    const urlRegex = /(https?:\/\/[^\s<>"]+|www\.[^\s<>"]+)/g;
    const projectLinks = new Set();

    messages.forEach(msg => {
        if (!msg.deleted && !msg.content.toLowerCase().includes('.cerebras.ai')) {
            const links = msg.content.match(urlRegex) || [];
            links.forEach(link => projectLinks.add(link));
        }
    });

    const links = Array.from(projectLinks);
    return links;
}

async function getMessagesPosted(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const textChannels = channels.filter(channel => channel.type === 0);
    let totalMessages = 0;

    const deletedMessages = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MESSAGE_DELETE, startDate);
    const deletedInPeriod = deletedMessages.filter(entry => {
        const deleteDate = moment(entry.created_at);
        return deleteDate.isSameOrAfter(startDate) && deleteDate.isSameOrBefore(endDate);
    });

    for (const channel of textChannels) {
        const messages = await getAllChannelMessages(channel.id, startDate, endDate);
        const validMessages = messages.filter(msg => !msg.deleted);
        totalMessages += validMessages.length;
    }


    totalMessages += deletedInPeriod.length;

    return totalMessages;
}

async function getActiveUsers(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const textChannels = channels.filter(channel => channel.type === 0);
    const activeUsers = new Set();

    const memberUpdates = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_UPDATE, startDate);
    memberUpdates.forEach(entry => {
        if (entry.target_id) {
            const updateDate = moment(entry.created_at);
            if (updateDate.isSameOrAfter(startDate) && updateDate.isSameOrBefore(endDate)) {
                activeUsers.add(entry.target_id);
            }
        }
    });

    for (const channel of textChannels) {
        const messages = await getAllChannelMessages(channel.id, startDate, endDate);
        messages.forEach(msg => {
            if (!msg.deleted && msg.author) {
                activeUsers.add(msg.author.id);
            }
        });
    }

    return activeUsers.size;
}

async function updateGoogleSheet(weekRange, metrics) {
    console.log('Updating Google Sheet', metrics);
    const auth = new google.auth.GoogleAuth({
        credentials: GOOGLE_CREDENTIALS,
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });

    const sheets = google.sheets({ version: 'v4', auth });

    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:H',
    });

    const rows = response.data.values || [];
    const existingRowIndex = rows.findIndex(row => row[0] === weekRange);

    const newRow = [
        weekRange,
        metrics.totalMembers,
        metrics.newMembers,
        metrics.activeUsers,
        metrics.messagesPosted,
        metrics.reactions,
        metrics.projectsShowcased,
        metrics.projectLinks.join('\n')
    ];

    if (existingRowIndex !== -1) {
        console.log(`Updating existing row at index ${existingRowIndex + 1}`);
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `Sheet1!A${existingRowIndex + 1}:H${existingRowIndex + 1}`,
            valueInputOption: 'RAW',
            resource: { values: [newRow] }
        });
    } else {
        console.log('Adding new row at top');
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: 'Sheet1!A2:H2',
            valueInputOption: 'RAW',
            resource: { values: [newRow] }
        });

        if (rows.length > 1) {
            const existingRows = rows.slice(1);
            await sheets.spreadsheets.values.update({
                spreadsheetId: SHEET_ID,
                range: `Sheet1!A3:H${rows.length + 1}`,
                valueInputOption: 'RAW',
                resource: { values: existingRows }
            });
        }
    }

}

app.post('/collect-analytics', async (req, res) => {
    res.json({ message: 'Analytics collection started' });

    try {
        const { weekRange } = req.body;

        const { startDate, endDate } = parseDateRange(weekRange);

        const [totalMembers, newMembers] = await Promise.all([
            getTotalMembers(endDate),
            getNewMembers(startDate, endDate)
        ]);

        const [activeUsers, messagesPosted, reactions, projectLinks] = await Promise.all([
            getActiveUsers(startDate, endDate),
            getMessagesPosted(startDate, endDate),
            getReactions(startDate, endDate),
            getProjectLinks(startDate, endDate)
        ]);

        const metrics = {
            totalMembers,
            newMembers,
            activeUsers,
            messagesPosted,
            reactions,
            projectLinks,
            projectsShowcased: projectLinks.length
        };

        console.log('Final metrics:', metrics);
        await updateGoogleSheet(weekRange, metrics);

    } catch (error) {
        console.log('Analytics collection failed', { error: error.message });
        console.error('Error:', error);
    }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
