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

const CHANNEL_TYPES = {
    TEXT: 0,
    PUBLIC_THREAD: 11,
    PRIVATE_THREAD: 12,
    FORUM: 15,
    FORUM_POST: 11
};

function isMessageableChannel(channel) {
    return channel.type === CHANNEL_TYPES.TEXT || 
           channel.type === CHANNEL_TYPES.FORUM;
}

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

async function getTotalMembers(startDate, endDate) {
    try {
        // Get current members and their join dates
        const allMembers = await getAllGuildMembers();
        
        // Get all leave logs up to endDate
        const leaveAuditLogs = await getMemberLeaves(startDate, endDate);
        
        // Create a map of member leaves with their timestamps
        const memberLeaves = new Map();
        leaveAuditLogs.forEach(log => {
            memberLeaves.set(log.target_id, moment(log.created_at));
        });

        const count = allMembers.filter(member => {
            if (!member.joined_at) return false;

            const joinedAt = adjustToLocalTime(member.joined_at);
            
            // Check if they joined before or during the period
            if (!joinedAt.isSameOrBefore(endDate)) {
                return false;
            }

            // Check if they left during the period
            const leaveDate = memberLeaves.get(member.user.id);
            if (leaveDate && leaveDate.isSameOrBefore(endDate)) {
                return false;
            }

            return true;
        }).length;

        // Add members who left during the period but were there at the start
        const additionalMembers = leaveAuditLogs.filter(log => {
            const leaveDate = moment(log.created_at);
            const member = allMembers.find(m => m.user.id === log.target_id);
            
            // If they're not in current members and left during period
            return !member && 
                   leaveDate.isSameOrAfter(startDate) && 
                   leaveDate.isSameOrBefore(endDate);
        }).length;

        return count + additionalMembers;

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
    let batchCount = 0;
    let rateLimitDelay = 100;

    // Get deleted message logs first
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

    // Helper function to fetch messages from a channel or thread
    async function fetchMessages(channelOrThreadId) {
        const channelMessages = [];
        let messageLastId = null;
        
        while (true) {
            try {
                const endpoint = `/channels/${channelOrThreadId}/messages?limit=100${messageLastId ? `&before=${messageLastId}` : ''}`;
                const batch = await makeDiscordRequest(endpoint, "GET", true);

                if (!batch || batch.length === 0) break;

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
                        channelMessages.push(msg);
                    }
                }

                if (reachedEnd) break;
                messageLastId = batch[batch.length - 1].id;

                await new Promise(resolve => setTimeout(resolve, rateLimitDelay));
                batchCount++;
                if (batchCount % 10 === 0) rateLimitDelay += 50;

            } catch (error) {
                if (error.message.includes('rate limited')) {
                    rateLimitDelay *= 2;
                    console.log(`Increased rate limit delay to ${rateLimitDelay}ms`);
                    continue;
                }
                if (!error.message.includes('404')) {
                    console.log(`Error fetching messages for channel/thread ${channelOrThreadId}`, { error: error.message });
                }
                break;
            }
        }
        return channelMessages;
    }

    // Get messages from the main channel
    try {
        const channelInfo = await makeDiscordRequest(`/channels/${channelId}`, "GET", true);
        
        if (!channelInfo) {
            console.log(`Channel ${channelId} not found or not accessible`);
            return messages;
        }

        // Get main channel messages
        const channelMessages = await fetchMessages(channelId);
        messages.push(...channelMessages);

        // Only try to get threads for channels that can have them
        if ([CHANNEL_TYPES.TEXT, CHANNEL_TYPES.FORUM, CHANNEL_TYPES.PUBLIC_THREAD].includes(channelInfo.type)) {
            // Try to get active threads first
            try {
                // Check if channel has thread_metadata (meaning it's already a thread)
                if (!channelInfo.thread_metadata) {
                    const activeThreadsEndpoint = `/channels/${channelId}/threads/active`;
                    const activeThreads = await makeDiscordRequest(activeThreadsEndpoint, "GET", true);
                    
                    if (activeThreads && activeThreads.threads) {
                        for (const thread of activeThreads.threads) {
                            try {
                                const threadMessages = await fetchMessages(thread.id);
                                messages.push(...threadMessages);
                            } catch (threadError) {
                                // Only log if it's not a 404
                                if (!threadError.message.includes('404')) {
                                    console.log(`Error fetching messages for thread ${thread.id}`, { error: threadError.message });
                                }
                                continue;
                            }
                        }
                    }
                }
            } catch (activeThreadsError) {
                // Only log if it's not a 404
                if (!activeThreadsError.message.includes('404')) {
                    console.log(`Error fetching active threads for channel ${channelId}`, { error: activeThreadsError.message });
                }
            }

            // Try to get archived threads if this isn't already a thread
            if (!channelInfo.thread_metadata) {
                try {
                    // Try to get archived public threads
                    const archivedPublicThreadsEndpoint = `/channels/${channelId}/threads/archived/public`;
                    const publicThreads = await makeDiscordRequest(archivedPublicThreadsEndpoint, "GET", true);
                    
                    if (publicThreads && publicThreads.threads) {
                        for (const thread of publicThreads.threads) {
                            if (moment(thread.thread_metadata.creation_timestamp).isSameOrAfter(startDate)) {
                                try {
                                    const threadMessages = await fetchMessages(thread.id);
                                    messages.push(...threadMessages);
                                } catch (threadError) {
                                    if (!threadError.message.includes('404')) {
                                        console.log(`Error fetching messages for archived public thread ${thread.id}`, { error: threadError.message });
                                    }
                                    continue;
                                }
                            }
                        }
                    }

                    // Try to get archived private threads
                    const archivedPrivateThreadsEndpoint = `/channels/${channelId}/threads/archived/private`;
                    const privateThreads = await makeDiscordRequest(archivedPrivateThreadsEndpoint, "GET", true);
                    
                    if (privateThreads && privateThreads.threads) {
                        for (const thread of privateThreads.threads) {
                            if (moment(thread.thread_metadata.creation_timestamp).isSameOrAfter(startDate)) {
                                try {
                                    const threadMessages = await fetchMessages(thread.id);
                                    messages.push(...threadMessages);
                                } catch (threadError) {
                                    if (!threadError.message.includes('404')) {
                                        console.log(`Error fetching messages for archived private thread ${thread.id}`, { error: threadError.message });
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                } catch (archivedThreadsError) {
                    if (!archivedThreadsError.message.includes('404')) {
                        console.log(`Error fetching archived threads for channel ${channelId}`, { error: archivedThreadsError.message });
                    }
                }
            }
        }
    } catch (error) {
        if (!error.message.includes('404')) {
            console.log(`Error checking channel type for ${channelId}`, { error: error.message });
        }
    }

    return messages;
}

    

async function getReactions(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const messageableChannels = channels.filter(isMessageableChannel);
    let totalReactions = 0;

    for (const channel of messageableChannels) {
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
    const urlRegex = /(?:(?:https?|ftp):\/\/)?[-\w@:%._\+~#=]{1,256}\.[a-z]{2,}\b(?:[-\w()@:%_\+.~#?&\/=]*)/gi;
    
    const projectLinks = new Set();

    messages.forEach(msg => {
        console.log("POTENTIAL LINK? : "+msg);
        if (!msg.deleted && !msg.content.toLowerCase().includes('.cerebras.ai')) {
            const links = msg.content.match(urlRegex) || [];
            links.forEach(link => projectLinks.add(link));
        }
    });

    const links = Array.from(projectLinks);
    console.log("LINKS FOUND:");
    console.log(links);
    return links;
}

async function getMessagesPosted(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const messageableChannels = channels.filter(isMessageableChannel);
    let totalMessages = 0;

    const deletedMessages = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MESSAGE_DELETE, startDate);
    const deletedInPeriod = deletedMessages.filter(entry => {
        const deleteDate = moment(entry.created_at);
        return deleteDate.isSameOrAfter(startDate) && deleteDate.isSameOrBefore(endDate);
    });

    for (const channel of messageableChannels) {
        const messages = await getAllChannelMessages(channel.id, startDate, endDate);
        const validMessages = messages.filter(msg => !msg.deleted);
        totalMessages += validMessages.length;
    }

    totalMessages += deletedInPeriod.length;

    return totalMessages;
}

async function getActiveUsers(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const messageableChannels = channels.filter(isMessageableChannel);
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

    for (const channel of messageableChannels) {
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
            getTotalMembers(startDate, endDate),
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
