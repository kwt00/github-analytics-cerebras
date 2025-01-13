// First part - setup and configuration
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

if (!TOKEN) throw new Error('TOKEN is required');
if (!GUILD_ID) throw new Error('GUILD_ID is required');
if (!CHANNEL_ID) throw new Error('CHANNEL_ID is required'); 
if (!SHEET_ID) throw new Error('SHEET_ID is required');

let GOOGLE_CREDENTIALS;
try {
   GOOGLE_CREDENTIALS = process.env.GOOGLE_CREDENTIALS ? 
       JSON.parse(process.env.GOOGLE_CREDENTIALS) : null;
   if (!GOOGLE_CREDENTIALS) throw new Error('GOOGLE_CREDENTIALS is required');
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

function log(message, data = null) {
   const timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
   console.log(`[${timestamp}] ${message}`);
   if (data) console.log(data);
}


// Time and request handling functions
function adjustToLocalTime(utcTimeStr) {
    const utcTime = moment.utc(utcTimeStr);
    if (!utcTime.isValid()) {
        log(`Invalid UTC time: ${utcTimeStr}`);
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
    
    log(`Date range parsed: ${startDate.format()} to ${endDate.format()}`);
    return { startDate, endDate };
 }
 
 function isWeekInProgress(endDate) {
    return moment().tz(LOCAL_TIMEZONE).isSameOrBefore(endDate);
 }
 
 async function makeDiscordRequest(endpoint, method = "GET", ignore403 = false) {
    const url = `${DISCORD_API_BASE}${endpoint}`;
    log(`Making Discord request: ${method} ${url}`);
    
    while (true) {
        try {
            const response = await fetch(url, { method, headers: HEADERS });
            
            if (response.status === 429) {
                const data = await response.json();
                const retryAfter = data.retry_after;
                log(`Rate limited. Waiting ${retryAfter} seconds...`);
                await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                continue;
            }
            
            if (response.status === 403 && ignore403) {
                log(`Ignoring 403 error for ${url}`);
                return null;
            }
            
            if (!response.ok) {
                throw new Error(`Discord API Error: ${response.status} ${response.statusText}`);
            }
            
            const data = await response.json();
            log(`Request successful: ${url}`);
            return data;
        } catch (error) {
            log(`Request failed: ${url}`, { error: error.message });
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
    
    while (true) {
        const batch = await getAuditLogs(actionType, lastId);
        if (!batch || !batch.audit_log_entries.length) break;
        
        const relevantEntries = batch.audit_log_entries.filter(entry => {
            const entryTime = moment(entry.created_at);
            return entryTime.isSameOrAfter(startDate);
        });
        
        if (relevantEntries.length < batch.audit_log_entries.length) break;
        
        logs.push(...relevantEntries);
        lastId = batch.audit_log_entries[batch.audit_log_entries.length - 1].id;
        await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    return logs;
 }

 // Member and message tracking functions
async function getGuildInfo() {
    log('Fetching guild info');
    return await makeDiscordRequest(`/guilds/${GUILD_ID}?with_counts=true`);
 }
 
 async function getHistoricalMemberCount(endDate) {
    const currentInfo = await getGuildInfo();
    const currentCount = currentInfo.approximate_member_count;
    
    // Get audit logs for joins and leaves after the end date
    const joinLogs = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_ADD, endDate);
    const leaveLogs = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_REMOVE, endDate);
    
    // Calculate historical count by working backwards
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
    
    log(`Historical member count for ${endDate.format()}: ${historicalCount}`);
    return historicalCount;
 }
 
 async function getTotalMembers(endDate) {
    log(`Getting total members before: ${endDate.format()}`);
    
    if (isWeekInProgress(endDate)) {
        const guildInfo = await getGuildInfo();
        return guildInfo.approximate_member_count;
    }
    
    return await getHistoricalMemberCount(endDate);
 }
 
 async function getNewMembers(startDate, endDate) {
    log(`Getting new members between ${startDate.format()} and ${endDate.format()}`);
    const joinLogs = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_ADD, startDate);
    
    const newMembers = joinLogs.filter(entry => {
        const joinDate = moment(entry.created_at);
        return joinDate.isSameOrAfter(startDate) && joinDate.isSameOrBefore(endDate);
    });
    
    log(`New members in period: ${newMembers.length}`);
    return newMembers.length;
 }
 
 async function getAllChannelMessages(channelId, startDate, endDate) {
    log(`Fetching messages for channel ${channelId}`);
    const messages = [];
    let lastId = null;
    let batchCount = 0;
    
    // Get deleted messages from audit log
    const deletedMessages = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MESSAGE_DELETE, startDate);
    const deletedMessageIds = new Set(deletedMessages.map(entry => entry.target_id));

    // Continue from previous getAllChannelMessages function
   while (true) {
    try {
        const endpoint = `/channels/${channelId}/messages?limit=100${lastId ? `&before=${lastId}` : ''}`;
        const batch = await makeDiscordRequest(endpoint, "GET", true);
        
        if (!batch || batch.length === 0) break;
        
        batchCount++;
        log(`Processing message batch ${batchCount}: ${batch.length} messages`);
        
        let reachedEnd = false;
        for (const msg of batch) {
            const msgTime = adjustToLocalTime(msg.timestamp);
            if (msgTime.isBefore(startDate)) {
                reachedEnd = true;
                break;
            }
            if (msgTime.isSameOrBefore(endDate)) {
                // Include deleted messages from audit log
                if (deletedMessageIds.has(msg.id)) {
                    messages.push({ ...msg, deleted: true });
                } else {
                    messages.push(msg);
                }
            }
        }
        
        if (reachedEnd) break;
        lastId = batch[batch.length - 1].id;
        await new Promise(resolve => setTimeout(resolve, 100));
        
    } catch (error) {
        log(`Error fetching messages for channel ${channelId}`, { error: error.message });
        break;
    }
}

log(`Total messages found in channel ${channelId}: ${messages.length}`);
return messages;
}

async function getReactions(startDate, endDate) {
log('Calculating total reactions');
const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
const textChannels = channels.filter(channel => channel.type === 0);
let totalReactions = 0;

for (const channel of textChannels) {
    const messages = await getAllChannelMessages(channel.id, startDate, endDate);
    let channelReactions = 0;
    messages.forEach(msg => {
        if (msg.reactions && !msg.deleted) {  // Don't count reactions on deleted messages
            channelReactions += msg.reactions.reduce((sum, reaction) => sum + reaction.count, 0);
        }
    });
    log(`Reactions in channel ${channel.id}: ${channelReactions}`);
    totalReactions += channelReactions;
}

log(`Total reactions across all channels: ${totalReactions}`);
return totalReactions;
}

async function getProjectLinks(startDate, endDate) {
log('Collecting project links');
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
log(`Found ${links.length} unique project links`);
return links;
}

async function getMessagesPosted(startDate, endDate) {
log('Counting total messages posted');
const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
const textChannels = channels.filter(channel => channel.type === 0);
let totalMessages = 0;

// Get all deleted messages in the period
const deletedMessages = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MESSAGE_DELETE, startDate);
const deletedInPeriod = deletedMessages.filter(entry => {
    const deleteDate = moment(entry.created_at);
    return deleteDate.isSameOrAfter(startDate) && deleteDate.isSameOrBefore(endDate);
});

for (const channel of textChannels) {
    const messages = await getAllChannelMessages(channel.id, startDate, endDate);
    const validMessages = messages.filter(msg => !msg.deleted);
    log(`Valid messages in channel ${channel.id}: ${validMessages.length}`);
    totalMessages += validMessages.length;
}

// Add deleted messages that were in the period
totalMessages += deletedInPeriod.length;

log(`Total messages across all channels (including deleted): ${totalMessages}`);
return totalMessages;
}

async function getActiveUsers(startDate, endDate) {
log('Counting active users');
const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
const textChannels = channels.filter(channel => channel.type === 0);
const activeUsers = new Set();

// Get member updates from audit log
const memberUpdates = await getAllAuditLogs(AUDIT_LOG_ACTIONS.MEMBER_UPDATE, startDate);
memberUpdates.forEach(entry => {
    if (entry.target_id) {
        const updateDate = moment(entry.created_at);
        if (updateDate.isSameOrAfter(startDate) && updateDate.isSameOrBefore(endDate)) {
            activeUsers.add(entry.target_id);
        }
    }
});

// Get message authors
for (const channel of textChannels) {
    const messages = await getAllChannelMessages(channel.id, startDate, endDate);
    messages.forEach(msg => {
        if (!msg.deleted && msg.author) {
            activeUsers.add(msg.author.id);
        }
    });
}

log(`Total unique active users: ${activeUsers.size}`);
return activeUsers.size;
}

async function updateGoogleSheet(weekRange, metrics) {
    log('Updating Google Sheet', metrics);
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
        log(`Updating existing row at index ${existingRowIndex + 1}`);
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `Sheet1!A${existingRowIndex + 1}:H${existingRowIndex + 1}`,
            valueInputOption: 'RAW',
            resource: { values: [newRow] }
        });
    } else {
        log('Adding new row at top');
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
    
    log('Sheet update completed');
 }
 
 app.post('/collect-analytics', async (req, res) => {
    log('Starting analytics collection');
    res.json({ message: 'Analytics collection started' });
 
    try {
        const { weekRange } = req.body;
        log(`Processing week range: ${weekRange}`);
        
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
        
        log('Final metrics:', metrics);
        await updateGoogleSheet(weekRange, metrics);
        
        log('Analytics collection completed successfully');
    } catch (error) {
        log('Analytics collection failed', { error: error.message });
        console.error('Error:', error);
    }
 });
 
 const port = process.env.PORT || 3000;
 app.listen(port, () => {
    log(`Server running on port ${port}`);
 });
