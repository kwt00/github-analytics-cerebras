const express = require('express');
const fetch = require('node-fetch');
const { google } = require('googleapis');
const moment = require('moment-timezone');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Configuration with validation
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

const HEADERS = {
    'Authorization': TOKEN.startsWith('Bot ') ? TOKEN : `Bot ${TOKEN}`,
    'User-Agent': 'DiscordBot (discord-analytics-bot, 1.0.0)',
    'Content-Type': 'application/json'
};

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

function isWeekInProgress(endDate) {
    return moment().tz(LOCAL_TIMEZONE).isSameOrBefore(endDate);
}

async function makeDiscordRequest(endpoint, method = "GET", ignore403 = false) {
    const url = `${DISCORD_API_BASE}${endpoint}`;
    
    while (true) {
        const response = await fetch(url, { method, headers: HEADERS });
        
        if (response.status === 429) {
            const data = await response.json();
            await new Promise(resolve => setTimeout(resolve, data.retry_after * 1000));
            continue;
        }
        
        if (response.status === 403 && ignore403) return null;
        if (!response.ok) throw new Error(`Discord API Error: ${response.status} ${response.statusText}`);
        
        return await response.json();
    }
}

async function getAllGuildMembers() {
    const allMembers = [];
    let after = '0';
    
    while (true) {
        const endpoint = `/guilds/${GUILD_ID}/members?limit=1000${after !== '0' ? `&after=${after}` : ''}`;
        const batch = await makeDiscordRequest(endpoint);
        
        if (!batch || batch.length === 0) break;
        allMembers.push(...batch);
        if (batch.length < 1000) break;
        
        after = batch[batch.length - 1].user.id;
        await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    return allMembers;
}

async function getTotalMembers(endDate) {
    const weekInProgress = isWeekInProgress(endDate);
    if (weekInProgress) {
        const guildInfo = await makeDiscordRequest(`/guilds/${GUILD_ID}`);
        return guildInfo.approximate_member_count;
    }
    
    const members = await getAllGuildMembers();
    return members.filter(member => {
        if (!member.joined_at) return false;
        return adjustToLocalTime(member.joined_at).isSameOrBefore(endDate);
    }).length;
}

async function getNewMembers(startDate, endDate) {
    const members = await getAllGuildMembers();
    return members.filter(member => {
        if (!member.joined_at) return false;
        const joinedAt = adjustToLocalTime(member.joined_at);
        return joinedAt.isSameOrAfter(startDate) && joinedAt.isSameOrBefore(endDate);
    }).length;
}

async function getAllChannelMessages(channelId, startDate, endDate) {
    const messages = [];
    let lastId = null;
    
    while (true) {
        try {
            const endpoint = `/channels/${channelId}/messages?limit=100${lastId ? `&before=${lastId}` : ''}`;
            const batch = await makeDiscordRequest(endpoint, "GET", true);
            
            if (!batch || batch.length === 0) break;
            
            for (const msg of batch) {
                const msgTime = adjustToLocalTime(msg.timestamp);
                if (msgTime.isBefore(startDate)) return messages;
                if (msgTime.isSameOrBefore(endDate)) messages.push(msg);
            }
            
            lastId = batch[batch.length - 1].id;
            await new Promise(resolve => setTimeout(resolve, 100));
        } catch (error) {
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
        for (const msg of messages) {
            if (msg.reactions) {
                totalReactions += msg.reactions.reduce((sum, reaction) => sum + reaction.count, 0);
            }
        }
    }
    
    return totalReactions;
}

async function getProjectLinks(startDate, endDate) {
    const messages = await getAllChannelMessages(CHANNEL_ID, startDate, endDate);
    const urlRegex = /(https?:\/\/[^\s<>"]+|www\.[^\s<>"]+)/g;
    const projectLinks = new Set();
    
    for (const msg of messages) {
        if (!msg.content.toLowerCase().includes('.cerebras.ai')) {
            const links = msg.content.match(urlRegex) || [];
            links.forEach(link => projectLinks.add(link));
        }
    }
    
    return Array.from(projectLinks);
}

async function getMessagesPosted(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const textChannels = channels.filter(channel => channel.type === 0);
    let messagesPosted = 0;
    
    for (const channel of textChannels) {
        const messages = await getAllChannelMessages(channel.id, startDate, endDate);
        messagesPosted += messages.length;
    }
    
    return messagesPosted;
}

async function getActiveUsers(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const textChannels = channels.filter(channel => channel.type === 0);
    const activeUsers = new Set();
    
    for (const channel of textChannels) {
        const messages = await getAllChannelMessages(channel.id, startDate, endDate);
        messages.forEach(msg => activeUsers.add(msg.author.id));
    }
    
    return activeUsers.size;
}

async function updateGoogleSheet(weekRange, metrics) {
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
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `Sheet1!A${existingRowIndex + 1}:H${existingRowIndex + 1}`,
            valueInputOption: 'RAW',
            resource: { values: [newRow] }
        });
    } else {
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
        
        const metrics = {
            totalMembers: await getTotalMembers(endDate),
            newMembers: await getNewMembers(startDate, endDate),
            activeUsers: await getActiveUsers(startDate, endDate),
            messagesPosted: await getMessagesPosted(startDate, endDate),
            reactions: await getReactions(startDate, endDate),
            projectLinks: await getProjectLinks(startDate, endDate)
        };
        
        metrics.projectsShowcased = metrics.projectLinks.length;
        await updateGoogleSheet(weekRange, metrics);
        
        console.log('Analytics collection completed successfully!');
    } catch (error) {
        console.error('Error:', error);
    }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
