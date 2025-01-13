// At the top of server.js, right after imports
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

// Validate required environment variables
if (!TOKEN) {
    throw new Error('DISCORD_TOKEN is required');
}
if (!GUILD_ID) {
    throw new Error('GUILD_ID is required');
}
if (!CHANNEL_ID) {
    throw new Error('CHANNEL_ID is required');
}
if (!SHEET_ID) {
    throw new Error('SHEET_ID is required');
}

// Parse Google credentials with error handling
let GOOGLE_CREDENTIALS;
try {
    GOOGLE_CREDENTIALS = process.env.GOOGLE_CREDENTIALS ? 
        JSON.parse(process.env.GOOGLE_CREDENTIALS) : null;
    if (!GOOGLE_CREDENTIALS) {
        throw new Error('GOOGLE_CREDENTIALS is required');
    }
} catch (error) {
    throw new Error(`Invalid GOOGLE_CREDENTIALS: ${error.message}`);
}

const LOCAL_TIMEZONE = "America/Los_Angeles";

// Make sure Bot prefix is included in token
const HEADERS = {
    'Authorization': TOKEN.startsWith('Bot ') ? TOKEN : `Bot ${TOKEN}`,
    'User-Agent': 'DiscordBot (discord-analytics-bot, 1.0.0)',
    'Content-Type': 'application/json'
};
// Helper Functions
function adjustToLocalTime(utcTimeStr) {
    return moment(utcTimeStr).tz(LOCAL_TIMEZONE);
}

function parseDateRange(weekRange) {
    const [startStr, endStr] = weekRange.split(' - ');
    
    const startDate = moment.tz(startStr, "MMM DD YYYY", LOCAL_TIMEZONE)
        .startOf('day');
    const endDate = moment.tz(endStr, "MMM DD YYYY", LOCAL_TIMEZONE)
        .endOf('day');
    
    return { startDate, endDate };
}

function isWeekInProgress(endDate) {
    return moment().tz(LOCAL_TIMEZONE).isBefore(endDate);
}

// Discord API Functions
async function makeDiscordRequest(endpoint, method = "GET", ignore403 = false) {
    const url = `${DISCORD_API_BASE}${endpoint}`;
    console.log(`Making request to: ${url}`);
    
    while (true) {
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
        
        return await response.json();
    }
}

async function getTotalMembers(endDate) {
    const members = await getAllMembers();
    return members.filter(member => 
        adjustToLocalTime(member.joined_at).isSameOrBefore(endDate)
    ).length;
}

async function getAllMembers() {
    let members = [];
    let after = '0';
    
    while (true) {
        const endpoint = `/guilds/${GUILD_ID}/members?limit=1000${after !== '0' ? `&after=${after}` : ''}`;
        const batch = await makeDiscordRequest(endpoint);
        
        if (!batch || batch.length === 0) break;
        
        members = members.concat(batch);
        
        if (batch.length < 1000) break;
        
        after = batch[batch.length - 1].user.id;
        await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    return members;
}

async function getNewMembers(startDate, endDate) {
    const members = await getAllMembers();
    return members.filter(member => {
        const joinedAt = adjustToLocalTime(member.joined_at);
        return joinedAt.isSameOrAfter(startDate) && joinedAt.isSameOrBefore(endDate);
    }).length;
}

async function getChannelMessages(channelId, startDate, endDate) {
    const messages = [];
    let before = null;
    
    while (true) {
        try {
            const endpoint = `/channels/${channelId}/messages?limit=100${before ? `&before=${before}` : ''}`;
            const batch = await makeDiscordRequest(endpoint, "GET", true);
            
            if (!batch) return [];
            if (batch.length === 0) break;
            
            for (const msg of batch) {
                const msgTime = adjustToLocalTime(msg.timestamp);
                if (msgTime.isSameOrAfter(startDate) && msgTime.isSameOrBefore(endDate)) {
                    messages.push(msg);
                } else if (msgTime.isBefore(startDate)) {
                    return messages;
                }
            }
            
            before = batch[batch.length - 1].id;
            await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (error) {
            console.error(`Error in channel ${channelId}:`, error);
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
        const messages = await getChannelMessages(channel.id, startDate, endDate);
        messages.forEach(msg => {
            if (msg.reactions) {
                totalReactions += msg.reactions.reduce((sum, reaction) => sum + reaction.count, 0);
            }
        });
    }
    
    return totalReactions;
}

async function getProjectLinks(startDate, endDate) {
    const messages = await getChannelMessages(CHANNEL_ID, startDate, endDate);
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
        const messages = await getChannelMessages(channel.id, startDate, endDate);
        messagesPosted += messages.length;
    }
    
    return messagesPosted;
}

async function getActiveUsers(startDate, endDate) {
    const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
    const textChannels = channels.filter(channel => channel.type === 0);
    const activeUsers = new Set();
    
    for (const channel of textChannels) {
        const messages = await getChannelMessages(channel.id, startDate, endDate);
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
    
    // Get current values
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:H',
    });
    
    const rows = response.data.values || [];
    
    // Create new row
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

    // Insert at top (after header)
    await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A2:H2',  // Always insert at row 2 (after header)
        valueInputOption: 'RAW',
        resource: {
            values: [newRow]
        }
    });

    // If there was existing data, move it down
    if (rows.length > 1) {  // If there's more than just the header
        const existingRows = rows.slice(1);  // Skip header
        await sheets.spreadsheets.values.update({
            spreadsheetId: SHEET_ID,
            range: `Sheet1!A3:H${rows.length + 1}`,
            valueInputOption: 'RAW',
            resource: {
                values: existingRows
            }
        });
    }
}

// Express Routes
app.get('/health', (req, res) => {
    res.json({ status: 'ok' });
});

app.post('/collect-analytics', async (req, res) => {
    // Send immediate response
    res.json({ message: 'Analytics collection started' });

    try {
        const { weekRange } = req.body;
        console.log('Starting analytics collection for:', weekRange);
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

// Start server
const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
