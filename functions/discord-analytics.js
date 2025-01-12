const fetch = require('node-fetch');
const { google } = require('googleapis');
const moment = require('moment-timezone');

// Configuration from Netlify environment variables
const DISCORD_API_BASE = "https://discord.com/api/v10";
const TOKEN = process.env.DISCORD_TOKEN;
const GUILD_ID = process.env.DISCORD_GUILD_ID;
const CHANNEL_ID = process.env.DISCORD_CHANNEL_ID;
const SHEET_ID = process.env.GOOGLE_SHEET_ID;
const GOOGLE_CREDENTIALS = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const LOCAL_TIMEZONE = "America/Los_Angeles";

const HEADERS = {
    'Authorization': `Bot ${TOKEN}`,
    'User-Agent': 'DiscordBot (discord-analytics-bot, 1.0.0)',
    'Content-Type': 'application/json'
};

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

async function makeDiscordRequest(endpoint) {
    const url = `${DISCORD_API_BASE}${endpoint}`;
    console.log('Making request to:', url);
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: HEADERS,
            timeout: 8000 // 8 second timeout
        });

        if (!response.ok) {
            console.error('Discord API error:', {
                status: response.status,
                statusText: response.statusText
            });
            throw new Error(`Discord API returned ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error('Request failed:', error);
        throw error;
    }
}

// Data Collection Functions
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

async function getTotalMembers(endDate) {
    const weekInProgress = isWeekInProgress(endDate);
    
    if (weekInProgress) {
        const guildData = await makeDiscordRequest(`/guilds/${GUILD_ID}?with_counts=true`);
        return guildData.approximate_member_count;
    }
    
    const members = await getAllMembers();
    return members.filter(member => 
        adjustToLocalTime(member.joined_at).isSameOrBefore(endDate)
    ).length;
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
            
            if (!batch) return []; // Channel inaccessible
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
            console.error(`Error fetching messages for channel ${channelId}:`, error);
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
        for (const msg of messages) {
            if (msg.reactions) {
                totalReactions += msg.reactions.reduce((sum, reaction) => sum + reaction.count, 0);
            }
        }
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

// Google Sheets Integration
async function updateGoogleSheet(auth, weekRange, metrics) {
    const sheets = google.sheets({ version: 'v4', auth });
    
    // Get existing data
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Sheet1!A:H',
    });
    
    const rows = response.data.values || [];
    let rowIndex = rows.findIndex(row => row[0] === weekRange);
    if (rowIndex === -1) {
        rowIndex = rows.length;
    }
    
    // Update data
    await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `Sheet1!A${rowIndex + 1}:H${rowIndex + 1}`,
        valueInputOption: 'RAW',
        resource: {
            values: [[
                weekRange,
                metrics.totalMembers,
                metrics.newMembers,
                metrics.activeUsers,
                metrics.messagesPosted,
                metrics.reactions,
                metrics.projectsShowcased,
                metrics.projectLinks.join('\n')
            ]]
        }
    });
}

// functions/discord-analytics.js

// ... (keep all the existing imports and config)

exports.handler = async function(event, context) {
    if (event.httpMethod === 'POST') {
        try {
            // Test Discord connection first
            const botUser = await makeDiscordRequest('/users/@me');
            console.log('Connected as:', botUser.username);

            return {
                statusCode: 200,
                body: JSON.stringify({
                    message: 'Connected successfully',
                    botName: botUser.username
                })
            };

        } catch (error) {
            console.error('Error:', error);
            return {
                statusCode: 500,
                body: JSON.stringify({ 
                    error: error.message,
                    details: 'Connection test failed'
                })
            };
        }
    }

    return {
        statusCode: 405,
        body: JSON.stringify({ error: 'Method not allowed' })
    };
};
