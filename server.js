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
           log(`Request successful: ${url}`, { responseStatus: response.status });
           return data;
       } catch (error) {
           log(`Request failed: ${url}`, { error: error.message });
           throw error;
       }
   }
}

async function getGuildInfo() {
   log('Fetching guild info');
   return await makeDiscordRequest(`/guilds/${GUILD_ID}?with_counts=true`);
}

async function getAllGuildMembers() {
   log('Fetching all guild members');
   const allMembers = [];
   let after = '0';
   let batchCount = 0;
   
   while (true) {
       const endpoint = `/guilds/${GUILD_ID}/members?limit=1000${after !== '0' ? `&after=${after}` : ''}`;
       const batch = await makeDiscordRequest(endpoint);
       
       if (!batch || batch.length === 0) break;
       
       batchCount++;
       log(`Fetched member batch ${batchCount}: ${batch.length} members`);
       allMembers.push(...batch);
       
       if (batch.length < 1000) break;
       after = batch[batch.length - 1].user.id;
       await new Promise(resolve => setTimeout(resolve, 100));
   }
   
   log(`Total members fetched: ${allMembers.length}`);
   return allMembers;
}

async function getTotalMembers(endDate) {
   log(`Getting total members before: ${endDate.format()}`);
   const guildInfo = await getGuildInfo();
   const totalCount = guildInfo.approximate_member_count;
   
   if (isWeekInProgress(endDate)) {
       log(`Week in progress, using current count: ${totalCount}`);
       return totalCount;
   }
   
   const members = await getAllGuildMembers();
   const historicalCount = members.filter(member => {
       if (!member.joined_at) return false;
       const joinedBefore = adjustToLocalTime(member.joined_at).isSameOrBefore(endDate);
       log(`Member ${member.user.id} joined at ${member.joined_at} - Included: ${joinedBefore}`);
       return joinedBefore;
   }).length;
   
   log(`Historical member count for ${endDate.format()}: ${historicalCount}`);
   return historicalCount;
}

async function getNewMembers(startDate, endDate) {
   log(`Getting new members between ${startDate.format()} and ${endDate.format()}`);
   const members = await getAllGuildMembers();
   const newMembers = members.filter(member => {
       if (!member.joined_at) return false;
       const joinedAt = adjustToLocalTime(member.joined_at);
       return joinedAt.isSameOrAfter(startDate) && joinedAt.isSameOrBefore(endDate);
   });
   
   log(`New members in period: ${newMembers.length}`);
   return newMembers.length;
}

async function getAllChannelMessages(channelId, startDate, endDate) {
   log(`Fetching messages for channel ${channelId} between ${startDate.format()} and ${endDate.format()}`);
   const messages = [];
   let lastId = null;
   let batchCount = 0;
   
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
                   messages.push(msg);
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
           if (msg.reactions) {
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
       if (!msg.content.toLowerCase().includes('.cerebras.ai')) {
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
   
   for (const channel of textChannels) {
       const messages = await getAllChannelMessages(channel.id, startDate, endDate);
       log(`Messages in channel ${channel.id}: ${messages.length}`);
       totalMessages += messages.length;
   }
   
   log(`Total messages across all channels: ${totalMessages}`);
   return totalMessages;
}

async function getActiveUsers(startDate, endDate) {
   log('Counting active users');
   const channels = await makeDiscordRequest(`/guilds/${GUILD_ID}/channels`);
   const textChannels = channels.filter(channel => channel.type === 0);
   const activeUsers = new Set();
   
   for (const channel of textChannels) {
       const messages = await getAllChannelMessages(channel.id, startDate, endDate);
       messages.forEach(msg => activeUsers.add(msg.author.id));
       log(`Active users in channel ${channel.id}: ${activeUsers.size}`);
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
       
       const metrics = {
           totalMembers: await getTotalMembers(endDate),
           newMembers: await getNewMembers(startDate, endDate),
           activeUsers: await getActiveUsers(startDate, endDate),
           messagesPosted: await getMessagesPosted(startDate, endDate),
           reactions: await getReactions(startDate, endDate),
           projectLinks: await getProjectLinks(startDate, endDate)
       };
       
       metrics.projectsShowcased = metrics.projectLinks.length;
       
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
