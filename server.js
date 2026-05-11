const https = require('https');
const WebSocket = require('ws');
const fs = require('fs');

const DB_FILE = '/opt/mesh-proxy/nodedb.json';
const MSG_FILE = '/opt/mesh-proxy/messages.json';
const MAX_MSG = 20000;
const MSG_MAX_AGE = 24 * 3600 * 1000;
const NODE_MAX_AGE = 30 * 24 * 3600 * 1000;

let nodeDb = {};
let messages = [];
let syncInProgress = false;
let lastSyncTime = 0;
let fetchQueue = new Set();
let fetchingNames = false;

function log(msg) { console.log(`[${new Date().toLocaleTimeString()}] ${msg}`); }

function load() {
    try { if (fs.existsSync(DB_FILE)) { const d = fs.readFileSync(DB_FILE, 'utf8'); if (d && d.trim()) { nodeDb = JSON.parse(d); log(`Загружено ${Object.keys(nodeDb).length} нод`); } } } catch(e) { log('Ошибка nodedb: ' + e.message); }
    try { if (fs.existsSync(MSG_FILE)) { const d = fs.readFileSync(MSG_FILE, 'utf8'); if (d && d.trim()) { messages = JSON.parse(d); messages.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()); const seen = new Set(); messages = messages.filter(m => { if (!m || !m.id || seen.has(m.id)) return false; seen.add(m.id); return true; }); log(`Загружено ${messages.length} сообщений`); } } } catch(e) { log('Ошибка сообщений: ' + e.message); }
}

function save() {
    try { fs.writeFileSync(DB_FILE + '.tmp', JSON.stringify(nodeDb, null, 2)); fs.renameSync(DB_FILE + '.tmp', DB_FILE); } catch(e) {}
    try { fs.writeFileSync(MSG_FILE + '.tmp', JSON.stringify(messages.slice(0, MAX_MSG), null, 2)); fs.renameSync(MSG_FILE + '.tmp', MSG_FILE); } catch(e) {}
}

load();

const REGIONS = ['RU/MSK', 'RU/SPB', 'RU/KRD', 'RU/NSK', 'RU/EKB', 'RU/VLD', 'RU/KLD', 'RU/RND', 'RU/KZN', 'RU/NNV', 'RU/VRN', 'RU/VLG', 'RU/PRM', 'RU/UFA', 'RU/OMS', 'RU/CHL', 'RU/SAR', 'RU/TYM', 'RU/KRY', 'RU/IRK', 'RU/KHB', 'RU/YAR', 'RU/TOM', 'RU/ORB', 'RU/KEM', 'RU/RZN', 'RU/PNZ', 'RU/LIP', 'RU/TUL', 'RU/KUR', 'RU/STV', 'RU/SCH', 'RU/TVE', 'RU/IZH', 'RU/BRN', 'RU/ULY', 'RU/VLA', 'RU/MUR', 'RU/AST', 'RU/KIR', 'RU/BRY', 'RU/BEL', 'RU/SMO', 'RU/KLG', 'RU/PSK', 'RU/SAM', 'RU/SVS', 'RU/SMF', 'RU/MKH', 'RU/GRZ', 'RU/NAL', 'RU/VKZ', 'RU/CHE', 'RU/YSH', 'RU/SRN', 'RU/CHB', 'RU/TMB', 'RU/KST', 'RU/VGD', 'RU/ARH', 'RU/STK', 'RU/YKT', 'RU/MGD', 'RU/USS', 'RU/CHI', 'RU/UUD'];

function fetchJSON(url) {
    return new Promise((resolve, reject) => {
        https.get(url, { timeout: 10000 }, (res) => {
            let data = ''; res.on('data', c => data += c); res.on('end', () => { if (res.statusCode === 200) try { resolve(JSON.parse(data)) } catch(e) { reject(e) } else reject(new Error(`HTTP ${res.statusCode}`)) });
        }).on('error', reject).setTimeout(10000, function() { this.destroy(); reject(new Error('timeout')) });
    });
}

function fetchNodeName(nodeId) {
    return new Promise((resolve) => {
        const decId = parseInt(nodeId, 16);
        https.get(`https://map.onemesh.ru/api/v1/nodes/${decId}`, { timeout: 3000 }, res => {
            let data = ''; res.on('data', c => data += c); res.on('end', () => {
                try { const j = JSON.parse(data); if (j && j.node && j.node.long_name) resolve({ sn: (j.node.short_name || j.node.long_name).slice(0, 4), ln: j.node.long_name }); else resolve(null); } catch(e) { resolve(null) }
            });
        }).on('error', () => resolve(null));
    });
}

async function processFetchQueue() {
    if (fetchingNames || fetchQueue.size === 0) return;
    fetchingNames = true;
    const ids = [...fetchQueue].slice(0, 20);
    fetchQueue = new Set([...fetchQueue].slice(20));
    for (const id of ids) {
        const hexId = id.replace('!', '');
        const name = await fetchNodeName(hexId);
        if (name && name.ln && nodeDb[id]) { nodeDb[id].sn = name.sn; nodeDb[id].ln = name.ln; nodeDb[id].ts = Date.now(); broadcastSingleNode(id); }
        await new Promise(r => setTimeout(r, 100));
    }
    fetchingNames = false;
    if (fetchQueue.size > 0) setTimeout(processFetchQueue, 2000);
}

function broadcastSingleNode(id) {
    if (!wss || !wss.clients) return;
    const json = JSON.stringify({ type: 'node_update', data: { [id]: nodeDb[id] } });
    wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) try { c.send(json) } catch(e) {} });
}

let knownIds = new Set();
if (messages.length > 0) messages.forEach(m => { if (m && m.id) knownIds.add(m.id) });

async function syncAll() {
    if (syncInProgress) return; syncInProgress = true; const now = Date.now(); let newMessages = []; let newNodesCount = 0;
    for (const region of REGIONS) {
        try {
            const data = await fetchJSON(`https://map.onemesh.ru/api/v1/text-messages?root_topic=${region}&count=30&order=desc`);
            const msgs = data.text_messages || [];
            for (const msg of msgs) {
                if (!msg || !msg.id || knownIds.has(msg.id)) continue;
                if (now - new Date(msg.created_at).getTime() > MSG_MAX_AGE) continue;
                knownIds.add(msg.id); newMessages.push(msg);
                if (msg.from) { const hexId = parseInt(msg.from).toString(16).padStart(8, '0'); const id = '!' + hexId; if (!nodeDb[id]) { nodeDb[id] = { sn: hexId.slice(0, 4), ln: '', ts: now }; fetchQueue.add(id); newNodesCount++; } else nodeDb[id].ts = now; }
                if (msg.gateway_id) { const hexGid = parseInt(msg.gateway_id).toString(16).padStart(8, '0'); const gid = '!' + hexGid; if (!nodeDb[gid]) { nodeDb[gid] = { sn: hexGid.slice(0, 4), ln: '', ts: now }; fetchQueue.add(gid); newNodesCount++; } else nodeDb[gid].ts = now; }
            }
        } catch(e) {}
    }
    if (fetchQueue.size > 0) processFetchQueue();
    if (newMessages.length > 0) { messages.unshift(...newMessages); messages.sort((a,b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()); }
    const msgCutoff = now - MSG_MAX_AGE; let oldMsgs = 0; messages = messages.filter(msg => { const k = new Date(msg.created_at).getTime() > msgCutoff; if (!k) { knownIds.delete(msg.id); oldMsgs++; } return k });
    if (messages.length > MAX_MSG) { const removed = messages.splice(MAX_MSG); removed.forEach(m => knownIds.delete(m.id)); }
    const nodeCutoff = now - NODE_MAX_AGE; let cleaned = 0; for (const id in nodeDb) { if (nodeDb[id].ts < nodeCutoff) { delete nodeDb[id]; cleaned++; } }
    lastSyncTime = now;
    if (newMessages.length > 0 || oldMsgs > 0 || newNodesCount > 0 || cleaned > 0) { log(`${new Date().toLocaleTimeString()} | +${newMessages.length} нов, -${oldMsgs} стар, всего: ${messages.length} | нод: ${Object.keys(nodeDb).length} (+${newNodesCount}, -${cleaned})`); save(); if (newMessages.length > 0) { const sorted = [...newMessages].sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime()); broadcastNewMessages(sorted); } }
    syncInProgress = false;
}

function broadcastNewMessages(newMessages) { if (!wss || !wss.clients || newMessages.length === 0) return; const json = JSON.stringify({ type: 'messages', data: newMessages }); wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) try { c.send(json) } catch(e) {} }); }

const ssl = { cert: fs.readFileSync('/etc/letsencrypt/live/ospeo.duckdns.org/fullchain.pem'), key: fs.readFileSync('/etc/letsencrypt/live/ospeo.duckdns.org/privkey.pem') };
const server = https.createServer(ssl, (req, res) => { res.writeHead(200, {'Content-Type': 'text/plain; charset=utf-8'}); const fresh = messages.filter(m => Date.now() - new Date(m.created_at).getTime() < 3600000).length; const clients = wss ? wss.clients.size : 0; const named = Object.values(nodeDb).filter(n => n.ln && n.ln.length > 0).length; res.end(`Mesh Proxy OK\nКлиентов: ${clients}\nНод: ${Object.keys(nodeDb).length} (с именами: ${named})\nСообщений: ${messages.length}\nЗа час: ${fresh}\nОчередь имён: ${fetchQueue.size}\nСинк: ${new Date(lastSyncTime).toLocaleTimeString()}`); });

const wss = new WebSocket.Server({ server, maxPayload: 1048576 });
wss.on('connection', (ws, req) => { log(`WSS +${req.socket.remoteAddress} (${wss.clients.size})`); ws.send(JSON.stringify({ type: 'nodes', data: nodeDb })); ws.send(JSON.stringify({ type: 'messages', data: messages.slice(0, 1000).reverse() })); ws.on('close', () => log(`WSS - (${wss.clients.size})`)); ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true }); });
setInterval(() => { wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping() }) }, 30000);

process.on('SIGTERM', () => { save(); server.close(); process.exit(0) });
process.on('SIGINT', () => { save(); server.close(); process.exit(0) });

server.listen(9443, '0.0.0.0', () => { log('=== Mesh Proxy WSS :9443 ==='); log(`Нод: ${Object.keys(nodeDb).length}, Сообщений: ${messages.length}`); syncAll(); });
setInterval(syncAll, 5000);
