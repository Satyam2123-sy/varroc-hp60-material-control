const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const APP = 'varroc-hp60-material-control';
const VERSION = 'fresh-rebuild';
const DATABASE_URL = process.env.DATABASE_URL || process.env.POSTGRES_URL || '';
const pool = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL, ssl: /render\.com|supabase\.co|neon\.tech/i.test(DATABASE_URL) ? { rejectUnauthorized: false } : undefined }) : null;

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  transports: ['websocket', 'polling'],
  reconnection: true,
  reconnectionAttempts: Infinity,
  reconnectionDelay: 500,
  reconnectionDelayMax: 10000,
  randomizationFactor: 0.25,
  maxHttpBufferSize: 1e6
});

app.use(express.json({ limit: '100kb' }));
app.use(express.static(path.join(__dirname, 'public'), { etag: true, maxAge: 0 }));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/build', (_req, res) => res.json({ app: APP, version: VERSION }));

let rooms = new Map();
let dbReady = false;

async function initDatabase() {
  if (!pool) {
    console.warn('DATABASE_URL is not configured. Persistent PostgreSQL storage is unavailable.');
    return;
  }
  await pool.query(`CREATE TABLE IF NOT EXISTS rooms (
    room_code TEXT PRIMARY KEY,
    room_data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
  const { rows } = await pool.query('SELECT room_code, room_data FROM rooms');
  for (const row of rows) rooms.set(row.room_code, row.room_data);
  dbReady = true;
  console.log(`Loaded ${rows.length} persistent room(s) from PostgreSQL.`);
}

async function saveRoom(room) {
  if (!pool) return;
  try {
    const copy = JSON.parse(JSON.stringify(room));
    delete copy.ownerSocketId;
    for (const m of Object.values(copy.members || {})) delete m.socketId;
    await pool.query(
      `INSERT INTO rooms (room_code, room_data, updated_at) VALUES ($1,$2,NOW())
       ON CONFLICT (room_code) DO UPDATE SET room_data=EXCLUDED.room_data, updated_at=NOW()`,
      [room.roomCode, copy]
    );
  } catch (err) { console.error('PostgreSQL persistence error:', err.message); }
}

async function deleteRoom(roomCode) {
  if (!pool) return;
  try { await pool.query('DELETE FROM rooms WHERE room_code=$1', [roomCode]); }
  catch (err) { console.error('PostgreSQL delete error:', err.message); }
}

function token() { return crypto.randomBytes(32).toString('hex'); }
function recoveryCode() { return `HP60-${crypto.randomBytes(7).toString('hex').toUpperCase().match(/.{1,4}/g).join('-')}`; }
function hashRecovery(value) { return crypto.createHash('sha256').update(String(value || '').trim().toUpperCase()).digest('hex'); }
function cleanCode(value) { return String(value || '').trim().toUpperCase(); }
function id() { return crypto.randomUUID(); }
function cleanName(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').slice(0, 80);
}
function validShift(shift) { return ['DAY','NIGHT','AUTO'].includes(shift); }
function detectShift(date = new Date()) {
  const h = date.getHours(), m = date.getMinutes(), mins = h * 60 + m;
  if (mins >= 420 && mins < 1140) return 'DAY';
  return 'NIGHT';
}
function effectiveShift(room, requested) {
  const s = validShift(requested) ? requested : room.settings.selectedShift;
  return s === 'AUTO' ? detectShift() : s;
}
function roomCode() {
  let code;
  do code = `VP3-${crypto.randomBytes(3).toString('hex').toUpperCase()}`; while (rooms.has(code));
  return code;
}
function ensureRoomShape(room) {
  room.members ||= {};
  room.materialEntries ||= [];
  room.breakdowns ||= [];
  room.attendance ||= { DAY: [], NIGHT: [] };
  room.settings ||= { selectedShift: 'AUTO' };
  for (const s of ['DAY','NIGHT']) room.attendance[s] ||= [];
  return room;
}
function publicRoom(room) {
  ensureRoomShape(room);
  return {
    roomCode: room.roomCode,
    ownerName: room.ownerName,
    createdAt: room.createdAt,
    settings: room.settings,
    materialEntries: room.materialEntries,
    breakdowns: room.breakdowns,
    attendance: room.attendance,
    ownerConnected: !!room.ownerSocketId,
    members: Object.values(room.members).map(m => ({ token: m.token, name: m.name, role: m.role, connected: !!m.connected, lastSeen: m.lastSeen }))
  };
}
function ack(cb, ok, data = {}) { if (typeof cb === 'function') cb({ ok, ...data }); }
function fail(cb, message, code = 'ERROR') { ack(cb, false, { message, code }); }
function requireSession(socket, payload) {
  const roomCode = String(payload?.roomCode || '').trim().toUpperCase();
  const room = rooms.get(roomCode);
  if (!room) return { error: 'Room not found', code: 'ROOM_NOT_FOUND' };
  ensureRoomShape(room);
  const auth = String(payload?.authenticationToken || payload?.token || '');
  if (!auth) return { error: 'You are not authorized', code: 'UNAUTHORIZED' };
  let session = null;
  if (auth === room.ownerToken) session = { role: 'OWNER', token: auth, name: room.ownerName };
  else if (room.members[auth]) session = { role: 'MEMBER', token: auth, name: room.members[auth].name };
  if (!session) return { error: 'You are not authorized', code: 'UNAUTHORIZED' };
  if (session.role === 'OWNER') room.ownerSocketId = socket.id;
  else room.members[auth].socketId = socket.id;
  return { room, session };
}
function broadcastRoom(room) {
  io.to(room.roomCode).emit('roomUpdate', publicRoom(room));
}
function requireOwner(socket, payload, cb) {
  const result = requireSession(socket, payload);
  if (result.error) { fail(cb, result.error, result.code); return null; }
  if (result.session.role !== 'OWNER') { fail(cb, 'Only the owner can perform this action', 'FORBIDDEN'); return null; }
  return result;
}
function mutateShift(room, shift) {
  if (!validShift(shift)) return false;
  room.settings.selectedShift = shift;
  return true;
}

io.on('connection', socket => {
  socket.on('createRoom', (payload, cb) => {
    const name = cleanName(payload?.userName);
    if (!name) return fail(cb, 'Enter your full name first', 'NAME_REQUIRED');
    const code = roomCode();
    const ownerToken = token();
    const newRecoveryCode = recoveryCode();
    const room = ensureRoomShape({
      roomCode: code, ownerToken, ownerName: name, recoveryHash: hashRecovery(newRecoveryCode), createdAt: new Date().toISOString(),
      members: {}, materialEntries: [], breakdowns: [], attendance: { DAY: [], NIGHT: [] }, settings: { selectedShift: 'AUTO' }
    });
    room.ownerSocketId = socket.id;
    rooms.set(code, room);
    socket.join(code);
    saveRoom(room);
    ack(cb, true, { roomCode: code, role: 'OWNER', authenticationToken: ownerToken, ownerRecoveryCode: newRecoveryCode, room: publicRoom(room) });
    io.to(code).emit('memberJoined', { name, role: 'OWNER' });
  });

  socket.on('joinRoom', (payload, cb) => {
    const name = cleanName(payload?.userName);
    const code = String(payload?.roomCode || '').trim().toUpperCase();
    const room = rooms.get(code);
    if (!room) return fail(cb, 'Room not found', 'ROOM_NOT_FOUND');
    if (room.closed) return fail(cb, 'Room is closed. Use OWNER RECOVERY to reopen it.', 'ROOM_CLOSED');
    if (!name) return fail(cb, 'Enter your full name first', 'NAME_REQUIRED');
    const memberToken = token();
    room.members[memberToken] = { token: memberToken, name, role: 'MEMBER', connected: true, socketId: socket.id, lastSeen: Date.now() };
    socket.join(code);
    saveRoom(room);
    ack(cb, true, { roomCode: code, role: 'MEMBER', authenticationToken: memberToken, room: publicRoom(room) });
    broadcastRoom(room);
    io.to(code).emit('memberJoined', { name, role: 'MEMBER' });
  });

  socket.on('recoverOwnerRoom', (payload, cb) => {
    const code = cleanCode(payload?.roomCode);
    const name = cleanName(payload?.userName);
    const secret = cleanCode(payload?.ownerRecoveryCode);
    const room = rooms.get(code);
    if (!room) return fail(cb, 'Room not found', 'ROOM_NOT_FOUND');
    if (!secret || !room.recoveryHash || hashRecovery(secret) !== room.recoveryHash) return fail(cb, 'Invalid owner recovery code', 'UNAUTHORIZED');
    if (room.ownerSocketId && room.ownerSocketId !== socket.id && io.sockets.sockets.has(room.ownerSocketId)) return fail(cb, 'Owner is currently connected. Reconnect normally instead.', 'OWNER_CONNECTED');
    if (name && cleanName(room.ownerName).toLowerCase() !== name.toLowerCase()) return fail(cb, 'Owner verification failed', 'UNAUTHORIZED');
    room.closed = false;
    room.ownerSocketId = socket.id;
    socket.join(code);
    saveRoom(room);
    ack(cb, true, { roomCode: code, role: 'OWNER', authenticationToken: room.ownerToken, ownerRecoveryCode: secret, room: publicRoom(room), recovered: true });
    broadcastRoom(room);
  });

  socket.on('rejoinRoom', (payload, cb) => {
    const result = requireSession(socket, payload);
    if (result.error) return fail(cb, result.error, result.code);
    const { room, session } = result;
    socket.join(room.roomCode);
    if (session.role === 'OWNER') room.ownerSocketId = socket.id;
    else { room.members[session.token].connected = true; room.members[session.token].lastSeen = Date.now(); room.members[session.token].socketId = socket.id; }
    saveRoom(room);
    ack(cb, true, { roomCode: room.roomCode, role: session.role, authenticationToken: session.token, room: publicRoom(room) });
    broadcastRoom(room);
  });

  socket.on('updateName', (payload, cb) => {
    const result = requireSession(socket, payload);
    if (result.error) return fail(cb, result.error, result.code);
    const name = cleanName(payload?.userName);
    if (name.length < 2) return fail(cb, 'Enter your full name', 'VALIDATION');
    if (result.session.role === 'OWNER') result.room.ownerName = name;
    else result.room.members[result.session.token].name = name;
    saveRoom(room);
    broadcastRoom(result.room);
    ack(cb, true, { room: publicRoom(result.room), userName: name });
  });

  socket.on('setShift', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    if (!mutateShift(result.room, payload.shift)) return fail(cb, 'Invalid shift');
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('addMaterial', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const material = String(payload.material || '').trim();
    const quantity = Number(payload.quantity);
    if (!['K2 REAR','K2 FRONT'].includes(material)) return fail(cb, 'Select K2 REAR or K2 FRONT', 'VALIDATION');
    if (!Number.isFinite(quantity) || quantity <= 0 || quantity > 100000000) return fail(cb, 'Enter a valid positive quantity', 'VALIDATION');
    const now = new Date();
    const shift = effectiveShift(result.room, result.room.settings.selectedShift);
    result.room.materialEntries.push({ id: id(), material, quantity, date: now.toLocaleDateString('en-IN'), time: now.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' }), timestamp: now.toISOString(), shift, userName: result.session.name });
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { message: 'Entry added successfully', room: publicRoom(result.room) });
  });

  socket.on('deleteMaterial', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const before = result.room.materialEntries.length;
    const ids = Array.isArray(payload.entryIds) ? payload.entryIds.map(String) : [String(payload.entryId || '')];
    result.room.materialEntries = result.room.materialEntries.filter(e => !ids.includes(String(e.id)));
    if (before === result.room.materialEntries.length) return fail(cb, 'Material entry not found', 'NOT_FOUND');
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('addBreakdown', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const text = String(payload.text || '').trim().replace(/\s+/g, ' ').slice(0, 160);
    if (!text) return fail(cb, 'Enter a breakdown', 'VALIDATION');
    const shift = effectiveShift(result.room, result.room.settings.selectedShift);
    result.room.breakdowns.push({ id: id(), text, shift, createdAt: new Date().toISOString(), userName: result.session.name });
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('deleteBreakdown', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const before = result.room.breakdowns.length;
    result.room.breakdowns = result.room.breakdowns.filter(e => e.id !== payload.breakdownId);
    if (before === result.room.breakdowns.length) return fail(cb, 'Breakdown not found', 'NOT_FOUND');
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('addAttendance', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const name = cleanName(payload.name);
    const shift = effectiveShift(result.room, payload.shift || result.room.settings.selectedShift);
    if (!['DAY','NIGHT'].includes(shift)) return fail(cb, 'Attendance requires DAY or NIGHT shift', 'VALIDATION');
    if (!name) return fail(cb, 'Enter a person name', 'VALIDATION');
    result.room.attendance[shift].push({ id: id(), name, createdAt: new Date().toISOString(), userName: result.session.name });
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('deleteAttendance', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const shift = String(payload.shift || '');
    if (!['DAY','NIGHT'].includes(shift)) return fail(cb, 'Invalid shift');
    const before = result.room.attendance[shift].length;
    result.room.attendance[shift] = result.room.attendance[shift].filter(e => e.id !== payload.attendanceId);
    if (before === result.room.attendance[shift].length) return fail(cb, 'Person not found', 'NOT_FOUND');
    saveRoom(result.room); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('requestRoomState', (payload, cb) => {
    const result = requireSession(socket, payload);
    if (result.error) return fail(cb, result.error, result.code);
    ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('closeRoom', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const code = result.room.roomCode;
    result.room.closed = true;
    result.room.ownerSocketId = null;
    io.to(code).emit('roomClosed', { message: 'Room closed by owner. Use OWNER RECOVERY to reopen it.' });
    saveRoom(result.room);
    ack(cb, true, { message: 'Room closed. Data retained for owner recovery.' });
  });

  socket.on('disconnect', () => {
    for (const room of rooms.values()) {
      if (room.ownerSocketId === socket.id) { room.ownerSocketId = null; saveRoom(room); }
      for (const member of Object.values(room.members)) {
        if (member.socketId === socket.id) { member.connected = false; member.socketId = null; member.lastSeen = Date.now(); io.to(room.roomCode).emit('memberLeft', { name: member.name, role: 'MEMBER' }); broadcastRoom(room); saveRoom(room); }
      }
    }
  });
});

initDatabase().then(() => server.listen(PORT, HOST, () => console.log(`${APP} ${VERSION} listening on ${HOST}:${PORT}`))).catch(err => { console.error('Database initialization failed:', err); process.exit(1); });
