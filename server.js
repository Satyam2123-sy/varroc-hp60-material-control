const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { Server } = require('socket.io');

const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const APP = 'varroc-hp20-material-control';
const VERSION = 'fresh-rebuild';
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'rooms.json');

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

fs.mkdirSync(DATA_DIR, { recursive: true });
let rooms = new Map();
try {
  const raw = fs.existsSync(DATA_FILE) ? fs.readFileSync(DATA_FILE, 'utf8') : '{}';
  const parsed = JSON.parse(raw || '{}');
  for (const [code, room] of Object.entries(parsed)) rooms.set(code, room);
} catch (err) {
  console.error('Could not load persisted rooms:', err.message);
}

let saveTimer = null;
function serializeRooms() {
  const out = {};
  for (const [code, room] of rooms) out[code] = room;
  return out;
}
function persistSoon() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const tmp = `${DATA_FILE}.tmp`;
    try {
      fs.writeFileSync(tmp, JSON.stringify(serializeRooms(), null, 2));
      fs.renameSync(tmp, DATA_FILE);
    } catch (err) { console.error('Persistence error:', err.message); }
  }, 150);
}
function token() { return crypto.randomBytes(32).toString('hex'); }
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
  // Migrate state from the previous A/B/C build if it exists.
  if (room.attendance.A || room.attendance.B || room.attendance.C) {
    room.attendance.DAY ||= []; room.attendance.NIGHT ||= [];
    room.attendance.DAY.push(...(room.attendance.A || []));
    room.attendance.NIGHT.push(...(room.attendance.B || []), ...(room.attendance.C || []));
    delete room.attendance.A; delete room.attendance.B; delete room.attendance.C;
  }
  if (['A','B','C'].includes(room.settings.selectedShift)) room.settings.selectedShift = room.settings.selectedShift === 'A' ? 'DAY' : 'NIGHT';
  for (const entry of room.materialEntries) { if (entry.material === '506') entry.material = 'K2 REAR'; else if (entry.material === '507') entry.material = 'K2 FRONT'; if (entry.shift === 'A') entry.shift='DAY'; else if (entry.shift === 'B' || entry.shift === 'C') entry.shift='NIGHT'; }
  for (const entry of room.breakdowns) { if (entry.shift === 'A') entry.shift='DAY'; else if (entry.shift === 'B' || entry.shift === 'C') entry.shift='NIGHT'; }
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
    const room = ensureRoomShape({
      roomCode: code, ownerToken, ownerName: name, createdAt: new Date().toISOString(),
      members: {}, materialEntries: [], breakdowns: [], attendance: { DAY: [], NIGHT: [] }, settings: { selectedShift: 'AUTO' }
    });
    room.ownerSocketId = socket.id;
    rooms.set(code, room);
    socket.join(code);
    persistSoon();
    ack(cb, true, { roomCode: code, role: 'OWNER', authenticationToken: ownerToken, room: publicRoom(room) });
    io.to(code).emit('memberJoined', { name, role: 'OWNER' });
  });

  socket.on('joinRoom', (payload, cb) => {
    const name = cleanName(payload?.userName);
    const code = String(payload?.roomCode || '').trim().toUpperCase();
    const room = rooms.get(code);
    if (!room) return fail(cb, 'Room not found', 'ROOM_NOT_FOUND');
    if (!name) return fail(cb, 'Enter your full name first', 'NAME_REQUIRED');
    const memberToken = token();
    room.members[memberToken] = { token: memberToken, name, role: 'MEMBER', connected: true, socketId: socket.id, lastSeen: Date.now() };
    socket.join(code);
    persistSoon();
    ack(cb, true, { roomCode: code, role: 'MEMBER', authenticationToken: memberToken, room: publicRoom(room) });
    broadcastRoom(room);
    io.to(code).emit('memberJoined', { name, role: 'MEMBER' });
  });

  socket.on('rejoinRoom', (payload, cb) => {
    const result = requireSession(socket, payload);
    if (result.error) return fail(cb, result.error, result.code);
    const { room, session } = result;
    socket.join(room.roomCode);
    if (session.role === 'OWNER') room.ownerSocketId = socket.id;
    else { room.members[session.token].connected = true; room.members[session.token].lastSeen = Date.now(); room.members[session.token].socketId = socket.id; }
    persistSoon();
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
    persistSoon();
    broadcastRoom(result.room);
    ack(cb, true, { room: publicRoom(result.room), userName: name });
  });

  socket.on('setShift', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    if (!mutateShift(result.room, payload.shift)) return fail(cb, 'Invalid shift');
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
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
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { message: 'Entry added successfully', room: publicRoom(result.room) });
  });

  socket.on('deleteMaterial', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const before = result.room.materialEntries.length;
    const ids = Array.isArray(payload.entryIds) ? payload.entryIds.map(String) : [String(payload.entryId || '')];
    result.room.materialEntries = result.room.materialEntries.filter(e => !ids.includes(String(e.id)));
    if (before === result.room.materialEntries.length) return fail(cb, 'Material entry not found', 'NOT_FOUND');
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('addBreakdown', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const text = String(payload.text || '').trim().replace(/\s+/g, ' ').slice(0, 160);
    if (!text) return fail(cb, 'Enter a breakdown', 'VALIDATION');
    const shift = effectiveShift(result.room, result.room.settings.selectedShift);
    result.room.breakdowns.push({ id: id(), text, shift, createdAt: new Date().toISOString(), userName: result.session.name });
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('deleteBreakdown', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const before = result.room.breakdowns.length;
    result.room.breakdowns = result.room.breakdowns.filter(e => e.id !== payload.breakdownId);
    if (before === result.room.breakdowns.length) return fail(cb, 'Breakdown not found', 'NOT_FOUND');
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('addAttendance', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const name = cleanName(payload.name);
    const shift = effectiveShift(result.room, payload.shift || result.room.settings.selectedShift);
    if (!['DAY','NIGHT'].includes(shift)) return fail(cb, 'Attendance requires a valid shift', 'VALIDATION');
    if (!name) return fail(cb, 'Enter a person name', 'VALIDATION');
    result.room.attendance[shift].push({ id: id(), name, createdAt: new Date().toISOString(), userName: result.session.name });
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('deleteAttendance', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const shift = String(payload.shift || '');
    if (!['DAY','NIGHT'].includes(shift)) return fail(cb, 'Invalid shift');
    const before = result.room.attendance[shift].length;
    result.room.attendance[shift] = result.room.attendance[shift].filter(e => e.id !== payload.attendanceId);
    if (before === result.room.attendance[shift].length) return fail(cb, 'Person not found', 'NOT_FOUND');
    persistSoon(); broadcastRoom(result.room); ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('requestRoomState', (payload, cb) => {
    const result = requireSession(socket, payload);
    if (result.error) return fail(cb, result.error, result.code);
    ack(cb, true, { room: publicRoom(result.room) });
  });

  socket.on('closeRoom', (payload, cb) => {
    const result = requireOwner(socket, payload, cb); if (!result) return;
    const code = result.room.roomCode;
    io.to(code).emit('roomClosed', { message: 'The owner closed this room.' });
    rooms.delete(code);
    persistSoon();
    ack(cb, true, { message: 'Room closed' });
  });

  socket.on('disconnect', () => {
    for (const room of rooms.values()) {
      if (room.ownerSocketId === socket.id) { room.ownerSocketId = null; persistSoon(); }
      for (const member of Object.values(room.members)) {
        if (member.socketId === socket.id) { member.connected = false; member.socketId = null; member.lastSeen = Date.now(); io.to(room.roomCode).emit('memberLeft', { name: member.name, role: 'MEMBER' }); broadcastRoom(room); persistSoon(); }
      }
    }
  });
});

server.listen(PORT, HOST, () => console.log(`${APP} ${VERSION} listening on ${HOST}:${PORT}`));
