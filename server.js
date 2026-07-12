const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 10 * 1024 * 1024,
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ['polling', 'websocket'],
  pingTimeout: 60000,
  pingInterval: 25000,
  connectTimeout: 30000
});

// In-memory storage
const rooms = new Map(); // roomCode -> { messages[], users[], createdAt }
const MAX_MESSAGES = 500;
const MAX_IMAGE_SIZE = 5 * 1024 * 1024; // 5MB

// Generate 6-digit room code
function generateCode() {
  return crypto.randomInt(100000, 999999).toString();
}

// Ensure room exists
function getOrCreateRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, { messages: [], users: [], createdAt: Date.now() });
  }
  return rooms.get(code);
}

// Multer for image uploads
const storage = multer.memoryStorage();
const upload = multer({
  storage,
  limits: { fileSize: MAX_IMAGE_SIZE },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/')) cb(null, true);
    else cb(new Error('Only images allowed'));
  }
});

// Strip /whatsapp prefix for Tailscale proxy support
app.use((req, res, next) => {
  if (req.url === '/whatsapp' || req.url === '/whatsapp/') {
    return res.sendFile(path.join(__dirname, 'public', 'index.html'));
  }
  if (req.url.startsWith('/whatsapp/')) {
    req.url = req.url.replace('/whatsapp', '');
  }
  next();
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '10mb' }));

// Create a new room
app.post('/api/rooms', (req, res) => {
  const code = generateCode();
  getOrCreateRoom(code);
  res.json({ code });
});

// Check if room exists
app.get('/api/rooms/:code', (req, res) => {
  const exists = rooms.has(req.params.code);
  res.json({ exists, userCount: exists ? rooms.get(req.params.code).users.length : 0 });
});

// Upload image
app.post('/api/upload', upload.single('image'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No image' });
  const b64 = req.file.buffer.toString('base64');
  const mime = req.file.mimetype;
  res.json({ data: `data:${mime};base64,${b64}` });
});

// Socket.io
io.on('connection', (socket) => {
  let currentRoom = null;
  let username = '';

  socket.on('join', ({ roomCode, name }) => {
    currentRoom = roomCode;
    username = name || 'Misafir';
    
    const room = getOrCreateRoom(roomCode);
    room.users.push(username);
    
    socket.join(roomCode);
    
    // Send message history
    socket.emit('history', room.messages.slice(-MAX_MESSAGES));
    
    // Notify others
    socket.to(roomCode).emit('user-joined', username);
    io.to(roomCode).emit('users-update', room.users);
    
    console.log(`${username} joined room ${roomCode}`);
  });

  socket.on('message', ({ text }) => {
    if (!currentRoom) return;
    if (!text || text.length > 5000) return;
    
    const room = getOrCreateRoom(currentRoom);
    const msg = {
      id: crypto.randomUUID(),
      type: 'text',
      content: text,
      sender: username,
      time: Date.now()
    };
    
    room.messages.push(msg);
    if (room.messages.length > MAX_MESSAGES) room.messages.shift();
    
    io.to(currentRoom).emit('message', msg);
  });

  socket.on('image', ({ data }) => {
    if (!currentRoom) return;
    if (!data || data.length > MAX_IMAGE_SIZE * 1.4) return;
    
    const room = getOrCreateRoom(currentRoom);
    const msg = {
      id: crypto.randomUUID(),
      type: 'image',
      content: data,
      sender: username,
      time: Date.now()
    };
    
    room.messages.push(msg);
    if (room.messages.length > MAX_MESSAGES) room.messages.shift();
    
    io.to(currentRoom).emit('message', msg);
  });

  socket.on('typing', () => {
    if (currentRoom) {
      socket.to(currentRoom).emit('typing', username);
    }
  });

  socket.on('disconnect', () => {
    if (currentRoom) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.users = room.users.filter(u => u !== username);
        if (room.users.length === 0) {
          // Keep room for 1 hour after last user leaves
          setTimeout(() => {
            if (room.users.length === 0) rooms.delete(currentRoom);
          }, 3600000);
        }
        io.to(currentRoom).emit('users-update', room.users);
        socket.to(currentRoom).emit('user-left', username);
      }
    }
  });
});

const PORT = process.env.PORT || 3456;
server.listen(PORT, () => {
  console.log(`NowChat running on http://localhost:${PORT}`);
});
