const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 10 * 1024 * 1024,
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ['polling', 'websocket'],
  pingTimeout: 60000,
  pingInterval: 25000
});

// === CONFIG ===
const ACCESS_CODE = '123456'; // Herkes bu kodla girer
const MAX_MESSAGES = 500;
const MAX_IMAGE_SIZE = 5 * 1024 * 1024;

// === STORAGE ===
const users = new Map();       // socketId -> { name, socketId, online }
const chats = new Map();       // chatId -> { id, users: [name1, name2], messages[] }
const profilePictures = new Map(); // username -> base64 data URL

function getChatId(user1, user2) {
  return [user1, user2].sort().join('::');
}

function getOrCreateChat(user1, user2) {
  const id = getChatId(user1, user2);
  if (!chats.has(id)) {
    chats.set(id, { id, users: [user1, user2].sort(), messages: [] });
  }
  return chats.get(id);
}

function getUserChats(username) {
  const result = [];
  for (const chat of chats.values()) {
    if (chat.users.includes(username)) {
      result.push({
        id: chat.id,
        otherUser: chat.users.find(u => u !== username),
        lastMessage: chat.messages.length > 0 ? chat.messages[chat.messages.length - 1] : null
      });
    }
  }
  return result;
}

// Middleware
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '10mb' }));

// Verify access code
app.post('/api/verify', (req, res) => {
  if (req.body.code === ACCESS_CODE) {
    res.json({ ok: true });
  } else {
    res.json({ ok: false });
  }
});

// Get list of users who have been active
app.get('/api/users', (req, res) => {
  const onlineUsers = [];
  const allUsernames = new Set();
  
  for (const user of users.values()) {
    allUsernames.add(user.name);
    if (user.online) onlineUsers.push(user.name);
  }
  
  // Also get users from chats
  for (const chat of chats.values()) {
    chat.users.forEach(u => allUsernames.add(u));
  }
  
  res.json({ 
    online: onlineUsers,
    all: [...allUsernames]
  });
});

// Helper: broadcast updated lists to all online users
function broadcastAllLists() {
  const onlineNow = [];
  for (const user of users.values()) {
    if (user.online) onlineNow.push(user.name);
  }
  const allUsernames = new Set();
  for (const chat of chats.values()) chat.users.forEach(u => allUsernames.add(u));
  for (const user of users.values()) allUsernames.add(user.name);
  
  // Collect profile pictures
  const pics = {};
  for (const [name, pic] of profilePictures) {
    pics[name] = pic;
  }
  
  for (const [sid, user] of users) {
    if (user.online) {
      io.to(sid).emit('chat-list', {
        chats: getUserChats(user.name),
        allUsers: [...allUsernames],
        onlineUsers: onlineNow,
        profilePictures: pics
      });
    }
  }
}

// Socket.io
io.on('connection', (socket) => {
  let username = '';

  socket.on('join', ({ name }) => {
    username = name || 'Misafir';
    
    users.set(socket.id, { name: username, socketId: socket.id, online: true });
    
    // Send their existing chats + all known users
    const myChats = getUserChats(username);
    const allUsernames = new Set();
    for (const chat of chats.values()) chat.users.forEach(u => allUsernames.add(u));
    for (const user of users.values()) allUsernames.add(user.name);
    allUsernames.add(username);
    
    const onlineNow = [];
    for (const user of users.values()) {
      if (user.online) onlineNow.push(user.name);
    }
    
    socket.emit('chat-list', {
      chats: myChats,
      allUsers: [...allUsernames],
      onlineUsers: onlineNow,
      profilePictures: Object.fromEntries(profilePictures)
    });
    
    // Notify others this user is online + refresh their lists
    socket.broadcast.emit('user-online', username);
    broadcastAllLists();
  });

  socket.on('join-chat', ({ chatId }) => {
    if (!username) return;
    
    // Leave previous chat room
    socket.rooms.forEach(room => {
      if (room !== socket.id) socket.leave(room);
    });
    
    socket.join(chatId);
    
    const chat = chats.get(chatId);
    if (chat) {
      socket.emit('chat-history', {
        chatId,
        messages: chat.messages.slice(-MAX_MESSAGES),
        otherUser: chat.users.find(u => u !== username)
      });
    }
  });

  socket.on('message', ({ chatId, text }) => {
    if (!username || !chatId || !text || text.length > 5000) return;
    
    const chat = chats.get(chatId);
    if (!chat) return;
    
    const msg = {
      id: crypto.randomUUID(),
      type: 'text',
      content: text,
      sender: username,
      time: Date.now()
    };
    
    chat.messages.push(msg);
    if (chat.messages.length > MAX_MESSAGES) chat.messages.shift();
    
    io.to(chatId).emit('message', { chatId, msg });
    
    // Update chat list for both participants
    broadcastAllLists();
  });

  socket.on('image', ({ chatId, data }) => {
    if (!username || !chatId || !data) return;
    
    const chat = chats.get(chatId);
    if (!chat) return;
    
    const msg = {
      id: crypto.randomUUID(),
      type: 'image',
      content: data,
      sender: username,
      time: Date.now()
    };
    
    chat.messages.push(msg);
    if (chat.messages.length > MAX_MESSAGES) chat.messages.shift();
    
    io.to(chatId).emit('message', { chatId, msg });
    
    chat.users.forEach(u => {
      for (const [sid, user] of users) {
        if (user.name === u && user.online) {
          io.to(sid).emit('chat-list-update', getUserChats(u));
        }
      }
    });
  });

  socket.on('typing', ({ chatId }) => {
    if (!username || !chatId) return;
    socket.to(chatId).emit('typing', { chatId, user: username });
  });

  // Start a chat with another user
  socket.on('start-chat', ({ withUser }) => {
    if (!username || !withUser || username === withUser) return;
    
    const chat = getOrCreateChat(username, withUser);
    broadcastAllLists();
    
    // Send the new chat to the requester
    socket.emit('chat-created', {
      id: chat.id,
      otherUser: withUser,
      lastMessage: null
    });
  });
  
  // Profile picture
  socket.on('set-profile-picture', ({ data }) => {
    if (!username || !data) return;
    // Limit size to ~500KB base64
    if (data.length > 700000) return;
    profilePictures.set(username, data);
    broadcastAllLists();
  });

  socket.on('disconnect', () => {
    if (username) {
      const user = users.get(socket.id);
      if (user) user.online = false;
      socket.broadcast.emit('user-offline', username);
      broadcastAllLists();
    }
    users.delete(socket.id);
  });
});

const PORT = process.env.PORT || 3456;
server.listen(PORT, () => {
  console.log(`NowChat running on http://localhost:${PORT}`);
});
