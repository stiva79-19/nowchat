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
const ACCESS_CODE = '123456';
const MAX_MESSAGES = 500;

// === STORAGE ===
const users = new Map();           // socketId -> { name, socketId, online }
const chats = new Map();           // chatId -> { id, users: [name1, name2], messages[] }
const groups = new Map();          // groupId -> { id, name, creator, users: [name, ...], messages[] }
const profilePictures = new Map(); // username -> base64

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
        type: 'direct',
        id: chat.id,
        otherUser: chat.users.find(u => u !== username),
        lastMessage: chat.messages.length > 0 ? chat.messages[chat.messages.length - 1] : null
      });
    }
  }
  return result;
}

function getUserGroups(username) {
  const result = [];
  for (const group of groups.values()) {
    if (group.users.includes(username)) {
      result.push({
        type: 'group',
        id: group.id,
        name: group.name,
        users: group.users,
        lastMessage: group.messages.length > 0 ? group.messages[group.messages.length - 1] : null
      });
    }
  }
  return result;
}

// Middleware
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '10mb' }));

app.post('/api/verify', (req, res) => {
  res.json({ ok: req.body.code === ACCESS_CODE });
});

app.get('/api/users', (req, res) => {
  const onlineUsers = [];
  const allUsernames = new Set();
  for (const user of users.values()) {
    allUsernames.add(user.name);
    if (user.online) onlineUsers.push(user.name);
  }
  for (const chat of chats.values()) chat.users.forEach(u => allUsernames.add(u));
  for (const group of groups.values()) group.users.forEach(u => allUsernames.add(u));
  res.json({ online: onlineUsers, all: [...allUsernames] });
});

function broadcastAllLists() {
  const onlineNow = [];
  for (const user of users.values()) {
    if (user.online) onlineNow.push(user.name);
  }
  const allUsernames = new Set();
  for (const chat of chats.values()) chat.users.forEach(u => allUsernames.add(u));
  for (const group of groups.values()) group.users.forEach(u => allUsernames.add(u));
  for (const user of users.values()) allUsernames.add(user.name);

  const pics = Object.fromEntries(profilePictures);

  for (const [sid, user] of users) {
    if (user.online) {
      io.to(sid).emit('chat-list', {
        chats: getUserChats(user.name),
        groups: getUserGroups(user.name),
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

    const allUsernames = new Set();
    for (const chat of chats.values()) chat.users.forEach(u => allUsernames.add(u));
    for (const group of groups.values()) group.users.forEach(u => allUsernames.add(u));
    for (const user of users.values()) allUsernames.add(user.name);
    allUsernames.add(username);

    const onlineNow = [];
    for (const user of users.values()) {
      if (user.online) onlineNow.push(user.name);
    }

    socket.emit('chat-list', {
      chats: getUserChats(username),
      groups: getUserGroups(username),
      allUsers: [...allUsernames],
      onlineUsers: onlineNow,
      profilePictures: Object.fromEntries(profilePictures)
    });

    socket.broadcast.emit('user-online', username);
    broadcastAllLists();
  });

  // Join direct or group chat
  socket.on('join-chat', ({ chatId }) => {
    if (!username) return;
    socket.rooms.forEach(room => { if (room !== socket.id) socket.leave(room); });
    socket.join(chatId);

    const chat = chats.get(chatId);
    const group = groups.get(chatId);
    if (chat) {
      socket.emit('chat-history', {
        chatId,
        type: 'direct',
        messages: chat.messages.slice(-MAX_MESSAGES),
        otherUser: chat.users.find(u => u !== username),
        name: chat.users.find(u => u !== username)
      });
    } else if (group) {
      socket.emit('chat-history', {
        chatId,
        type: 'group',
        messages: group.messages.slice(-MAX_MESSAGES),
        name: group.name,
        users: group.users
      });
    }
  });

  // Message (handles both direct and group)
  socket.on('message', ({ chatId, text }) => {
    if (!username || !chatId || !text || text.length > 5000) return;

    const chat = chats.get(chatId);
    const group = groups.get(chatId);
    const target = chat || group;
    if (!target) return;

    const msg = {
      id: crypto.randomUUID(),
      type: 'text',
      content: text,
      sender: username,
      time: Date.now()
    };
    target.messages.push(msg);
    if (target.messages.length > MAX_MESSAGES) target.messages.shift();
    io.to(chatId).emit('message', { chatId, msg });
    broadcastAllLists();
  });

  // Image (handles both direct and group)
  socket.on('image', ({ chatId, data }) => {
    if (!username || !chatId || !data) return;

    const chat = chats.get(chatId);
    const group = groups.get(chatId);
    const target = chat || group;
    if (!target) return;

    const msg = {
      id: crypto.randomUUID(),
      type: 'image',
      content: data,
      sender: username,
      time: Date.now()
    };
    target.messages.push(msg);
    if (target.messages.length > MAX_MESSAGES) target.messages.shift();
    io.to(chatId).emit('message', { chatId, msg });
    broadcastAllLists();
  });

  // Typing (handles both direct and group)
  socket.on('typing', ({ chatId }) => {
    if (!username || !chatId) return;
    socket.to(chatId).emit('typing', { chatId, user: username });
  });

  // Start direct chat
  socket.on('start-chat', ({ withUser }) => {
    if (!username || !withUser || username === withUser) return;
    const chat = getOrCreateChat(username, withUser);
    broadcastAllLists();
    socket.emit('chat-created', { type: 'direct', id: chat.id, otherUser: withUser });
  });

  // Create group
  socket.on('create-group', ({ groupName, members }) => {
    if (!username || !groupName || !members || !Array.isArray(members) || members.length === 0) return;
    // Ensure creator is in the group
    const allMembers = [username];
    members.forEach(m => { if (m !== username && !allMembers.includes(m)) allMembers.push(m); });

    const groupId = 'group:' + crypto.randomUUID().substring(0, 8);
    const group = {
      id: groupId,
      name: groupName,
      creator: username,
      users: allMembers,
      messages: []
    };
    groups.set(groupId, group);
    broadcastAllLists();
    socket.emit('chat-created', {
      type: 'group',
      id: groupId,
      name: groupName,
      users: allMembers
    });
  });

  // Profile picture
  socket.on('set-profile-picture', ({ data }) => {
    if (!username || !data || data.length > 700000) return;
    profilePictures.set(username, data);
    broadcastAllLists();
  });

  // === WEBRTC SIGNALING ===
  // 1-on-1 call
  socket.on('call-user', ({ to, chatId }) => {
    if (!username || !to) return;
    // Find target user's socket
    for (const [sid, user] of users) {
      if (user.name === to && user.online) {
        io.to(sid).emit('incoming-call', { from: username, chatId });
        return;
      }
    }
    socket.emit('call-failed', { reason: 'Kullanıcı çevrimdışı' });
  });

  socket.on('call-accept', ({ to, chatId }) => {
    for (const [sid, user] of users) {
      if (user.name === to && user.online) {
        io.to(sid).emit('call-accepted', { from: username, chatId });
        return;
      }
    }
  });

  socket.on('call-reject', ({ to }) => {
    for (const [sid, user] of users) {
      if (user.name === to && user.online) {
        io.to(sid).emit('call-rejected', { from: username });
        return;
      }
    }
  });

  socket.on('call-end', ({ to }) => {
    if (to) {
      for (const [sid, user] of users) {
        if (user.name === to && user.online) {
          io.to(sid).emit('call-ended', { from: username });
        }
      }
    }
  });

  // WebRTC SDP/ICE forwarding
  socket.on('webrtc-offer', ({ to, offer, callId }) => {
    for (const [sid, user] of users) {
      if (user.name === to && user.online) {
        io.to(sid).emit('webrtc-offer', { from: username, offer, callId });
        return;
      }
    }
  });

  socket.on('webrtc-answer', ({ to, answer, callId }) => {
    for (const [sid, user] of users) {
      if (user.name === to && user.online) {
        io.to(sid).emit('webrtc-answer', { from: username, answer, callId });
        return;
      }
    }
  });

  socket.on('ice-candidate', ({ to, candidate, callId }) => {
    for (const [sid, user] of users) {
      if (user.name === to && user.online) {
        io.to(sid).emit('ice-candidate', { from: username, candidate, callId });
        return;
      }
    }
  });

  // Group call: join call room
  socket.on('group-call-start', ({ groupId }) => {
    if (!username || !groupId) return;
    const group = groups.get(groupId);
    if (!group || !group.users.includes(username)) return;
    
    // Notify all group members
    group.users.forEach(u => {
      if (u === username) return;
      for (const [sid, user] of users) {
        if (user.name === u && user.online) {
          io.to(sid).emit('group-call-started', { groupId, from: username });
        }
      }
    });
    
    // Send caller the list of online members to connect to
    socket.emit('group-call-members', {
      groupId,
      members: group.users.filter(u => u !== username && [...users.values()].some(v => v.name === u && v.online))
    });
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
