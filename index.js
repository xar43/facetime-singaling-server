/**
 * ============================================================
 *  VibeCall — WebRTC Signaling Server
 *  Built with Express + Socket.IO
 * ============================================================
 *
 *  Events this server handles (incoming from clients):
 *    register-user   → register a user with their userId + name
 *    join-room       → join a call room by roomId
 *    leave-room      → leave the current room
 *    make-offer      → relay SDP offer to a target peer
 *    send-answer     → relay SDP answer to a target peer
 *    ice-candidate   → relay ICE candidate to a target peer
 *    end-call        → notify all room peers that this user hung up
 *
 *  Events this server emits (outgoing to clients):
 *    registered      → confirms registration, sends back userId
 *    room-joined     → confirms join, sends list of existing peers in room
 *    peer-joined     → notifies existing peers that a new peer joined
 *    peer-left       → notifies room peers that a peer disconnected
 *    incoming-offer  → relays SDP offer to the callee
 *    incoming-answer → relays SDP answer to the caller
 *    ice-candidate   → relays ICE candidates between peers
 *    call-ended      → notifies peers that the call was terminated
 *    user-list       → sends updated list of all online registered users
 *    error           → sends error message to specific client
 * ============================================================
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

// Serve the compiled Flutter Web client static files if they exist
const webBuildPath = path.join(__dirname, '../build/web');
app.use(express.static(webBuildPath));

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  pingTimeout: 60000,
  pingInterval: 25000,
});

const PORT = process.env.PORT || 5001;

// ─── In-Memory State ──────────────────────────────────────────────────────────

/**
 * users: Map<socketId, { userId, name, roomId | null, socketId }>
 * rooms: Map<roomId, Set<socketId>>
 */
const users = new Map();   // socketId -> userInfo
const rooms = new Map();   // roomId   -> Set<socketId>

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(tag, message, data = '') {
  const timestamp = new Date().toISOString().substring(11, 23);
  console.log(`[${timestamp}] [${tag}] ${message}`, data !== '' ? JSON.stringify(data) : '');
}

/** Get info object for a socketId (safe — returns null if not found) */
function getUser(socketId) {
  return users.get(socketId) || null;
}

/** Broadcast the full online user list to ALL connected sockets */
function broadcastUserList() {
  const onlineUsers = [...users.values()].map(u => ({
    userId: u.userId,
    name: u.name,
    roomId: u.roomId,
    socketId: u.socketId,
  }));
  io.emit('user-list', onlineUsers);
}

/** Send updated peer list for a room to everyone in it */
function broadcastRoomPeers(roomId) {
  const socketIds = [...(rooms.get(roomId) || new Set())];
  const peers = socketIds.map(sid => {
    const u = getUser(sid);
    return u ? { socketId: sid, userId: u.userId, name: u.name } : null;
  }).filter(Boolean);
  io.to(roomId).emit('room-peers', peers);
}

/** Remove a socketId from its current room. Cleans up empty rooms. */
function leaveCurrentRoom(socketId) {
  const user = getUser(socketId);
  if (!user || !user.roomId) return;

  const roomId = user.roomId;
  const roomSockets = rooms.get(roomId);
  if (roomSockets) {
    roomSockets.delete(socketId);
    if (roomSockets.size === 0) {
      rooms.delete(roomId);
      log('ROOM', `Room ${roomId} is now empty, deleted`);
    } else {
      // Notify remaining peers
      io.to(roomId).emit('peer-left', {
        socketId,
        userId: user.userId,
        name: user.name,
      });
      broadcastRoomPeers(roomId);
    }
  }

  user.roomId = null;
}

// ─── REST Endpoints ───────────────────────────────────────────────────────────

// Health check
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: '🚀 VibeCall WebRTC Signaling Server is running!',
    connectedClients: users.size,
    activeRooms: rooms.size,
  });
});

// Get all active rooms
app.get('/rooms', (req, res) => {
  const roomList = [...rooms.entries()].map(([roomId, socketSet]) => ({
    roomId,
    peerCount: socketSet.size,
  }));
  res.json(roomList);
});

// Get all online users
app.get('/users', (req, res) => {
  const userList = [...users.values()];
  res.json(userList);
});

// ─── Socket.IO Event Handlers ─────────────────────────────────────────────────

io.on('connection', (socket) => {
  log('CONN', `New socket connected: ${socket.id}`);

  // ── 1. register-user ──────────────────────────────────────────────────────
  /**
   * Client sends: { userId: String, name: String }
   * Server responds: registered { socketId, userId, name }
   */
  socket.on('register-user', ({ userId, name }) => {
    if (!userId || !name) {
      socket.emit('error', { message: 'register-user requires userId and name' });
      return;
    }

    const userInfo = {
      socketId: socket.id,
      userId,
      name,
      roomId: null,
    };
    users.set(socket.id, userInfo);

    log('REGISTER', `User registered: ${name} (${userId}) @ socket ${socket.id}`);

    socket.emit('registered', { socketId: socket.id, userId, name });
    broadcastUserList();
  });

  // ── 2. join-room ──────────────────────────────────────────────────────────
  /**
   * Client sends: { roomId: String }
   * Server responds to caller: room-joined { roomId, peers: [...] }
   * Server notifies existing peers: peer-joined { socketId, userId, name }
   */
  socket.on('join-room', ({ roomId }) => {
    const user = getUser(socket.id);
    if (!user) {
      socket.emit('error', { message: 'Please register before joining a room.' });
      return;
    }

    // Leave previous room if any
    leaveCurrentRoom(socket.id);

    socket.join(roomId);
    user.roomId = roomId;

    if (!rooms.has(roomId)) {
      rooms.set(roomId, new Set());
    }

    const roomSockets = rooms.get(roomId);

    // Build peer list (excluding the joining socket)
    const existingPeers = [...roomSockets].map(sid => {
      const u = getUser(sid);
      return u ? { socketId: sid, userId: u.userId, name: u.name } : null;
    }).filter(Boolean);

    // Now add the new socket to the room
    roomSockets.add(socket.id);

    log('JOIN', `${user.name} joined room ${roomId}. Room now has ${roomSockets.size} peers`);

    // Tell the joining user about existing peers
    socket.emit('room-joined', {
      roomId,
      socketId: socket.id,
      peers: existingPeers,
    });

    // Tell existing peers about the new joiner
    socket.to(roomId).emit('peer-joined', {
      socketId: socket.id,
      userId: user.userId,
      name: user.name,
    });

    broadcastUserList();
    broadcastRoomPeers(roomId);
  });

  // ── 3. leave-room ─────────────────────────────────────────────────────────
  socket.on('leave-room', () => {
    const user = getUser(socket.id);
    if (user) {
      log('LEAVE', `${user.name} manually left room ${user.roomId}`);
    }
    leaveCurrentRoom(socket.id);
    broadcastUserList();
  });

  // ── 4. make-offer (SDP Offer relay) ───────────────────────────────────────
  /**
   * Client sends: { targetId: String (socketId of callee), sdp: { type, sdp } }
   * Server forwards to target: incoming-offer { fromId, fromName, sdp }
   */
  socket.on('make-offer', ({ targetId, sdp }) => {
    const user = getUser(socket.id);
    if (!user) return;

    log('OFFER', `${user.name} → ${targetId}`);
    io.to(targetId).emit('incoming-offer', {
      fromId: socket.id,
      fromName: user.name,
      fromUserId: user.userId,
      sdp,
    });
  });

  // ── 5. send-answer (SDP Answer relay) ─────────────────────────────────────
  /**
   * Client sends: { targetId: String, sdp: { type, sdp } }
   * Server forwards to target: incoming-answer { fromId, sdp }
   */
  socket.on('send-answer', ({ targetId, sdp }) => {
    const user = getUser(socket.id);
    if (!user) return;

    log('ANSWER', `${user.name} → ${targetId}`);
    io.to(targetId).emit('incoming-answer', {
      fromId: socket.id,
      sdp,
    });
  });

  // ── 6. ice-candidate relay ────────────────────────────────────────────────
  /**
   * Client sends: { targetId: String, candidate: RTCIceCandidateInit }
   * Server forwards to target: ice-candidate { fromId, candidate }
   */
  socket.on('ice-candidate', ({ targetId, candidate }) => {
    io.to(targetId).emit('ice-candidate', {
      fromId: socket.id,
      candidate,
    });
  });

  // ── 7. end-call ───────────────────────────────────────────────────────────
  /**
   * Client sends: { targetId?: String }  (optional, if no targetId → broadcast to room)
   */
  socket.on('end-call', ({ targetId } = {}) => {
    const user = getUser(socket.id);
    if (!user) return;

    log('END_CALL', `${user.name} ended call`);

    if (targetId) {
      io.to(targetId).emit('call-ended', { fromId: socket.id });
    } else if (user.roomId) {
      socket.to(user.roomId).emit('call-ended', { fromId: socket.id });
    }
  });

  // ── 8. disconnect ─────────────────────────────────────────────────────────
  socket.on('disconnect', (reason) => {
    const user = getUser(socket.id);
    if (user) {
      log('DISCONNECT', `${user.name} (${user.userId}) disconnected: ${reason}`);
      leaveCurrentRoom(socket.id);
      users.delete(socket.id);
      broadcastUserList();
    } else {
      log('DISCONNECT', `Unknown socket disconnected: ${socket.id}`);
    }
  });
});

// ─── Start Server ─────────────────────────────────────────────────────────────

server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('  ╔══════════════════════════════════════════╗');
  console.log('  ║   🎥 VibeCall Signaling Server Started   ║');
  console.log(`  ║   Port  : ${PORT}                          ║`);
  console.log(`  ║   URL   : http://localhost:${PORT}           ║`);
  console.log('  ║   WS    : ws://localhost:' + PORT + '            ║');
  console.log('  ╚══════════════════════════════════════════╝');
  console.log('');
  console.log('  📡 Waiting for WebRTC clients...');
  console.log('');
});
