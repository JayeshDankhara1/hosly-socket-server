require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require('mysql2/promise');

/**
 * 🛠️ Hosly Socket Server - Production Optimized (v2)
 */

// 1. Environment & Port
const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.SOCKET_ALLOWED_ORIGINS || '').split(',').filter(Boolean);

// 2. Database Connection Pool
const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT || 3306,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Helper: Query Database
const query = async (sql, params) => {
    try {
        const [results] = await pool.execute(sql, params);
        return results;
    } catch (err) {
        console.error('❌ Database Error:', err.message);
        throw err;
    }
};

// 3. Health Check Server (Hostinger Friendly)
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active\n");
});

// 4. Socket.io Setup
const io = new Server(httpServer, {
    cors: {
        origin: ALLOWED_ORIGINS.length > 0 ? ALLOWED_ORIGINS : "*",
        methods: ["GET", "POST"]
    }
});

// 5. State Management
// onlineUsers Map: identifier (UserId or GuestId) -> Set of Socket IDs
const onlineUsers = new Map();

io.on("connection", (socket) => {
    console.log(`🔌 New Connection: ${socket.id}`);

    /**
     * User/Guest Connection Handshake
     */
    socket.on("user_connected", async (data) => {
        const identifier = data.userId || data.guestId;
        if (!identifier) return;

        socket.trackingId = identifier;
        socket.isGuest = !!data.guestId;

        // Add to online map
        if (!onlineUsers.has(identifier)) {
            onlineUsers.set(identifier, new Set());
        }
        onlineUsers.get(identifier).add(socket.id);
        
        // Broadcast unified list to everyone
        io.emit("online_users", Array.from(onlineUsers.keys()));
        io.emit("user_status_changed", { userId: identifier, status: 'online' });

        console.log(`✅ ${socket.isGuest ? 'Guest' : 'User'} ${identifier} Online`);

        // Auto-join existing chat rooms
        try {
            let chats = [];
            if (socket.isGuest) {
                chats = await query('SELECT id FROM chats WHERE guest_id = ?', [identifier]);
            } else {
                chats = await query('SELECT chat_id as id FROM chat_users WHERE user_id = ?', [identifier]);
            }
            
            chats.forEach(c => {
                socket.join(`chat_${c.id}`);
            });
        } catch (e) {
            console.error('Failed to auto-join rooms:', e.message);
        }
    });

    socket.on("join_chat", (data) => {
        if (data.chatId) socket.join(`chat_${data.chatId}`);
    });

    socket.on("disconnect", () => {
        const identifier = socket.trackingId;
        if (identifier && onlineUsers.has(identifier)) {
            const sessions = onlineUsers.get(identifier);
            sessions.delete(socket.id);
            if (sessions.size === 0) {
                onlineUsers.delete(identifier);
                io.emit("online_users", Array.from(onlineUsers.keys()));
                io.emit("user_status_changed", { userId: identifier, status: 'offline' });
                console.log(`👋 ${socket.isGuest ? 'Guest' : 'User'} ${identifier} Offline`);
            }
        }
    });

    // Handle generic events (broadcast to rooms)
    socket.on("send_message", (data) => {
        if (data.chatId) io.to(`chat_${data.chatId}`).emit("receive_message", data);
    });

    socket.on("typing", (data) => {
        if (data.chatId) socket.to(`chat_${data.chatId}`).emit("user_typing", data);
    });

    socket.on("stop_typing", (data) => {
        if (data.chatId) socket.to(`chat_${data.chatId}`).emit("user_stop_typing", data);
    });
});

// 6. Start Server
httpServer.listen(PORT, () => {
    console.log(`🚀 Socket Server running on port ${PORT}`);
});
