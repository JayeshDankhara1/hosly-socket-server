require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require('mysql2/promise');

/**
 * 🚀 Hosly Socket Server - Unified Production Version
 */

// 0. Global Error Handling
process.on('uncaughtException', (err) => {
    console.error('🔥 CRITICAL: Uncaught Exception:', err.message);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥 CRITICAL: Unhandled Rejection at:', promise, 'reason:', reason);
});

// 1. Environment & Port
const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.SOCKET_ALLOWED_ORIGINS || '').split(',').filter(Boolean);

// 2. Database Connection (Safe Mode)
let pool = null;
if (process.env.DB_DATABASE && process.env.DB_DATABASE !== '***********') {
    pool = mysql.createPool({
        host: process.env.DB_HOST || 'localhost',
        user: process.env.DB_USERNAME,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_DATABASE,
        port: process.env.DB_PORT || 3306,
        waitForConnections: true,
        connectionLimit: 10,
        timezone: 'Z'
    });
    console.log("⏳ Connecting to Database...");
    
    pool.getConnection()
        .then(conn => {
            console.log("✅ Database Connected successfully!");
            conn.release();
        })
        .catch(err => {
            console.warn("⚠️ Running in REAL-TIME ONLY mode:", err.message);
            pool = null;
        });
}

// 3. Server Setup
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active\n");
});

const io = new Server(httpServer, {
    cors: { origin: true, methods: ["GET", "POST"], credentials: true },
    pingTimeout: 60000,
    pingInterval: 25000
});

// 4. State Management
const onlineUsers = new Map(); // identifier -> Set of socketIds

io.on("connection", (socket) => {
    console.log(`🔌 Connection: ${socket.id}`);

    /** Handshake */
    socket.on("user_connected", async (data) => {
        const identifier = data.userId || data.guestId;
        if (!identifier) return;

        socket.trackingId = identifier;
        socket.userId = data.userId;
        socket.guestId = data.guestId;

        if (!onlineUsers.has(identifier)) onlineUsers.set(identifier, new Set());
        onlineUsers.get(identifier).add(socket.id);
        
        io.emit("online_users", Array.from(onlineUsers.keys()));
        io.emit("user_status_changed", { userId: identifier, status: 'online' });
        console.log(`✅ ${socket.guestId ? 'Guest' : 'User'} ${identifier} Online`);

        // Auto-join rooms
        if (pool) {
            try {
                const sql = socket.guestId ? 'SELECT id FROM chats WHERE guest_id = ?' : 'SELECT chat_id as id FROM chat_users WHERE user_id = ?';
                const [chats] = await pool.execute(sql, [identifier]);
                chats.forEach(c => socket.join(`chat_${c.id}`));
            } catch (e) { console.error('Join Error:', e.message); }
        }
    });

    /** Messaging */
    socket.on("send_message", async (data) => {
        const chatId = data.chatId || data.chat_id;
        if (!chatId) return;

        const fullMessage = {
            ...data,
            id: Date.now(),
            chat_id: chatId,
            created_at: new Date()
        };

        // Broadcast immediately
        io.to(`chat_${chatId}`).emit("receive_message", fullMessage);

        if (pool) {
            try {
                const [res] = await pool.execute(
                    'INSERT INTO messages (chat_id, user_id, guest_id, message, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW())',
                    [chatId, data.userId || null, data.guestId || null, data.message]
                );
                await pool.execute('UPDATE chats SET last_message_at = NOW() WHERE id = ?', [chatId]);
                fullMessage.id = res.insertId;
            } catch (e) { console.error('DB Save Error:', e.message); }
        }
    });

    /** Receipts */
    socket.on("mark_read", async (data) => {
        const chatId = data.chatId || data.chat_id;
        if (pool && chatId) {
            try {
                const id = data.userId || data.guestId;
                await pool.execute('UPDATE messages SET read_at = NOW() WHERE chat_id = ? AND (user_id != ? OR guest_id != ?) AND read_at IS NULL', 
                    [chatId, data.userId || 0, data.guestId || '']);
                io.to(`chat_${chatId}`).emit("messages_read", { chatId, read_at: new Date() });
            } catch (e) { console.error('Read Error:', e.message); }
        }
    });

    socket.on("typing", (data) => data.chatId && socket.to(`chat_${data.chatId}`).emit("user_typing", data));
    socket.on("stop_typing", (data) => data.chatId && socket.to(`chat_${data.chatId}`).emit("user_stop_typing", data));
    socket.on("join_chat", (data) => (data.chatId || data.chat_id) && socket.join(`chat_${data.chatId || data.chat_id}`));

    socket.on("disconnect", () => {
        const id = socket.trackingId;
        if (id && onlineUsers.has(id)) {
            onlineUsers.get(id).delete(socket.id);
            if (onlineUsers.get(id).size === 0) {
                onlineUsers.delete(id);
                io.emit("online_users", Array.from(onlineUsers.keys()));
                io.emit("user_status_changed", { userId: id, status: 'offline' });
            }
        }
    });
});

httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Unified Socket Server running on port ${PORT}`);
});
