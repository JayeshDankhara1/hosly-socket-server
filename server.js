require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require('mysql2/promise');

/**
 * 🚀 Hosly Socket Server - Full Feature v2 (Safe Mode)
 * Includes: History, Receipts, Online Status, and Guest Support.
 */

const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.SOCKET_ALLOWED_ORIGINS || '').split(',').filter(Boolean);

// 1. Database Connection (Safe Mode)
let pool = null;
const isDbConfigured = process.env.DB_DATABASE && process.env.DB_DATABASE !== '***********';

if (isDbConfigured) {
    pool = mysql.createPool({
        host: process.env.DB_HOST || 'localhost',
        user: process.env.DB_USERNAME,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_DATABASE,
        port: process.env.DB_PORT || 3306,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0
    });

    pool.getConnection()
        .then(conn => {
            console.log("✅ Database Connected successfully!");
            conn.release();
        })
        .catch(err => {
            console.warn("⚠️ Database Connection Failed, running in REAL-TIME ONLY mode:", err.message);
            pool = null; // Disable DB features but keep server alive
        });
}

// 2. Health Check Server
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active (Full Mode)\n");
});

// 3. Socket.io Setup
const io = new Server(httpServer, {
    cors: {
        origin: ALLOWED_ORIGINS.length > 0 ? ALLOWED_ORIGINS : "*",
        methods: ["GET", "POST"]
    }
});

// 4. State Management
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

        if (!onlineUsers.has(identifier)) {
            onlineUsers.set(identifier, new Set());
        }
        onlineUsers.get(identifier).add(socket.id);
        
        io.emit("online_users", Array.from(onlineUsers.keys()));
        io.emit("user_status_changed", { userId: identifier, status: 'online' });

        console.log(`✅ ${socket.isGuest ? 'Guest' : 'User'} ${identifier} Online`);

        // Auto-join existing rooms (DB required)
        if (pool) {
            try {
                const sql = socket.isGuest 
                    ? 'SELECT id FROM chats WHERE guest_id = ?' 
                    : 'SELECT chat_id as id FROM chat_users WHERE user_id = ?';
                const [chats] = await pool.execute(sql, [identifier]);
                chats.forEach(c => socket.join(`chat_${c.id}`));
            } catch (e) {
                console.error('Auto-join failed:', e.message);
            }
        }
    });

    /**
     * Messaging Logic
     */
    socket.on("send_message", async (data) => {
        if (!data.chatId) return;

        const msgToBroadcast = {
            ...data,
            id: Date.now(),
            created_at: new Date()
        };

        // Broadcast real-time
        io.to(`chat_${data.chatId}`).emit("receive_message", msgToBroadcast);

        // Save to DB
        if (pool) {
            try {
                await pool.execute(
                    'INSERT INTO messages (chat_id, user_id, guest_id, message, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW())',
                    [data.chatId, data.userId || null, data.guestId || null, data.message]
                );
                await pool.execute('UPDATE chats SET last_message_at = NOW() WHERE id = ?', [data.chatId]);
            } catch (e) {
                console.error('Failed to save message:', e.message);
            }
        }
    });

    /**
     * History Fetching
     */
    socket.on("fetch_messages", async (data) => {
        if (!pool || !data.chatId) return;
        try {
            const [messages] = await pool.execute(`
                SELECT m.*, u.first_name, u.last_name, u.profile_picture 
                FROM messages m 
                LEFT JOIN users u ON m.user_id = u.id 
                WHERE m.chat_id = ? 
                ORDER BY m.created_at ASC`, [data.chatId]);
            socket.emit("messages_loaded", { chatId: data.chatId, messages });
        } catch (e) {
            console.error('Fetch messages failed:', e.message);
        }
    });

    /**
     * Read Receipts
     */
    socket.on("mark_read", async (data) => {
        if (!pool || !data.chatId) return;
        try {
            const identifier = data.userId || data.guestId;
            await pool.execute(
                'UPDATE messages SET read_at = NOW() WHERE chat_id = ? AND (user_id != ? OR guest_id != ?) AND read_at IS NULL',
                [data.chatId, data.userId || 0, data.guestId || '']
            );
            io.to(`chat_${data.chatId}`).emit("messages_read", { 
                chatId: data.chatId, 
                read_at: new Date() 
            });
        } catch (e) {
            console.error('Read receipt failed:', e.message);
        }
    });

    /**
     * Indicators
     */
    socket.on("typing", (data) => {
        if (data.chatId) socket.to(`chat_${data.chatId}`).emit("user_typing", data);
    });

    socket.on("stop_typing", (data) => {
        if (data.chatId) socket.to(`chat_${data.chatId}`).emit("user_stop_typing", data);
    });

    socket.on("join_chat", (data) => {
        if (data.chatId) socket.join(`chat_${data.chatId}`);
    });

    /**
     * Disconnection
     */
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
});

// Start Server
httpServer.listen(PORT, () => {
    console.log(`🚀 Full Socket Server running on port ${PORT}`);
});
