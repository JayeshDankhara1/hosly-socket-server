require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require('mysql2/promise');

/**
 * 🛠️ Hosly Socket Server - Production Optimized
 * Verified for Hostinger Deployment
 */

// 1. Database Configuration
const dbConfig = {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    timezone: 'Z'
};

const pool = mysql.createPool(dbConfig);

// 2. Server setup
const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.SOCKET_ALLOWED_ORIGINS || '').split(',').filter(Boolean);

// Health Check Server (Hostinger Friendly)
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active\n");
});

// Test Database Connection on Startup
pool.getConnection()
    .then(conn => {
        console.log("✅ Database Connected successfully!");
        conn.release();
    })
    .catch(err => {
        console.error("❌ Database Connection Failed:", err.message);
    });

// 3. Socket.io Setup
const io = new Server(httpServer, {
    cors: {
        origin: ALLOWED_ORIGINS.length > 0 ? ALLOWED_ORIGINS : "*",
        methods: ["GET", "POST"],
        credentials: true
    },
    pingTimeout: 60000,
    pingInterval: 25000
});

// 4. State Management
const onlineUsers = new Map(); // userId -> Set of socketIds

// 5. Helper: Database Interaction
const query = async (sql, params) => {
    try {
        const [results] = await pool.execute(sql, params);
        return results;
    } catch (err) {
        console.error('❌ Database Error:', err.message);
        throw err;
    }
};

// 6. Socket Core logic
io.on("connection", (socket) => {
    console.log(`🔌 New Connection: ${socket.id}`);

    /**
     * User Connection & Status Tracking
     */
    socket.on("user_connected", async (data) => {
        const userId = data.userId ? Number(data.userId) : null;
        const guestId = data.guestId || null;
        
        if (!userId && !guestId) return;
        
        const trackingId = userId || guestId;
        socket.userId = userId;
        socket.guestId = guestId;

        if (!onlineUsers.has(trackingId)) {
            onlineUsers.set(trackingId, new Set());
        }
        onlineUsers.get(trackingId).add(socket.id);
        
        // Notify everyone about online status
        io.emit("online_users", Array.from(onlineUsers.keys()));
        console.log(`✅ ${userId ? 'User ' + userId : 'Guest ' + guestId} Online [${onlineUsers.get(trackingId).size} sessions]`);

        // Auto-join existing chats for real-time notifications
        try {
            const chats = await query('SELECT chat_id FROM chat_users WHERE user_id = ?', [userId]);
            chats.forEach(c => {
                socket.join(`chat_${c.chat_id}`);
            });
        } catch (e) {
            console.error('Failed to auto-join rooms:', e.message);
        }
    });

    /**
     * Messaging Logic
     */
    socket.on("send_message", async (data) => {
        const { chatId, userId, message, type = 'text' } = data;
        if (!chatId || !userId || !message) {
            console.warn('⚠️ Invalid message data received:', data);
            return;
        }

        console.log(`📩 Processing message from ${userId} for chat ${chatId}`);

        try {
            // Save to DB
            const [result] = await pool.execute(
                'INSERT INTO messages (chat_id, user_id, message, type, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW())',
                [chatId, userId, message, type]
            );
            console.log(`💾 Message saved to DB with ID: ${result.insertId}`);

            // Update chat timestamp
            await pool.execute('UPDATE chats SET last_message_at = NOW() WHERE id = ?', [chatId]);

            // Fetch formatted message with user profile
            const [rows] = await pool.execute(`
                SELECT m.*, u.first_name, u.last_name, u.profile_picture 
                FROM messages m 
                JOIN users u ON m.user_id = u.id 
                WHERE m.id = ?
            `, [result.insertId]);

            const fullMessage = rows[0];
            fullMessage.user = {
                first_name: fullMessage.first_name,
                last_name: fullMessage.last_name,
                profile_picture: fullMessage.profile_picture
            };

            // Broadcast to room
            console.log(`📡 Broadcasting to room: chat_${chatId}`);
            io.to(`chat_${chatId}`).emit("receive_message", fullMessage);
        } catch (err) {
            console.error('❌ Send Message Error Details:', err);
        }
    });

    /**
     * Delivery & Read Receipts
     */
    socket.on("mark_delivered", async (data) => {
        const { chatId, messageIds, userId } = data;
        if (!messageIds?.length) return;

        try {
            await pool.query(
                'UPDATE messages SET delivered_at = NOW() WHERE id IN (?) AND user_id != ? AND delivered_at IS NULL', 
                [messageIds, userId]
            );
            io.to(`chat_${chatId}`).emit("message_delivered", { 
                chatId, 
                messageIds, 
                delivered_at: new Date() 
            });
        } catch (err) {
            console.error('❌ Mark Delivered Error:', err.message);
        }
    });

    socket.on("mark_read", async (data) => {
        const { chatId, userId, messageIds } = data;
        if (!messageIds?.length) return;

        try {
            await pool.query(
                'UPDATE messages SET read_at = NOW(), delivered_at = COALESCE(delivered_at, NOW()) WHERE id IN (?) AND user_id != ?', 
                [messageIds, userId]
            );
            io.to(`chat_${chatId}`).emit("messages_read", { 
                chatId, 
                messageIds, 
                read_at: new Date() 
            });
        } catch (err) {
            console.error('❌ Mark Read Error:', err.message);
        }
    });

    /**
     * Data Fetchers
     */
    socket.on("fetch_chats", async (data) => {
        const userId = Number(data.userId);
        try {
            /**
             * 🚀 N+1 Optimized Query
             * Fetches all chats, last messages, unread counts, and participant lists in one execution.
             */
            const [chats] = await pool.execute(`
                SELECT 
                    c.*, 
                    JSON_OBJECT(
                        'id', lm.id, 
                        'message', lm.message, 
                        'created_at', lm.created_at, 
                        'delivered_at', lm.delivered_at, 
                        'read_at', lm.read_at
                    ) as last_message,
                    COUNT(DISTINCT CASE WHEN m_unread.user_id != ? AND m_unread.read_at IS NULL THEN m_unread.id END) as unread_count,
                    JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'id', u.id, 
                            'first_name', u.first_name, 
                            'last_name', u.last_name, 
                            'profile_picture', u.profile_picture
                        )
                    ) as users
                FROM chats c
                INNER JOIN chat_users cu ON cu.chat_id = c.id AND cu.user_id = ?
                LEFT JOIN messages lm ON lm.id = (
                    SELECT id FROM messages 
                    WHERE chat_id = c.id 
                    ORDER BY created_at DESC LIMIT 1
                )
                LEFT JOIN messages m_unread ON m_unread.chat_id = c.id
                LEFT JOIN chat_users cu_all ON cu_all.chat_id = c.id
                LEFT JOIN users u ON u.id = cu_all.user_id
                GROUP BY c.id, lm.id
                ORDER BY c.last_message_at DESC
            `, [userId, userId]);
            
            socket.emit("chats_loaded", chats);
        } catch (err) {
            console.error('❌ Fetch Chats Error:', err.message);
        }
    });

    socket.on("fetch_messages", async (data) => {
        const { chatId, lastId } = data;
        try {
            let sql = `
                SELECT m.*, u.first_name, u.last_name, u.profile_picture 
                FROM messages m 
                JOIN users u ON m.user_id = u.id 
                WHERE m.chat_id = ?
            `;
            let params = [chatId];
            if (lastId) {
                sql += ' AND m.id > ?';
                params.push(lastId);
            }
            sql += ' ORDER BY m.created_at ASC';
            
            const [messages] = await pool.execute(sql, params);
            const formatted = messages.map(msg => ({
                ...msg,
                user: { first_name: msg.first_name, last_name: msg.last_name, profile_picture: msg.profile_picture }
            }));

            socket.emit("messages_loaded", { chatId, messages: formatted });
        } catch (err) {
            console.error('❌ Fetch Messages Error:', err.message);
        }
    });

    /**
     * Indicators
     */
    socket.on("typing", (data) => {
        socket.to(`chat_${data.chatId}`).emit("user_typing", { chatId: data.chatId, userName: data.userName });
    });

    socket.on("stop_typing", (data) => {
        socket.to(`chat_${data.chatId}`).emit("user_stop_typing", { chatId: data.chatId });
    });

    /**
     * Room Management
     */
    socket.on("join_chat", (data) => {
        if (data.chatId) socket.join(`chat_${data.chatId}`);
    });

    socket.on("clear_chat", async (data) => {
        try {
            await pool.execute('DELETE FROM messages WHERE chat_id = ?', [data.chatId]);
            io.to(`chat_${data.chatId}`).emit("chat_cleared", { chatId: data.chatId });
        } catch (err) {
            console.error('❌ Clear Chat Error:', err.message);
        }
    });

    socket.on("delete_message", async (data) => {
        try {
            if (data.type === 'everyone') {
                await pool.execute('DELETE FROM messages WHERE id = ?', [data.messageId]);
                io.to(`chat_${data.chatId}`).emit("message_deleted", { messageId: data.messageId });
            }
        } catch (e) {
            console.error('❌ Delete Message Error:', e.message);
        }
    });

    /**
     * Disconnection
     */
    socket.on("disconnect", () => {
        const trackingId = socket.userId || socket.guestId;
        
        if (trackingId && onlineUsers.has(trackingId)) {
            const sessions = onlineUsers.get(trackingId);
            sessions.delete(socket.id);
            if (sessions.size === 0) {
                onlineUsers.delete(trackingId);
                io.emit("online_users", Array.from(onlineUsers.keys()));
                console.log(`👤 ${socket.userId ? 'User ' + socket.userId : 'Guest ' + socket.guestId} Offline`);
            }
        }
        console.log(`🔌 Disconnected: ${socket.id}`);
    });
});

// 7. Start Server
httpServer.listen(PORT, () => {
    console.log(`🚀 Socket Server running on port ${PORT}`);
    console.log(`🌍 Health Check: http://localhost:${PORT}`);
});
