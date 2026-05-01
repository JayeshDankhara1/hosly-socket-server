require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require('mysql2/promise');

/**
 * 🚀 Hosly Socket Server - Production Optimized v2
 * Verified for Hostinger Deployment with Guest Support
 */

// 1. Database Configuration (Safe Mode)
let pool = null;
const isDbConfigured = process.env.DB_DATABASE && process.env.DB_DATABASE !== '***********';

if (isDbConfigured) {
    pool = mysql.createPool({
        host: process.env.DB_HOST || 'localhost',
        port: process.env.DB_PORT || 3306,
        user: process.env.DB_USERNAME,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_DATABASE,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0,
        timezone: 'Z'
    });

    pool.getConnection()
        .then(conn => {
            console.log("✅ Database Connected successfully!");
            conn.release();
        })
        .catch(err => {
            console.warn("⚠️ Database Connection Failed, running in REAL-TIME ONLY mode:", err.message);
            pool = null;
        });
}

// 2. Server setup
const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.SOCKET_ALLOWED_ORIGINS || '').split(',').filter(Boolean);

const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active (Production Mode)\n");
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
const onlineUsers = new Map(); // identifier -> Set of socketIds

// 5. Helper: Database Interaction
const query = async (sql, params) => {
    if (!pool) return null;
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
        socket.trackingId = trackingId;

        if (!onlineUsers.has(trackingId)) {
            onlineUsers.set(trackingId, new Set());
        }
        onlineUsers.get(trackingId).add(socket.id);
        
        // Notify everyone about online status
        io.emit("online_users", Array.from(onlineUsers.keys()));
        io.emit("user_status_changed", { userId: trackingId, status: 'online' });
        
        console.log(`✅ ${userId ? 'User ' + userId : 'Guest ' + guestId} Online`);

        // Auto-join existing chats for real-time notifications
        if (pool) {
            try {
                let chats = [];
                if (userId) {
                    chats = await query('SELECT chat_id FROM chat_users WHERE user_id = ?', [userId]);
                } else if (guestId) {
                    chats = await query('SELECT id as chat_id FROM chats WHERE guest_id = ?', [guestId]);
                }
                
                chats.forEach(c => {
                    socket.join(`chat_${c.chat_id}`);
                    console.log(`🏠 Joined: chat_${c.chat_id}`);
                });
            } catch (e) {
                console.error('Auto-join failed:', e.message);
            }
        }
    });

    /**
     * Messaging Logic
     */
    socket.on("send_message", async (data) => {
        const chatId = data.chatId || data.chat_id;
        const userId = data.userId || data.user_id;
        const guestId = data.guestId || data.guest_id;
        const message = data.message;
        const type = data.type || 'text';

        if (!chatId || (!userId && !guestId) || !message) {
            console.warn('⚠️ Invalid message data:', data);
            return;
        }

        console.log(`📩 Message from ${userId || guestId} for chat ${chatId}`);

        let fullMessage = {
            id: Date.now(),
            temp_id: data.tempId || data.temp_id || null, // Echo back tempId for frontend replacement
            chat_id: chatId,
            user_id: userId,
            guest_id: guestId,
            message: message,
            type: type,
            created_at: new Date(),
            user: data.user || {}
        };

        try {
            if (pool) {
                // Save to DB
                const [result] = await pool.execute(
                    'INSERT INTO messages (chat_id, user_id, guest_id, message, type, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NOW(), NOW())',
                    [chatId, userId || null, guestId || null, message, type]
                );
                
                await pool.execute('UPDATE chats SET last_message_at = NOW() WHERE id = ?', [chatId]);

                // Fetch sender profile for complete broadcast
                if (userId) {
                    const [rows] = await pool.execute('SELECT first_name, last_name, profile_picture FROM users WHERE id = ?', [userId]);
                    if (rows.length > 0) {
                        fullMessage.user = rows[0];
                    }
                }
                fullMessage.id = result.insertId;
            }

            // Broadcast to room
            io.to(`chat_${chatId}`).emit("receive_message", fullMessage);
        } catch (err) {
            console.error('❌ Send Message Error:', err.message);
            // Fallback broadcast if DB fails
            io.to(`chat_${chatId}`).emit("receive_message", fullMessage);
        }
    });

    /**
     * Delivery & Read Receipts
     */
    socket.on("mark_delivered", async (data) => {
        const chatId = data.chatId || data.chat_id;
        const messageIds = data.messageIds || data.message_ids;
        const userId = data.userId || data.user_id;
        const guestId = data.guestId || data.guest_id;

        if (!pool || !chatId || !messageIds?.length) return;

        try {
            // Mark as delivered only if message is NOT from the current user/guest
            const sql = userId 
                ? 'UPDATE messages SET delivered_at = NOW() WHERE id IN (?) AND user_id != ? AND delivered_at IS NULL'
                : 'UPDATE messages SET delivered_at = NOW() WHERE id IN (?) AND guest_id != ? AND delivered_at IS NULL';
            
            await pool.query(sql, [messageIds, userId || guestId]);
            
            io.to(`chat_${chatId}`).emit("message_delivered", { 
                chat_id: chatId, 
                message_ids: messageIds, 
                delivered_at: new Date() 
            });
        } catch (err) {
            console.error('❌ Mark Delivered Error:', err.message);
        }
    });

    socket.on("mark_read", async (data) => {
        const chatId = data.chatId || data.chat_id;
        const userId = data.userId || data.user_id;
        const guestId = data.guestId || data.guest_id;
        const messageIds = data.messageIds || data.message_ids;

        if (!pool || !chatId) return;

        try {
            if (messageIds?.length) {
                // Specific messages
                const sql = userId 
                    ? 'UPDATE messages SET read_at = NOW(), delivered_at = COALESCE(delivered_at, NOW()) WHERE id IN (?) AND user_id != ?'
                    : 'UPDATE messages SET read_at = NOW(), delivered_at = COALESCE(delivered_at, NOW()) WHERE id IN (?) AND guest_id != ?';
                await pool.query(sql, [messageIds, userId || guestId]);
            } else {
                // Bulk mark all as read for this chat
                const sql = userId
                    ? 'UPDATE messages SET read_at = NOW(), delivered_at = COALESCE(delivered_at, NOW()) WHERE chat_id = ? AND user_id != ? AND read_at IS NULL'
                    : 'UPDATE messages SET read_at = NOW(), delivered_at = COALESCE(delivered_at, NOW()) WHERE chat_id = ? AND guest_id != ? AND read_at IS NULL';
                await pool.query(sql, [chatId, userId || guestId]);
            }

            io.to(`chat_${chatId}`).emit("messages_read", { 
                chat_id: chatId, 
                message_ids: messageIds || [], 
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
        const userId = data.userId ? Number(data.userId) : null;
        const guestId = data.guestId || null;
        if (!pool || (!userId && !guestId)) return;

        try {
            const querySql = `
                SELECT 
                    c.*, 
                    JSON_OBJECT(
                        'id', lm.id, 
                        'message', lm.message, 
                        'created_at', lm.created_at, 
                        'delivered_at', lm.delivered_at, 
                        'read_at', lm.read_at
                    ) as last_message,
                    COUNT(DISTINCT CASE WHEN 
                        (${userId ? 'm_unread.user_id != ?' : 'm_unread.user_id IS NOT NULL'}) 
                        AND m_unread.read_at IS NULL THEN m_unread.id END) as unread_count,
                    JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'id', u.id, 
                            'first_name', u.first_name, 
                            'last_name', u.last_name, 
                            'profile_picture', u.profile_picture
                        )
                    ) as users
                FROM chats c
                ${userId 
                    ? 'INNER JOIN chat_users cu ON cu.chat_id = c.id AND cu.user_id = ?' 
                    : 'WHERE c.guest_id = ?'}
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
            `;
            
            const params = userId ? [userId, userId] : [guestId];
            const [chats] = await pool.execute(querySql, params);
            socket.emit("chats_loaded", chats);
        } catch (err) {
            console.error('❌ Fetch Chats Error:', err.message);
        }
    });

    socket.on("fetch_messages", async (data) => {
        const chatId = data.chatId || data.chat_id;
        if (!pool || !chatId) return;
        try {
            const [messages] = await pool.execute(`
                SELECT m.*, u.first_name, u.last_name, u.profile_picture 
                FROM messages m 
                LEFT JOIN users u ON m.user_id = u.id 
                WHERE m.chat_id = ? 
                ORDER BY m.created_at ASC`, [chatId]);
            
            socket.emit("messages_loaded", { chat_id: chatId, messages });
        } catch (err) {
            console.error('❌ Fetch Messages Error:', err.message);
        }
    });

    /**
     * Indicators
     */
    socket.on("typing", (data) => {
        const chatId = data.chatId || data.chat_id;
        if (chatId) socket.to(`chat_${chatId}`).emit("user_typing", data);
    });

    socket.on("stop_typing", (data) => {
        const chatId = data.chatId || data.chat_id;
        if (chatId) socket.to(`chat_${chatId}`).emit("user_stop_typing", data);
    });

    /**
     * Room Management
     */
    socket.on("join_chat", (data) => {
        const chatId = data.chatId || data.chat_id;
        if (chatId) socket.join(`chat_${chatId}`);
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
                console.log(`👋 ${socket.guestId ? 'Guest' : 'User'} ${identifier} Offline`);
            }
        }
    });
});

// Start Server
httpServer.listen(PORT, () => {
    console.log(`🚀 Hosly Socket Server (Full Mode) running on port ${PORT}`);
});
