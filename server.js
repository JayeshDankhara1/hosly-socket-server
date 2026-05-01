require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require('mysql2/promise');

// Database configuration
const dbConfig = {
    host: process.env.DB_HOST || '127.0.0.1',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USERNAME || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_DATABASE || 'hosly',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// Create a connection pool
const pool = mysql.createPool(dbConfig);

// Test database connection
async function testDbConnection() {
    try {
        const connection = await pool.getConnection();
        console.log('✅ Database connected successfully');
        connection.release();
    } catch (err) {
        console.error('❌ Database connection failed:', err.message);
    }
}

testDbConnection();

const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.SOCKET_ALLOWED_ORIGINS || '').split(',').filter(Boolean);

// Always add app URL and domain patterns if not explicitly in the list
if (process.env.APP_URL && !ALLOWED_ORIGINS.includes(process.env.APP_URL)) {
    ALLOWED_ORIGINS.push(process.env.APP_URL);
}

// Create HTTP server for health checks
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Server is running...\n");
});

// Initialize Socket.io with Security and CORS
const io = new Server(httpServer, {
    cors: {
        origin: (origin, callback) => {
            // Allow connections with no origin (like mobile apps or curl)
            if (!origin) return callback(null, true);
            
            const isAllowed = ALLOWED_ORIGINS.some(allowed => origin.startsWith(allowed)) || 
                             origin.endsWith('.hosly.app');
            
            if (isAllowed) {
                callback(null, true);
            } else {
                console.log(`🚫 Blocked origin: ${origin}`);
                callback(new Error('Not allowed by CORS'));
            }
        },
        methods: ["GET", "POST"]
    }
});

io.on("connection", (socket) => {
    console.log(`🚀 Client connected: ${socket.id}`);

    // Join a specific chat room
    socket.on("join_chat", (data) => {
        const { chatId } = data;
        if (chatId) {
            socket.join(`chat_${chatId}`);
            console.log(`👥 User ${socket.id} joined chat_${chatId}`);
        }
    });

    // Handle sending messages (WhatsApp style)
    socket.on("send_message", async (data) => {
        const { chatId, userId, message, type = 'text' } = data;
        
        if (!chatId || !userId || !message) {
            return socket.emit("error", { message: "Missing required fields" });
        }

        try {
            // 1. Save message to database
            const [result] = await pool.execute(
                'INSERT INTO messages (chat_id, user_id, message, type, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW())',
                [chatId, userId, message, type]
            );

            const newMessage = {
                id: result.insertId,
                chat_id: chatId,
                user_id: userId,
                message: message,
                type: type,
                created_at: new Date()
            };

            // 2. Update last_message_at in chats table
            await pool.execute(
                'UPDATE chats SET last_message_at = NOW() WHERE id = ?',
                [chatId]
            );

            // 3. Broadcast to everyone in the room (including sender for confirmation)
            io.to(`chat_${chatId}`).emit("receive_message", newMessage);
            
            console.log(`📩 Message saved and broadcasted for chat_${chatId}`);
        } catch (err) {
            console.error('❌ Error saving message:', err);
            socket.emit("error", { message: "Failed to save message" });
        }
    });

    // Typing indicators
    socket.on("typing", (data) => {
        const { chatId, userName } = data;
        socket.to(`chat_${chatId}`).emit("user_typing", { userName });
    });

    socket.on("stop_typing", (data) => {
        const { chatId } = data;
        socket.to(`chat_${chatId}`).emit("user_stop_typing");
    });

    // Read receipts
    socket.on("mark_read", async (data) => {
        const { chatId, userId, messageIds } = data;
        try {
            if (messageIds && messageIds.length > 0) {
                await pool.query(
                    'UPDATE messages SET read_at = NOW() WHERE id IN (?) AND user_id != ?',
                    [messageIds, userId]
                );
                io.to(`chat_${chatId}`).emit("messages_read", { messageIds, read_at: new Date() });
            }
        } catch (err) {
            console.error('❌ Error marking messages as read:', err);
        }
    });

    socket.on("disconnect", () => {
        console.log(`🔌 Client disconnected: ${socket.id}`);
    });
});

httpServer.listen(PORT, () => {
    console.log(`\n✨ Hosly Socket server is listening on port ${PORT}`);
    console.log(`🔗 Local access: http://localhost:${PORT}`);
    console.log(`🛡️ Security: CORS restricted to hosly.app domains\n`);
});
