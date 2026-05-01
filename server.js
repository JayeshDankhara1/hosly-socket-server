require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const { initializeDatabase } = require('./db');
const { registerHandlers } = require('./socketHandlers');

/**
 * 🚀 Hosly Socket Server - Modular Version
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

// 2. Database Initialization
initializeDatabase();

// 3. Server Setup
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active (Modular MySQL Mode)\n");
});

const io = new Server(httpServer, {
    cors: { 
        origin: (origin, callback) => {
            // Allow if no origin (local tools) or if in local mode
            if (!origin || process.env.APP_ENV === 'local') return callback(null, true);
            
            const allowed = (process.env.SOCKET_ALLOWED_ORIGINS || "").split(',').map(o => o.trim().replace(/\/$/, "").toLowerCase());
            const normalizedOrigin = origin.replace(/\/$/, "").toLowerCase();

            if (allowed.includes(normalizedOrigin)) {
                callback(null, true);
            } else {
                console.error(`🚫 CORS Blocked: ${origin}`);
                callback(new Error('Not allowed by CORS'));
            }
        },
        methods: ["GET", "POST"], 
        credentials: true 
    },
    pingTimeout: 60000,
    pingInterval: 25000
});

// 4. Security Middleware (Authentication)
io.use((socket, next) => {
    // TIP: In production, pass a token from the frontend and verify it here.
    // const token = socket.handshake.auth.token;
    // if (!token && process.env.APP_ENV === 'production') return next(new Error('Authentication error'));
    
    // For now, we trust the handshake data but log it
    console.log(`🔒 Handshake: ${socket.id} from ${socket.handshake.headers.origin}`);
    next();
});

// 5. Register Handlers

io.on("connection", (socket) => {
    registerHandlers(io, socket);
});

httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Hosly Socket Server running on port ${PORT} (Modular MySQL Mode)`);
});
