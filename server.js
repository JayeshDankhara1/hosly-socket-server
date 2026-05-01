const { Server } = require("socket.io");
const http = require("http");

/**
 * Minimal Socket Server
 * 
 * To run:
 * 1. cd hosly-socket-server
 * 2. node server.js
 */

const PORT = process.env.PORT || 3000;

// Create HTTP server for health checks
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Socket server is running...\n");
});

// Initialize Socket.io with CORS enabled
const io = new Server(httpServer, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

io.on("connection", (socket) => {
    console.log(`🚀 Client connected: ${socket.id}`);

    // Listen for events
    socket.on("chat_message", (msg) => {
        console.log(`📩 Message from ${socket.id}:`, msg);
        
        // Broadcast to everyone including sender
        io.emit("chat_message", {
            user: socket.id,
            message: msg,
            time: new Date().toLocaleTimeString()
        });
    });

    socket.on("disconnect", () => {
        console.log(`🔌 Client disconnected: ${socket.id}`);
    });
});

httpServer.listen(PORT, () => {
    console.log(`\n✨ Socket server is listening on port ${PORT}`);
    console.log(`🔗 Local access: http://localhost:${PORT}\n`);
});
