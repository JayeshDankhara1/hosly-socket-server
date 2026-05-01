require('dotenv').config();
const { Server } = require("socket.io");
const http = require("http");
const mysql = require("mysql2/promise");

// 1. IMPORTANT: Use Hostinger's dynamic port
const PORT = process.env.PORT || 3000;

// 2. Health Check Server
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Hosly Socket Backend is Active\n");
});

// 3. Database Connection Pool
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

// Test Database Connection
pool.getConnection()
    .then(conn => {
        console.log("✅ Database Connected successfully!");
        conn.release();
    })
    .catch(err => {
        console.error("❌ Database Connection Failed:", err.message);
    });

// 4. Socket.io Setup
const io = new Server(httpServer, {
    cors: {
        origin: process.env.SOCKET_ALLOWED_ORIGINS ? process.env.SOCKET_ALLOWED_ORIGINS.split(',') : "*",
        methods: ["GET", "POST"]
    }
});

io.on("connection", (socket) => {
    console.log("🔌 User Connected:", socket.id);

    socket.on("user_connected", (data) => {
        socket.userId = data.userId;
        console.log(`👤 User ${data.userId} is online`);
    });

    socket.on("disconnect", () => {
        console.log("👋 User Disconnected:", socket.id);
    });
});

// 5. Start Server
httpServer.listen(PORT, () => {
    console.log(`🚀 Socket Server running on port ${PORT}`);
});
