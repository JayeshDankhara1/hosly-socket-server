const mysql = require('mysql2/promise');
require('dotenv').config();

const pool = mysql.createPool({
    host: process.env.CHAT_DB_HOST || 'localhost',
    port: process.env.CHAT_DB_PORT || 3306,
    user: process.env.CHAT_DB_USERNAME || 'root',
    password: process.env.CHAT_DB_PASSWORD || '',
    database: process.env.CHAT_DB_DATABASE || 'hosly_chat',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});



async function initializeDatabase() {
    let connection;
    try {
        // Try connecting to the specified database
        connection = await pool.getConnection();
        console.log(`✅ Connected to MySQL database: ${process.env.CHAT_DB_DATABASE}`);


        // Create tables if they don't exist
        await connection.query(`
            CREATE TABLE IF NOT EXISTS chats (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NULL,
                avatar VARCHAR(255) NULL,
                type ENUM('direct', 'group') DEFAULT 'direct',
                status ENUM('pending', 'active', 'accepted', 'rejected') DEFAULT 'active',
                creator_id VARCHAR(255) NULL,
                guest_id VARCHAR(255) NULL,
                last_message_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        `);

        await connection.query(`
            CREATE TABLE IF NOT EXISTS chat_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id INT NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
            )
        `);

        await connection.query(`
            CREATE TABLE IF NOT EXISTS messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id INT NOT NULL,
                user_id VARCHAR(255) NULL,
                guest_id VARCHAR(255) NULL,
                message TEXT NOT NULL,
                type VARCHAR(50) DEFAULT 'text',
                file_path TEXT NULL,
                file_name VARCHAR(255) NULL,
                is_edited BOOLEAN DEFAULT FALSE,
                read_at TIMESTAMP NULL,
                delivered_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted_at TIMESTAMP NULL,
                FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
            )
        `);

        console.log("✅ MySQL tables initialized successfully");
    } catch (err) {
        if (err.code === 'ER_BAD_DB_ERROR') {
            console.error(`❌ Database '${process.env.DB_DATABASE}' does not exist.`);
            console.log(`💡 Please create the database manually: CREATE DATABASE ${process.env.DB_DATABASE};`);
        } else {
            console.error("❌ MySQL initialization error:", err.message);
        }
    } finally {
        if (connection) connection.release();
    }
}

module.exports = { pool, initializeDatabase };
