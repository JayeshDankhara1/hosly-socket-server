const { pool, initializeDatabase } = require('./db');

async function test() {
    console.log("🔍 Testing MySQL connection and initialization...");
    try {
        await initializeDatabase();
        
        const [rows] = await pool.query('SHOW TABLES');
        console.log("📋 Current tables in database:");
        rows.forEach(row => console.log(`- ${Object.values(row)[0]}`));
        
        console.log("✅ MySQL test completed successfully.");
        process.exit(0);
    } catch (err) {
        console.error("❌ MySQL test failed:", err.message);
        process.exit(1);
    }
}

test();
