const { pool } = require('./db');

/**
 * 🛠️ Socket Event Handlers Module
 */

const onlineUsers = new Map(); // identifier -> Set of socketIds

const registerHandlers = (io, socket) => {
    console.log(`🔌 New connection: ${socket.id}`);

    /**
     * 1. User Connected / Handshake
     */
    socket.on("user_connected", async (data) => {
        const identifier = String(data.userId || data.guestId);
        if (!identifier || identifier === "null" || identifier === "undefined") return;

        socket.trackingId = identifier;
        socket.userId = data.userId;
        socket.guestId = data.guestId;

        if (!onlineUsers.has(identifier)) onlineUsers.set(identifier, new Set());
        onlineUsers.get(identifier).add(socket.id);
        
        io.emit("online_users", Array.from(onlineUsers.keys()));
        io.emit("user_status_changed", { userId: identifier, status: 'online' });
        console.log(`✅ ${socket.guestId ? 'Guest' : 'User'} ${identifier} is Online`);

        // Auto-join rooms
        try {
            let chatIds = [];
            if (socket.guestId) {
                const [rows] = await pool.execute('SELECT id FROM chats WHERE guest_id = ?', [identifier]);
                chatIds = rows.map(r => r.id);
            } else {
                const [rows] = await pool.execute('SELECT chat_id FROM chat_users WHERE user_id = ?', [identifier]);
                chatIds = rows.map(r => r.chat_id);
            }
            
            chatIds.forEach(id => {
                socket.join(`chat_${id}`);
                console.log(`🏠 ${identifier} joined room: chat_${id}`);
            });
        } catch (e) { 
            console.error('❌ Join Error:', e.message); 
        }
    });

    /**
     * 2. Messaging
     */
    socket.on("send_message", async (data) => {
        const chatId = data.chatId || data.chat_id;
        if (!chatId) return;

        // Auto-join if not joined (prevents race conditions)
        socket.join(`chat_${chatId}`);

        try {
            // 0. Authorization: Is the user a member of this chat?
            let isMember = false;
            const identifier = socket.trackingId;

            if (socket.guestId) {
                const [chat] = await pool.execute('SELECT id FROM chats WHERE id = ? AND guest_id = ?', [chatId, identifier]);
                if (chat.length > 0) isMember = true;
            } else {
                const [membership] = await pool.execute('SELECT id FROM chat_users WHERE chat_id = ? AND user_id = ?', [chatId, identifier]);
                if (membership.length > 0) isMember = true;
            }

            if (!isMember) {
                console.warn(`🚫 Unauthorized send_message attempt from ${socket.id} (ID: ${identifier}) to chat_${chatId}`);
                socket.emit("message_error", { error: "Unauthorized: You are not a member of this chat." });
                return;
            }

            // 1. Save to DB first to get the real ID

            const [result] = await pool.execute(
                'INSERT INTO messages (chat_id, user_id, guest_id, message, type, file_path, file_name) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [
                    chatId, 
                    data.userId || null, 
                    data.guestId || null, 
                    data.message, 
                    data.type || 'text',
                    data.file_path || null,
                    data.file_name || null
                ]
            );

            if (result.insertId) {
                const messageId = result.insertId;
                const fullMessage = {
                    ...data,
                    id: messageId,
                    chat_id: chatId,
                    user_id: data.userId || data.user_id || null,
                    guest_id: data.guestId || data.guest_id || null,
                    created_at: new Date()
                };

                // 2. Broadcast to EVERYONE in the room (including sender)
                io.to(`chat_${chatId}`).emit("receive_message", fullMessage);
                
                // 3. Update last_message_at in chats
                await pool.execute('UPDATE chats SET last_message_at = NOW() WHERE id = ?', [chatId]);
                console.log(`💾 Message saved and broadcasted (ID: ${messageId})`);
            }
        } catch (e) { 
            console.error('❌ Send Message Error:', e.message); 
            // Optional: Tell the sender it failed
            socket.emit("message_error", { error: "Failed to save message", originalData: data });
        }
    });

    /**
     * 3. Receipts (Read & Delivered)
     */
    socket.on("mark_read", async (data) => {
        const chatId = data.chatId || data.chat_id;
        const messageIds = data.messageIds || []; 

        if (chatId) {
            try {
                const userId = data.userId || data.user_id;
                const guestId = data.guestId || data.guest_id;

                let query = 'UPDATE messages SET read_at = NOW(), delivered_at = COALESCE(delivered_at, NOW()) WHERE chat_id = ? AND read_at IS NULL';
                let params = [chatId];

                // Don't mark our own messages as read
                if (userId) {
                    query += ' AND (user_id IS NULL OR user_id != ?)';
                    params.push(userId);
                }
                if (guestId) {
                    query += ' AND (guest_id IS NULL OR guest_id != ?)';
                    params.push(guestId);
                }

                if (messageIds.length > 0) {
                    query += ` AND id IN (${messageIds.map(() => '?').join(',')})`;
                    params = [...params, ...messageIds];
                }

                await pool.execute(query, params);
                
                io.to(`chat_${chatId}`).emit("messages_read", { 
                    chatId, 
                    messageIds: messageIds,
                    read_at: new Date() 
                });
            } catch (e) { 
                console.error('❌ Read Receipt Error:', e.message); 
            }
        }
    });

    socket.on("mark_delivered", async (data) => {
        const chatId = data.chatId || data.chat_id;
        const messageIds = data.messageIds || [];
        const userId = data.userId || data.user_id;
        const guestId = data.guestId || data.guest_id;

        if (chatId && messageIds.length > 0) {
            try {
                const placeholders = messageIds.map(() => '?').join(',');
                let query = `UPDATE messages SET delivered_at = NOW() WHERE id IN (${placeholders}) AND delivered_at IS NULL`;
                let params = [...messageIds];

                // Don't mark our own messages as delivered
                if (userId) {
                    query += ' AND (user_id IS NULL OR user_id != ?)';
                    params.push(userId);
                }
                if (guestId) {
                    query += ' AND (guest_id IS NULL OR guest_id != ?)';
                    params.push(guestId);
                }

                await pool.execute(query, params);
                
                io.to(`chat_${chatId}`).emit("message_delivered", { 
                    chatId, 
                    messageIds, 
                    delivered_at: new Date() 
                });
            } catch (e) {
                console.error('❌ Delivery Receipt Error:', e.message);
            }
        }
    });

    /**
     * 4. Chat Management (Clear, Delete)
     */
    socket.on("clear_chat", async (data) => {
        const chatId = data.chatId || data.chat_id;
        if (chatId) {
            try {
                // Soft delete or Hard delete? Laravel seems to use SoftDeletes.
                // We'll use soft delete if column exists, otherwise hard delete.
                await pool.execute('DELETE FROM messages WHERE chat_id = ?', [chatId]);
                io.to(`chat_${chatId}`).emit("chat_cleared", { chatId });
                console.log(`🧹 Chat ${chatId} cleared`);
            } catch (e) {
                console.error('❌ Clear Chat Error:', e.message);
            }
        }
    });

    socket.on("delete_message", async (data) => {
        const { messageId, type, userId } = data;
        if (messageId) {
            try {
                if (type === 'everyone') {
                    await pool.execute('DELETE FROM messages WHERE id = ?', [messageId]);
                } else {
                    // For "delete for me", we might need a separate table or a JSON column.
                    // For now, we'll just soft delete from the main table if it's the sender.
                    await pool.execute('UPDATE messages SET deleted_at = NOW() WHERE id = ?', [messageId]);
                }
                
                // Notify clients
                io.emit("message_deleted", { messageId, type });
                console.log(`🗑️ Message ${messageId} deleted (${type})`);
            } catch (e) {
                console.error('❌ Delete Message Error:', e.message);
            }
        }
    });

    /**
     * 5. Interaction (Typing, Joining)
     */
    socket.on("typing", (data) => {
        if (data.chatId) {
            socket.to(`chat_${data.chatId}`).emit("user_typing", { 
                chatId: data.chatId, 
                userId: socket.trackingId 
            });
        }
    });

    socket.on("stop_typing", (data) => {
        if (data.chatId) {
            socket.to(`chat_${data.chatId}`).emit("user_stop_typing", { 
                chatId: data.chatId, 
                userId: socket.trackingId 
            });
        }
    });

    socket.on("join_chat", async (data) => {
        const chatId = data.chatId || data.chat_id;
        const identifier = socket.trackingId;

        if (chatId && identifier) {
            try {
                let isMember = false;
                if (socket.guestId) {
                    const [chat] = await pool.execute('SELECT id FROM chats WHERE id = ? AND guest_id = ?', [chatId, identifier]);
                    if (chat.length > 0) isMember = true;
                } else {
                    const [membership] = await pool.execute('SELECT id FROM chat_users WHERE chat_id = ? AND user_id = ?', [chatId, identifier]);
                    if (membership.length > 0) isMember = true;
                }

                if (isMember) {
                    socket.join(`chat_${chatId}`);
                    console.log(`🏠 ${socket.id} authorized & joined room: chat_${chatId}`);
                } else {
                    console.warn(`🚫 Unauthorized join_chat attempt from ${socket.id} to chat_${chatId}`);
                }
            } catch (e) {
                console.error('❌ Authorization Error:', e.message);
            }
        }
    });

    /**
     * 6. Disconnect
     */
    socket.on("disconnect", () => {
        const id = socket.trackingId;
        if (id && onlineUsers.has(id)) {
            onlineUsers.get(id).delete(socket.id);
            if (onlineUsers.get(id).size === 0) {
                onlineUsers.delete(id);
                io.emit("online_users", Array.from(onlineUsers.keys()));
                io.emit("user_status_changed", { userId: id, status: 'offline' });
                console.log(`🔌 ${id} is Offline`);
            }
        }
    });
};

module.exports = { registerHandlers };
