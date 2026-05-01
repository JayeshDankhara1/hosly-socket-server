# Minimal Socket Server

A lightweight, single-file Node.js socket server built with Socket.io.

## Setup

1. **Navigate to the directory**:
   ```bash
   cd hosly-socket-server
   ```

2. **Start the server**:
   ```bash
   npm start
   ```

## Features
- **CORS enabled**: Accessible from any frontend origin.
- **Auto-Broadcasting**: Messages received on `chat_message` are broadcast to all connected clients.
- **Port**: Default is `3000` (can be changed via `PORT` environment variable).

## Example Client Code (JavaScript)

```javascript
const socket = io("http://localhost:3000");

socket.on("connect", () => {
  console.log("Connected to server!", socket.id);
});

// Sending a message
socket.emit("chat_message", "Hello everyone!");

// Receiving messages
socket.on("chat_message", (data) => {
  console.log("New message:", data);
});
```
