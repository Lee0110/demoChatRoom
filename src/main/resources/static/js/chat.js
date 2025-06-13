// ==================== 全局变量 ====================
let socket;
let currentUser;

// ==================== DOM元素 ====================
const elements = {
    usernameModal: document.getElementById('username-modal'),
    chatInterface: document.getElementById('chat-interface'),
    usernameInput: document.getElementById('username-input'),
    enterChatBtn: document.getElementById('enter-chat-btn'),
    messageInput: document.getElementById('message-input'),
    sendBtn: document.getElementById('send-btn'),
    usernameDisplay: document.getElementById('username-display'),
    chatContainer: document.getElementById('chat-container')
};

// 新增全局变量用于状态管理
let reconnectAttempts = 0; // 记录重连次数
let reconnectTimeout = 3000; // 初始重连间隔（毫秒）
let heartbeatInterval; // 心跳定时器
let reconnectTimer; // 重连定时器
// ==================== WebSocket连接 ====================
function initWebSocket() {
    socket = new WebSocket("ws://localhost:8081/chat");
    // 连接成功
    socket.onopen = () => {
        console.log("WebSocket连接已建立");
        // 清除重连计时器
        clearTimeout(reconnectTimer);
        reconnectAttempts = 0; // 重置重连次数

        // 启动心跳
        heartbeatInterval = setInterval(() => {
            if (socket.readyState === WebSocket.OPEN) {
                socket.send("ping");
            }
        }, 30000); // 每30秒发送一次心跳
    };
    // 接收消息
    socket.onmessage = (event) => {
        if ("pong" === event.data) return;
        const message = JSON.parse(event.data);
        appendMessage(message.user, message.content);
    };
    // 连接关闭
    socket.onclose = (event) => {
        console.log("WebSocket连接已关闭，正在尝试重新连接...");

        // 清除心跳
        clearInterval(heartbeatInterval);
        // 指数退避策略
        reconnectTimeout = Math.min(reconnectTimeout * 1.5, 30000);
        reconnectTimer = setTimeout(() => {
            initWebSocket();
        }, reconnectTimeout);
        reconnectAttempts++;
        console.log(`第${reconnectAttempts}次重连尝试`);
    };
    // 连接错误
    socket.onerror = (error) => {
        console.error("WebSocket错误:", error);
        // 保持重连机制继续工作
    };
}

// ==================== 消息处理 ====================
function sendMessage() {
    const content = elements.messageInput.value.trim();
    if (!content) return;

    const message = {
        user: currentUser,
        content: content
    };

    socket.send(JSON.stringify(message));
    elements.messageInput.value = "";
    elements.messageInput.focus();
}

function appendMessage(user, content) {
    const messageDiv = document.createElement("div");
    messageDiv.className = "message";
    messageDiv.innerHTML = `<strong>${user}</strong>: ${content}`;
    elements.chatContainer.appendChild(messageDiv);
    elements.chatContainer.scrollTop = elements.chatContainer.scrollHeight;
}

// ==================== 用户名处理 ====================
function checkUsername() {
    currentUser = sessionStorage.getItem("chat_username");
    if (!currentUser) {
        elements.usernameModal.style.display = "block";
        elements.chatInterface.style.display = "none";
    } else {
        elements.usernameModal.style.display = "none";
        elements.chatInterface.style.display = "block";
        elements.usernameDisplay.textContent = currentUser;
        initWebSocket();
    }
}

// 重置用户名功能
function resetUsername() {
    sessionStorage.removeItem("chat_username");
    location.reload(); // 刷新页面重新输入
}

function saveUsername() {
    currentUser = elements.usernameInput.value.trim();
    if (!currentUser) {
        alert("用户名不能为空！");
        return;
    }

    sessionStorage.setItem("chat_username", currentUser);
    checkUsername();
}

// ==================== 事件绑定 ====================
function bindEvents() {
    // 进入聊天室按钮
    elements.enterChatBtn.addEventListener('click', saveUsername);

    // 发送按钮
    elements.sendBtn.addEventListener('click', sendMessage);

    // 回车发送
    elements.messageInput.addEventListener('keypress', (e) => {
        if (e.key === "Enter") sendMessage();
    });

    // 初始化检查
    window.addEventListener('DOMContentLoaded', checkUsername);
}

// 启动
bindEvents();
