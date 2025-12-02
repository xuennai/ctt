/**
 * Part 1: Infrastructure, Config & Helpers
 */

// --- 1. Global Variables & Config ---
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;
let CTX;
let WORKER_URL;

let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000;
let isInitialized = false;

// Caches
const processedMessages = new Set();
const processedCallbacks = new Set();
const mediaGroupCache = new Map();
const topicCreationLocks = new Map();
const settingsCache = new Map();

// --- 2. LRU Cache Class ---
class LRUCache {
    constructor(maxSize) {
        this.maxSize = maxSize;
        this.cache = new Map();
    }
    get(key) {
        const value = this.cache.get(key);
        if (value !== undefined) {
            this.cache.delete(key);
            this.cache.set(key, value);
        }
        return value;
    }
    set(key, value) {
        if (this.cache.size >= this.maxSize) {
            this.cache.delete(this.cache.keys().next().value);
        }
        this.cache.set(key, value);
    }
    delete(key) { return this.cache.delete(key); }
    clear() { this.cache.clear(); }
}

// Instance Caches
const userInfoCache = new LRUCache(1000);
const topicIdCache = new LRUCache(1000);
const userStateCache = new LRUCache(1000);
const messageRateCache = new LRUCache(1000);

// --- 3. Database Helper (Reduces boilerplate) ---
const DB = {
    async get(d1, sql, params = []) {
        return await d1.prepare(sql).bind(...params).first();
    },
    async run(d1, sql, params = []) {
        return await d1.prepare(sql).bind(...params).run();
    },
    async all(d1, sql, params = []) {
        return await d1.prepare(sql).bind(...params).all();
    },
    async exec(d1, sql) {
        return await d1.exec(sql);
    },
    async batch(d1, statements) {
        return await d1.batch(statements);
    }
};

// --- 4. API Client Wrapper ---
async function telegramRequest(method, payload, retries = 3) {
    const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
    const options = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    };

    for (let i = 0; i < retries; i++) {
        try {
            const response = await fetch(url, options);
            if (response.ok) return await response.json();

            // Handle 429 (Rate Limit)
            if (response.status === 429) {
                const retryAfter = parseInt(response.headers.get('Retry-After') || '5');
                await new Promise(r => setTimeout(r, retryAfter * 1000));
                continue;
            }

            // Client errors (4xx) should not be retried (except 429)
            if (response.status >= 400 && response.status < 500) {
                const errText = await response.text();
                throw new Error(`Telegram API Error ${response.status}: ${errText}`);
            }

            throw new Error(`Server Error ${response.status}`);
        } catch (error) {
            if (error.message.includes('Telegram API Error')) throw error; // Don't retry client errors
            if (i === retries - 1) throw error;
            await new Promise(r => setTimeout(r, 1000 * Math.pow(2, i)));
        }
    }
}

/**
 * Part 2: Core Logic & Handlers
 */
export default {
    async fetch(request, env, ctx) {
        BOT_TOKEN = env.BOT_TOKEN_ENV;
        GROUP_ID = env.GROUP_ID_ENV;
        MAX_MESSAGES_PER_MINUTE = parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV || '40');
        CTX = ctx;

        if (!env.D1 || !BOT_TOKEN || !GROUP_ID) {
            return new Response('Config Error', { status: 500 });
        }

        if (!isInitialized) {
            await initialize(env.D1);
            isInitialized = true;
        }

        const url = new URL(request.url);

        // 自动获取 Worker URL (从任何请求中提取 origin)
        if (!WORKER_URL) {
            WORKER_URL = url.origin;
        }

        if (url.pathname === '/webhook' && request.method === 'POST') {
            try {
                const update = await request.json();
                ctx.waitUntil(handleUpdate(env, update));
                return new Response('OK');
            } catch (e) { return new Response('Bad Request', { status: 400 }); }
        }

        // Verification Page Route (Mini App)
        if (url.pathname === '/verify_page') {
            const chatId = url.searchParams.get('chat_id');
            const token = url.searchParams.get('token');
            if (!chatId || !token) {
                return new Response('Missing parameters', { status: 400 });
            }
            return await renderVerifyPage(env, chatId, token);
        }

        // Verification Submit Route
        if (url.pathname === '/verify_submit' && request.method === 'POST') {
            return await handleVerifySubmit(env, request);
        }

        // Simple routes
        switch (url.pathname) {
            case '/registerWebhook': return await registerWebhook(request);
            case '/unRegisterWebhook': return await unRegisterWebhook();
            case '/checkTables':
                await checkAndRepairTables(env.D1);
                return new Response('Tables checked', { status: 200 });
            default: return new Response('Not Found', { status: 404 });
        }
    }
};

async function initialize(d1) {
    // 首次启动时确保数据库表存在
    await checkAndRepairTables(d1);
    // 清理过期的验证码
    await cleanExpiredVerificationCodes(d1);
}

async function handleUpdate(env, update) {
    if (update.message) {
        const key = `${update.message.chat.id}:${update.message.message_id}`;
        if (processedMessages.has(key)) return;
        processedMessages.add(key);
        if (processedMessages.size > 5000) processedMessages.clear(); // Reduced size to save memory
        await onMessage(env, update.message);
    } else if (update.edited_message) {
        await onEditedMessage(env, update.edited_message);
    } else if (update.callback_query) {
        await onCallbackQuery(env, update.callback_query);
    }
}

async function onMessage(env, message) {
    const chatId = message.chat.id.toString();
    const text = message.text || message.caption || '';
    const messageId = message.message_id;

    // --- 1. Admin/Group Logic (群组侧逻辑) ---
    if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (!topicId) return;

        const privateChatId = await getPrivateChatId(env.D1, topicId);

        // Command: /delete (删除指令)
        if (/^\/delete(@\w+)?$/i.test(text)) {
            if (!privateChatId) return;
            if (await guardRateLimit(env.D1, GROUP_ID, topicId, 'general')) return;
            try { await deleteMessage(GROUP_ID, messageId); } catch (e) { }

            let targetGroupMsgId = null;
            // 如果是对某条消息回复 /delete
            if (message.reply_to_message && !message.reply_to_message.forum_topic_created) {
                targetGroupMsgId = message.reply_to_message.message_id.toString();
            } else {
                // 否则，自动查找最后一条由 Admin 发送的消息
                const lastAdminMsg = await DB.get(env.D1, 'SELECT group_message_id FROM message_mappings WHERE private_chat_id = ? AND sender_type = ? ORDER BY created_at DESC LIMIT 1', [privateChatId, 'admin']);
                if (lastAdminMsg) targetGroupMsgId = lastAdminMsg.group_message_id;
            }

            if (targetGroupMsgId) {
                // 执行双向删除
                await handleSyncedDelete(env.D1, targetGroupMsgId, null);
                // 添加删除成功的提示
                // await sendTempMessage(chatId, topicId, "🗑 消息已删除。");
            } else {
                await sendTempMessage(chatId, topicId, "⚠️ 未找到可删除的关联消息。");
            }
            return;
        }

        // Command: /wipe (批量撤回)
        if (privateChatId && /^\/wipe(@\w+)?(\s+\d+)?$/i.test(text)) {
            if (await guardRateLimit(env.D1, GROUP_ID, topicId, 'wipe')) return;
            const count = Math.min(Math.max(parseInt(text.split(/\s+/)[1] || '3'), 1), 50);
            await handleBatchDelete(env.D1, privateChatId, count, 'admin');
            try { await deleteMessage(chatId, messageId); } catch (e) { }
            await sendTempMessage(chatId, topicId, `🗑 已撤回最近 ${count} 条消息。`);
            return;
        }

        // Command: /admin (管理面板)
        if (privateChatId && /^\/admin(@\w+)?$/i.test(text)) {
            if (await guardRateLimit(env.D1, GROUP_ID, topicId, 'general', true)) return;
            // 并行执行：删除命令消息 + 发送面板
            await Promise.all([
                deleteMessage(chatId, messageId),
                sendAdminPanel(env, chatId, topicId, privateChatId, null, false)
            ]);
            return;
        }

        // Normal Reply (Forward to User) (普通回复转发)
        if (privateChatId) {
            // Check Block/Verification status
            const userState = await getUserState(env.D1, privateChatId);

            if (userState.is_blocked) {
                await sendTempMessage(chatId, topicId, "🚫 发送失败：该用户已被拉黑。");
                return;
            }
            const verifyEnabled = (await getSetting(env.D1, 'verification_enabled')) === 'true';
            if (verifyEnabled && !userState.is_verified) {
                await sendTempMessage(chatId, topicId, "⏳ 发送失败：用户未通过验证。");
                return;
            }

            const sentMsgId = await forwardMessageToPrivateChat(privateChatId, message);
            if (sentMsgId) await saveMessageMapping(env.D1, messageId.toString(), privateChatId, sentMsgId.toString(), 'admin');
        }
        return;
    }

    // --- 2. User/Private Logic (用户私聊逻辑) ---
    // 强制从数据库刷新用户状态（不使用缓存），确保获取最新验证状态
    userStateCache.delete(chatId);
    const userState = await getUserState(env.D1, chatId);

    // 被拉黑后给予提示
    if (userState.is_blocked) {
        await sendMessageToUser(chatId, "🚫 您已被拉黑，无法发送消息，请联系管理员。");
        return;
    }

    // --- Verification Logic ---
    // 检查 Turnstile 密钥是否配置，未配置则强制跳过验证
    const hasTurnstileKeys = env.TURNSTILE_SITE_KEY && env.TURNSTILE_SECRET_KEY;
    const verifyEnabled = hasTurnstileKeys && (await getSetting(env.D1, 'verification_enabled')) === 'true';
    console.log(`[Verify] chatId=${chatId}, hasTurnstileKeys=${hasTurnstileKeys}, verifyEnabled=${verifyEnabled}, userState=`, JSON.stringify(userState));

    if (verifyEnabled) {
        const now = Math.floor(Date.now() / 1000);
        const isVerifiedValid = userState.is_verified && (!userState.verified_expiry || now < userState.verified_expiry);
        console.log(`[Verify] now=${now}, isVerifiedValid=${isVerifiedValid}, is_verified=${userState.is_verified}, verified_expiry=${userState.verified_expiry}`);

        if (isVerifiedValid) {
            // 用户状态正常，什么都不做，让代码继续往下走去转发消息
            console.log(`[Verify] User verified, proceeding to forward message`);
        } else {
            // 检查验证码格式：新 Token 格式为 "chatId_timestamp_random"，旧数学答案是纯数字
            const isNewTokenFormat = userState.verification_code && userState.verification_code.includes('_');

            // 如果有旧格式数据（旧验证码或无效的 code_expiry），清理它们
            const hasOldData = (userState.verification_code && !isNewTokenFormat) ||
                (userState.code_expiry && !userState.verification_code) ||
                (userState.code_expiry && userState.code_expiry > now + 86400); // 超过24小时的过期时间肯定是旧数据

            if (hasOldData) {
                console.log(`[Verify] Old data detected, clearing: code=${userState.verification_code}, expiry=${userState.code_expiry}`);
                userState.verification_code = null;
                userState.code_expiry = null;
                userState.is_verifying = false;
                userStateCache.set(chatId, userState);
                await DB.run(env.D1, 'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?', [chatId]);
                // 清理后继续往下走，发送新的验证
            }

            // 条件：处于惩罚冷却期内（有 code_expiry 但没有 verification_code，且 code_expiry 在合理范围内）
            if (userState.code_expiry && now < userState.code_expiry && !userState.verification_code) {
                const remainingSeconds = userState.code_expiry - now;
                // 冷却期最多 5 分钟，超过的话说明是旧数据
                if (remainingSeconds <= 300) {
                    const waitText = remainingSeconds > 60 ? `${Math.ceil(remainingSeconds / 60)} 分钟` : `${remainingSeconds} 秒`;
                    console.log(`[Verify] User in cooldown, remaining=${waitText}`);
                    await sendMessageToUser(chatId, `⏳ 请等待 ${waitText} 后再试。`);
                    return;
                }
            }

            // 条件：有验证码正在验证中（新格式）
            if (userState.verification_code && userState.is_verifying && isNewTokenFormat) {
                // 检查 Token 是否过期
                if (userState.code_expiry && now < userState.code_expiry) {
                    const remaining = userState.code_expiry - now;
                    const remainingText = remaining > 60 ? `${Math.ceil(remaining / 60)} 分钟` : `${remaining} 秒`;
                    console.log(`[Verify] User already in verification process with valid token`);
                    await sendMessageToUser(chatId, `👆 请点击上方按钮完成验证（剩余 ${remainingText}）`);
                    return; // 阻断消息
                }

                // Token 已过期 - 进入惩罚冷却期
                if (userState.last_verification_message_id) {
                    try {
                        await telegramRequest('editMessageText', {
                            chat_id: chatId,
                            message_id: userState.last_verification_message_id,
                            text: "⏰ 验证已超时，请按下方提示重新操作。",
                            reply_markup: { inline_keyboard: [] } // 清空按钮
                        });
                    } catch (e) {
                        // 忽略编辑失败（可能消息已被用户删了）
                    }
                }

                // 增加验证失败次数
                const attempts = (userState.verification_attempts || 0) + 1;
                // 惩罚时间：首次 30 秒，之后每次翻倍，最多 5 分钟
                const cooldownSeconds = Math.min(30 * Math.pow(2, attempts - 1), 300);
                const cooldownExpiry = now + cooldownSeconds;

                // 更新状态：清除验证码，设置冷却期
                userState.verification_code = null;
                userState.is_verifying = false;
                userState.code_expiry = cooldownExpiry;
                userState.verification_attempts = attempts;
                userStateCache.set(chatId, userState);

                await DB.run(env.D1,
                    'UPDATE user_states SET verification_code = NULL, is_verifying = FALSE, code_expiry = ?, verification_attempts = ? WHERE chat_id = ?',
                    [cooldownExpiry, attempts, chatId]);

                await sendMessageToUser(chatId, `⏰ 验证超时！请等待 ${cooldownSeconds} 秒后重试。`);
                return;
            }

            const prompt = userState.is_first_verification
                ? "👋 初次对话请先完成人机验证，"
                : "⚠️ 验证过期或检测到异常，请重新验证，";

            console.log(`[Verify] Sending verification to user, prompt=${prompt}`);
            await handleVerification(env.D1, chatId, null, prompt, userState);
            return;
        }
    }

    // User Commands
    if (/^\/start(@\w+)?$/i.test(text)) {
        if (await guardRateLimit(env.D1, chatId, null, 'start')) return;
        await sendMessageToUser(chatId, `你好，欢迎使用私聊机器人！`, { disable_web_page_preview: true });
        const info = await getUserInfo(chatId);
        await ensureUserTopic(env.D1, chatId, info);
        return;
    }

    // User Self-Delete (/delete)
    if (/^\/delete(@\w+)?$/i.test(text)) {
        if (await guardRateLimit(env.D1, chatId, null, 'general')) return;
        let targetGroupMsgId = null;
        if (message.reply_to_message) {
            const mapping = await DB.get(env.D1, 'SELECT group_message_id FROM message_mappings WHERE private_chat_id = ? AND private_message_id = ?', [chatId, message.reply_to_message.message_id.toString()]);
            if (mapping) targetGroupMsgId = mapping.group_message_id;
        } else {
            const lastUserMsg = await DB.get(env.D1, 'SELECT group_message_id FROM message_mappings WHERE private_chat_id = ? AND sender_type = ? ORDER BY created_at DESC LIMIT 1', [chatId, 'user']);
            if (lastUserMsg) targetGroupMsgId = lastUserMsg.group_message_id;
        }

        if (targetGroupMsgId) {
            await handleSyncedDelete(env.D1, targetGroupMsgId, messageId, chatId);
            // 用户侧删除通常不需要额外提示“删除成功”，因为消息视觉上消失了
            // 如果你需要提示，可以在这里加：await sendMessageToUser(chatId, "已删除");
        } else {
            await deleteMessage(chatId, messageId); // 仅删除指令
        }
        return;
    }

    // User Batch Delete (/wipe)
    if (/^\/wipe(@\w+)?(\s+\d+)?$/i.test(text)) {
        if (await guardRateLimit(env.D1, chatId, null, 'wipe')) return;
        const count = Math.min(Math.max(parseInt(text.split(/\s+/)[1] || '3'), 1), 50);
        await handleBatchDelete(env.D1, chatId, count, 'user');
        try { await deleteMessage(chatId, messageId); } catch (e) { }
        return;
    }

    // --- Forwarding Logic (转发核心) ---
    const userInfo = await getUserInfo(chatId);
    if (!userInfo) return;

    let topicId;
    try {
        topicId = await ensureUserTopic(env.D1, chatId, userInfo);
    } catch (e) {
        await sendMessageToUser(chatId, "系统繁忙，无法创建话题。");
        return;
    }

    if (message.media_group_id) {
        await handleMediaGroupBuffer(env.D1, chatId, topicId, message, messageId);
    } else {
        await forwardUserMessageWithRetry(env.D1, chatId, topicId, message, userInfo, messageId);
    }
}

// --- Media Group Handling ---
async function handleMediaGroupBuffer(d1, chatId, topicId, message, originalMessageId) {
    const groupId = message.media_group_id;

    if (!mediaGroupCache.has(groupId)) {
        let resolveFunc;
        const promise = new Promise(resolve => { resolveFunc = resolve; });
        mediaGroupCache.set(groupId, { messages: [], timer: null, resolve: resolveFunc, promise: promise });
    }

    const groupData = mediaGroupCache.get(groupId);
    groupData.messages.push(message);
    if (groupData.timer) clearTimeout(groupData.timer);

    groupData.timer = setTimeout(async () => {
        const currentGroup = mediaGroupCache.get(groupId);
        if (!currentGroup) return;
        mediaGroupCache.delete(groupId);

        // Sort by ID to match user sending order
        const msgs = currentGroup.messages.sort((a, b) => a.message_id - b.message_id);
        const msgIds = msgs.map(m => m.message_id);

        try {
            // Forward the whole album
            const result = await telegramRequest('forwardMessages', {
                chat_id: GROUP_ID,
                from_chat_id: chatId,
                message_ids: msgIds,
                message_thread_id: topicId,
                disable_notification: true
            });

            // Map EVERY forwarded message, not just the first one
            if (result && result.ok && Array.isArray(result.result)) {
                // The result array generally matches the order of input message_ids
                for (let i = 0; i < result.result.length; i++) {
                    const newMsg = result.result[i];
                    const originalMsg = msgs[i]; // Corresponds to the sorted input

                    if (newMsg && originalMsg) {
                        await saveMessageMapping(d1, newMsg.message_id.toString(), chatId, originalMsg.message_id.toString(), 'user', groupId);
                    }
                }
            }
        } catch (e) {
            console.error('Error forwarding media group:', e);
        } finally {
            currentGroup.resolve();
        }
    }, 2000); // 2 seconds buffer

    await groupData.promise;
}

/**
 * Part 3: Callback Handling & Admin Logic
 */

// --- 1. Callback Query Entry Point (Fixed with User Notification) ---
async function onCallbackQuery(env, query) {
    const data = query.data;
    const chatId = query.message.chat.id.toString();
    const messageId = query.message.message_id;
    const callbackId = query.id;

    if (processedCallbacks.has(callbackId)) return;
    processedCallbacks.add(callbackId);
    if (processedCallbacks.size > 2000) processedCallbacks.clear();

    // === A. 旧验证按钮已移除，Mini App 验证不使用 callback ===

    // === B. 管理员权限检查 ===
    const senderId = query.from.id.toString();
    const isAdmin = await checkIfAdmin(senderId);
    if (!isAdmin) {
        await telegramRequest('answerCallbackQuery', {
            callback_query_id: callbackId,
            text: '❌ 只有管理员可以使用此功能',
            show_alert: true
        });
        return;
    }

    // === C. 管理员操作路由 ===
    let toastText = '';
    let shouldRefreshPanel = true;

    let action = data;
    let param = '';

    const prefixes = [
        'block_', 'unblock_',
        'toggle_verification_', 'check_blocklist_', 'toggle_user_raw_',
        'pre_del_keep_', 'pre_del_wipe_',
        'del_keep_', 'del_wipe_',
        'close_admin_panel_', 'back_admin_'
    ];

    for (const prefix of prefixes) {
        if (data.startsWith(prefix)) {
            action = prefix.slice(0, -1);
            param = data.slice(prefix.length);
            break;
        }
    }

    try {
        switch (action) {
            case 'close_admin_panel':
                await deleteMessage(chatId, messageId);
                toastText = '面板已关闭';
                shouldRefreshPanel = false;
                break;

            case 'block':
                await DB.run(env.D1, 'INSERT OR IGNORE INTO user_states (chat_id) VALUES (?)', [param]);
                await DB.run(env.D1, 'UPDATE user_states SET is_blocked = TRUE WHERE chat_id = ?', [param]);
                userStateCache.delete(param);
                toastText = `用户 ${param} 已拉黑`;
                break;

            case 'unblock':
                await DB.run(env.D1, 'UPDATE user_states SET is_blocked = FALSE, is_verified = FALSE, is_first_verification = TRUE WHERE chat_id = ?', [param]);
                userStateCache.delete(param);
                toastText = `用户 ${param} 已解除拉黑`;
                break;

            case 'toggle_verification':
                const vState = await getSetting(env.D1, 'verification_enabled');
                const vNew = vState === 'true' ? 'false' : 'true';
                await setSetting(env.D1, 'verification_enabled', vNew);
                toastText = `验证功能已${vNew === 'true' ? '开启' : '关闭'}`;
                break;

            case 'toggle_user_raw':
                const rState = await getSetting(env.D1, 'user_raw_enabled');
                const rNew = rState === 'true' ? 'false' : 'true';
                await setSetting(env.D1, 'user_raw_enabled', rNew);
                toastText = `Raw 链接已${rNew === 'true' ? '开启' : '关闭'}`;
                break;

            case 'check_blocklist':
                const blocks = await DB.all(env.D1, 'SELECT chat_id FROM user_states WHERE is_blocked = TRUE');
                const listText = blocks.results.length > 0 ? blocks.results.map(r => r.chat_id).join('\n') : '无';
                if (query.message.message_thread_id) {
                    await sendMessageToTopic(query.message.message_thread_id, `🚫 黑名单列表：\n${listText}`);
                } else {
                    await telegramRequest('sendMessage', { chat_id: chatId, text: `🚫 黑名单列表：\n${listText}` });
                }
                toastText = '查询完成';
                break;

            // === 删除/重置 预确认 ===
            case 'pre_del_keep':
            case 'pre_del_wipe':
                const isWipe = action === 'pre_del_wipe';
                const warning = isWipe
                    ? `⚠️ <b>危险操作</b>\n确定要 <b>彻底删除</b> 用户 <code>${param}</code> 吗？\n这将删除数据库记录并关闭 Topic。`
                    : `⚠️ <b>重置确认</b>\n确定要重置用户 <code>${param}</code> 的状态吗？\nTopic 将保留。`;
                const confirmBtn = isWipe ? `del_wipe_${param}` : `del_keep_${param}`;

                await telegramRequest('editMessageText', {
                    chat_id: chatId, message_id: messageId, text: warning, parse_mode: 'HTML',
                    reply_markup: {
                        inline_keyboard: [[
                            { text: '⚠️ 确认执行', callback_data: confirmBtn },
                            { text: '🔙 返回', callback_data: `back_admin_${param}` }
                        ]]
                    }
                });
                shouldRefreshPanel = false;
                break;

            // === 删除/重置 执行 ===
            case 'del_keep':
            case 'del_wipe':
                // 在执行删除前，先通知用户
                try {
                    await sendMessageToUser(param, "⚠️ 您的会话记录已被管理员重置/删除，如需继续聊天，请重新发送消息或输入 /start。");
                } catch (e) {
                    console.warn(`Failed to notify user ${param}:`, e);
                }

                await performUserDeletion(env, param, action === 'del_wipe');
                await deleteMessage(chatId, messageId);

                if (action === 'del_keep' && query.message.message_thread_id) {
                    await sendMessageToTopic(query.message.message_thread_id, `用户 ${param} 状态已重置。`);
                }

                toastText = action === 'del_wipe' ? '用户已彻底删除' : '用户已重置';
                shouldRefreshPanel = false;
                break;

            case 'back_admin':
                break;

            default:
                console.log(`Unknown action: ${action}`);
                toastText = `未知操作: ${action}`;
        }

        if (shouldRefreshPanel) {
            await sendAdminPanel(env, chatId, query.message.message_thread_id, param, messageId, true);
        }

        await telegramRequest('answerCallbackQuery', {
            callback_query_id: callbackId,
            text: toastText,
            show_alert: false
        });

    } catch (e) {
        console.error('Callback Error:', e);
        await telegramRequest('answerCallbackQuery', { callback_query_id: callbackId, text: '操作失败: ' + e.message, show_alert: true });
    }
}

// --- 2. Admin Panel UI ---
async function sendAdminPanel(env, chatId, topicId, privateChatId, messageId, isEdit) {
    const d1 = env.D1;
    const [vEnabled, rEnabled] = await Promise.all([
        getSetting(d1, 'verification_enabled'),
        getSetting(d1, 'user_raw_enabled')
    ]);

    // 检查 Turnstile 密钥是否配置
    const hasTurnstileKeys = env.TURNSTILE_SITE_KEY && env.TURNSTILE_SECRET_KEY;

    // 状态可视化：如果没配置密钥，显示警告图标
    const vIcon = !hasTurnstileKeys ? '⚠️' : (vEnabled === 'true' ? '✅' : '🔴');
    const rIcon = rEnabled === 'true' ? '✅' : '🔴';

    const buttons = [
        [
            { text: '🚫 拉黑用户', callback_data: `block_${privateChatId}` },
            { text: '🟢 解除拉黑', callback_data: `unblock_${privateChatId}` }
        ],
        [
            { text: `${vIcon} 验证功能`, callback_data: `toggle_verification_${privateChatId}` },
            { text: '📜 查询黑名单', callback_data: `check_blocklist_${privateChatId}` }
        ],
        [
            { text: `${rIcon} Raw 链接`, callback_data: `toggle_user_raw_${privateChatId}` },
            { text: '🔗 GitHub', url: 'https://github.com/xuennai/ctt' }
        ],
        [
            { text: '🔄 重置用户', callback_data: `pre_del_keep_${privateChatId}` },
            { text: '🔥 彻底删除', callback_data: `pre_del_wipe_${privateChatId}` }
        ],
        [
            { text: '❌ 关闭面板', callback_data: `close_admin_panel_${privateChatId}` }
        ]
    ];

    // 面板标题：如果没配置密钥则显示警告
    let text = `🔧 <b>管理员控制台</b>`;
    if (!hasTurnstileKeys) {
        text += `\n\n⚠️ <i>未配置 Turnstile 密钥，验证功能已禁用</i>`;
    }

    const payload = {
        chat_id: chatId,
        text: text,
        parse_mode: 'HTML',
        reply_markup: { inline_keyboard: buttons }
    };

    if (isEdit) {
        payload.message_id = messageId;
        try { await telegramRequest('editMessageText', payload); } catch (e) { }
    } else {
        payload.message_thread_id = topicId;
        await telegramRequest('sendMessage', payload);
        // 如果是新发送的面板，把原来的 /admin 命令删掉保持整洁
        if (messageId) await deleteMessage(chatId, messageId);
    }
}

// --- 3. Verification Specific Logic (已迁移到 Mini App + Turnstile) ---
// 旧的 handleVerificationCallback 已移除，验证现在通过 /verify_page 和 /verify_submit 路由处理

/**
 * Part 4: Topic Management & Forwarding Core
 */

// --- 1. Topic Creation with Locking ---
async function ensureUserTopic(d1, chatId, userInfo) {
    let lock = topicCreationLocks.get(chatId);
    if (lock) {
        await lock;
        const cached = await getExistingTopicId(d1, chatId);
        if (cached) return cached;
    }

    const createLogic = async () => {
        try {
            let existing = await getExistingTopicId(d1, chatId);
            if (existing) return existing;

            const name = userInfo.nickname || userInfo.username || `User ${chatId}`;
            // 截断名称防止报错
            const res = await telegramRequest('createForumTopic', {
                chat_id: GROUP_ID,
                name: name.substring(0, 127)
            });

            if (!res.ok) throw new Error('Create topic failed');
            const topicId = res.result.message_thread_id;

            // 发送置顶信息（包含 Notification）
            await sendTopicIntroMessage(topicId, userInfo, chatId);

            await DB.run(d1, 'INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)', [chatId, topicId]);
            topicIdCache.set(chatId, topicId);

            return topicId;
        } catch (e) {
            console.error(`Create topic error for ${chatId}:`, e);
            throw e;
        }
    };

    const newLock = createLogic();
    topicCreationLocks.set(chatId, newLock);
    try {
        return await newLock;
    } finally {
        if (topicCreationLocks.get(chatId) === newLock) {
            topicCreationLocks.delete(chatId);
        }
    }
}

// --- 2. Topic Intro Message (with Notification) ---
async function sendTopicIntroMessage(topicId, userInfo, userId) {
    const time = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });

    // 获取通知内容
    const notificationContent = await getNotificationContent();

    const text =
        `<b>🛡 用户信息卡片</b>
昵称: ${userInfo.nickname}
用户名: ${userInfo.username ? '@' + userInfo.username : '无'}
ID: <code>${userId}</code>
时间: ${time}

${notificationContent}`;

    const res = await sendMessageToTopic(topicId, text, { parse_mode: 'HTML' });

    // 如果 HTML 解析失败（通常因为通知内容里有特殊字符），降级为纯文本发送
    if (!res || !res.ok) {
        const plainText = `🛡 用户信息卡片\n昵称: ${userInfo.nickname}\nID: ${userId}\n时间: ${time}\n\n${notificationContent}`;
        await sendMessageToTopic(topicId, plainText);
    } else if (res.result) {
        // 置顶消息
        await telegramRequest('pinChatMessage', {
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            message_id: res.result.message_id
        });
    }
}

// 缓存 TTL 和远程内容变量
const CACHE_TTL = 3600 * 1000; // 1小时缓存
let cachedNotification = null;
let cachedNotificationTime = 0;

async function getNotificationContent() {
    const now = Date.now();
    if (cachedNotification !== null && (now - cachedNotificationTime) < CACHE_TTL) {
        return cachedNotification;
    }

    try {
        const response = await fetch('https://raw.githubusercontent.com/xuennai/ctt/refs/heads/main/CFTeleTrans/notification.md');
        if (!response.ok) {
            cachedNotification = '';
            cachedNotificationTime = now;
            return '';
        }
        cachedNotification = (await response.text()).trim();
        cachedNotificationTime = now;
        return cachedNotification;
    } catch (e) {
        console.warn('Failed to fetch notification:', e);
        cachedNotification = '';
        cachedNotificationTime = now;
        return '';
    }
}

// --- 3. Robust Forwarding (User -> Group) ---
async function forwardUserMessageWithRetry(d1, chatId, topicId, message, userInfo, originalMessageId) {
    try {
        await performForward(d1, chatId, topicId, message, originalMessageId);
    } catch (error) {
        const errStr = error.toString().toLowerCase();
        if (errStr.includes('thread not found') || errStr.includes('topic not found') || errStr.includes('thread is invalid')) {
            console.log(`Topic invalid for ${chatId}, recreating...`);

            await DB.run(d1, 'DELETE FROM chat_topic_mappings WHERE chat_id = ?', [chatId]);
            topicIdCache.delete(chatId);

            const newTopicId = await ensureUserTopic(d1, chatId, userInfo);
            if (newTopicId) {
                await performForward(d1, chatId, newTopicId, message, originalMessageId);
            }
        } else {
            throw error;
        }
    }
}

async function performForward(d1, chatId, topicId, message, originalMessageId) {
    const res = await telegramRequest('forwardMessage', {
        chat_id: GROUP_ID,
        from_chat_id: chatId,
        message_id: message.message_id,
        message_thread_id: topicId
    });

    if (res.ok && res.result) {
        await saveMessageMapping(d1, res.result.message_id.toString(), chatId, originalMessageId.toString(), 'user');
    }
}

// --- 4. Forwarding (Group -> User) ---
async function forwardMessageToPrivateChat(privateChatId, message) {
    const res = await telegramRequest('copyMessage', {
        chat_id: privateChatId,
        from_chat_id: message.chat.id,
        message_id: message.message_id
    });

    if (res.ok && res.result) {
        return res.result.message_id;
    }
    return null;
}



/**
 * Part 5: Helpers, Rate Limiting & DB Maintenance
 */

// --- 1. Mapping Helpers ---
// 修改函数签名，增加 mediaGroupId 参数，默认为 null
async function saveMessageMapping(d1, groupMsgId, privateChatId, privateMsgId, senderType, mediaGroupId = null) {
    const now = Math.floor(Date.now() / 1000);
    // 修改 SQL 语句，插入 media_group_id
    await DB.run(d1,
        'INSERT OR REPLACE INTO message_mappings (group_message_id, private_chat_id, private_message_id, created_at, sender_type, media_group_id) VALUES (?, ?, ?, ?, ?, ?)',
        [groupMsgId, privateChatId, privateMsgId, now, senderType, mediaGroupId]
    );
}

async function performUserDeletion(env, chatId, isWipe) {
    // 1. 清除缓存
    userStateCache.delete(chatId);
    messageRateCache.delete(chatId);
    topicIdCache.delete(chatId);

    // 2. 数据库清理
    await DB.batch(env.D1, [
        env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(chatId),
        env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(chatId),
        env.D1.prepare('DELETE FROM message_mappings WHERE private_chat_id = ?').bind(chatId)
    ]);

    if (isWipe) {
        // 彻底删除模式：还要删 Topic 和 映射表
        const topicId = await getExistingTopicId(env.D1, chatId);
        if (topicId) {
            await DB.run(env.D1, 'DELETE FROM chat_topic_mappings WHERE chat_id = ?', [chatId]);
            // 尝试关闭 Topic
            await telegramRequest('deleteForumTopic', {
                chat_id: GROUP_ID,
                message_thread_id: topicId
            });
        }
    }
}

// --- 2. User Info Helper ---
async function getUserInfo(chatId) {
    // 优先查缓存
    let info = userInfoCache.get(chatId);
    if (info) return info;

    // 调 API 查
    const res = await telegramRequest('getChat', { chat_id: chatId });
    if (res.ok && res.result) {
        const r = res.result;
        const name = [r.first_name, r.last_name].filter(Boolean).join(' ');
        info = {
            id: r.id,
            username: r.username,
            nickname: name || r.username || `User ${chatId}`
        };
    } else {
        info = { id: chatId, username: null, nickname: `User ${chatId}` };
    }
    userInfoCache.set(chatId, info);
    return info;
}

// --- 3. Rate Limiting ---
/**
 * @param {D1Database} d1 数据库
 * @param {string} chatId 用户ID或群组ID
 * @param {string|null} topicId 话题ID (私聊传 null)
 * @param {string} type 限流类型 ('start' | 'wipe' | 'general')
 * @param {boolean} silent 是否静默拦截 (不发送提示消息)，默认 false
 * @returns {Promise<boolean>} 如果被限流返回 true，否则返回 false
 */
async function guardRateLimit(d1, chatId, topicId, type, silent = false) {
    const now = Date.now();

    // === 配置中心 ===
    const config = {
        'start': {
            max: 2,
            window: 5 * 60 * 1000,
            cols: ['start_count', 'start_window_start'],
            msg: '⏳ /start 频率过高，请稍后再试。'
        },
        'wipe': {
            max: 2,
            window: 60 * 1000,
            cols: ['wipe_count', 'wipe_window_start'],
            msg: '⏳ Wipe 操作过于频繁，请休息一下。'
        },
        'general': {
            max: 15,
            window: 60 * 1000,
            cols: ['cmd_count', 'cmd_window_start'],
            msg: '⏳ 操作太快，请稍后再试。'
        }
    };

    const cfg = config[type];
    if (!cfg) return false;

    const [colCount, colStart] = cfg.cols;
    let data = await DB.get(d1, `SELECT ${colCount} as count, ${colStart} as start FROM message_rates WHERE chat_id = ?`, [chatId]);
    if (!data) {
        await DB.run(d1, 'INSERT OR IGNORE INTO message_rates (chat_id) VALUES (?)', [chatId]);
        data = { count: 0, start: now };
    }

    let count = data.count || 0;
    let start = data.start || now;
    if (now - start > cfg.window) {
        count = 1;
        start = now;
    } else {
        count++;
    }

    // 后台写入，不阻塞当前请求
    const updatePromise = DB.run(d1, `UPDATE message_rates SET ${colCount} = ?, ${colStart} = ? WHERE chat_id = ?`, [count, start, chatId]);
    if (CTX) CTX.waitUntil(updatePromise);

    if (count > cfg.max) {
        // 如果未静默，且配置了消息，则发送临时通知
        if (!silent && cfg.msg) {
            await sendTempMessage(chatId, topicId, cfg.msg);
        }
        return true;
    }
    return false;
}

// --- 4. Messaging Utilities ---
async function sendMessageToTopic(topicId, text, opts = {}) {
    return await telegramRequest('sendMessage', {
        chat_id: GROUP_ID,
        message_thread_id: topicId,
        text: text,
        ...opts
    });
}

async function sendMessageToUser(chatId, text, opts = {}) {
    return await telegramRequest('sendMessage', {
        chat_id: chatId,
        text: text,
        ...opts
    });
}

async function sendTempMessage(chatId, topicId, text) {
    let res;
    if (topicId) {
        res = await sendMessageToTopic(topicId, text);
    } else {
        res = await sendMessageToUser(chatId, text);
    }

    if (res && res.result && CTX) {
        // 使用 CTX.waitUntil 确保异步删除执行
        CTX.waitUntil(new Promise(r => setTimeout(async () => {
            await deleteMessage(topicId ? GROUP_ID : chatId, res.result.message_id);
            r();
        }, 5000))); // 5秒后删除
    }
}

async function getExistingTopicId(d1, chatId) {
    // 查缓存
    const cached = topicIdCache.get(chatId);
    if (cached) return cached;
    // 查库
    const row = await DB.get(d1, 'SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?', [chatId]);
    const val = row ? row.topic_id : null;
    if (val) topicIdCache.set(chatId, val);
    return val;
}

// 缓存远程欢迎消息
let cachedStartMessage = null;
let cachedStartMessageTime = 0;

async function getVerificationSuccessMessage(d1) {
    const rawEnabled = await getSetting(d1, 'user_raw_enabled');
    if (rawEnabled !== 'true') return '✅ 验证成功！';

    const now = Date.now();
    if (cachedStartMessage && (now - cachedStartMessageTime) < CACHE_TTL) {
        return cachedStartMessage;
    }

    // 尝试获取远程欢迎语
    try {
        const res = await fetch('https://raw.githubusercontent.com/xuennai/ctt/refs/heads/main/CFTeleTrans/start.md');
        if (res.ok) {
            cachedStartMessage = await res.text();
            cachedStartMessageTime = now;
            return cachedStartMessage;
        }
    } catch (e) { }
    return '✅ 验证成功！您现在可以发送消息了。';
}

/**
 * Part 6: Missing Implementations & Utilities (Final)
 */

// --- 1. Database Maintenance ---
async function checkAndRepairTables(d1) {
    const tables = {
        user_states: "chat_id TEXT PRIMARY KEY, is_blocked BOOLEAN DEFAULT FALSE, is_verified BOOLEAN DEFAULT FALSE, verified_expiry INTEGER, verification_code TEXT, code_expiry INTEGER, last_verification_message_id TEXT, is_first_verification BOOLEAN DEFAULT TRUE, is_rate_limited BOOLEAN DEFAULT FALSE, is_verifying BOOLEAN DEFAULT FALSE, verification_attempts INTEGER DEFAULT 0",
        message_rates: "chat_id TEXT PRIMARY KEY, message_count INTEGER DEFAULT 0, window_start INTEGER, start_count INTEGER DEFAULT 0, start_window_start INTEGER, cmd_count INTEGER DEFAULT 0, cmd_window_start INTEGER, wipe_count INTEGER DEFAULT 0, wipe_window_start INTEGER",
        chat_topic_mappings: "chat_id TEXT PRIMARY KEY, topic_id TEXT NOT NULL",
        settings: "key TEXT PRIMARY KEY, value TEXT",
        message_mappings: "group_message_id TEXT PRIMARY KEY, private_chat_id TEXT NOT NULL, private_message_id TEXT NOT NULL, created_at INTEGER, sender_type TEXT DEFAULT 'user'"
    };

    for (const [name, schema] of Object.entries(tables)) {
        await DB.exec(d1, `CREATE TABLE IF NOT EXISTS ${name} (${schema})`);
    }

    // 迁移：为旧表添加新字段（每个字段独立 try-catch，避免一个失败全部跳过）
    const alterStatements = [
        "ALTER TABLE message_rates ADD COLUMN wipe_count INTEGER DEFAULT 0",
        "ALTER TABLE message_rates ADD COLUMN wipe_window_start INTEGER",
        "ALTER TABLE message_rates ADD COLUMN cmd_count INTEGER DEFAULT 0",
        "ALTER TABLE message_rates ADD COLUMN cmd_window_start INTEGER",
        "ALTER TABLE message_mappings ADD COLUMN sender_type TEXT DEFAULT 'user'",
        "ALTER TABLE message_mappings ADD COLUMN media_group_id TEXT"
    ];

    for (const sql of alterStatements) {
        try {
            await DB.exec(d1, sql);
        } catch (e) {
            // 字段已存在时会报错，忽略即可
        }
    }

    // Indices for performance
    await DB.exec(d1, 'CREATE INDEX IF NOT EXISTS idx_mappings_private ON message_mappings (private_chat_id, private_message_id)');
    await DB.exec(d1, 'CREATE INDEX IF NOT EXISTS idx_settings_key ON settings (key)');

    // Default settings
    await DB.run(d1, 'INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ['verification_enabled', 'true']);
    await DB.run(d1, 'INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ['user_raw_enabled', 'true']);
    await DB.run(d1, 'INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ['delete_logic_mode', '1']);

    // Preload settings into cache
    const allSettings = await DB.all(d1, 'SELECT key, value FROM settings');
    if (allSettings.results) {
        for (const row of allSettings.results) settingsCache.set(row.key, row.value);
    }
}

async function cleanExpiredVerificationCodes(d1) {
    const now = Date.now();
    if (now - lastCleanupTime < CLEANUP_INTERVAL) return;

    const nowSec = Math.floor(now / 1000);
    // Cleanup expired codes
    await DB.run(d1, 'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE code_expiry IS NOT NULL AND code_expiry < ?', [nowSec]);
    // Cleanup old mappings (older than 48h) to save space
    await DB.run(d1, 'DELETE FROM message_mappings WHERE created_at < ?', [nowSec - 172800]);
    lastCleanupTime = now;
}

// --- 2. Message Editing Logic (Fixed for Forwarding) ---
async function onEditedMessage(env, message) {
    const chatId = message.chat.id.toString();
    const messageId = message.message_id.toString();
    const newContent = message.text || message.caption || "";

    if (chatId === GROUP_ID) {
        // === 情况 A: 管理员在群里编辑 (Group -> Private) ===
        // 管理员发给用户的消息是 copyMessage，属于 Bot 发送的普通消息，可以编辑
        const mapping = await DB.get(env.D1, 'SELECT private_chat_id, private_message_id FROM message_mappings WHERE group_message_id = ?', [messageId]);
        if (mapping) {
            await editRemoteMessage(mapping.private_chat_id, mapping.private_message_id, newContent, message);
        }
    } else {
        // === 情况 B: 用户在私聊编辑 (Private -> Group) ===
        // 关键点：用户发到群里的消息是 forwardMessage，原生转发不支持编辑！
        // 解决方案：删除群里的旧消息 -> 重新转发新消息

        // 1. 查出对应的旧群组消息 ID 和 发送类型
        const mapping = await DB.get(env.D1, 'SELECT group_message_id, sender_type FROM message_mappings WHERE private_chat_id = ? AND private_message_id = ?', [chatId, messageId]);

        if (mapping) {
            if (mapping.sender_type === 'user') {
                // ---> 如果是用户发送的 (sender_type='user')，说明是转发消息，必须“删旧发新”

                // 1. 获取 Topic ID
                const topicId = await getExistingTopicId(env.D1, chatId);
                if (topicId) {
                    // 2. 删除群里旧的那条转发
                    await deleteMessage(GROUP_ID, mapping.group_message_id);

                    // 3. 从数据库移除旧的映射 (防止堆积垃圾数据)
                    await DB.run(env.D1, 'DELETE FROM message_mappings WHERE group_message_id = ?', [mapping.group_message_id]);

                    // 4. 重新转发这条已编辑的消息 (performForward 会自动建立新的数据库映射)
                    // 注意：这里 message 已经是编辑后的最新对象了
                    await performForward(env.D1, chatId, topicId, message, messageId);
                }
            } else {
                // ---> 如果 sender_type 不是 user (极少见，或者是旧数据)，尝试常规编辑
                await editRemoteMessage(GROUP_ID, mapping.group_message_id, newContent, message);
            }
        }
    }
}

async function editRemoteMessage(targetChatId, targetMessageId, text, originalMessage) {
    const mediaInput = getMediaInput(originalMessage, text);
    try {
        if (mediaInput) {
            await telegramRequest('editMessageMedia', {
                chat_id: targetChatId,
                message_id: targetMessageId,
                media: mediaInput
            });
        } else {
            await telegramRequest('editMessageText', {
                chat_id: targetChatId,
                message_id: targetMessageId,
                text: text
            });
        }
    } catch (e) {
        console.warn(`Edit sync failed: ${e.message}`);
    }
}

// 3. 构造媒体对象 (用于 editMessageMedia)
function getMediaInput(message, caption) {
    let type = '';
    let fileId = '';

    // 判断媒体类型并提取 file_id
    if (message.photo && message.photo.length > 0) {
        type = 'photo';
        fileId = message.photo[message.photo.length - 1].file_id; // 取最高清图
    } else if (message.document) {
        type = 'document';
        fileId = message.document.file_id;
    } else if (message.video) {
        type = 'video';
        fileId = message.video.file_id;
    } else if (message.audio) {
        type = 'audio';
        fileId = message.audio.file_id;
    } else if (message.animation) {
        type = 'animation';
        fileId = message.animation.file_id;
    } else {
        // 纯文本消息，没有媒体
        return null;
    }

    // 返回 Telegram API 需要的 InputMedia 对象结构
    return {
        type: type,
        media: fileId,
        caption: caption
    };
}

// --- 3. Verification Generation (Mini App + Turnstile) ---
// 修复：恢复 DB 同步写入，防止用户点击过快导致数据库还没存入 Token
async function handleVerification(d1, chatId, messageIdToEdit = null, prefixText = '', userState = null) {
    console.log(`[handleVerification] Starting for chatId=${chatId}`);
    
    if (!WORKER_URL) {
        await sendMessageToUser(chatId, `${prefixText}⚠️ 系统配置错误。`);
        return;
    }

    if (!userState) {
        userState = await getUserState(d1, chatId);
    }

    // 1. 生成验证 Token
    const token = generateVerifyToken(chatId);
    const nowSec = Math.floor(Date.now() / 1000);
    const tokenExpiry = nowSec + 180; // 3分钟

    // 2. 更新内存对象
    userState.verification_code = token;
    userState.code_expiry = tokenExpiry;
    userState.is_verifying = true;
    userStateCache.set(chatId, userState);

    // 3. 数据库同步更新 (关键修复：必须 await，确保数据落地)
    await DB.run(d1,
        'UPDATE user_states SET verification_code = ?, code_expiry = ?, is_verifying = TRUE WHERE chat_id = ?',
        [token, tokenExpiry, chatId]);

    // 4. 构建 URL 并发送消息
    const verifyUrl = `${WORKER_URL}/verify_page?chat_id=${chatId}&token=${encodeURIComponent(token)}`;
    
    const payload = {
        chat_id: chatId,
        text: `${prefixText}请在 3 分钟内点击下方按钮完成人机验证`,
        reply_markup: {
            inline_keyboard: [[
                { text: '点击验证', web_app: { url: verifyUrl } }
            ]]
        }
    };

    let res;
    try {
        if (messageIdToEdit) {
            payload.message_id = messageIdToEdit;
            res = await telegramRequest('editMessageText', payload);
        } else {
            res = await telegramRequest('sendMessage', payload);
        }
    } catch (error) {
        console.error(`[handleVerification] Send failed:`, error.message);
        try {
           await sendMessageToUser(chatId, `${prefixText}验证链接：\n${verifyUrl}`);
        } catch(e) {}
    }

    // 5. 保存消息 ID (这个可以异步，因为不影响验证流程)
    if (res && res.ok && res.result && !messageIdToEdit) {
        const verifyMsgId = res.result.message_id.toString();
        userState.last_verification_message_id = verifyMsgId;
        userStateCache.set(chatId, userState);

        const saveIdPromise = DB.run(d1,
            'UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?',
            [verifyMsgId, chatId]);
        if (CTX) CTX.waitUntil(saveIdPromise);
    }
}

// 生成验证 Token
function generateVerifyToken(chatId) {
    const timestamp = Date.now().toString(36);
    const random = Math.random().toString(36).substring(2, 10);
    return `${chatId}_${timestamp}_${random}`;
}

// 渲染验证页面 (Mini App HTML)
async function renderVerifyPage(env, chatId, token) {
    const turnstileSiteKey = env.TURNSTILE_SITE_KEY || '1x00000000000000000000AA';
    
    // 1. 预检查
    let isExpired = false;
    let userState = null;
    
    try {
        userState = await getUserState(env.D1, chatId);
        const nowSec = Math.floor(Date.now() / 1000);
        
        if (!userState.verification_code || userState.verification_code !== token || (userState.code_expiry && nowSec > userState.code_expiry)) {
            isExpired = true;
        }
    } catch (e) {
        console.error('Pre-check failed:', e);
    }

    // 2. 如果已过期：前端强制销毁
    if (isExpired) {
        // A. 后台：异步立刻删按钮 (不阻塞)
        if (userState && userState.last_verification_message_id) {
            const editPromise = telegramRequest('editMessageText', {
                chat_id: chatId,
                message_id: userState.last_verification_message_id,
                text: "⏰ 验证已超时，请重新发送消息。",
                reply_markup: { inline_keyboard: [] } 
            }).catch(() => {});
            
            if (CTX) CTX.waitUntil(editPromise);
        }

        // B. 前端：引入SDK -> 初始化 -> 强制关闭 (加了双重保险)
        return new Response(
            `<!DOCTYPE html>
            <html>
            <head>
                <script src="https://telegram.org/js/telegram-web-app.js"></script>
            </head>
            <body style="background:transparent;">
                <script>
                    // 确保对象存在
                    var tg = window.Telegram.WebApp;
                    tg.ready();
                    
                    // 策略1: 立即关闭
                    tg.close();
                    
                    // 策略2: 延迟50ms再次关闭 (防止SDK未完全就绪)
                    setTimeout(function() { tg.close(); }, 50);
                    
                    // 策略3: 延迟200ms再次关闭 (最后一道保险)
                    setTimeout(function() { tg.close(); }, 200);
                </script>
            </body>
            </html>`, 
            { headers: { 'Content-Type': 'text/html' } }
        );
    }

    // 3. Token 有效：渲染验证页 (保持不变)
    const html = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="https://challenges.cloudflare.com/turnstile/v0/api.js?onload=onLoad" async></script>
<style>
body{display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background:#fff}
</style>
</head>
<body>
<div id="t"></div>
<script>
const tg=window.Telegram.WebApp;tg.ready();tg.expand();
const C='${chatId}',T='${token}',K='${turnstileSiteKey}';

function onLoad(){
    turnstile.render('#t',{
        sitekey: K,
        theme: 'light',
        callback: async function(token) {
            try {
                const req = await fetch('/verify_submit',{
                    method:'POST',
                    headers:{'Content-Type':'application/json'},
                    body:JSON.stringify({chat_id:C,token:T,turnstile_token:token})
                });
                const res = await req.json();
                
                if (res.success) {
                    tg.close();
                } else {
                    if (res.is_fatal) {
                        tg.close();
                    } else {
                        turnstile.reset();
                    }
                }
            } catch(e) {
                turnstile.reset();
            }
        }
    });
}
</script>
</body>
</html>`;

    return new Response(html, {
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
}

// 处理验证提交
async function handleVerifySubmit(env, request) {
    try {
        const body = await request.json();
        const { chat_id, token, turnstile_token } = body;

        if (!chat_id || !token || !turnstile_token) {
            return jsonResponse({ success: false, error: '参数不完整' });
        }

        // 1. 验证 Token 是否有效
        const userState = await getUserState(env.D1, chat_id);
        const nowSec = Math.floor(Date.now() / 1000);

        const isExpired = (userState.code_expiry && nowSec > userState.code_expiry);
        const isInvalidToken = (!userState.verification_code || userState.verification_code !== token);

        if (isInvalidToken || isExpired) {
            // A. 如果是过期的，执行惩罚逻辑
            if (isExpired) {
                // 计算惩罚时间 (翻倍机制)
                const attempts = (userState.verification_attempts || 0) + 1;
                const cooldownSeconds = Math.min(30 * Math.pow(2, attempts - 1), 300); // 最多5分钟
                const cooldownExpiry = nowSec + cooldownSeconds;

                // 更新数据库：清空验证码，设置冷却时间
                await DB.run(env.D1,
                    'UPDATE user_states SET verification_code = NULL, is_verifying = FALSE, code_expiry = ?, verification_attempts = ? WHERE chat_id = ?',
                    [cooldownExpiry, attempts, chat_id]
                );
                userStateCache.delete(chat_id); // 清缓存

                // B. 编辑旧消息，提示已过期
                if (userState.last_verification_message_id) {
                    try {
                        await telegramRequest('editMessageText', {
                            chat_id: chat_id,
                            message_id: userState.last_verification_message_id,
                            text: `⏰ <b>验证已超时</b>\n\n您未在规定时间内完成验证。请等待 ${cooldownSeconds} 秒后重新发送消息触发验证。`,
                            parse_mode: 'HTML'
                        });
                    } catch (e) { }
                }

                return jsonResponse({
                    success: false,
                    error: `验证已超时，请等待 ${cooldownSeconds} 秒`,
                    is_fatal: true // <--- 告诉前端强制退出
                });
            } else {
                return jsonResponse({
                    success: false,
                    error: '验证链接已失效',
                    is_fatal: true // <--- 告诉前端强制退出
                });
            }
        }

        // 2. 验证 Turnstile Token
        const turnstileSecret = env.TURNSTILE_SECRET_KEY || '1x0000000000000000000000000000000AA'; // 测试 secret
        const turnstileResult = await verifyTurnstile(turnstile_token, turnstileSecret);

        if (!turnstileResult.success) {
            return jsonResponse({ success: false, error: '人机验证失败' });
        }

        // 3. 验证成功，更新用户状态
        const verifiedExpiry = nowSec + (7 * 24 * 3600); // 7天有效

        await DB.run(env.D1,
            `UPDATE user_states SET is_verified = TRUE, verified_expiry = ?, verification_code = NULL,
             code_expiry = NULL, is_verifying = FALSE, is_first_verification = FALSE, verification_attempts = 0
             WHERE chat_id = ?`,
            [verifiedExpiry, chat_id]
        );

        // 清除缓存（因为 Workers 请求间缓存不共享，这里的 set 没意义，但删除可以确保下次从 DB 读取）
        userStateCache.delete(chat_id);

        // 重置消息速率
        await DB.run(env.D1, 'UPDATE message_rates SET message_count = 0 WHERE chat_id = ?', [chat_id]);
        messageRateCache.delete(chat_id);

        // 4. 发送验证成功消息（使用缓存的远程消息）
        const successMsg = await getVerificationSuccessMessage(env.D1);
        await sendMessageToUser(chat_id, successMsg, { disable_web_page_preview: true });

        // 5. 确保用户话题存在（后台执行，不阻塞响应）
        const info = await getUserInfo(chat_id);
        if (CTX) {
            CTX.waitUntil(ensureUserTopic(env.D1, chat_id, info));
        } else {
            await ensureUserTopic(env.D1, chat_id, info);
        }

        // 6. 删除验证消息
        if (userState.last_verification_message_id) {
            await deleteMessage(chat_id, userState.last_verification_message_id);
        }

        return jsonResponse({ success: true });

    } catch (error) {
        console.error('Verify submit error:', error);
        return jsonResponse({ success: false, error: '服务器错误' });
    }
}

// 验证 Turnstile Token
async function verifyTurnstile(token, secretKey) {
    try {
        const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                secret: secretKey,
                response: token
            })
        });
        return await response.json();
    } catch (error) {
        console.error('Turnstile verification error:', error);
        return { success: false };
    }
}

// JSON 响应辅助函数
function jsonResponse(data, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json' }
    });
}

// --- 4. Action Helpers ---
async function deleteMessage(chatId, messageId) {
    if (!chatId || !messageId) return;
    try {
        await telegramRequest('deleteMessage', { chat_id: chatId, message_id: messageId });
    } catch (e) { /* Ignore delete errors (msg might not exist) */ }
}

// --- 双向同步删除 (支持相册秒删) ---
async function handleSyncedDelete(d1, groupMsgId, commandMsgId, commandChatId = GROUP_ID) {
    // 1. 查询当前消息的映射信息
    const target = await DB.get(d1, 'SELECT private_chat_id, private_message_id, media_group_id FROM message_mappings WHERE group_message_id = ?', [groupMsgId]);

    // 使用 Set 自动去重
    const groupIds = new Set([parseInt(groupMsgId)]);
    const privateIds = new Set();
    let privateChatId = null;

    if (target) {
        privateChatId = target.private_chat_id;
        // 把当前这条的私聊ID加进去
        if (target.private_message_id) privateIds.add(parseInt(target.private_message_id));

        // 2. 关键点：如果是相册 (Media Group)，把同组的所有 ID 都查出来
        if (target.media_group_id) {
            const siblings = await DB.all(d1,
                'SELECT group_message_id, private_message_id FROM message_mappings WHERE media_group_id = ?',
                [target.media_group_id]
            );

            if (siblings && siblings.results) {
                for (const row of siblings.results) {
                    groupIds.add(parseInt(row.group_message_id));
                    privateIds.add(parseInt(row.private_message_id));
                }
            }
        }
    }

    // 转为数组供 API 使用
    const groupIdsArr = Array.from(groupIds);
    const privateIdsArr = Array.from(privateIds);

    // 3. 并行执行删除请求 (速度最快)
    const tasks = [];

    // A. 删群消息 (使用批量接口)
    if (groupIdsArr.length > 0) {
        // 复用之前写好的 deleteMessagesBatch，一次请求删多条
        tasks.push(deleteMessagesBatch(GROUP_ID, groupIdsArr));
    }

    // B. 删私聊消息 (使用批量接口)
    if (privateChatId && privateIdsArr.length > 0) {
        tasks.push(deleteMessagesBatch(privateChatId, privateIdsArr));
    }

    // C. 删指令消息 (例如用户的 /delete)
    if (commandMsgId) {
        tasks.push(deleteMessage(commandChatId, commandMsgId));
    }

    // 等待所有删除请求发送完毕
    await Promise.all(tasks);

    // 4. 一次性清理数据库映射
    if (groupIdsArr.length > 0) {
        const ph = groupIdsArr.map(() => '?').join(',');
        await DB.run(d1, `DELETE FROM message_mappings WHERE group_message_id IN (${ph})`, groupIdsArr);
    }
}

async function handleBatchDelete(d1, privateChatId, count, senderType) {
    // 1. 获取需要删除的消息记录 (包含 media_group_id)
    const rows = await DB.all(d1,
        'SELECT group_message_id, private_message_id, media_group_id FROM message_mappings WHERE private_chat_id = ? AND sender_type = ? ORDER BY created_at DESC LIMIT ?',
        [privateChatId, senderType, count]
    );

    if (!rows.results || rows.results.length === 0) return;

    // 2. 智能补全相册 (如果删到了相册的一部分，把剩下的也找出来)
    const messagesToDelete = new Map();
    const mediaGroupIds = new Set();

    for (const row of rows.results) {
        messagesToDelete.set(row.group_message_id, row);
        if (row.media_group_id) mediaGroupIds.add(row.media_group_id);
    }

    if (mediaGroupIds.size > 0) {
        const ids = Array.from(mediaGroupIds);
        const placeholders = ids.map(() => '?').join(',');
        const siblings = await DB.all(d1,
            `SELECT group_message_id, private_message_id FROM message_mappings WHERE media_group_id IN (${placeholders})`,
            ids
        );
        if (siblings.results) {
            for (const row of siblings.results) {
                messagesToDelete.set(row.group_message_id, row);
            }
        }
    }

    // 3. 分类收集 ID
    const groupMsgIds = [];
    const privateMsgIds = [];

    for (const msg of messagesToDelete.values()) {
        groupMsgIds.push(parseInt(msg.group_message_id));
        privateMsgIds.push(parseInt(msg.private_message_id));
    }

    // 4. 并行执行批量删除 (核心优化点)
    // 使用 Promise.all 让群组删除和私聊删除同时发生
    const tasks = [];

    if (groupMsgIds.length > 0) {
        tasks.push(deleteMessagesBatch(GROUP_ID, groupMsgIds));
    }

    // 注意：Bot 只能批量删除它自己发送的消息。
    // 如果 senderType 是 'user' (用户发给Bot的)，Bot 无法在私聊里删除用户的消息，这里会报错或忽略，
    // 但为了逻辑统一，我们还是尝试调用，Telegram 会自动忽略删不掉的消息。
    if (privateMsgIds.length > 0) {
        tasks.push(deleteMessagesBatch(privateChatId, privateMsgIds));
    }

    // 所有的网络请求同时发出去，速度最快
    await Promise.all(tasks);

    // 5. 批量清理数据库
    if (groupMsgIds.length > 0) {
        // 构建 DELETE IN (...) 语句
        const ph = groupMsgIds.map(() => '?').join(',');
        await DB.run(d1, `DELETE FROM message_mappings WHERE group_message_id IN (${ph})`, groupMsgIds);
    }
}

// --- 批量删除辅助函数 (优化速度核心) ---
async function deleteMessagesBatch(chatId, messageIds) {
    if (!messageIds || messageIds.length === 0) return;

    // Telegram API 限制每次最多删 100 条
    const chunkSize = 100;
    const promises = [];

    for (let i = 0; i < messageIds.length; i += chunkSize) {
        const chunk = messageIds.slice(i, i + chunkSize);
        // 并行发送请求，不用 await 阻塞循环
        promises.push(telegramRequest('deleteMessages', {
            chat_id: chatId,
            message_ids: chunk
        }));
    }

    // 等待所有批次请求完成
    await Promise.all(promises);
}

// --- 5. State & Settings Accessors  ---
async function getUserState(d1, chatId) {
    let s = userStateCache.get(chatId);
    if (!s) {
        s = await DB.get(d1, 'SELECT * FROM user_states WHERE chat_id = ?', [chatId]);
        if (!s) {
            s = { is_blocked: false, is_first_verification: true, is_verified: false, is_verifying: false };
            await DB.run(d1, 'INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified) VALUES (?, ?, ?, ?)', [chatId, false, true, false]);
        }
        userStateCache.set(chatId, s);
    }
    return s;
}

async function getSetting(d1, key) {
    if (settingsCache.has(key)) return settingsCache.get(key);
    const row = await DB.get(d1, 'SELECT value FROM settings WHERE key = ?', [key]);
    const val = row ? row.value : null;
    settingsCache.set(key, val);
    return val;
}

async function setSetting(d1, key, val) {
    await DB.run(d1, 'INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', [key, val]);
    settingsCache.set(key, val);
}

async function getPrivateChatId(d1, topicId) {
    // Cache check in TopicIdCache (Reverse lookup)
    for (const [cid, tid] of topicIdCache.cache) {
        if (tid.toString() === topicId.toString()) return cid;
    }
    const row = await DB.get(d1, 'SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?', [topicId]);
    return row ? row.chat_id : null;
}

async function checkIfAdmin(userId) {
    const res = await telegramRequest('getChatMember', { chat_id: GROUP_ID, user_id: userId });
    return res && res.ok && ['administrator', 'creator'].includes(res.result.status);
}

// --- 6. Webhook Management  ---
async function registerWebhook(request) {
    const webhookUrl = `${new URL(request.url).origin}/webhook`;
    const res = await telegramRequest('setWebhook', { url: webhookUrl });
    return new Response(res && res.ok ? 'Webhook set' : 'Failed', { status: 200 });
}

async function unRegisterWebhook() {
    const res = await telegramRequest('setWebhook', { url: '' });
    return new Response(res && res.ok ? 'Webhook removed' : 'Failed', { status: 200 });
}
