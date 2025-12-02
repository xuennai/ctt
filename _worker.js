/**
 * Part 1: Infrastructure, Config & Helpers
 */

// --- 1. Global Variables & Config ---
let BOT_TOKEN, GROUP_ID, MAX_MESSAGES_PER_MINUTE, CTX, WORKER_URL;
let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 86400000; // 24h
const VERIFY_TIMEOUT_SECONDS = 180; // 3min
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
        if (this.cache.size >= this.maxSize) this.cache.delete(this.cache.keys().next().value);
        this.cache.set(key, value);
    }
    delete(key) { return this.cache.delete(key); }
    clear() { this.cache.clear(); }
}

const userInfoCache = new LRUCache(1000);
const topicIdCache = new LRUCache(1000);
const userStateCache = new LRUCache(1000);
const messageRateCache = new LRUCache(1000);

// --- 3. Database Helper ---
const DB = {
    async get(d1, sql, params = []) { return await d1.prepare(sql).bind(...params).first(); },
    async run(d1, sql, params = []) { return await d1.prepare(sql).bind(...params).run(); },
    async all(d1, sql, params = []) { return await d1.prepare(sql).bind(...params).all(); },
    async exec(d1, sql) { return await d1.exec(sql); },
    async batch(d1, statements) { return await d1.batch(statements); }
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

            if (response.status === 429) {
                const retryAfter = parseInt(response.headers.get('Retry-After') || '5');
                await new Promise(r => setTimeout(r, retryAfter * 1000));
                continue;
            }
            if (response.status >= 400 && response.status < 500) {
                // Client errors should not be retried usually
                throw new Error(`Telegram API Error ${response.status}`);
            }
            throw new Error(`Server Error ${response.status}`);
        } catch (error) {
            if (i === retries - 1) throw error;
            await new Promise(r => setTimeout(r, 1000 * Math.pow(2, i)));
        }
    }
}

/**
 * Part 2: Core Logic - Entry & Group Handling
 */

export default {
    async fetch(request, env, ctx) {
        BOT_TOKEN = env.BOT_TOKEN_ENV;
        GROUP_ID = env.GROUP_ID_ENV;
        MAX_MESSAGES_PER_MINUTE = parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV || '40');
        CTX = ctx;
        if (!env.D1 || !BOT_TOKEN || !GROUP_ID) return new Response('Config Error', { status: 500 });
        if (!isInitialized) {
            await initialize(env.D1);
            isInitialized = true;
        }
        const url = new URL(request.url);
        if (!WORKER_URL) WORKER_URL = url.origin;
        if (url.pathname === '/webhook' && request.method === 'POST') {
            try {
                const update = await request.json();
                ctx.waitUntil(handleUpdate(env, update));
                return new Response('OK');
            } catch (e) { return new Response('Bad Request', { status: 400 }); }
        }
        if (url.pathname === '/verify_page') return renderVerifyPage(env, url.searchParams.get('chat_id'), url.searchParams.get('token'));
        if (url.pathname === '/verify_submit' && request.method === 'POST') return await handleVerifySubmit(env, request);
        switch (url.pathname) {
            case '/registerWebhook': return await registerWebhook(request);
            case '/unRegisterWebhook': return await unRegisterWebhook();
            case '/checkTables': await checkAndRepairTables(env.D1); return new Response('Tables checked', { status: 200 });
            default: return new Response('Not Found', { status: 404 });
        }
    }
};

async function initialize(d1) {
    await checkAndRepairTables(d1);
    await cleanExpiredVerificationCodes(d1);
}

async function handleUpdate(env, update) {
    if (update.message) {
        const key = `${update.message.chat.id}:${update.message.message_id}`;
        if (processedMessages.has(key)) return;
        processedMessages.add(key);
        if (processedMessages.size > 5000) processedMessages.clear();
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

    // --- Admin/Group Logic ---
    if (chatId === GROUP_ID) {
        const topicId = message.message_thread_id;
        if (!topicId) return;
        const privateChatId = await getPrivateChatId(env.D1, topicId);

        // Command: /delete
        if (/^\/delete(@\w+)?$/i.test(text)) {
            if (!privateChatId || await guardRateLimit(env.D1, GROUP_ID, topicId, 'general')) return;
            try { await deleteMessage(GROUP_ID, messageId); } catch (e) { }
            let targetGroupMsgId = null;
            if (message.reply_to_message && !message.reply_to_message.forum_topic_created) {
                targetGroupMsgId = message.reply_to_message.message_id.toString();
            } else {
                const lastAdminMsg = await DB.get(env.D1, 'SELECT group_message_id FROM message_mappings WHERE private_chat_id = ? AND sender_type = ? ORDER BY created_at DESC LIMIT 1', [privateChatId, 'admin']);
                if (lastAdminMsg) targetGroupMsgId = lastAdminMsg.group_message_id;
            }
            if (targetGroupMsgId) await handleSyncedDelete(env.D1, targetGroupMsgId, null);
            else await sendTempMessage(chatId, topicId, "⚠️ 未找到可删除的关联消息。");
            return;
        }

        // Command: /wipe
        if (privateChatId && /^\/wipe(@\w+)?(\s+\d+)?$/i.test(text)) {
            if (await guardRateLimit(env.D1, GROUP_ID, topicId, 'wipe')) return;
            const count = Math.min(Math.max(parseInt(text.split(/\s+/)[1] || '3'), 1), 50);
            await handleBatchDelete(env.D1, privateChatId, count, 'admin');
            try { await deleteMessage(chatId, messageId); } catch (e) { }
            await sendTempMessage(chatId, topicId, `🗑 已撤回最近 ${count} 条消息。`);
            return;
        }

        // Command: /admin
        if (privateChatId && /^\/admin(@\w+)?$/i.test(text)) {
            if (await guardRateLimit(env.D1, GROUP_ID, topicId, 'general', true)) return;
            await Promise.all([deleteMessage(chatId, messageId), sendAdminPanel(env, chatId, topicId, privateChatId, null, false)]);
            return;
        }

        // Reply Forwarding
        if (privateChatId) {
            const userState = await getUserState(env.D1, privateChatId);
            if (userState.is_blocked) return await sendTempMessage(chatId, topicId, "🚫 发送失败：该用户已被拉黑。");

            const verifyEnabled = (await getSetting(env.D1, 'verification_enabled')) === 'true';
            if (verifyEnabled && !userState.is_verified) return await sendTempMessage(chatId, topicId, "⏳ 发送失败：用户未通过验证。");

            if (message.media_group_id) {
                await handleUnifiedMediaGroup(env.D1, message, privateChatId, null, true);
            } else {
                const sentMsgId = await forwardMessageToPrivateChat(privateChatId, message);
                if (sentMsgId) await saveMessageMapping(env.D1, messageId.toString(), privateChatId, sentMsgId.toString(), 'admin');
            }
        }
        return;
    }

    userStateCache.delete(chatId); // Force refresh
    const userState = await getUserState(env.D1, chatId);
    if (userState.is_blocked) {
        await sendMessageToUser(chatId, "🚫 您已被拉黑，无法发送消息，请联系管理员。");
        return;
    }

    // --- Verification Logic ---
    const hasTurnstileKeys = env.TURNSTILE_SITE_KEY && env.TURNSTILE_SECRET_KEY;
    const verifyEnabled = hasTurnstileKeys && (await getSetting(env.D1, 'verification_enabled')) === 'true';
    if (verifyEnabled) {
        const now = Math.floor(Date.now() / 1000);
        const isVerifiedValid = userState.is_verified && (!userState.verified_expiry || now < userState.verified_expiry);
        if (!isVerifiedValid) {
            const isNewTokenFormat = userState.verification_code && userState.verification_code.includes('_');
            const hasOldData = (userState.verification_code && !isNewTokenFormat) || (userState.code_expiry && !userState.verification_code) || (userState.code_expiry && userState.code_expiry > now + 86400);
            if (hasOldData) {
                userState.verification_code = null; userState.code_expiry = null; userState.is_verifying = false;
                await DB.run(env.D1, 'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE chat_id = ?', [chatId]);
            }

            // Cooldown check
            if (userState.code_expiry && now < userState.code_expiry && !userState.verification_code) {
                const remaining = userState.code_expiry - now;
                if (remaining <= 300) {
                    await sendMessageToUser(chatId, `⏳ 请等待 ${remaining > 60 ? Math.ceil(remaining / 60) + ' 分钟' : remaining + ' 秒'} 后再试。`);
                    return;
                }
            }

            // Active verification check
            if (userState.verification_code && userState.is_verifying && isNewTokenFormat) {
                if (userState.code_expiry && now < userState.code_expiry) {
                    const remaining = userState.code_expiry - now;
                    await sendMessageToUser(chatId, `👆 请点击上方按钮完成验证（剩余 ${remaining > 60 ? Math.ceil(remaining / 60) + ' 分钟' : remaining + ' 秒'}）`);
                    return;
                }

                // Timeout penalty
                const attempts = (userState.verification_attempts || 0) + 1;
                const cooldown = Math.min(30 * Math.pow(2, attempts - 1), 300);
                await DB.run(env.D1, 'UPDATE user_states SET verification_code = NULL, is_verifying = FALSE, code_expiry = ?, verification_attempts = ? WHERE chat_id = ?', [now + cooldown, attempts, chatId]);
                await sendMessageToUser(chatId, `⏰ 验证超时！请等待 ${cooldown} 秒后重试。`);
                return;
            }

            const prompt = userState.is_first_verification ? "👋 初次对话请先完成人机验证，" : "⚠️ 验证过期或检测到异常，请重新验证，";
            await handleVerification(env.D1, chatId, null, prompt);
            return;
        }
    }

    // User Commands
    if (/^\/start(@\w+)?$/i.test(text)) {
        if (await guardRateLimit(env.D1, chatId, null, 'start')) return;
        await sendMessageToUser(chatId, `你好，欢迎使用私聊机器人！`, { disable_web_page_preview: true });
        await ensureUserTopic(env.D1, chatId, await getUserInfo(chatId));
        return;
    }

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

        if (targetGroupMsgId) await handleSyncedDelete(env.D1, targetGroupMsgId, messageId, chatId);
        else await deleteMessage(chatId, messageId);
        return;
    }

    if (/^\/wipe(@\w+)?(\s+\d+)?$/i.test(text)) {
        if (await guardRateLimit(env.D1, chatId, null, 'wipe')) return;
        const count = Math.min(Math.max(parseInt(text.split(/\s+/)[1] || '3'), 1), 50);
        await handleBatchDelete(env.D1, chatId, count, 'user');
        try { await deleteMessage(chatId, messageId); } catch (e) { }
        await sendTempMessage(chatId, null, `🗑 已撤回最近 ${count} 条消息。`);
        return;
    }

    // Forwarding
    const userInfo = await getUserInfo(chatId);
    let topicId;
    try { topicId = await ensureUserTopic(env.D1, chatId, userInfo); }
    catch (e) { await sendMessageToUser(chatId, "系统繁忙，无法创建话题。"); return; }
    if (await guardRateLimit(env.D1, chatId, null, 'forward')) return;

    if (message.media_group_id) await handleUnifiedMediaGroup(env.D1, message, GROUP_ID, topicId, false);
    else await forwardUserMessageWithRetry(env.D1, chatId, topicId, message, userInfo, messageId);
}

// Media Group Buffer
async function handleUnifiedMediaGroup(d1, message, targetChatId, targetThreadId, isCopyMode) {
    const groupId = message.media_group_id;
    const fromChatId = message.chat.id.toString();
    if (!mediaGroupCache.has(groupId)) {
        let resolveFunc;
        const promise = new Promise(resolve => { resolveFunc = resolve; });
        mediaGroupCache.set(groupId, {
            messages: [],
            timer: null,
            resolve: resolveFunc,
            promise: promise,
            targetChatId,
            targetThreadId,
            isCopyMode,
            fromChatId
        });
    }
    const groupData = mediaGroupCache.get(groupId);
    groupData.messages.push(message);
    if (groupData.timer) clearTimeout(groupData.timer);
    groupData.timer = setTimeout(async () => {
        const ctx = mediaGroupCache.get(groupId);
        if (!ctx) return;
        mediaGroupCache.delete(groupId);
        const msgs = ctx.messages.sort((a, b) => a.message_id - b.message_id);
        const msgIds = msgs.map(m => m.message_id);
        try {
            const method = ctx.isCopyMode ? 'copyMessages' : 'forwardMessages';
            const payload = {
                chat_id: ctx.targetChatId,
                from_chat_id: ctx.fromChatId,
                message_ids: msgIds
            };
            if (ctx.targetThreadId) payload.message_thread_id = ctx.targetThreadId;
            const res = await telegramRequest(method, payload);
            if (res && res.ok && Array.isArray(res.result)) {
                for (let i = 0; i < res.result.length; i++) {
                    const newMsg = res.result[i];
                    const oldMsg = msgs[i];
                    if (newMsg && oldMsg) {
                        const newMsgId = newMsg.message_id.toString();
                        const oldMsgId = oldMsg.message_id.toString();

                        if (ctx.isCopyMode) {
                            await saveMessageMapping(d1, oldMsgId, ctx.targetChatId, newMsgId, 'admin', groupId);
                        } else {
                            await saveMessageMapping(d1, newMsgId, ctx.fromChatId, oldMsgId, 'user', groupId);
                        }
                    }
                }
            }
        } catch (e) {
            console.error('UnifiedMediaGroup Error:', e);
        } finally {
            ctx.resolve();
        }
    }, 2000);
    await groupData.promise;
}

/**
 * Part 3: Callback & Admin Panel
 */

async function onCallbackQuery(env, query) {
    const data = query.data;
    const chatId = query.message.chat.id.toString();
    const messageId = query.message.message_id;
    const callbackId = query.id;
    if (processedCallbacks.has(callbackId)) return;
    processedCallbacks.add(callbackId);
    if (processedCallbacks.size > 2000) processedCallbacks.clear();
    
    const isAdmin = await checkIfAdmin(query.from.id.toString());
    if (!isAdmin) {
        return await telegramRequest('answerCallbackQuery', { callback_query_id: callbackId, text: '❌ 只有管理员可以使用此功能', show_alert: true });
    }

    let toastText = '', shouldRefreshPanel = true;
    let action = data, param = '';
    const prefixes = ['block_', 'unblock_', 'toggle_verification_', 'check_blocklist_', 'toggle_user_raw_', 'pre_del_keep_', 'pre_del_wipe_', 'del_keep_', 'del_wipe_', 'close_admin_panel_', 'back_admin_'];
    for (const prefix of prefixes) { if (data.startsWith(prefix)) { action = prefix.slice(0, -1); param = data.slice(prefix.length); break; } }
    try {
        switch (action) {
            case 'close_admin_panel':
                await deleteMessage(chatId, messageId);
                toastText = '面板已关闭'; shouldRefreshPanel = false; break;
            case 'block':
                await DB.run(env.D1, 'INSERT OR IGNORE INTO user_states (chat_id) VALUES (?)', [param]);
                await DB.run(env.D1, 'UPDATE user_states SET is_blocked = TRUE WHERE chat_id = ?', [param]);
                userStateCache.delete(param); toastText = `用户 ${param} 已拉黑`; break;
            case 'unblock':
                await DB.run(env.D1, 'UPDATE user_states SET is_blocked = FALSE, is_verified = FALSE, is_first_verification = TRUE WHERE chat_id = ?', [param]);
                userStateCache.delete(param); toastText = `用户 ${param} 已解除拉黑`; break;
            case 'toggle_verification':
                const vState = await getSetting(env.D1, 'verification_enabled');
                const vNew = vState === 'true' ? 'false' : 'true';
                await setSetting(env.D1, 'verification_enabled', vNew); toastText = `验证功能已${vNew === 'true' ? '开启' : '关闭'}`; break;
            case 'toggle_user_raw':
                const rState = await getSetting(env.D1, 'user_raw_enabled');
                const rNew = rState === 'true' ? 'false' : 'true';
                await setSetting(env.D1, 'user_raw_enabled', rNew); toastText = `Raw 链接已${rNew === 'true' ? '开启' : '关闭'}`; break;
            case 'check_blocklist':
                const blocks = await DB.all(env.D1, 'SELECT chat_id FROM user_states WHERE is_blocked = TRUE');
                const listText = blocks.results.length > 0 ? blocks.results.map(r => r.chat_id).join('\n') : '无';
                const msgOpts = { text: `🚫 黑名单列表：\n${listText}` };
                if (query.message.message_thread_id) await sendMessageToTopic(query.message.message_thread_id, msgOpts.text);
                else await telegramRequest('sendMessage', { chat_id: chatId, ...msgOpts });
                toastText = '查询完成'; break;
            case 'pre_del_keep':
            case 'pre_del_wipe':
                const isWipe = action === 'pre_del_wipe';
                const warning = isWipe
                    ? `⚠️ <b>危险操作</b>\n确定要 <b>彻底删除</b> 用户 <code>${param}</code> 吗？\n这将删除数据库记录并关闭 Topic。`
                    : `⚠️ <b>重置确认</b>\n确定要重置用户 <code>${param}</code> 的状态吗？\nTopic 将保留。`;
                await telegramRequest('editMessageText', {
                    chat_id: chatId, message_id: messageId, text: warning, parse_mode: 'HTML',
                    reply_markup: { inline_keyboard: [[{ text: '⚠️ 确认执行', callback_data: isWipe ? `del_wipe_${param}` : `del_keep_${param}` }, { text: '🔙 返回', callback_data: `back_admin_${param}` }]] }
                });
                shouldRefreshPanel = false; break;
            case 'del_keep':
            case 'del_wipe':
                try { await sendMessageToUser(param, "⚠️ 您的会话记录已被管理员重置/删除，如需继续聊天，请重新发送消息或输入 /start。"); } catch (e) { }
                await performUserDeletion(env, param, action === 'del_wipe');
                await deleteMessage(chatId, messageId);
                if (action === 'del_keep' && query.message.message_thread_id) await sendMessageToTopic(query.message.message_thread_id, `用户 ${param} 状态已重置。`);
                toastText = action === 'del_wipe' ? '用户已彻底删除' : '用户已重置'; shouldRefreshPanel = false; break;
            case 'back_admin': break;
            default: toastText = `未知操作: ${action}`;
        }
        if (shouldRefreshPanel) await sendAdminPanel(env, chatId, query.message.message_thread_id, param, messageId, true);
        await telegramRequest('answerCallbackQuery', { callback_query_id: callbackId, text: toastText, show_alert: false });
    } catch (e) {
        await telegramRequest('answerCallbackQuery', { callback_query_id: callbackId, text: '操作失败: ' + e.message, show_alert: true });
    }
}

async function sendAdminPanel(env, chatId, topicId, privateChatId, messageId, isEdit) {
    const d1 = env.D1;
    const [vEnabled, rEnabled] = await Promise.all([getSetting(d1, 'verification_enabled'), getSetting(d1, 'user_raw_enabled')]);
    const hasTurnstileKeys = env.TURNSTILE_SITE_KEY && env.TURNSTILE_SECRET_KEY;
    const vIcon = !hasTurnstileKeys ? '⚠️' : (vEnabled === 'true' ? '✅' : '🔴');
    const rIcon = rEnabled === 'true' ? '✅' : '🔴';
    const buttons = [
        [{ text: '🚫 拉黑用户', callback_data: `block_${privateChatId}` }, { text: '🟢 解除拉黑', callback_data: `unblock_${privateChatId}` }],
        [{ text: `${vIcon} 验证功能`, callback_data: `toggle_verification_${privateChatId}` }, { text: '📜 查询黑名单', callback_data: `check_blocklist_${privateChatId}` }],
        [{ text: `${rIcon} Raw 链接`, callback_data: `toggle_user_raw_${privateChatId}` }, { text: '🔗 GitHub', url: 'https://github.com/xuennai/ctt' }],
        [{ text: '🔄 重置用户', callback_data: `pre_del_keep_${privateChatId}` }, { text: '🔥 彻底删除', callback_data: `pre_del_wipe_${privateChatId}` }],
        [{ text: '❌ 关闭面板', callback_data: `close_admin_panel_${privateChatId}` }]
    ];
    let text = `🔧 <b>管理员控制台</b>${!hasTurnstileKeys ? '\n\n⚠️ <i>未配置 Turnstile 密钥，验证功能已禁用</i>' : ''}`;
    const payload = { chat_id: chatId, text: text, parse_mode: 'HTML', reply_markup: { inline_keyboard: buttons } };
    if (isEdit) {
        payload.message_id = messageId;
        try { await telegramRequest('editMessageText', payload); } catch (e) { }
    } else {
        payload.message_thread_id = topicId;
        await telegramRequest('sendMessage', payload);
        if (messageId) await deleteMessage(chatId, messageId);
    }
}

/**
 * Part 4: Topic Management & Forwarding
 */

// --- Topic Creation with Locking ---
async function ensureUserTopic(d1, chatId, userInfo) {
    let lock = topicCreationLocks.get(chatId);
    if (lock) { await lock; const cached = await getExistingTopicId(d1, chatId); if (cached) return cached; }

    const createLogic = async () => {
        try {
            let existing = await getExistingTopicId(d1, chatId);
            if (existing) return existing;

            const name = (userInfo.nickname || userInfo.username || `User ${chatId}`).substring(0, 127);
            const res = await telegramRequest('createForumTopic', { chat_id: GROUP_ID, name: name });

            if (!res.ok) throw new Error('Create topic failed');
            const topicId = res.result.message_thread_id;

            await sendTopicIntroMessage(topicId, userInfo, chatId);
            await DB.run(d1, 'INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)', [chatId, topicId]);
            topicIdCache.set(chatId, topicId);
            return topicId;
        } catch (e) { console.error(`Create topic error for ${chatId}:`, e); throw e; }
    };

    const newLock = createLogic();
    topicCreationLocks.set(chatId, newLock);
    try { return await newLock; }
    finally { if (topicCreationLocks.get(chatId) === newLock) topicCreationLocks.delete(chatId); }
}

async function sendTopicIntroMessage(topicId, userInfo, userId) {
    const time = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
    const notificationContent = await getNotificationContent();
    const text = `<b>🛡 用户信息卡片</b>\n昵称: ${userInfo.nickname}\n用户名: ${userInfo.username ? '@' + userInfo.username : '无'}\nID: <code>${userId}</code>\n时间: ${time}\n\n${notificationContent}`;
    let res = await sendMessageToTopic(topicId, text, { parse_mode: 'HTML' });
    if (!res || !res.ok) {
        // Fallback for HTML parse errors
        res = await sendMessageToTopic(topicId, text.replace(/<[^>]*>?/gm, ''));
    }
    if (res && res.result) {
        await telegramRequest('pinChatMessage', { chat_id: GROUP_ID, message_thread_id: topicId, message_id: res.result.message_id });
    }
}

// Notification Content Cache
let cachedNotification = null, cachedNotificationTime = 0;
const CACHE_TTL = 3600000;

async function getNotificationContent() {
    const now = Date.now();
    if (cachedNotification !== null && (now - cachedNotificationTime) < CACHE_TTL) return cachedNotification;
    try {
        const response = await fetch('https://raw.githubusercontent.com/xuennai/ctt/refs/heads/main/CFTeleTrans/notification.md');
        cachedNotification = response.ok ? (await response.text()).trim() : '';
    } catch (e) { cachedNotification = ''; }
    cachedNotificationTime = now;
    return cachedNotification;
}

// Robust Forwarding
async function forwardUserMessageWithRetry(d1, chatId, topicId, message, userInfo, originalMessageId) {
    try {
        await performForward(d1, chatId, topicId, message, originalMessageId);
    } catch (error) {
        const errStr = error.toString().toLowerCase();
        if (errStr.includes('thread not found') || errStr.includes('topic not found') || errStr.includes('thread is invalid')) {
            await DB.run(d1, 'DELETE FROM chat_topic_mappings WHERE chat_id = ?', [chatId]);
            topicIdCache.delete(chatId);
            const newTopicId = await ensureUserTopic(d1, chatId, userInfo);
            if (newTopicId) await performForward(d1, chatId, newTopicId, message, originalMessageId);
        } else { throw error; }
    }
}

async function performForward(d1, chatId, topicId, message, originalMessageId) {
    const res = await telegramRequest('forwardMessage', {
        chat_id: GROUP_ID, from_chat_id: chatId, message_id: message.message_id, message_thread_id: topicId
    });
    if (res.ok && res.result) {
        await saveMessageMapping(d1, res.result.message_id.toString(), chatId, originalMessageId.toString(), 'user');
    }
}

async function forwardMessageToPrivateChat(privateChatId, message) {
    const res = await telegramRequest('copyMessage', { chat_id: privateChatId, from_chat_id: message.chat.id, message_id: message.message_id });
    return (res.ok && res.result) ? res.result.message_id : null;
}

/**
 * Part 5: Verification Mini App
 */

async function handleVerification(d1, chatId, messageIdToEdit = null, prefixText = '') {
    if (!WORKER_URL) {
        await sendMessageToUser(chatId, `${prefixText}⚠️ 系统配置错误，请稍后重试。`);
        return;
    }
    const timestamp36 = Date.now().toString(36); 
    const token = `${chatId}_${timestamp36}_${Math.random().toString(36).substring(2, 10)}`;
    const tokenExpiry = Math.floor(Date.now() / 1000) + VERIFY_TIMEOUT_SECONDS;
    let userState = await getUserState(d1, chatId);
    userState.verification_code = token; userState.code_expiry = tokenExpiry; userState.is_verifying = true;
    userStateCache.set(chatId, userState);
    const dbPromise = DB.run(d1, 'UPDATE user_states SET verification_code = ?, code_expiry = ?, is_verifying = TRUE WHERE chat_id = ?', [token, tokenExpiry, chatId]);
    if (CTX) CTX.waitUntil(dbPromise);
    const verifyUrl = `${WORKER_URL}/verify_page?chat_id=${chatId}&token=${encodeURIComponent(token)}`;
    const payload = {
        chat_id: chatId,
        text: `${prefixText}请在 3 分钟内点击下方按钮完成人机验证`,
        reply_markup: { inline_keyboard: [[{ text: '点击验证', web_app: { url: verifyUrl } }]] }
    };
    let res;
    try {
        res = messageIdToEdit ? await telegramRequest('editMessageText', { ...payload, message_id: messageIdToEdit }) : await telegramRequest('sendMessage', payload);
    } catch (e) {
        res = await sendMessageToUser(chatId, `${prefixText}🛡️ 请点击以下链接完成人机验证：\n${verifyUrl}`);
    }
    if (res && res.ok && res.result && !messageIdToEdit) {
        if (CTX) CTX.waitUntil(DB.run(d1, 'UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?', [res.result.message_id.toString(), chatId]));
    }
}

function renderVerifyPage(env, chatId, token) {
    const siteKey = env.TURNSTILE_SITE_KEY || '1x00000000000000000000AA';
    const timeoutMs = VERIFY_TIMEOUT_SECONDS * 1000;
    const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no"><title>验证</title><script src="https://telegram.org/js/telegram-web-app.js"></script><script src="https://challenges.cloudflare.com/turnstile/v0/api.js?onload=onLoad" async></script><style>body{margin:0;padding:0;height:100vh;display:flex;justify-content:center;align-items:center;font-family:-apple-system,BlinkMacSystemFont,sans-serif;background-color:var(--tg-theme-bg-color,#f0f2f5);color:var(--tg-theme-text-color,#000);overflow:hidden}.c{background-color:var(--tg-theme-secondary-bg-color,#fff);padding:24px;border-radius:16px;box-shadow:0 4px 12px rgba(0,0,0,0.1);text-align:center;width:90%;max-width:360px;box-sizing:border-box}.i{font-size:48px;margin-bottom:12px}h3{margin:0 0 8px;font-size:20px;font-weight:600}.d{margin-bottom:20px;opacity:0.7;font-size:14px}#s{margin-top:16px;font-size:14px;min-height:20px;font-weight:500}.l{color:var(--tg-theme-button-color,#3390ec)}.ok{color:#34c759}.er{color:#ff3b30}</style></head><body><div class="c"><div class="i">🛡️</div><h3>安全验证</h3><div class="d">请完成验证以继续</div><div id="t" style="display:flex;justify-content:center"></div><div id="s"></div></div><script>const tg=window.Telegram.WebApp;tg.ready();tg.expand();const C='${chatId}',T='${token}',K='${siteKey}';let isExpired=false;try{let p=T.split('_');if(p.length>1){const ts=parseInt(p[1],36);if(Date.now()-ts>${timeoutMs}){isExpired=true}}}catch(e){}if(isExpired){document.querySelector('.c').innerHTML='<div class="i">⏰</div><h3>验证已过期</h3><div class="d">请关闭此页面，重新发送消息获取新的验证链接</div>';fetch('/verify_submit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({chat_id:C,token:T,expired_check:true})});setTimeout(()=>tg.close(),3000)}else{function onLoad(){turnstile.render('#t',{sitekey:K,callback:V,theme:'auto'})}}async function V(t){const s=document.getElementById('s');s.className='l';s.textContent='退出中...';try{const r=await fetch('/verify_submit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({chat_id:C,token:T,turnstile_token:t})});const d=await r.json();if(d.success){s.className='ok';s.textContent='退出成功';setTimeout(()=>tg.close(),500)}else{if(d.expired){document.querySelector('.c').innerHTML='<div class="i">⏰</div><h3>验证已过期</h3><div class="d">请关闭此页面，重新发送消息获取新的验证链接</div>';setTimeout(()=>tg.close(),2000);return}s.className='er';s.textContent='❌ '+(d.error||'失败');turnstile.reset()}}catch(e){s.className='er';s.textContent='❌ 网络错误';turnstile.reset()}}</script></body></html>`;
    return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
}

async function handleVerifySubmit(env, request) {
    try {
        const body = await request.json();
        const { chat_id, token, turnstile_token, expired_check } = body;
        if (!chat_id || !token) return jsonResponse({ success: false, error: '参数不完整' });

        const nowSec = Math.floor(Date.now() / 1000);
        let tokenTs = 0;
        try {
            const parts = token.split('_');
            if (parts.length >= 2) tokenTs = parseInt(parts[1], 36) / 1000;
        } catch (e) {}
        
        if (tokenTs > 0 && (nowSec - tokenTs > VERIFY_TIMEOUT_SECONDS)) {
             return handleExpired(env, chat_id);
        }
        const userState = await getUserState(env.D1, chat_id);
        const isExpiredByDB = userState.code_expiry && nowSec > userState.code_expiry;
        if (isExpiredByDB || expired_check === true) {
            return handleExpired(env, chat_id, userState);
        }
        if (!userState.verification_code || userState.verification_code !== token) {
            return jsonResponse({ success: false, error: '验证链接已失效' });
        }
        if (!turnstile_token) return jsonResponse({ success: false, error: '参数不完整' });

        // --- Turnstile ---
        const tsRes = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
            method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ secret: env.TURNSTILE_SECRET_KEY || '1x0000000000000000000000000000000AA', response: turnstile_token })
        });
        const tsData = await tsRes.json();
        if (!tsData.success) return jsonResponse({ success: false, error: '人机验证失败' });

        const expiryDay = 180; 
        const batch = [
            env.D1.prepare(`UPDATE user_states SET is_verified = TRUE, verified_expiry = ?, verification_code = NULL, code_expiry = NULL, is_verifying = FALSE, is_first_verification = FALSE, verification_attempts = 0 WHERE chat_id = ?`).bind(nowSec + expiryDay * 86400, chat_id),
            env.D1.prepare('UPDATE message_rates SET message_count = 0 WHERE chat_id = ?').bind(chat_id)
        ];
        await env.D1.batch(batch);

        userStateCache.delete(chat_id);
        messageRateCache.delete(chat_id);
        if (CTX) {
            CTX.waitUntil((async () => {
                try {
                    await sendMessageToUser(chat_id, await getVerificationSuccessMessage(env.D1), { disable_web_page_preview: true });
                } catch (e) {
                    console.error(`验证成功消息发送失败 (ChatID: ${chat_id}):`, e);
                    // ignore
                }

                try {
                    let verifyMsgId = userState.last_verification_message_id;
                    if (!verifyMsgId) {
                         const dbRes = await DB.get(env.D1, 'SELECT last_verification_message_id FROM user_states WHERE chat_id = ?', [chat_id]);
                         if (dbRes) verifyMsgId = dbRes.last_verification_message_id;
                    }
                    if (verifyMsgId) await deleteMessage(chat_id, verifyMsgId);
                } catch (e) {}

                try {
                    await ensureUserTopic(env.D1, chat_id, await getUserInfo(chat_id));
                } catch (e) {}
            })());
        }
        return jsonResponse({ success: true });
    } catch (error) { 
        return jsonResponse({ success: false, error: '服务器错误' }); 
    }
}

async function handleExpired(env, chat_id, userState = null) {
    const attempts = userState ? (userState.verification_attempts || 0) + 1 : 1;
    const cooldown = Math.min(30 * Math.pow(2, attempts - 1), 300);
    const nowSec = Math.floor(Date.now() / 1000);
    
    await DB.run(env.D1, 'UPDATE user_states SET verification_code = NULL, is_verifying = FALSE, code_expiry = ?, verification_attempts = ? WHERE chat_id = ?', [nowSec + cooldown, attempts, chat_id]);
    userStateCache.delete(chat_id);

    if (CTX) {
        CTX.waitUntil((async () => {
             let msgId = userState ? userState.last_verification_message_id : null;
             if (!msgId) {
                 const dbRes = await DB.get(env.D1, 'SELECT last_verification_message_id FROM user_states WHERE chat_id = ?', [chat_id]);
                 if (dbRes) msgId = dbRes.last_verification_message_id;
             }
             if (msgId) {
                 try {
                     await telegramRequest('editMessageText', {
                        chat_id: chat_id, message_id: msgId,
                        text: `⏰ <b>验证已过期</b>\n请重新发送任意消息获取新的验证链接。`,
                        parse_mode: 'HTML', reply_markup: { inline_keyboard: [] }
                    });
                 } catch(e) {}
             }
        })());
    }
    
    return jsonResponse({ success: false, expired: true, error: '验证已超时' });
}

let cachedStartMsg = null, cachedStartMsgTime = 0;
async function getVerificationSuccessMessage(d1) {
    if ((await getSetting(d1, 'user_raw_enabled')) !== 'true') return '✅ 验证成功！';
    const now = Date.now();
    if (cachedStartMsg && (now - cachedStartMsgTime) < CACHE_TTL) return cachedStartMsg;
    try { const res = await fetch('https://raw.githubusercontent.com/xuennai/ctt/refs/heads/main/CFTeleTrans/start.md'); if (res.ok) { cachedStartMsg = await res.text(); cachedStartMsgTime = now; return cachedStartMsg; } } catch (e) { }
    return '✅ 验证成功！您现在可以发送消息了。';
}
function jsonResponse(data, status = 200) { return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json' } }); }

/**
 * Part 6: Database Maintenance & Utils
 */
async function checkAndRepairTables(d1) {
    const tables = {
        user_states: "chat_id TEXT PRIMARY KEY, is_blocked BOOLEAN DEFAULT FALSE, is_verified BOOLEAN DEFAULT FALSE, verified_expiry INTEGER, verification_code TEXT, code_expiry INTEGER, last_verification_message_id TEXT, is_first_verification BOOLEAN DEFAULT TRUE, is_rate_limited BOOLEAN DEFAULT FALSE, is_verifying BOOLEAN DEFAULT FALSE, verification_attempts INTEGER DEFAULT 0",
        message_rates: "chat_id TEXT PRIMARY KEY, message_count INTEGER DEFAULT 0, window_start INTEGER, start_count INTEGER DEFAULT 0, start_window_start INTEGER, cmd_count INTEGER DEFAULT 0, cmd_window_start INTEGER, wipe_count INTEGER DEFAULT 0, wipe_window_start INTEGER",
        chat_topic_mappings: "chat_id TEXT PRIMARY KEY, topic_id TEXT NOT NULL",
        settings: "key TEXT PRIMARY KEY, value TEXT",
        message_mappings: "group_message_id TEXT PRIMARY KEY, private_chat_id TEXT NOT NULL, private_message_id TEXT NOT NULL, created_at INTEGER, sender_type TEXT DEFAULT 'user', media_group_id TEXT"
    };
    for (const [name, schema] of Object.entries(tables)) await DB.exec(d1, `CREATE TABLE IF NOT EXISTS ${name} (${schema})`);
    const alters = [
        "ALTER TABLE message_rates ADD COLUMN wipe_count INTEGER DEFAULT 0", "ALTER TABLE message_rates ADD COLUMN wipe_window_start INTEGER",
        "ALTER TABLE message_rates ADD COLUMN cmd_count INTEGER DEFAULT 0", "ALTER TABLE message_rates ADD COLUMN cmd_window_start INTEGER",
        "ALTER TABLE message_mappings ADD COLUMN sender_type TEXT DEFAULT 'user'", "ALTER TABLE message_mappings ADD COLUMN media_group_id TEXT"
    ];
    for (const sql of alters) try { await DB.exec(d1, sql); } catch (e) { }
    await DB.exec(d1, 'CREATE INDEX IF NOT EXISTS idx_mappings_private ON message_mappings (private_chat_id, private_message_id)');
    await DB.run(d1, "INSERT OR IGNORE INTO settings (key, value) VALUES ('verification_enabled', 'true')");
    await DB.run(d1, "INSERT OR IGNORE INTO settings (key, value) VALUES ('user_raw_enabled', 'true')");
    const all = await DB.all(d1, 'SELECT key, value FROM settings');
    if (all.results) for (const row of all.results) settingsCache.set(row.key, row.value);
}

async function cleanExpiredVerificationCodes(d1) {
    const now = Date.now();
    if (now - lastCleanupTime < CLEANUP_INTERVAL) return;
    const nowSec = Math.floor(now / 1000);
    await DB.run(d1, 'UPDATE user_states SET verification_code = NULL, code_expiry = NULL, is_verifying = FALSE WHERE code_expiry IS NOT NULL AND code_expiry < ?', [nowSec]);
    await DB.run(d1, 'DELETE FROM message_mappings WHERE created_at < ?', [nowSec - 172800]);
    lastCleanupTime = now;
}

// --- Message Editing & Deleting ---
async function onEditedMessage(env, message) {
    const chatId = message.chat.id.toString(), messageId = message.message_id.toString();
    const newContent = message.text || message.caption || "";

    if (chatId === GROUP_ID) {
        const mapping = await DB.get(env.D1, 'SELECT private_chat_id, private_message_id FROM message_mappings WHERE group_message_id = ?', [messageId]);
        if (mapping) await editRemoteMessage(mapping.private_chat_id, mapping.private_message_id, newContent, message);
    } else {
        const mapping = await DB.get(env.D1, 'SELECT group_message_id, sender_type FROM message_mappings WHERE private_chat_id = ? AND private_message_id = ?', [chatId, messageId]);
        if (mapping) {
            if (mapping.sender_type === 'user') {
                const topicId = await getExistingTopicId(env.D1, chatId);
                if (topicId) {
                    await deleteMessage(GROUP_ID, mapping.group_message_id);
                    await DB.run(env.D1, 'DELETE FROM message_mappings WHERE group_message_id = ?', [mapping.group_message_id]);
                    await performForward(env.D1, chatId, topicId, message, messageId);
                }
            } else {
                await editRemoteMessage(GROUP_ID, mapping.group_message_id, newContent, message);
            }
        }
    }
}

async function editRemoteMessage(chatId, msgId, text, msg) {
    try {
        const media = getMediaInput(msg, text);
        media ? await telegramRequest('editMessageMedia', { chat_id: chatId, message_id: msgId, media })
            : await telegramRequest('editMessageText', { chat_id: chatId, message_id: msgId, text });
    } catch (e) { }
}

function getMediaInput(msg, caption) {
    if (msg.photo) return { type: 'photo', media: msg.photo[msg.photo.length - 1].file_id, caption };
    if (msg.document) return { type: 'document', media: msg.document.file_id, caption };
    if (msg.video) return { type: 'video', media: msg.video.file_id, caption };
    if (msg.audio) return { type: 'audio', media: msg.audio.file_id, caption };
    return null;
}

async function handleSyncedDelete(d1, groupMsgId, commandMsgId, commandChatId = GROUP_ID) {
    const target = await DB.get(d1, 'SELECT private_chat_id, private_message_id, media_group_id FROM message_mappings WHERE group_message_id = ?', [groupMsgId]); 
    const groupIds = new Set([groupMsgId.toString()]);
    const privateIds = new Set();
    let privateChatId = target ? target.private_chat_id : null;
    if (target) {
        if (target.private_message_id) {
            privateIds.add(target.private_message_id.toString());
        }
        if (target.media_group_id) {
            const siblings = await DB.all(d1, 'SELECT group_message_id, private_message_id FROM message_mappings WHERE media_group_id = ?', [target.media_group_id]);
            if (siblings.results) {
                for (const row of siblings.results) {
                    groupIds.add(row.group_message_id.toString());
                    if (row.private_message_id) {
                        privateIds.add(row.private_message_id.toString());
                    }
                }
            }
        }
    }
    const tasks = [deleteMessagesBatch(GROUP_ID, Array.from(groupIds))];
    if (privateChatId && privateIds.size > 0) { tasks.push(deleteMessagesBatch(privateChatId, Array.from(privateIds))); } 
    if (commandMsgId) { tasks.push(deleteMessage(commandChatId, commandMsgId)); }
    await Promise.all(tasks);
    if (groupIds.size > 0) {
        const ids = Array.from(groupIds);
        const placeholders = ids.map(() => '?').join(',');
        await DB.run(d1, `DELETE FROM message_mappings WHERE group_message_id IN (${placeholders})`, ids);
    }
}

async function handleBatchDelete(d1, privateChatId, count, senderType) {
    const rows = await DB.all(d1, 'SELECT group_message_id, private_message_id, media_group_id FROM message_mappings WHERE private_chat_id = ? AND sender_type = ? ORDER BY created_at DESC LIMIT ?', [privateChatId, senderType, count]);
    if (!rows.results || !rows.results.length) return;

    const groupIds = new Set();
    const privateIds = new Set();  
    const mediaGroups = new Set();
    rows.results.forEach(r => {
        if (r.media_group_id) mediaGroups.add(r.media_group_id);
    });
    if (mediaGroups.size > 0) {
        const ph = Array.from(mediaGroups).map(() => '?').join(',');
        const siblings = await DB.all(d1, `SELECT group_message_id, private_message_id FROM message_mappings WHERE media_group_id IN (${ph})`, Array.from(mediaGroups));
        
        if (siblings.results) {
            siblings.results.forEach(r => {
                groupIds.add(r.group_message_id.toString());
                if (r.private_message_id) privateIds.add(r.private_message_id.toString());
            });
        }
    }
    rows.results.forEach(r => {
        groupIds.add(r.group_message_id.toString());
        if (r.private_message_id) privateIds.add(r.private_message_id.toString());
    });
    await Promise.all([
        deleteMessagesBatch(GROUP_ID, Array.from(groupIds)),
        deleteMessagesBatch(privateChatId, Array.from(privateIds))
    ]);
    if (groupIds.size > 0) {
        const ids = Array.from(groupIds);
        const placeholders = ids.map(() => '?').join(',');
        await DB.run(d1, `DELETE FROM message_mappings WHERE group_message_id IN (${placeholders})`, ids);
    }
}

async function deleteMessagesBatch(chatId, messageIds) {
    if (!messageIds.length) return;
    const promises = [];
    for (let i = 0; i < messageIds.length; i += 100) {
        promises.push(telegramRequest('deleteMessages', { chat_id: chatId, message_ids: messageIds.slice(i, i + 100) }));
    }
    await Promise.all(promises);
}

// --- 5. State & Settings Accessors (Expanded for Readability) ---
async function deleteMessage(chatId, messageId) {
    if (!chatId || !messageId) return;
    try {
        await telegramRequest('deleteMessage', { chat_id: chatId, message_id: messageId });
    } catch (e) {
        // ignore errors
    }
}

async function saveMessageMapping(d1, groupMsgId, privateChatId, privateMsgId, senderType, mediaGroupId = null) {
    const now = Math.floor(Date.now() / 1000);
    await DB.run(d1, 
        'INSERT OR REPLACE INTO message_mappings (group_message_id, private_chat_id, private_message_id, created_at, sender_type, media_group_id) VALUES (?, ?, ?, ?, ?, ?)', 
        [groupMsgId, privateChatId, privateMsgId, now, senderType, mediaGroupId]
    );
}

async function performUserDeletion(env, chatId, isWipe) {
    userStateCache.delete(chatId);
    messageRateCache.delete(chatId);
    topicIdCache.delete(chatId);
    await DB.batch(env.D1, [
        env.D1.prepare('DELETE FROM user_states WHERE chat_id = ?').bind(chatId),
        env.D1.prepare('DELETE FROM message_rates WHERE chat_id = ?').bind(chatId),
        env.D1.prepare('DELETE FROM message_mappings WHERE private_chat_id = ?').bind(chatId)
    ]);
    if (isWipe) {
        const topicId = await getExistingTopicId(env.D1, chatId);
        if (topicId) {
            await DB.run(env.D1, 'DELETE FROM chat_topic_mappings WHERE chat_id = ?', [chatId]);
            await telegramRequest('deleteForumTopic', { chat_id: GROUP_ID, message_thread_id: topicId });
        }
    }
}

async function getUserState(d1, chatId) {
    let s = userStateCache.get(chatId);
    if (!s) {
        s = await DB.get(d1, 'SELECT * FROM user_states WHERE chat_id = ?', [chatId]);
    }
    if (!s) {
        s = { 
            is_blocked: false, 
            is_first_verification: true, 
            is_verified: false, 
            is_verifying: false 
        };
        await DB.run(d1, 'INSERT INTO user_states (chat_id, is_blocked, is_first_verification, is_verified) VALUES (?, ?, ?, ?)', [chatId, false, true, false]);
    }
    userStateCache.set(chatId, s);
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

// Helper function: Get ChatID by TopicID
async function getPrivateChatId(d1, topicId) {
    for (const [cid, tid] of topicIdCache.cache) {
        if (tid.toString() === topicId.toString()) return cid;
    }
    const row = await DB.get(d1, 'SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?', [topicId]);
    return row ? row.chat_id : null;
}

async function getExistingTopicId(d1, chatId) {
    const cached = topicIdCache.get(chatId);
    if (cached) return cached;

    const row = await DB.get(d1, 'SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?', [chatId]);
    if (row) topicIdCache.set(chatId, row.topic_id);
    return row ? row.topic_id : null;
}

async function getUserInfo(chatId) {
    let info = userInfoCache.get(chatId);
    if (info) return info;

    const res = await telegramRequest('getChat', { chat_id: chatId });
    if (res.ok && res.result) {
        const r = res.result;
        const name = [r.first_name, r.last_name].filter(Boolean).join(' ');
        info = {
            id: r.id,
            username: r.username,
            nickname: name || r.username || `User ${chatId}`
        };
    } else { info = { id: chatId, username: null, nickname: `User ${chatId}` }; }
    userInfoCache.set(chatId, info);
    return info;
}

async function checkIfAdmin(userId) {
    const res = await telegramRequest('getChatMember', { chat_id: GROUP_ID, user_id: userId });
    return res && res.ok && ['administrator', 'creator'].includes(res.result.status);
}

// --- 6. Rate Limiter ---
async function guardRateLimit(d1, chatId, topicId, type, silent = false) {
    const configMap = {
        'start': { 
            max: 2, 
            win: 300000, // 5min
            cols: ['start_count', 'start_window_start'], 
            msg: '⏳ /start 频率过高，请稍等5分钟再试。' 
        },
        'wipe': { 
            max: 2, 
            win: 60000, // 1min
            cols: ['wipe_count', 'wipe_window_start'], 
            msg: '⏳ Wipe 操作过于频繁，请休息1分钟。' 
        },
        'general': { 
            max: 15, 
            win: 60000, // 1min
            cols: ['cmd_count', 'cmd_window_start'], 
            msg: '⏳ 操作太快，请1分钟后再试。' 
        },
        'forward': {
            max: MAX_MESSAGES_PER_MINUTE, // 使用配置的每分钟最大值 (例如 40)
            win: 60000, // 1min
            cols: ['message_count', 'window_start'], // 使用 message_rates 表的默认字段
            msg: `⏳ 消息发送过于频繁，请等待 1 分钟后再试（限制 ${MAX_MESSAGES_PER_MINUTE} 条/分钟）。`
        }
    };
    const cfg = configMap[type];
    if (!cfg) return false;

    const now = Date.now();
    const [colCount, colStart] = cfg.cols;
    let data = await DB.get(d1, `SELECT ${colCount} as c, ${colStart} as s FROM message_rates WHERE chat_id = ?`, [chatId]);
    if (!data) {
        await DB.run(d1, 'INSERT OR IGNORE INTO message_rates (chat_id) VALUES (?)', [chatId]);
        data = { c: 0, s: now };
    }
    let count = data.c || 0;
    let start = data.s || now;
    if (now - start > cfg.win) {
        count = 1;
        start = now;
    } else {
        count++;
    }
    const updatePromise = DB.run(d1, `UPDATE message_rates SET ${colCount} = ?, ${colStart} = ? WHERE chat_id = ?`, [count, start, chatId]);
    if (CTX) CTX.waitUntil(updatePromise);

    if (count > cfg.max) {
        if (!silent && cfg.msg) {
            await sendTempMessage(chatId, topicId, cfg.msg);
        }
        return true;
    }
    return false;
}

// --- 7. Messaging Wrappers ---
async function sendMessageToTopic(topicId, text, opts = {}) {
    return await telegramRequest('sendMessage', { 
        chat_id: GROUP_ID, 
        message_thread_id: topicId, 
        text, 
        ...opts 
    });
}

async function sendMessageToUser(chatId, text, opts = {}) {
    return await telegramRequest('sendMessage', { 
        chat_id: chatId, 
        text, 
        ...opts 
    });
}

// Send temporary message (auto-delete after 5 seconds)
async function sendTempMessage(chatId, topicId, text) {
    const res = topicId 
        ? await sendMessageToTopic(topicId, text) 
        : await sendMessageToUser(chatId, text);
    if (res && res.result && CTX) {
        CTX.waitUntil(new Promise(resolve => {
            setTimeout(async () => {
                await deleteMessage(topicId ? GROUP_ID : chatId, res.result.message_id);
                resolve();
            }, 5000);
        }));
    }
}

// --- 8. Webhook Utilities ---
async function registerWebhook(request) {
    const url = new URL(request.url);
    const webhookUrl = `${url.origin}/webhook`;
    const res = await telegramRequest('setWebhook', { url: webhookUrl });
    return new Response(res.ok ? 'Webhook set' : 'Failed', { status: 200 });
}

async function unRegisterWebhook() {
    const res = await telegramRequest('setWebhook', { url: '' });
    return new Response(res.ok ? 'Webhook removed' : 'Failed', { status: 200 });
}