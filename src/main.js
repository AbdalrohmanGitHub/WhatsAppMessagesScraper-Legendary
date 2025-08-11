/*
  WhatsApp Web real-time message scraper (clone-style of extremescrapes/whatsapp-messages-scraper)
  - QR login in terminal
  - Listens for new messages (groups and private)
  - Flexible filters (groups/contacts/message types/fromMe)
  - Optional media handling (skip | base64 | key-value store)
  - Outputs to Apify Dataset and optional webhook
  - Basic reconnection and rate limiting
*/

'use strict';

const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const zlib = require('zlib');
const qrcodeTerminal = require('qrcode-terminal');
const Bottleneck = require('bottleneck');
const pino = require('pino');
const { Actor } = require('apify');
const { Client, LocalAuth } = require('whatsapp-web.js');
const QRCode = require('qrcode');
const { FingerprintGenerator } = require('fingerprint-generator');
const { FingerprintInjector } = require('fingerprint-injector');
const { createCursor } = require('ghost-cursor');
const { Kafka } = require('kafkajs');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
require('dotenv').config();

/** Logger setup */
const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

/** Default configuration */
const DEFAULTS = {
  headless: true,
  sessionDir: '.wwebjs_auth',
  sessionId: 'default',
  rateLimitPerSecond: 10,
  maxConcurrency: 2,
  listenChats: 'all', // 'all' | 'groups' | 'private'
  includeGroups: [],
  excludeGroups: [],
  includeContacts: [],
  excludeContacts: [],
    includeChatIds: [],
    excludeChatIds: [],
  includeMessageTypes: [], // empty means all
    includeTextPatterns: [],
    excludeTextPatterns: [],
  includeFromMe: true,
  mediaHandling: 'skip', // 'skip' | 'base64' | 'kv-store' | 's3'
  maxMediaSizeMB: 25,
  inlineQuotedMessage: true,
  webhookUrl: '',
  webhookHeaders: {},
    webhooks: [], // [{ url, headers, maxRetries, baseBackoffMs }]
    webhookMaxRetries: 3,
    webhookBaseBackoffMs: 1000,
    webhookSignature: { enabled: false, algorithm: 'sha256', secret: '', headerName: 'X-Signature' },
    outputMode: 'rich', // 'rich' | 'extremescrapes'
    webhookPayloadMode: 'match', // 'match' | 'rich' | 'extremescrapes'
    includeReactions: true,
    includeRevocations: true,
    includeAcks: false,
    includeGroupEvents: false,
    includeEdits: false,
    includeRevocationsMe: false,
    includeProfilePicUrl: false,
    redact: { maskPhoneNumbers: false, maskNames: false, strategy: 'mask' },
    datasetName: '',
    storeQrToKv: false,
    persistDedup: false,
    dedupMaxIds: 10000,
    dedupFlushSecs: 30,
    targetMode: 'all', // 'all' | 'selected'
    selectedChats: [], // flexible: ["id:1203...", "name:Team", "Team Group"]
    humanMode: {
      enabled: false,
      minIntervalMs: 45000,
      maxIntervalMs: 180000,
      scrollPixelsMin: 200,
      scrollPixelsMax: 1200,
    },
    stealth: {
      enabled: true,
      emulateTimezone: '', // e.g., 'Europe/Berlin'
      languages: [], // e.g., ['en-US', 'en']
      platform: '', // e.g., 'MacIntel'
      userAgent: '', // override UA
      viewport: { width: 0, height: 0 }, // 0 means leave as default
      hardwareConcurrency: 0, // 0 means leave as default
    },
    proxyUrl: '',
    extraPuppeteerArgs: [],
    readMode: {
      focusSelectedChatsUI: false,
      cycleIntervalMs: 120000,
      cycleRandomJitterMs: 30000
    },
    fingerprint: { enabled: false, options: { browsers: ['chrome'], devices: ['desktop'] } },
    ghostCursor: { enabled: false },
    kafka: { enabled: false, brokers: [], clientId: 'wa-scraper', topic: 'whatsapp-messages', ssl: false, sasl: { mechanism: '', username: '', password: '' } },
    s3: { enabled: false, bucket: '', prefix: 'whatsapp/', region: 'us-east-1', acl: 'private', sse: 'AES256', batchFlushMessages: 500, batchFlushMs: 30000 },
    nlp: { enabled: false },
    compliance: { denyNumbers: [], denyIds: [] },
    coordinator: { enabled: false, lockKey: 'instance_lock', lockTtlSecs: 60, renewSecs: 30, exitIfNoLock: true },
    history: { backfillPerChat: 0, selectedOnly: true, afterUnix: 0, maxTotal: 0 },
    exportChatsOnce: false,
    exportChatsAndExit: false,
    restoreSessionFromKv: false,
    persistSessionToKv: false,
    compressSessionBackup: true,
    maxQueuedJobs: 1000,
    maxRssMB: 0,
    maxHeapMB: 0,
    maxMessages: 0,
    backfill: { enabled: false, perChatCount: 0, onlySelected: true },
    controlPollingSecs: 10,
    statsFlushSecs: 30,
    watchdogStaleMs: 600000,
  shutdownAfterMinutes: 0,
};

/** Utility helpers */
const toIso = (unixSeconds) => new Date((unixSeconds || 0) * 1000).toISOString();
const normalize = (s) => (s || '').toString().trim().toLowerCase();
const matchesAny = (text, patterns) => {
  if (!patterns || patterns.length === 0) return true;
  const t = normalize(text);
  return patterns.some((p) => {
    if (!p) return false;
    const v = normalize(p);
    // simple substring match; allow regex with prefix /.../
    if (v.startsWith('/') && v.endsWith('/')) {
      try {
        const re = new RegExp(v.slice(1, -1), 'i');
        return re.test(text || '');
      } catch {
        return t.includes(v);
      }
    }
    return t.includes(v);
  });
};

const matchesNone = (text, patterns) => {
  if (!patterns || patterns.length === 0) return true;
  const t = normalize(text);
  return patterns.every((p) => {
    if (!p) return true;
    const v = normalize(p);
    if (v.startsWith('/') && v.endsWith('/')) {
      try {
        const re = new RegExp(v.slice(1, -1), 'i');
        return !re.test(text || '');
      } catch {
        return !t.includes(v);
      }
    }
    return !t.includes(v);
  });
};

const patternMatches = (value, pattern) => {
  if (!pattern) return false;
  const v = (value || '').toString();
  const p = (pattern || '').toString();
  if (p.startsWith('/') && p.endsWith('/')) {
    try { return new RegExp(p.slice(1, -1), 'i').test(v); } catch { return v.toLowerCase().includes(p.toLowerCase()); }
  }
  return v.toLowerCase().includes(p.toLowerCase());
};

const selectedChatMatcher = (chat, selectedChats) => {
  if (!Array.isArray(selectedChats) || selectedChats.length === 0) return true;
  const chatId = chat?.id?._serialized || chat?.id || '';
  const chatName = chat?.name || '';
  const chatUser = chat?.id?.user || '';
  return selectedChats.some((entryRaw) => {
    const entry = (entryRaw || '').toString();
    if (entry.startsWith('id:')) {
      const target = entry.slice(3);
      return chatId === target || chatUser === target;
    }
    if (entry.startsWith('name:')) {
      const target = entry.slice(5);
      return patternMatches(chatName, target);
    }
    return patternMatches(chatName, entry) || patternMatches(chatId, entry) || patternMatches(chatUser, entry);
  });
};

const postJson = async (url, payload, headers = {}) => {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Webhook HTTP ${res.status} ${res.statusText} ${body}`);
    }
  } catch (err) {
    logger.warn({ err }, 'Webhook POST failed');
  }
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const postJsonWithRetry = async (url, payload, headers, maxRetries, baseBackoffMs) => {
  let attempt = 0;
  let lastErr;
  while (attempt <= maxRetries) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(headers || {}) },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Webhook HTTP ${res.status} ${res.statusText} ${body}`);
      }
      return true;
    } catch (err) {
      lastErr = err;
      const delay = Math.min(60_000, (baseBackoffMs || 1000) * Math.pow(2, attempt));
      await sleep(delay + Math.floor(Math.random() * 250));
      attempt += 1;
    }
  }
  logger.warn({ url, lastErr }, 'Webhook POST exhausted retries');
  return false;
};

/** Build record structure for dataset/webhook */
const extractLinks = (text) => {
  if (!text) return [];
  const urlRegex = /https?:\/\/[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:\/?#\[\]@!$&'()*+,;=.]+/gi;
  const set = new Set();
  for (const m of text.matchAll(urlRegex)) {
    if (m && m[0]) set.add(m[0]);
  }
  return Array.from(set);
};

const buildRecord = async (msg, chat, contact, quotedMsg) => {
  const isGroup = Boolean(chat?.isGroup);
  const chatName = chat?.name || contact?.name || contact?.pushname || '';
  const senderName = contact?.pushname || contact?.name || '';
  const senderNumber = contact?.number || '';
  const messageText = msg?.body || '';
  const messageCaption = msg?.caption || '';

  const record = {
    scrapedAt: new Date().toISOString(),
    message: {
      id: msg?.id?._serialized || msg?.id || '',
      type: msg?.type || 'unknown',
      text: messageText,
      caption: messageCaption,
      timestampUnix: msg?.timestamp || 0,
      timestampIso: toIso(msg?.timestamp),
      fromMe: Boolean(msg?.fromMe),
      hasMedia: Boolean(msg?.hasMedia),
      ack: msg?.ack ?? null,
      links: extractLinks(`${messageText} ${messageCaption}`),
      mentionedIds: Array.isArray(msg?.mentionedIds) ? msg.mentionedIds : [],
    },
    chat: {
      id: chat?.id?._serialized || chat?.id || msg?.from || '',
      name: chatName,
      isGroup,
    },
    sender: {
      id: msg?.author || contact?.id?._serialized || contact?.id || '',
      name: senderName,
      number: senderNumber,
      verifiedName: contact?.verifiedName || '',
      shortName: contact?.shortName || '',
      isMe: Boolean(msg?.fromMe),
    },
  };

  if (isGroup) {
    record.group = {
      participantsCount: Array.isArray(chat?.participants) ? chat.participants.length : undefined,
      owner: chat?.owner?._serialized || chat?.owner || undefined,
      announce: chat?.announce,
      noFrequentlyForwarded: chat?.noFrequentlyForwarded,
      isLocked: chat?.isLocked,
      isAnnounceGrpRestrict: chat?.isAnnounceGrpRestrict,
    };
  }

  if (quotedMsg) {
    record.quoted = {
      id: quotedMsg?.id?._serialized || quotedMsg?.id || '',
      type: quotedMsg?.type || 'unknown',
      text: quotedMsg?.body || '',
      timestampUnix: quotedMsg?.timestamp || 0,
      timestampIso: toIso(quotedMsg?.timestamp),
      fromMe: Boolean(quotedMsg?.fromMe),
      hasMedia: Boolean(quotedMsg?.hasMedia),
    };
  }

  return record;
};

/** Filter decision */
const shouldProcess = (input, { chat, contact, msg }) => {
  const isGroup = Boolean(chat?.isGroup);
  if (input.listenChats === 'groups' && !isGroup) return false;
  if (input.listenChats === 'private' && isGroup) return false;

  // fromMe filter
  if (!input.includeFromMe && msg?.fromMe) return false;

  const chatName = chat?.name || contact?.name || contact?.pushname || '';
  const senderName = contact?.pushname || contact?.name || '';
  const senderNumber = contact?.number || '';

  // group filters
  if (isGroup) {
    if (input.includeGroups?.length && !matchesAny(chatName, input.includeGroups)) return false;
    if (input.excludeGroups?.length && matchesAny(chatName, input.excludeGroups)) return false;
  } else {
    if (input.includeContacts?.length && !matchesAny(senderName, input.includeContacts)) return false;
    if (input.excludeContacts?.length && matchesAny(senderName, input.excludeContacts)) return false;
  }

  // chat id filters
  const chatIdSerialized = chat?.id?._serialized || chat?.id || msg?.from || '';
  if (Array.isArray(input.compliance?.denyIds) && input.compliance.denyIds.includes(chatIdSerialized)) return false;
  if (Array.isArray(input.compliance?.denyNumbers) && input.compliance.denyNumbers.includes(senderNumber)) return false;
  if (input.includeChatIds?.length && !input.includeChatIds.includes(chatIdSerialized)) return false;
  if (input.excludeChatIds?.length && input.excludeChatIds.includes(chatIdSerialized)) return false;

  // message type filter
  if (Array.isArray(input.includeMessageTypes) && input.includeMessageTypes.length > 0) {
    const type = (msg?.type || '').toLowerCase();
    if (!input.includeMessageTypes.map((t) => (t || '').toLowerCase()).includes(type)) {
      return false;
    }
  }

  // text content filters
  const combinedText = `${msg?.body || ''} ${msg?.caption || ''}`;
  if (!matchesAny(combinedText, input.includeTextPatterns)) return false;
  if (!matchesNone(combinedText, input.excludeTextPatterns)) return false;

  // target mode selected filter
  if (input.targetMode === 'selected') {
    if (!selectedChatMatcher(chat, input.selectedChats)) return false;
  }

  return true;
};

/** Media handling helpers */
const approxBytesFromBase64 = (b64) => Math.floor((b64?.length || 0) * 0.75);
const extFromMime = (m) => {
  if (!m) return '';
  const parts = m.split('/');
  return parts[1] || '';
};

const storeMediaToKv = async ({ key, b64, mime }) => {
  const buffer = Buffer.from(b64, 'base64');
  await Actor.setValue(key, buffer, { contentType: mime });
  const env = Actor.getEnv();
  return {
    key,
    storeId: env?.defaultKeyValueStoreId,
    hint: 'Use Apify.getValue(key) or the API to retrieve the binary.',
  };
};

const walkDir = (dirPath) => {
  const results = [];
  const list = fs.readdirSync(dirPath, { withFileTypes: true });
  for (const dirent of list) {
    const full = path.join(dirPath, dirent.name);
    if (dirent.isDirectory()) {
      results.push(...walkDir(full));
    } else if (dirent.isFile()) {
      results.push(full);
    }
  }
  return results;
};

const backupSessionDirToKv = async (sessionDir, compress) => {
  try {
    if (!fs.existsSync(sessionDir)) return;
    const files = walkDir(sessionDir);
    const payload = [];
    for (const filePath of files) {
      try {
        const rel = path.relative(sessionDir, filePath);
        const data = fs.readFileSync(filePath);
        payload.push({ rel, b64: data.toString('base64') });
      } catch {}
    }
    const json = JSON.stringify({ files: payload, ts: new Date().toISOString() });
    if (compress) {
      const gz = zlib.gzipSync(Buffer.from(json));
      await Apify.setValue('session_backup.gz', gz, { contentType: 'application/gzip' });
    } else {
      await Apify.setValue('session_backup.json', JSON.parse(json));
    }
  } catch (err) {
    logger.warn({ err }, 'Failed to backup session directory');
  }
};

const restoreSessionDirFromKv = async (sessionDir) => {
  try {
    let saved = await Actor.getValue('session_backup.json');
    if (!saved) {
      const gz = await Actor.getValue('session_backup.gz', { buffer: true });
      if (gz) {
        try { saved = JSON.parse(zlib.gunzipSync(gz).toString('utf8')); } catch {}
      }
    }
    if (!saved || !Array.isArray(saved.files)) return false;
    for (const f of saved.files) {
      const dest = path.join(sessionDir, f.rel);
      const ddir = path.dirname(dest);
      fs.mkdirSync(ddir, { recursive: true });
      fs.writeFileSync(dest, Buffer.from(f.b64 || '', 'base64'));
    }
    return true;
  } catch (err) {
    logger.warn({ err }, 'Failed to restore session directory');
    return false;
  }
};

Actor.main(async () => {
  const rawInput = (await Actor.getInput()) || {};
  const input = { ...DEFAULTS, ...rawInput };

  // Resolve session directory absolute path
  const sessionDir = path.resolve(process.cwd(), input.sessionDir || DEFAULTS.sessionDir);
  if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true });
  }
  if (input.restoreSessionFromKv) {
    const restored = await restoreSessionDirFromKv(sessionDir);
    if (restored) logger.info('Restored WhatsApp session from KV store');
  }

  const limiter = new Bottleneck({
    minTime: Math.max(0, Math.floor(1000 / Math.max(1, Number(input.rateLimitPerSecond)))) ,
    maxConcurrent: Math.max(1, Number(input.maxConcurrency)),
  });
  limiter.on('error', (err) => logger.warn({ err }, 'Limiter error'));
  limiter.on('failed', (err) => logger.warn({ err }, 'Limiter job failed'));
  limiter.on('received', (info) => {
    const q = limiter.jobs("QUEUED").length + limiter.jobs("RUNNING").length;
    if (Number(input.maxQueuedJobs) > 0 && q > Number(input.maxQueuedJobs)) {
      logger.warn({ q }, 'Backpressure: queue too large, pausing intake');
      paused = true;
      setTimeout(() => { paused = false; }, Math.min(60_000, 5_000 + q * 10)).unref();
    }
  });

  logger.info({
    headless: input.headless,
    listenChats: input.listenChats,
    includeFromMe: input.includeFromMe,
    includeGroups: input.includeGroups,
    excludeGroups: input.excludeGroups,
    includeContacts: input.includeContacts,
    excludeContacts: input.excludeContacts,
    includeMessageTypes: input.includeMessageTypes,
    includeTextPatterns: input.includeTextPatterns,
    excludeTextPatterns: input.excludeTextPatterns,
    mediaHandling: input.mediaHandling,
    maxMediaSizeMB: input.maxMediaSizeMB,
    webhook: Boolean(input.webhookUrl),
    outputMode: input.outputMode,
    datasetName: input.datasetName,
    targetMode: input.targetMode,
    selectedChatsCount: Array.isArray(input.selectedChats) ? input.selectedChats.length : 0,
    humanModeEnabled: Boolean(input.humanMode?.enabled),
    stealthEnabled: Boolean(input.stealth?.enabled),
  }, 'Starting WhatsApp scraper');

  const puppeteerLib = (() => { try { return require('puppeteer'); } catch { return null; } })();
  const resolveSystemChrome = () => {
    const candidates = [
      process.env.PUPPETEER_EXECUTABLE_PATH,
      process.env.CHROME_EXECUTABLE_PATH,
      process.env.CHROME_PATH,
      process.env.CHROME_BIN,
      '/usr/bin/chromium-browser', // Alpine Linux
      '/usr/bin/chromium',
      '/usr/bin/google-chrome-stable',
      '/usr/bin/google-chrome',
    ].filter(Boolean);
    for (const p of candidates) {
      try { if (fs.existsSync(p)) return p; } catch {}
    }
    // fall back to Puppeteer's downloaded Chrome if present
    try { if (puppeteerLib?.executablePath) return puppeteerLib.executablePath(); } catch {}
    return undefined;
  };
  const executablePath = resolveSystemChrome();
  
  if (!executablePath) {
    logger.warn('No Chrome/Chromium executable found. Puppeteer will try to download one.');
  } else {
    logger.info({ executablePath }, 'Using Chrome/Chromium executable');
  }
  
  const client = new Client({
    puppeteer: {
      headless: Boolean(input.headless),
      executablePath: executablePath || undefined,
      defaultViewport: { width: 1366, height: 768 },
      ignoreDefaultArgs: ['--enable-automation'],
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--no-zygote',
        '--no-first-run',
        '--disable-web-security',
        '--disable-blink-features=AutomationControlled',
        '--lang=en-US,en;q=0.9',
        '--password-store=basic',
        `--user-agent=${process.env.CHROME_UA || 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}`,
        ...(input.proxyUrl ? [`--proxy-server=${input.proxyUrl}`] : []),
        ...(Array.isArray(input.extraPuppeteerArgs) ? input.extraPuppeteerArgs : []),
      ],
    },
    webVersionCache: { type: 'none' },
    authTimeoutMs: 180000,
    qrMaxRetries: 30,
    authStrategy: new LocalAuth({ dataPath: sessionDir, clientId: input.sessionId || DEFAULTS.sessionId }),
    takeoverOnConflict: true,
    takeoverTimeoutMs: 60000,
    restartOnAuthFail: true,
  });

  // Optional external sinks
  let s3Client = null;
  if (input.s3?.enabled && input.s3.bucket && input.s3.region) {
    try {
      s3Client = new S3Client({ region: input.s3.region });
      logger.info({ bucket: input.s3.bucket, region: input.s3.region }, 'S3 client ready');
    } catch (err) {
      logger.warn({ err }, 'Failed to initialize S3 client');
    }
  }

  let kafkaProducer = null;
  if (input.kafka?.enabled && Array.isArray(input.kafka.brokers) && input.kafka.brokers.length > 0) {
    try {
      const kafka = new Kafka({
        clientId: input.kafka.clientId || 'wa-scraper',
        brokers: input.kafka.brokers,
        ssl: Boolean(input.kafka.ssl),
        sasl: input.kafka.sasl && input.kafka.sasl.mechanism ? input.kafka.sasl : undefined,
      });
      kafkaProducer = kafka.producer();
      await kafkaProducer.connect();
      logger.info('Kafka producer connected');
    } catch (err) {
      logger.warn({ err }, 'Failed to connect Kafka producer');
      kafkaProducer = null;
    }
  }

  // Optional coordinator lock (single active instance)
  const instanceId = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const tryAcquireLock = async () => {
    if (!input.coordinator?.enabled) return true;
    try {
      const now = Date.now();
      const lockKey = input.coordinator.lockKey || 'instance_lock';
      const current = (await Actor.getValue(lockKey)) || {};
      if (!current.owner || !current.expiresAt || current.expiresAt < now) {
        const ttl = Math.max(10_000, Number(input.coordinator.lockTtlSecs) * 1000 || 60_000);
        await Actor.setValue(lockKey, { owner: instanceId, acquiredAt: now, expiresAt: now + ttl });
        return true;
      }
      if (current.owner === instanceId) return true;
      if (input.coordinator.exitIfNoLock) {
        logger.warn({ current }, 'Coordinator: another instance holds lock, exiting');
        await Actor.setValue('coord_conflict.json', { current, at: new Date().toISOString() });
        await Actor.exit();
      }
      return false;
    } catch (err) {
      logger.warn({ err }, 'Coordinator lock acquire failed');
      return true; // fail-open
    }
  };
  await tryAcquireLock();
  if (input.coordinator?.enabled) {
    setInterval(async () => {
      try {
        const lockKey = input.coordinator.lockKey || 'instance_lock';
        const now = Date.now();
        const ttl = Math.max(10_000, Number(input.coordinator.lockTtlSecs) * 1000 || 60_000);
        const renew = Math.max(5_000, Number(input.coordinator.renewSecs) * 1000 || 30_000);
        const current = (await Actor.getValue(lockKey)) || {};
        if (!current.owner || current.owner === instanceId) {
          await Actor.setValue(lockKey, { owner: instanceId, acquiredAt: current.acquiredAt || now, expiresAt: now + ttl });
        }
      } catch {}
    }, Math.max(5_000, Number(input.coordinator.renewSecs) * 1000 || 30_000)).unref();
  }

  let preAuthStealthApplied = false;
  client.on('qr', async (qr) => {
    logger.info('QR code received. Scan with your WhatsApp mobile app.');
    try {
      // ASCII QR for terminals
      qrcodeTerminal.generate(qr, { small: true });
      // Also store plain text QR for apps that parse it directly
      await Actor.setValue('qr.txt', qr, { contentType: 'text/plain; charset=utf-8' }).catch(() => {});
      // Apply stealth before authentication if possible
      if (!preAuthStealthApplied) {
        try {
          const page = client.pupPage || (client.pupBrowser && (await client.pupBrowser.pages())[0]);
          if (page && input.stealth?.enabled) {
            if (input.stealth.userAgent) { try { await page.setUserAgent(input.stealth.userAgent); } catch {} }
            if (input.stealth.viewport && Number(input.stealth.viewport.width) > 0 && Number(input.stealth.viewport.height) > 0) {
              try { await page.setViewport({ width: Number(input.stealth.viewport.width), height: Number(input.stealth.viewport.height) }); } catch {}
            }
            await page.evaluateOnNewDocument(() => {
              Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
              // eslint-disable-next-line no-undef
              window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
              // eslint-disable-next-line no-undef
              const originalQuery = window.navigator.permissions.query;
              // eslint-disable-next-line no-undef
              window.navigator.permissions.query = (parameters) => parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters);
            });
            if (Array.isArray(input.stealth?.languages) && input.stealth.languages.length > 0) {
              try { await page.evaluateOnNewDocument((langs) => {
                Object.defineProperty(navigator, 'languages', { get: () => langs });
              }, input.stealth.languages); } catch {}
            }
            if (input.stealth?.emulateTimezone) {
              try { await page.emulateTimezone(input.stealth.emulateTimezone); } catch {}
            }
            preAuthStealthApplied = true;
            logger.info('Applied pre-auth stealth');
          }
        } catch (err) {
          logger.warn({ err }, 'Pre-auth stealth injection failed');
        }
      }
      // Data URL for visual references in rich logs (if needed)
      const dataUrl = await QRCode.toDataURL(qr);
      logger.debug({ qrDataUrl: dataUrl.slice(0, 64) + '...' }, 'QR DataURL preview');
      if (input.storeQrToKv) {
        const base64 = dataUrl.split(',')[1];
        const buffer = Buffer.from(base64, 'base64');
        await Actor.setValue('qr.png', buffer, { contentType: 'image/png' });
        const env = Actor.getEnv();
        const ref = { storeId: env?.defaultKeyValueStoreId, key: 'qr.png', type: 'qr' };
        await Actor.pushData({ event: 'qr', scrapedAt: new Date().toISOString(), qr: ref });
        logger.info({ ref }, 'Stored QR image to KV store');
      }
    } catch (err) {
      logger.warn({ err }, 'Failed to render QR');
    }
  });

  client.on('authenticated', () => logger.info('Authenticated'));
  client.on('auth_failure', (msg) => {
    logger.error({ msg }, 'Authentication failed');
  });
  let pageRef = null;
  let lastEventAt = Date.now();

  client.on('ready', async () => {
    logger.info('WhatsApp is ready');
    try {
      const page = client.pupPage || (client.pupBrowser && (await client.pupBrowser.pages())[0]);
      pageRef = page || pageRef;
      lastEventAt = Date.now();
      if (page) {
        // Fingerprint injection
        if (input.fingerprint?.enabled) {
          try {
            const generator = new FingerprintGenerator(input.fingerprint.options || {});
            const fp = generator.getFingerprint({ browsers: ['chrome'], devices: ['desktop'] });
            const injector = new FingerprintInjector();
            await injector.attachFingerprintToPuppeteer(page, fp);
            if (!input.stealth.userAgent && fp?.headers?.['user-agent']) {
              try { await page.setUserAgent(fp.headers['user-agent']); } catch {}
            }
            logger.info('Fingerprint injected');
          } catch (err) {
            logger.warn({ err }, 'Fingerprint injection failed');
          }
        }
        // Stealth hardening
        if (input.stealth?.enabled) {
          try {
            // UA
            if (input.stealth.userAgent) {
              await page.setUserAgent(input.stealth.userAgent);
            }
            // Viewport
            if (input.stealth.viewport && Number(input.stealth.viewport.width) > 0 && Number(input.stealth.viewport.height) > 0) {
              await page.setViewport({ width: Number(input.stealth.viewport.width), height: Number(input.stealth.viewport.height) });
            }
            await page.evaluateOnNewDocument(() => {
              try {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                // Spoof permissions to avoid obvious automation checks
                const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
                if (originalQuery) {
                  window.navigator.permissions.query = (parameters) => (
                    parameters && parameters.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : originalQuery(parameters)
                  );
                }
                // Minimal chrome object
                if (!window.chrome) {
                  window.chrome = { runtime: {} };
                }
              } catch {}
            });
            if (Array.isArray(input.stealth.languages) && input.stealth.languages.length > 0) {
              const langs = input.stealth.languages;
              await page.evaluate((ls) => {
                try {
                  Object.defineProperty(navigator, 'languages', { get: () => ls });
                } catch {}
              }, langs);
            }
            if (input.stealth.platform) {
              const platform = input.stealth.platform;
              await page.evaluate((pf) => {
                try {
                  Object.defineProperty(navigator, 'platform', { get: () => pf });
                } catch {}
              }, platform);
            }
            if (Number(input.stealth.hardwareConcurrency) > 0) {
              const hc = Number(input.stealth.hardwareConcurrency);
              await page.evaluate((n) => {
                try {
                  Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => n });
                } catch {}
              }, hc);
            }
            if (input.stealth.emulateTimezone) {
              try { await page.emulateTimezone(input.stealth.emulateTimezone); } catch {}
            }
          } catch (err) {
            logger.warn({ err }, 'Stealth hardening failed');
          }
        }

        // Human mode simulation
        if (input.humanMode?.enabled) {
          const minMs = Math.max(10_000, Number(input.humanMode.minIntervalMs) || 45_000);
          const maxMs = Math.max(minMs + 1, Number(input.humanMode.maxIntervalMs) || 180_000);
          const minPx = Math.max(100, Number(input.humanMode.scrollPixelsMin) || 200);
          const maxPx = Math.max(minPx + 1, Number(input.humanMode.scrollPixelsMax) || 1200);
          const doStep = async () => {
            try {
              await page.evaluate((min, max) => {
                const rand = (a, b) => Math.floor(a + Math.random() * (b - a + 1));
                const tryScroll = (el) => { if (el) el.scrollTop = Math.max(0, el.scrollTop + rand(-max, max)); };
                const q = (sel) => document.querySelector(sel);
                const candidates = [
                  '[data-testid="chat-list"]',
                  '[role="grid"]',
                  '[data-testid="conversation-panel-body"]',
                  '[aria-label="Message list"]',
                ];
                for (const sel of candidates) tryScroll(q(sel));
                // occasional minor mouse movement/jitter near the center
                try {
                  const evt = new MouseEvent('mousemove', { clientX: 400 + rand(-20, 20), clientY: 400 + rand(-20, 20), bubbles: true });
                  document.body.dispatchEvent(evt);
                } catch {}
              }, minPx, maxPx);
              if (input.ghostCursor?.enabled) {
                try {
                  const cursor = createCursor(page);
                  if (cursor && cursor.move) {
                    await cursor.move({ x: 200 + Math.random() * 600, y: 200 + Math.random() * 400 });
                  }
                } catch {}
              }
            } catch {}
            const wait = Math.floor(minMs + Math.random() * (maxMs - minMs));
            setTimeout(doStep, wait).unref();
          };
          const initialWait = Math.floor(minMs + Math.random() * (maxMs - minMs));
          setTimeout(doStep, initialWait).unref();
          logger.info({ minMs, maxMs }, 'Human mode enabled');
        }
        // Focus cycling over selected chats to simulate reading threads periodically
        if (input.readMode?.focusSelectedChatsUI && input.targetMode === 'selected' && Array.isArray(input.selectedChats) && input.selectedChats.length > 0) {
          const cycleOnce = async () => {
            try {
              const targets = input.selectedChats.slice(0, 25);
              for (const target of targets) {
                // Click/search chat by name or id in the sidebar
                await page.evaluate((needle) => {
                  const normalize = (s) => (s || '').toString().toLowerCase();
                  const match = (a, b) => normalize(a).includes(normalize(b));
                  const items = Array.from(document.querySelectorAll('[data-testid="chatlist-status-v3"]')).map((el) => el.closest('[role="row"]'));
                  const clickItem = (rowEl) => { if (rowEl) { rowEl.dispatchEvent(new MouseEvent('mousedown', { bubbles: true })); rowEl.click(); } };
                  for (const row of items) {
                    const nameEl = row?.querySelector('[data-testid="cell-frame-title"]') || row?.querySelector('[role="gridcell"]');
                    const idText = row?.getAttribute('data-id') || '';
                    const nameText = nameEl?.textContent || '';
                    if (match(nameText, needle) || match(idText, needle)) { clickItem(row); break; }
                  }
                }, target);
                await page.waitForTimeout(500 + Math.floor(Math.random() * 800));
                await page.evaluate(() => {
                  const candidates = [
                    '[data-testid="conversation-panel-body"]',
                    '[aria-label="Message list"]',
                  ];
                  const el = candidates.map((s) => document.querySelector(s)).find(Boolean);
                  if (el) el.scrollTop = el.scrollHeight; // scroll to bottom like a reader
                });
                await page.waitForTimeout(400 + Math.floor(Math.random() * 900));
              }
            } catch (err) { /* ignore cycle errors */ }
          };
          const schedule = () => {
            const base = Math.max(30_000, Number(input.readMode.cycleIntervalMs) || 120_000);
            const jit = Math.max(0, Number(input.readMode.cycleRandomJitterMs) || 30_000);
            const next = base + Math.floor(Math.random() * jit);
            setTimeout(async () => { await cycleOnce(); schedule(); }, next).unref();
          };
          schedule();
          logger.info('Focus cycling over selected chats enabled');
        }
        // Export chats index once
        if (input.exportChatsOnce) {
          try {
            const chats = await client.getChats();
            for (const c of chats) {
              const rec = {
                event: 'chat_index',
                scrapedAt: new Date().toISOString(),
                chat: {
                  id: c?.id?._serialized || c?.id || '',
                  user: c?.id?.user || '',
                  server: c?.id?.server || '',
                  name: c?.name || '',
                  isGroup: Boolean(c?.isGroup),
                  isReadOnly: Boolean(c?.isReadOnly),
                  isMuted: Boolean(c?.isMuted),
                },
              };
              await dataset.pushData(rec);
            }
            logger.info({ count: chats.length }, 'Exported chat index');
            if (input.exportChatsAndExit) {
              return shutdown('export-chats');
            }
          } catch (err) {
            logger.warn({ err }, 'Failed to export chat index');
          }
        }

        // Optional backfill of recent messages per chat via whatsapp-web.js API (shallow, not historical export)
        if (input.backfill?.enabled && Number(input.backfill.perChatCount) > 0) {
          try {
            const chats = await client.getChats();
            const targetChats = input.backfill.onlySelected && input.targetMode === 'selected'
              ? chats.filter((c) => selectedChatMatcher(c, input.selectedChats))
              : chats;
            for (const c of targetChats) {
              const count = Math.min(200, Number(input.backfill.perChatCount));
              try {
                const msgs = await c.fetchMessages({ limit: count });
                for (const m of msgs) {
                  await handleMessage(m); // reuse pipeline with dedupe guard
                }
              } catch (err) {
                logger.warn({ err, chat: c?.id?._serialized || c?.id || '' }, 'Backfill fetch failed');
              }
            }
            logger.info('Backfill pass completed');
          } catch (err) {
            logger.warn({ err }, 'Backfill stage failed');
          }
        }
      }
    } catch (err) {
      logger.warn({ err }, 'Post-ready setup failed');
    }
  });
  client.on('auth_failure', (m) => logger.warn({ m }, 'Auth failure'));
  client.on('disconnected', (r) => logger.warn({ reason: r }, 'Client disconnected'));

  const processedIds = new Set();
  let persistentDedupIds = [];
  let dedupDirty = false;
  const counters = {
    startTime: new Date().toISOString(),
    messages: 0,
    groups: 0,
    privates: 0,
    reactions: 0,
    revocations: 0,
    acks: 0,
    errors: 0,
  };
  let paused = false;

  // load persistent dedup ids if enabled
  if (input.persistDedup) {
    try {
      const stored = await Actor.getValue('seen_ids.json');
      if (stored && Array.isArray(stored.ids)) {
        persistentDedupIds = stored.ids.slice(-Math.max(0, Number(input.dedupMaxIds)));
        for (const id of persistentDedupIds) processedIds.add(id);
        logger.info({ count: persistentDedupIds.length }, 'Loaded persistent dedup ids');
      }
    } catch (err) {
      logger.warn({ err }, 'Failed to load persistent dedup ids');
    }
  }

  // dataset selection
  const dataset = input.datasetName && input.datasetName.trim().length > 0
    ? await Actor.openDataset(input.datasetName)
    : await Actor.openDataset();

  const applyRedaction = (nameOrNumber, type) => {
    const strategy = input.redact?.strategy || 'mask';
    const enabledForNames = input.redact?.maskNames && type === 'name';
    const enabledForNumbers = input.redact?.maskPhoneNumbers && type === 'number';
    if (!(enabledForNames || enabledForNumbers)) return nameOrNumber;
    const value = (nameOrNumber || '').toString();
    if (strategy === 'hash') {
      // simple non-cryptographic hash for obscuring; avoid bringing a new dep
      let h = 0;
      for (let i = 0; i < value.length; i += 1) {
        h = (h << 5) - h + value.charCodeAt(i);
        h |= 0;
      }
      return `red_${Math.abs(h)}`;
    }
    // default mask: keep last 2 chars
    if (value.length <= 2) return '*'.repeat(value.length);
    return `${'*'.repeat(value.length - 2)}${value.slice(-2)}`;
  };

  const toExtremeSchema = (msg, chat, contact, record) => {
    const isGroup = Boolean(chat?.isGroup);
    const mentionedNumbers = (record?.message?.mentionedIds || [])
      .map((id) => (id || '').toString())
      .map((id) => id.split('@')[0])
      .filter(Boolean);

    const senderNumber = applyRedaction(contact?.number || '', 'number');
    const senderName = applyRedaction(contact?.pushname || contact?.name || '', 'name');
    const groupName = isGroup ? applyRedaction(chat?.name || '', 'name') : '';

    return {
      messageText: record?.message?.text || '',
      senderNumber,
      senderName,
      timestamp: msg?.timestamp || 0,
      isGroupMessage: isGroup,
      isPrivateMessage: !isGroup,
      groupName,
      groupId: isGroup ? chat?.id?.user || chat?.id?._serialized || '' : '',
      chatId: chat?.id?.user || chat?.id?._serialized || '',
      rawMessageId: msg?.id?.id || '',
      deviceType: msg?.deviceType || 'unknown',
      fromMe: Boolean(msg?.fromMe),
      isForwarded: Boolean(msg?.isForwarded),
      hasMedia: Boolean(msg?.hasMedia),
      isStarred: Boolean(msg?.isStarred || msg?.starred),
      mentionedNumbers,
      isStatus: Boolean(msg?.isStatus),
      links: record?.message?.links || [],
    };
  };

  const dispatchWebhooks = async (payload, mode) => {
    const maxRetries = Number(input.webhookMaxRetries) || 3;
    const baseBackoff = Number(input.webhookBaseBackoffMs) || 1000;
    const computeSig = (body) => {
      if (!input.webhookSignature?.enabled) return null;
      const secret = input.webhookSignature.secret || process.env.WEBHOOK_SECRET || process.env.APIFY_WEBHOOK_SECRET || '';
      if (!secret) return null;
      const algo = input.webhookSignature.algorithm || 'sha256';
      const h = crypto.createHmac(algo, secret);
      h.update(JSON.stringify(body));
      return h.digest('hex');
    };
    const signature = computeSig(payload);
    const sigHeaderName = input.webhookSignature?.headerName || 'X-Signature';
    const withSig = (headers) => (signature ? { ...(headers || {}), [sigHeaderName]: signature } : (headers || {}));
    const sendOne = (url, headers) => postJsonWithRetry(url, payload, withSig(headers), maxRetries, baseBackoff);
    const tasks = [];
    if (input.webhookUrl) tasks.push(sendOne(input.webhookUrl, input.webhookHeaders));
    if (Array.isArray(input.webhooks)) {
      for (const w of input.webhooks) {
        if (w && w.url) tasks.push(postJsonWithRetry(w.url, payload, w.headers || {}, w.maxRetries ?? maxRetries, w.baseBackoffMs ?? baseBackoff));
      }
    }
    await Promise.allSettled(tasks);
  };

  const maybeShutdownOnLimit = async () => {
    if (Number(input.maxMessages) > 0 && counters.messages >= Number(input.maxMessages)) {
      logger.info({ processed: counters.messages }, 'Max messages reached, shutting down');
      await shutdown('max-messages');
    }
  };

  const handleMessage = async (msg) => {
    try {
      lastEventAt = Date.now();
      if (paused) return;
      const msgId = msg?.id?._serialized || msg?.id;
      if (processedIds.has(msgId)) return; // simple local dedupe per run
      processedIds.add(msgId);
      if (input.persistDedup) {
        persistentDedupIds.push(msgId);
        if (persistentDedupIds.length > Number(input.dedupMaxIds)) persistentDedupIds.shift();
        dedupDirty = true;
      }

      const chat = await msg.getChat();
      const contact = await msg.getContact();

      if (!shouldProcess(input, { chat, contact, msg })) return;

      let quotedMsg = null;
      try {
        if (input.inlineQuotedMessage && msg.hasQuotedMsg) {
          quotedMsg = await msg.getQuotedMessage();
        }
      } catch {
        // ignore quoted retrieval errors
      }

      const record = await buildRecord(msg, chat, contact, quotedMsg);

      // Media handling
      if (input.mediaHandling !== 'skip' && msg.hasMedia) {
        try {
          const media = await limiter.schedule(() => msg.downloadMedia());
          if (media && media.data) {
            const bytes = approxBytesFromBase64(media.data);
            const sizeMB = bytes / (1024 * 1024);
            if (sizeMB <= Number(input.maxMediaSizeMB)) {
              const ext = extFromMime(media.mimetype);
              const mediaKey = `media-${record.message.id}${ext ? `.${ext}` : ''}`;

              if (input.mediaHandling === 'base64') {
                record.media = {
                  mimeType: media.mimetype,
                  sizeBytes: bytes,
                  base64: media.data,
                  fileName: media.filename || mediaKey,
                };
              } else if (input.mediaHandling === 'kv-store') {
                const linkMeta = await storeMediaToKv({ key: mediaKey, b64: media.data, mime: media.mimetype });
                record.media = {
                  mimeType: media.mimetype,
                  sizeBytes: bytes,
                  store: linkMeta,
                  fileName: media.filename || mediaKey,
                };
              } else if (input.mediaHandling === 's3' && s3Client && input.s3?.bucket) {
                try {
                  const key = `${(input.s3.prefix || 'whatsapp/')}${mediaKey}`;
                  const put = new PutObjectCommand({
                    Bucket: input.s3.bucket,
                    Key: key,
                    Body: Buffer.from(media.data, 'base64'),
                    ACL: input.s3.acl || 'private',
                    ContentType: media.mimetype,
                    ServerSideEncryption: input.s3.sse || undefined,
                  });
                  await s3Client.send(put);
                  record.media = {
                    mimeType: media.mimetype,
                    sizeBytes: bytes,
                    s3: { bucket: input.s3.bucket, key },
                    fileName: media.filename || mediaKey,
                  };
                } catch (err) {
                  logger.warn({ err }, 'S3 upload failed; falling back to KV');
                  const linkMeta = await storeMediaToKv({ key: mediaKey, b64: media.data, mime: media.mimetype });
                  record.media = {
                    mimeType: media.mimetype,
                    sizeBytes: bytes,
                    store: linkMeta,
                    fileName: media.filename || mediaKey,
                  };
                }
              }
            } else {
              record.media = { skipped: true, reason: `Media too large (${sizeMB.toFixed(2)} MB)` };
            }
          }
        } catch (err) {
          logger.warn({ err }, 'Failed to download media');
        }
      }

      const payloadMode = input.webhookPayloadMode === 'match' ? input.outputMode : input.webhookPayloadMode;
      const datasetRecord = input.outputMode === 'extremescrapes' ? toExtremeSchema(msg, chat, contact, record) : record;
      await limiter.schedule(() => dataset.pushData(datasetRecord));

      const webhookPayload = payloadMode === 'extremescrapes' ? toExtremeSchema(msg, chat, contact, record) : record;
      await dispatchWebhooks(webhookPayload, payloadMode);

      // Kafka sink
      if (kafkaProducer) {
        try {
          await kafkaProducer.send({ topic: input.kafka.topic || 'whatsapp-messages', messages: [{ value: JSON.stringify(datasetRecord) }] });
        } catch (err) {
          logger.warn({ err }, 'Kafka send failed');
        }
      }

      // stats
      counters.messages += 1;
      if (chat?.isGroup) counters.groups += 1; else counters.privates += 1;
      await maybeShutdownOnLimit();
    } catch (err) {
      logger.error({ err }, 'Message processing failed');
      counters.errors += 1;
    }
  };

  client.on('message', handleMessage);
  client.on('message_create', handleMessage);

  if (input.includeReactions) {
    client.on('message_reaction', async (reaction) => {
      try {
        lastEventAt = Date.now();
        const base = {
          scrapedAt: new Date().toISOString(),
          event: 'reaction',
          reaction,
        };
        await limiter.schedule(() => dataset.pushData(base));
        await dispatchWebhooks(base, 'rich');
        counters.reactions += 1;
      } catch (err) {
        logger.warn({ err }, 'Failed to handle reaction event');
        counters.errors += 1;
      }
    });
  }

  if (input.includeRevocations) {
    client.on('message_revoke_everyone', async (before, after) => {
      try {
        lastEventAt = Date.now();
        const base = {
          scrapedAt: new Date().toISOString(),
          event: 'revocation',
          before,
          after,
        };
        await limiter.schedule(() => dataset.pushData(base));
        await dispatchWebhooks(base, 'rich');
        counters.revocations += 1;
      } catch (err) {
        logger.warn({ err }, 'Failed to handle revocation event');
        counters.errors += 1;
      }
    });
  }

  if (input.includeAcks) {
    client.on('message_ack', async (msg, ack) => {
      try {
        lastEventAt = Date.now();
        const base = {
          scrapedAt: new Date().toISOString(),
          event: 'ack',
          messageId: msg?.id?._serialized || msg?.id || '',
          ack,
        };
        await limiter.schedule(() => dataset.pushData(base));
        await dispatchWebhooks(base, 'rich');
        counters.acks += 1;
      } catch (err) {
        logger.warn({ err }, 'Failed to handle ack event');
        counters.errors += 1;
      }
    });
  }

  if (input.includeGroupEvents) {
    const pushGroupEvent = async (event, notif) => {
      const base = {
        scrapedAt: new Date().toISOString(),
        event,
        notification: notif,
      };
      await limiter.schedule(() => dataset.pushData(base));
      await dispatchWebhooks(base, 'rich');
      if (kafkaProducer) {
        try { await kafkaProducer.send({ topic: input.kafka.topic || 'whatsapp-messages', messages: [{ value: JSON.stringify(base) }] }); } catch {}
      }
    };
    client.on('group_join', (n) => { lastEventAt = Date.now(); pushGroupEvent('group_join', n); });
    client.on('group_leave', (n) => { lastEventAt = Date.now(); pushGroupEvent('group_leave', n); });
    client.on('group_update', (n) => { lastEventAt = Date.now(); pushGroupEvent('group_update', n); });
  }

  const shutdown = async (why) => {
    try {
      logger.info({ why }, 'Shutting down');
      try { await Actor.setValue('seen_ids.json', { ids: persistentDedupIds, updatedAt: new Date().toISOString() }); } catch {}
      try { await Actor.setValue('stats.json', { ...counters, endTime: new Date().toISOString(), reason: why }); } catch {}
      if (input.persistSessionToKv) {
        await backupSessionDirToKv(sessionDir);
      }
      await client.destroy().catch(() => {});
    } finally {
      await Actor.exit();
    }
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  client.on('change_state', (state) => logger.info({ state }, 'Client state changed'));
  client.on('disconnected', async (reason) => {
    logger.warn({ reason }, 'Client disconnected – attempting reinit in 10s');
    let delay = 5000;
    const tryReinit = async () => {
      try { await client.initialize(); logger.info('Reinit succeeded'); }
      catch (err) { logger.error({ err }, 'Reinit failed'); delay = Math.min(delay * 2, 120_000); setTimeout(tryReinit, delay).unref(); }
    };
    setTimeout(tryReinit, delay).unref();
  });

  // Keep-alive and periodic dedup flush
  setInterval(async () => {
    try { await client.getState(); } catch { /* ignore */ }
    if (input.persistDedup && dedupDirty) {
      try {
        await Actor.setValue('seen_ids.json', { ids: persistentDedupIds, updatedAt: new Date().toISOString() });
        dedupDirty = false;
      } catch (err) {
        logger.warn({ err }, 'Failed to flush dedup ids');
      }
    }
    // stats
    try {
      await Actor.setValue('stats.json', { ...counters, now: new Date().toISOString() });
    } catch {}
  }, Math.max(5_000, Number(input.dedupFlushSecs) * 1000)).unref();

  // Watchdog: if nothing happens for a while, try reload or reinit
  setInterval(async () => {
    try {
      const staleMs = Number(input.watchdogStaleMs) || 600000;
      if (staleMs <= 0) return;
      const idle = Date.now() - lastEventAt;
      if (idle > staleMs) {
        logger.warn({ idle }, 'Watchdog: stale detected, reloading page');
        try {
          if (pageRef && !pageRef.isClosed()) {
            await pageRef.reload({ waitUntil: ['domcontentloaded', 'networkidle0'] });
            lastEventAt = Date.now();
          } else {
            await client.initialize();
          }
        } catch (err) {
          logger.error({ err }, 'Watchdog reload failed, attempting reinit');
          try { await client.initialize(); } catch {}
        }
      }
    } catch {}
  }, Math.max(30_000, Math.floor((Number(input.watchdogStaleMs) || 600000) / 2))).unref();

  // Control polling: pause/resume/shutdown or update selectors
  setInterval(async () => {
    try {
      const ctrl = await Actor.getValue('control.json');
      if (!ctrl || typeof ctrl !== 'object') return;
      if (ctrl.shutdown === true) return shutdown('control-shutdown');
      if (typeof ctrl.pause === 'boolean') paused = Boolean(ctrl.pause);
      if (Array.isArray(ctrl.selectedChats)) {
        input.selectedChats = ctrl.selectedChats;
      }
      if (typeof ctrl.targetMode === 'string') input.targetMode = ctrl.targetMode;
      if (typeof ctrl.includeFromMe === 'boolean') input.includeFromMe = ctrl.includeFromMe;
      if (typeof ctrl.rateLimitPerSecond === 'number') {
        // Adjust limiter minTime dynamically
        const minTime = Math.max(0, Math.floor(1000 / Math.max(1, ctrl.rateLimitPerSecond)));
        try { limiter.updateSettings({ minTime }); } catch {}
      }
    } catch {}
  }, Math.max(5_000, Number(input.controlPollingSecs) * 1000)).unref();

  if (Number(input.shutdownAfterMinutes) > 0) {
    setTimeout(() => shutdown('time-limit'), Number(input.shutdownAfterMinutes) * 60 * 1000).unref();
  }

  await client.initialize();
});


