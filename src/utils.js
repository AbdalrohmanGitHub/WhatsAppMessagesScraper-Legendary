'use strict';

// Small, testable helper functions extracted from main.js

const toIso = (unixSeconds) => new Date((unixSeconds || 0) * 1000).toISOString();
const normalize = (s) => (s || '').toString().trim().toLowerCase();

const matchesAny = (text, patterns) => {
  if (!patterns || patterns.length === 0) return true;
  const t = normalize(text);
  return patterns.some((p) => {
    if (!p) return false;
    const v = normalize(p);
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

const extractLinks = (text) => {
  if (!text) return [];
  const urlRegex = /https?:\/\/[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:\/?#\[\]@!$&'()*+,;=.]+/gi;
  const set = new Set();
  for (const m of (text.matchAll?.(urlRegex) || [])) {
    if (m && m[0]) set.add(m[0]);
  }
  return Array.from(set);
};

const approxBytesFromBase64 = (b64) => Math.floor((b64?.length || 0) * 0.75);
const extFromMime = (m) => {
  if (!m) return '';
  const parts = m.split('/');
  return parts[1] || '';
};

const applyRedaction = (value, type, strategy = 'mask', maskNames = false, maskNumbers = false) => {
  const enabledForNames = maskNames && type === 'name';
  const enabledForNumbers = maskNumbers && type === 'number';
  if (!(enabledForNames || enabledForNumbers)) return value;
  const v = (value || '').toString();
  if (strategy === 'hash') {
    let h = 0;
    for (let i = 0; i < v.length; i += 1) {
      h = (h << 5) - h + v.charCodeAt(i);
      h |= 0;
    }
    return `red_${Math.abs(h)}`;
  }
  if (v.length <= 2) return '*'.repeat(v.length);
  return `${'*'.repeat(v.length - 2)}${v.slice(-2)}`;
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

const shouldProcess = (input, { chat, contact, msg }) => {
  const isGroup = Boolean(chat?.isGroup);
  if (input.listenChats === 'groups' && !isGroup) return false;
  if (input.listenChats === 'private' && isGroup) return false;

  if (!input.includeFromMe && msg?.fromMe) return false;

  const chatName = chat?.name || contact?.name || contact?.pushname || '';
  const senderName = contact?.pushname || contact?.name || '';
  const senderNumber = contact?.number || '';

  if (isGroup) {
    if (input.includeGroups?.length && !matchesAny(chatName, input.includeGroups)) return false;
    if (input.excludeGroups?.length && matchesAny(chatName, input.excludeGroups)) return false;
  } else {
    if (input.includeContacts?.length && !matchesAny(senderName, input.includeContacts)) return false;
    if (input.excludeContacts?.length && matchesAny(senderName, input.excludeContacts)) return false;
  }

  const chatIdSerialized = chat?.id?._serialized || chat?.id || msg?.from || '';
  if (Array.isArray(input.compliance?.denyIds) && input.compliance.denyIds.includes(chatIdSerialized)) return false;
  if (Array.isArray(input.compliance?.denyNumbers) && input.compliance.denyNumbers.includes(senderNumber)) return false;
  if (input.includeChatIds?.length && !input.includeChatIds.includes(chatIdSerialized)) return false;
  if (input.excludeChatIds?.length && input.excludeChatIds.includes(chatIdSerialized)) return false;

  if (Array.isArray(input.includeMessageTypes) && input.includeMessageTypes.length > 0) {
    const type = (msg?.type || '').toLowerCase();
    if (!input.includeMessageTypes.map((t) => (t || '').toLowerCase()).includes(type)) {
      return false;
    }
  }

  const combinedText = `${msg?.body || ''} ${msg?.caption || ''}`;
  if (!matchesAny(combinedText, input.includeTextPatterns)) return false;
  if (!matchesNone(combinedText, input.excludeTextPatterns)) return false;

  if (input.targetMode === 'selected') {
    if (!selectedChatMatcher(chat, input.selectedChats)) return false;
  }

  return true;
};

const toExtremeSchema = (msg, chat, contact, record, redactCfg = { maskPhoneNumbers: false, maskNames: false, strategy: 'mask' }) => {
  const isGroup = Boolean(chat?.isGroup);
  const mentionedNumbers = (record?.message?.mentionedIds || [])
    .map((id) => (id || '').toString())
    .map((id) => id.split('@')[0])
    .filter(Boolean);

  const senderNumber = applyRedaction(contact?.number || '', 'number', redactCfg.strategy, redactCfg.maskNames, redactCfg.maskPhoneNumbers);
  const senderName = applyRedaction(contact?.pushname || contact?.name || '', 'name', redactCfg.strategy, redactCfg.maskNames, redactCfg.maskPhoneNumbers);
  const groupName = isGroup ? applyRedaction(chat?.name || '', 'name', redactCfg.strategy, redactCfg.maskNames, redactCfg.maskPhoneNumbers) : '';

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

module.exports = {
  toIso,
  normalize,
  matchesAny,
  matchesNone,
  patternMatches,
  selectedChatMatcher,
  extractLinks,
  approxBytesFromBase64,
  extFromMime,
  applyRedaction,
  buildRecord,
  shouldProcess,
  toExtremeSchema,
};


