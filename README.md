## WhatsApp Messages Scraper (Clone)

Clone-style implementation inspired by `extremescrapes/whatsapp-messages-scraper` for listening to new WhatsApp messages (groups and private) via WhatsApp Web. This listens to new messages only while running; it does not fetch historical chats.

### Features
- QR code login in terminal
- Real-time message stream (groups and private)
- Filters: groups/contacts/message types/fromMe and text include/exclude patterns (supports substrings or regex `/.../`)
- Optional media handling: skip | base64 | Apify Key-Value Store
- Output modes: rich JSON or compatibility mode matching `extremescrapes/whatsapp-messages-scraper` fields
- Optional named dataset; webhook payload mode can mirror output mode or be set independently
- Reactions/revocations/acks capture (configurable), reconnection, keep-alive, watchdog reload
- Optional redaction for names and phone numbers (mask or hash)
- Optional persisted dedup across runs; optional QR image saved to KV store; optional session backup/restore (compressed)
- Targeting: listen to all chats or only selected chats (by id or by name/regex)
- Human mode: randomized scrolls, optional focus cycling of selected chats, small mouse jitter
- Stealth options: UA/viewport/timezone/languages/platform/hardwareConcurrency; webdriver off; AutomationControlled off; permissions spoof
- Multi-webhook fan-out with HMAC signatures and retries/backoff; queue backpressure guard; runtime control via `control.json`

### Legal Notice
Using automated tools to access WhatsApp may violate WhatsApp's Terms of Service and laws. Use only for your own data with consent. Prefer the official WhatsApp Business API for compliant production use.

### Requirements
- Node.js >= 18.17

### Quick Start (Local)
1. Install dependencies:
   ```bash
   npm install
   ```
2. Create `apify_storage/key_value_stores/default/INPUT.json` or run with environment `APIFY_INPUT`.
3. Run:
   ```bash
   npm run start
   ```
4. Scan the QR code from your phone's WhatsApp app.

### Example INPUT.json
```json
{
  "headless": true,
  "sessionId": "default",
  "sessionDir": ".wwebjs_auth",
  "rateLimitPerSecond": 10,
  "maxConcurrency": 2,
  "listenChats": "all",
  "includeGroups": [],
  "excludeGroups": [],
  "includeContacts": [],
  "excludeContacts": [],
  "includeChatIds": [],
  "excludeChatIds": [],
  "includeMessageTypes": [],
  "includeTextPatterns": [],
  "excludeTextPatterns": [],
  "includeFromMe": true,
  "mediaHandling": "skip",
  "maxMediaSizeMB": 25,
  "inlineQuotedMessage": true,
  "webhookUrl": "",
  "webhookHeaders": {},
  "webhooks": [],
  "webhookMaxRetries": 3,
  "webhookBaseBackoffMs": 1000,
  "webhookSignature": { "enabled": false, "algorithm": "sha256", "secret": "", "headerName": "X-Signature" },
  "outputMode": "rich",
  "webhookPayloadMode": "match",
  "includeReactions": true,
  "includeRevocations": true,
  "includeAcks": false,
  "includeGroupEvents": false,
  "redact": { "maskPhoneNumbers": false, "maskNames": false, "strategy": "mask" },
  "datasetName": "",
  "storeQrToKv": false,
  "persistDedup": false,
  "dedupMaxIds": 10000,
  "dedupFlushSecs": 30,
  "restoreSessionFromKv": false,
  "persistSessionToKv": false,
  "compressSessionBackup": true,
  "maxQueuedJobs": 1000,
  "maxRssMB": 0,
  "maxHeapMB": 0,
  "targetMode": "all",
  "selectedChats": ["id:1203634...", "name:Team", "Marketing"],
  "humanMode": { "enabled": false, "minIntervalMs": 45000, "maxIntervalMs": 180000, "scrollPixelsMin": 200, "scrollPixelsMax": 1200 },
  "stealth": { "enabled": true, "emulateTimezone": "", "languages": [], "platform": "", "userAgent": "", "viewport": { "width": 0, "height": 0 }, "hardwareConcurrency": 0 },
  "proxyUrl": "",
  "extraPuppeteerArgs": [],
  "shutdownAfterMinutes": 0
}

### Operational best practices
- Start broad, then target: run with `targetMode: "all"`, export chats (`exportChatsOnce`) to capture ids, then switch to `targetMode: "selected"` with ids.
- Use stealth defaults first; add UA/viewport/timezone/languages only when needed to avoid fingerprint churn.
- Enable `humanMode` only if you need UI activity; otherwise, rely on events to minimize UI noise.
- For webhooks, enable signatures and retries; in n8n verify signature with a shared secret.
- Keep `rateLimitPerSecond` modest; monitor `stats.json` and increase slowly if needed.
- Consider session backup/restore for seamless restarts; backups are gzipped by default.
- Use `control.json` in KV to pause/resume and retarget without restarting the actor.
```

### Output
Records are pushed to the default Apify Dataset. If `webhookUrl` is set, each record is also POSTed to that URL as JSON.

### Troubleshooting
- If QR keeps reappearing, delete `sessionDir` (default `.wwebjs_auth`) and retry.
- Ensure Chromium dependencies exist if running headless in containers.

### Compliance
Consider the official WhatsApp Business API for compliant, production-grade messaging.


