/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║              ClipForge — Mux Video Clip API (Production)                ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  Stack:   Node.js 18+ · Express 4 · @mux/mux-node · ESM                ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  Endpoints:                                                              ║
 * ║    GET    /health                    → Uptime + Mux credentials check    ║
 * ║    POST   /api/upload-url            → Get Mux direct upload URL         ║
 * ║    GET    /api/asset/:uploadId       → Poll asset status after upload     ║
 * ║    POST   /api/clip                  → Create clip (custom|first|last)    ║
 * ║    GET    /api/clip/:clipAssetId     → Poll clip status + download URL    ║
 * ║    DELETE /api/asset/:assetId        → Delete one asset (clip or source)  ║
 * ║    DELETE /api/assets/batch          → Delete multiple assets at once     ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  Required env vars:                                                      ║
 * ║    MUX_TOKEN_ID       — from dashboard.mux.com → API Access Tokens       ║
 * ║    MUX_TOKEN_SECRET   — same location                                    ║
 * ║    ALLOWED_ORIGIN     — your Base44 app URL, or * for local dev          ║
 * ║                                                                          ║
 * ║  Optional env vars:                                                      ║
 * ║    PORT               — default 3000                                     ║
 * ║    NODE_ENV           — 'development' | 'production'                     ║
 * ║    RATE_LIMIT_MAX     — requests per 15 min per IP, default 100          ║
 * ║    MAX_CLIP_BATCH     — max IDs per batch delete, default 20             ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

import Mux               from '@mux/mux-node';
import express           from 'express';
import cors              from 'cors';
import rateLimit         from 'express-rate-limit';
import { randomUUID }    from 'crypto';

// ─────────────────────────────────────────────────────────────────────────────
// ENVIRONMENT & BOOT GUARD
// ─────────────────────────────────────────────────────────────────────────────

const {
  MUX_TOKEN_ID,
  MUX_TOKEN_SECRET,
  ALLOWED_ORIGIN  = '*',
  PORT            = 3000,
  NODE_ENV        = 'development',
  RATE_LIMIT_MAX  = '100',
  MAX_CLIP_BATCH  = '20',
} = process.env;

const IS_PROD = NODE_ENV === 'production';

if (!MUX_TOKEN_ID || !MUX_TOKEN_SECRET) {
  console.error('[BOOT] ❌  MUX_TOKEN_ID and MUX_TOKEN_SECRET are required.');
  console.error('[BOOT]     Set them in your Railway / Render environment variables.');
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// MUX CLIENT
// ─────────────────────────────────────────────────────────────────────────────

const { video } = new Mux({
  tokenId:     MUX_TOKEN_ID,
  tokenSecret: MUX_TOKEN_SECRET,
});

// ─────────────────────────────────────────────────────────────────────────────
// EXPRESS APP
// ─────────────────────────────────────────────────────────────────────────────

const app = express();

// Video bytes never pass through this server — 50kb is plenty for JSON bodies
app.use(express.json({ limit: '50kb' }));

// ── CORS ──────────────────────────────────────────────────────────────────────
const allowedOrigins = ALLOWED_ORIGIN === '*'
  ? '*'
  : ALLOWED_ORIGIN.split(',').map(o => o.trim());

app.use(cors({
  origin:         allowedOrigins,
  methods:        ['GET', 'POST', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Request-ID'],
  exposedHeaders: ['X-Request-ID'],
}));

app.options('*', cors());

// ── REQUEST ID + REQUEST LOGGER ───────────────────────────────────────────────
app.use((req, res, next) => {
  req.id = (req.headers['x-request-id'] || randomUUID());
  res.setHeader('X-Request-ID', req.id);
  const start = Date.now();
  res.on('finish', () => {
    const ms  = Date.now() - start;
    const lvl = res.statusCode >= 500 ? 'ERROR'
              : res.statusCode >= 400 ? 'WARN '
              : 'INFO ';
    console.log(
      `[${new Date().toISOString()}] [${lvl}] [${req.id.slice(0, 8)}] ` +
      `${req.method.padEnd(6)} ${req.path.padEnd(35)} ${res.statusCode} (${ms}ms)`
    );
  });
  next();
});

// ── RATE LIMITER ──────────────────────────────────────────────────────────────
app.use('/api/', rateLimit({
  windowMs:        15 * 60 * 1000,
  max:             parseInt(RATE_LIMIT_MAX, 10),
  standardHeaders: true,
  legacyHeaders:   false,
  keyGenerator:    (req) => req.ip,
  message:         { error: 'Too many requests — please wait and try again.' },
}));

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Parses HH:MM:SS, MM:SS, or a raw number/string of seconds → float.
 * Throws a descriptive Error on any invalid input.
 */
function parseTimeInput(input) {
  if (input === null || input === undefined) {
    throw new Error('Time input is required');
  }
  if (typeof input === 'number') {
    if (isNaN(input) || input < 0) throw new Error(`Invalid time value: ${input}`);
    return input;
  }
  const str   = String(input).trim();
  const parts = str.split(':').map(Number);
  if (parts.length > 3 || parts.some(isNaN) || parts.some(v => v < 0)) {
    throw new Error(`Invalid time format "${input}". Use HH:MM:SS, MM:SS, or plain seconds.`);
  }
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60   + parts[1];
  return parts[0];
}

/**
 * Formats float seconds → "HH:MM:SS"
 */
function formatDuration(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return [h, m, s].map(v => String(v).padStart(2, '0')).join(':');
}

/**
 * Formats float seconds → human-readable "1h 2m 10s" / "5m 30s" / "45s"
 */
function formatHuman(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

/**
 * Resolves the best MP4 download URL from Mux static_renditions.
 * Falls back to /high.mp4 if renditions not yet listed.
 */
function buildDownloadUrl(playbackId, staticRenditions) {
  if (!playbackId) return null;
  const files = staticRenditions?.files ?? [];
  for (const name of ['high.mp4', 'medium.mp4', 'low.mp4']) {
    if (files.some(f => f.name === name)) {
      return `https://stream.mux.com/${playbackId}/${name}`;
    }
  }
  if (files.length > 0) {
    return `https://stream.mux.com/${playbackId}/${files[0].name}`;
  }
  // Fallback — Mux serves this once mp4_support processing finishes
  return `https://stream.mux.com/${playbackId}/high.mp4`;
}

/**
 * Extracts a readable error message from a Mux SDK error object.
 */
function muxErrorMessage(err) {
  return err?.error?.messages?.[0]
      ?? err?.error?.message
      ?? err?.message
      ?? 'Unknown Mux API error';
}

/**
 * Wraps async route handlers — unhandled promise rejections go to Express
 * global error handler instead of crashing the process.
 */
const asyncHandler = (fn) => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(next);

// ─────────────────────────────────────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────────────────────────────────────

// ── GET /health ───────────────────────────────────────────────────────────────
/**
 * Uptime + credentials check. Used by Railway/Render health monitors.
 * Performs a lightweight Mux API call to confirm token validity.
 */
app.get('/health', asyncHandler(async (_req, res) => {
  let muxStatus = 'ok';
  try {
    await video.assets.list({ limit: 1 });
  } catch {
    muxStatus = 'unreachable';
  }
  res.json({
    status:    muxStatus === 'ok' ? 'ok' : 'degraded',
    mux:       muxStatus,
    timestamp: new Date().toISOString(),
    env:       NODE_ENV,
  });
}));

// ── POST /api/upload-url ──────────────────────────────────────────────────────
/**
 * Creates a Mux direct upload URL.
 * The browser PUTs the video directly to this URL — your server never
 * handles raw video bytes, keeping bandwidth costs at zero.
 *
 * Response: { uploadUrl, uploadId }
 */
app.post('/api/upload-url', asyncHandler(async (_req, res) => {
  let upload;
  try {
    upload = await video.uploads.create({
      cors_origin:         ALLOWED_ORIGIN,
      new_asset_settings: {
        playback_policy: ['public'],
        mp4_support:     'standard',    // required for MP4 clip downloads
      },
    });
  } catch (err) {
    const msg = muxErrorMessage(err);
    console.error(`[upload-url] Mux error: ${msg}`);
    return res.status(502).json({ error: `Mux upload creation failed: ${msg}` });
  }

  res.json({
    uploadUrl: upload.url,
    uploadId:  upload.id,
  });
}));

// ── GET /api/asset/:uploadId ──────────────────────────────────────────────────
/**
 * Polls asset processing status after a browser direct-upload completes.
 * Call every 3–5 seconds until status = 'ready'.
 *
 * Response:
 *   status            — 'waiting' | 'preparing' | 'ready' | 'errored'
 *   assetId           — Mux asset ID (null while waiting)
 *   playbackId        — Mux playback ID (null until ready)
 *   duration          — float seconds (null until ready)
 *   durationFormatted — "HH:MM:SS"
 *   durationHuman     — "1h 24m 37s"
 *   streamUrl         — HLS .m3u8 URL for the video player
 *   thumbnailUrl      — JPG thumbnail from Mux Image API
 */
app.get('/api/asset/:uploadId', asyncHandler(async (req, res) => {
  const { uploadId } = req.params;

  if (!uploadId?.trim()) {
    return res.status(400).json({ error: 'uploadId is required' });
  }

  let upload;
  try {
    upload = await video.uploads.retrieve(uploadId);
  } catch (err) {
    if (err?.status === 404) {
      return res.status(404).json({ error: `Upload not found: ${uploadId}` });
    }
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  // Mux hasn't created the asset yet — still ingesting bytes
  if (!upload.asset_id) {
    return res.json({ status: 'waiting', assetId: null });
  }

  let asset;
  try {
    asset = await video.assets.retrieve(upload.asset_id);
  } catch (err) {
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  if (asset.status === 'errored') {
    return res.status(422).json({
      status:  'errored',
      assetId: asset.id,
      error:   'Mux video processing failed. Please try uploading again.',
    });
  }

  const playbackId = asset.playback_ids?.[0]?.id ?? null;

  res.json({
    status:            asset.status,
    assetId:           asset.id,
    playbackId,
    duration:          asset.duration         ?? null,
    durationFormatted: asset.duration != null  ? formatDuration(asset.duration) : null,
    durationHuman:     asset.duration != null  ? formatHuman(asset.duration)    : null,
    streamUrl:         playbackId ? `https://stream.mux.com/${playbackId}.m3u8`             : null,
    thumbnailUrl:      playbackId ? `https://image.mux.com/${playbackId}/thumbnail.jpg`      : null,
  });
}));

// ── POST /api/clip ────────────────────────────────────────────────────────────
/**
 * Creates a clip asset from an existing Mux source asset.
 * Three modes supported:
 *   custom — exact start/end timestamps
 *   first  — from 00:00:00 for X minutes
 *   last   — from (duration - X minutes) to end
 *
 * Request body:
 *   assetId    string          — source Mux asset ID
 *   duration   number          — total video duration in seconds
 *   mode       string          — 'custom' | 'first' | 'last'
 *   startTime  string|number   — (custom only) HH:MM:SS or seconds
 *   endTime    string|number   — (custom only) HH:MM:SS or seconds
 *   minutes    number          — (first/last only) decimals OK: 1.5 = 90s
 *
 * Response:
 *   clipAssetId, mode, clipStart, clipEnd, clipDuration,
 *   clipStartFormatted, clipEndFormatted, clipDurationFormatted,
 *   clipDurationHuman, message
 */
app.post('/api/clip', asyncHandler(async (req, res) => {
  const { assetId, duration, mode, startTime, endTime, minutes } = req.body;

  // ── Validate required fields ──────────────────────────────────────────────
  if (!assetId?.trim()) {
    return res.status(400).json({ error: 'assetId is required' });
  }
  if (duration === undefined || duration === null || isNaN(parseFloat(duration))) {
    return res.status(400).json({ error: 'duration (total video seconds) is required' });
  }
  if (!['custom', 'first', 'last'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be: custom | first | last' });
  }

  const totalDuration = parseFloat(duration);
  if (totalDuration <= 0) {
    return res.status(400).json({ error: 'duration must be greater than 0' });
  }

  let start, end;

  // ── Compute start / end from mode ─────────────────────────────────────────
  if (mode === 'custom') {
    if (startTime == null) return res.status(400).json({ error: 'startTime is required for custom mode' });
    if (endTime   == null) return res.status(400).json({ error: 'endTime is required for custom mode' });
    try {
      start = parseTimeInput(startTime);
      end   = parseTimeInput(endTime);
    } catch (e) {
      return res.status(400).json({ error: e.message });
    }

  } else if (mode === 'first') {
    const mins = parseFloat(minutes);
    if (!minutes || isNaN(mins) || mins <= 0) {
      return res.status(400).json({ error: 'minutes must be a positive number for first mode' });
    }
    start = 0;
    end   = Math.min(mins * 60, totalDuration);

  } else {
    // last
    const mins = parseFloat(minutes);
    if (!minutes || isNaN(mins) || mins <= 0) {
      return res.status(400).json({ error: 'minutes must be a positive number for last mode' });
    }
    start = Math.max(0, totalDuration - mins * 60);
    end   = totalDuration;
  }

  // ── Cross-validate boundaries ─────────────────────────────────────────────
  if (start < 0) {
    return res.status(400).json({ error: 'Start time cannot be negative' });
  }
  // 0.5s float tolerance — Mux duration values have minor imprecision
  if (end > totalDuration + 0.5) {
    return res.status(400).json({
      error: `End time ${formatDuration(end)} exceeds video duration ${formatDuration(totalDuration)}`,
    });
  }
  if (end <= start) {
    return res.status(400).json({
      error: `End time (${formatDuration(end)}) must be after start time (${formatDuration(start)})`,
    });
  }
  if ((end - start) < 1) {
    return res.status(400).json({ error: 'Clip must be at least 1 second long' });
  }

  // Clamp end to exact duration to prevent Mux boundary rejections
  end = Math.min(end, totalDuration);

  // ── Create the Mux clip asset ─────────────────────────────────────────────
  let clip;
  try {
    clip = await video.assets.create({
      input: [{
        url:        `mux-asset://${assetId}`,
        start_time: parseFloat(start.toFixed(3)),
        end_time:   parseFloat(end.toFixed(3)),
      }],
      playback_policy: ['public'],
      mp4_support:     'standard',
    });
  } catch (err) {
    const msg = muxErrorMessage(err);
    console.error(`[clip] Mux error: ${msg}`);
    if (err?.status === 404) {
      return res.status(404).json({ error: `Source asset not found: ${assetId}` });
    }
    return res.status(502).json({ error: `Mux clip creation failed: ${msg}` });
  }

  const clipDuration = end - start;

  res.status(201).json({
    clipAssetId:           clip.id,
    mode,
    clipStart:             start,
    clipEnd:               end,
    clipDuration,
    clipStartFormatted:    formatDuration(start),
    clipEndFormatted:      formatDuration(end),
    clipDurationFormatted: formatDuration(clipDuration),
    clipDurationHuman:     formatHuman(clipDuration),
    message: `Clip queued: ${formatDuration(start)} → ${formatDuration(end)} (${formatHuman(clipDuration)})`,
  });
}));

// ── GET /api/clip/:clipAssetId ────────────────────────────────────────────────
/**
 * Polls clip processing status. Call every 5 seconds until status = 'ready'.
 *
 * Response:
 *   status       — 'preparing' | 'ready' | 'errored'
 *   playbackId   — Mux playback ID
 *   streamUrl    — HLS URL for inline preview player
 *   downloadUrl  — MP4 URL (populated when ready)
 *   thumbnailUrl — JPG thumbnail
 */
app.get('/api/clip/:clipAssetId', asyncHandler(async (req, res) => {
  const { clipAssetId } = req.params;

  if (!clipAssetId?.trim()) {
    return res.status(400).json({ error: 'clipAssetId is required' });
  }

  let asset;
  try {
    asset = await video.assets.retrieve(clipAssetId);
  } catch (err) {
    if (err?.status === 404) {
      return res.status(404).json({ error: `Clip asset not found: ${clipAssetId}` });
    }
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  if (asset.status === 'errored') {
    return res.status(422).json({
      status: 'errored',
      error:  'Clip processing failed in Mux. Try creating the clip again.',
    });
  }

  const playbackId  = asset.playback_ids?.[0]?.id ?? null;
  const downloadUrl = asset.status === 'ready'
    ? buildDownloadUrl(playbackId, asset.static_renditions)
    : null;

  res.json({
    status:       asset.status,
    playbackId,
    streamUrl:    playbackId ? `https://stream.mux.com/${playbackId}.m3u8`             : null,
    downloadUrl,
    thumbnailUrl: playbackId ? `https://image.mux.com/${playbackId}/thumbnail.jpg`      : null,
  });
}));

// ── DELETE /api/asset/:assetId ────────────────────────────────────────────────
/**
 * Deletes a single Mux asset — works for both source videos and clips.
 * A 404 from Mux is treated as a success (asset already gone).
 *
 * Response: { deleted: true, assetId }
 */
app.delete('/api/asset/:assetId', asyncHandler(async (req, res) => {
  const { assetId } = req.params;

  if (!assetId?.trim()) {
    return res.status(400).json({ error: 'assetId is required' });
  }

  try {
    await video.assets.delete(assetId);
  } catch (err) {
    if (err?.status === 404) {
      // Idempotent — already gone is fine
      return res.json({ deleted: true, assetId, note: 'Asset already deleted or not found' });
    }
    const msg = muxErrorMessage(err);
    console.error(`[delete] Mux error deleting ${assetId}: ${msg}`);
    return res.status(502).json({ error: `Mux delete failed: ${msg}` });
  }

  res.json({ deleted: true, assetId });
}));

// ── DELETE /api/assets/batch ──────────────────────────────────────────────────
/**
 * Deletes multiple assets in one request (used by "Delete All Clips").
 * Processes sequentially to be kind to the Mux API.
 * 404s are treated as success (idempotent).
 *
 * Request body:
 *   { assetIds: string[] }   — max MAX_CLIP_BATCH (default 20)
 *
 * Response:
 *   {
 *     results:      [ { assetId, deleted, note?, error? } ]
 *     deletedCount: number
 *     failedCount:  number
 *   }
 */
app.delete('/api/assets/batch', asyncHandler(async (req, res) => {
  const { assetIds } = req.body;
  const maxBatch     = parseInt(MAX_CLIP_BATCH, 10);

  if (!Array.isArray(assetIds) || assetIds.length === 0) {
    return res.status(400).json({ error: 'assetIds must be a non-empty array' });
  }
  if (assetIds.length > maxBatch) {
    return res.status(400).json({ error: `Cannot delete more than ${maxBatch} assets per request` });
  }
  if (assetIds.some(id => typeof id !== 'string' || !id.trim())) {
    return res.status(400).json({ error: 'All assetIds must be non-empty strings' });
  }

  const results = [];

  for (const assetId of assetIds) {
    try {
      await video.assets.delete(assetId);
      results.push({ assetId, deleted: true });
    } catch (err) {
      if (err?.status === 404) {
        results.push({ assetId, deleted: true, note: 'Already deleted or not found' });
      } else {
        const msg = muxErrorMessage(err);
        console.error(`[batch-delete] Failed ${assetId}: ${msg}`);
        results.push({ assetId, deleted: false, error: msg });
      }
    }
  }

  res.json({
    results,
    deletedCount: results.filter(r =>  r.deleted).length,
    failedCount:  results.filter(r => !r.deleted).length,
  });
}));

// ─────────────────────────────────────────────────────────────────────────────
// 404 HANDLER
// ─────────────────────────────────────────────────────────────────────────────

app.use((req, res) => {
  res.status(404).json({
    error: `Route not found: ${req.method} ${req.path}`,
    routes: [
      'GET    /health',
      'POST   /api/upload-url',
      'GET    /api/asset/:uploadId',
      'POST   /api/clip',
      'GET    /api/clip/:clipAssetId',
      'DELETE /api/asset/:assetId',
      'DELETE /api/assets/batch',
    ],
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// GLOBAL ERROR HANDLER
// ─────────────────────────────────────────────────────────────────────────────

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, _next) => {
  const statusCode = err.status ?? err.statusCode ?? 500;
  const message    = err.message ?? 'Internal server error';

  console.error(
    `[${new Date().toISOString()}] [ERROR] [${(req.id ?? '').slice(0, 8)}] ` +
    `${req.method} ${req.path} → ${statusCode}: ${message}`
  );
  if (!IS_PROD) console.error(err.stack);

  res.status(statusCode).json({
    error: message,
    ...(!IS_PROD && { stack: err.stack }),
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// SERVER START + GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────────────────────

const server = app.listen(PORT, () => {
  console.log('');
  console.log('  ╔══════════════════════════════════════════╗');
  console.log(`  ║  ClipForge API  ·  port ${String(PORT).padEnd(17)}║`);
  console.log(`  ║  env  : ${NODE_ENV.padEnd(33)}║`);
  console.log(`  ║  CORS : ${String(ALLOWED_ORIGIN).slice(0, 33).padEnd(33)}║`);
  console.log(`  ║  rate : ${RATE_LIMIT_MAX.padEnd(4)} req / 15 min / IP           ║`);
  console.log('  ╚══════════════════════════════════════════╝');
  console.log('');
});

/**
 * Graceful shutdown — drains in-flight requests before exiting.
 * Railway sends SIGTERM before each deploy; this prevents dropped requests.
 */
function gracefulShutdown(signal) {
  console.log(`\n[SHUTDOWN] ${signal} — closing server...`);
  server.close((err) => {
    if (err) {
      console.error('[SHUTDOWN] Close error:', err.message);
      process.exit(1);
    }
    console.log('[SHUTDOWN] All connections closed. Goodbye.');
    process.exit(0);
  });
  // Force-exit after 10s if connections stall
  setTimeout(() => {
    console.error('[SHUTDOWN] Force exit after 10s timeout.');
    process.exit(1);
  }, 10_000).unref();
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

// Log unhandled rejections but don't crash — keeps the server alive
process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});
