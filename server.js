/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║              ClipForge — Mux Video Clip API (Production)                ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  Stack:   Node.js 18+ · Express 4 · @mux/mux-node · ESM                ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  VOD Endpoints:                                                          ║
 * ║    GET    /health                    → Uptime + Mux credentials check    ║
 * ║    GET    /api/assets                → List all Mux assets               ║
 * ║    POST   /api/upload-url            → Get Mux direct upload URL         ║
 * ║    GET    /api/asset/:uploadId       → Poll asset status after upload     ║
 * ║    POST   /api/clip                  → Create clip (custom|first|last)    ║
 * ║    GET    /api/clip/:clipAssetId     → Poll clip status + download URL    ║
 * ║    DELETE /api/asset/:assetId        → Delete one asset (clip or source)  ║
 * ║    DELETE /api/assets/batch          → Delete multiple assets at once     ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║  Live Stream Endpoints:                                                  ║
 * ║    POST   /api/livestream            → Create a new Mux live stream      ║
 * ║    GET    /api/livestream/:id        → Get stream status + ingest URL     ║
 * ║    POST   /api/livestream/:id/end    → End / disable a live stream        ║
 * ║    DELETE /api/livestream/:id        → Delete stream + recording asset    ║
 * ║    GET    /api/livestream/:id/window → Get rolling DVR window info        ║
 * ║    POST   /api/livestream/:id/clip   → Clip from live stream recording    ║
 * ║    GET    /api/livestreams           → List all live streams              ║
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
 * ║    DVR_WINDOW_MINS    — rolling DVR window in minutes, default 60        ║
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
  DVR_WINDOW_MINS = '60',
} = process.env;

const IS_PROD = NODE_ENV === 'production';

if (!MUX_TOKEN_ID || !MUX_TOKEN_SECRET) {
  console.error('[BOOT] ❌  MUX_TOKEN_ID and MUX_TOKEN_SECRET are required.');
  console.error('[BOOT]     Set them in your Railway / Render environment variables.');
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// IN-MEMORY LIVE STREAM REGISTRY
// Tracks active streams + their rolling window config across requests.
// On server restart this resets — Mux is the source of truth for stream state.
// ─────────────────────────────────────────────────────────────────────────────

const streamRegistry = new Map();
// Entry shape:
// {
//   streamId:       string,   — Mux live stream ID
//   playbackId:     string,
//   ingestUrl:      string,   — rtmps://global-live.mux.com:443/app/{streamKey}
//   streamKey:      string,
//   startedAt:      Date,
//   windowMins:     number,   — rolling window setting (how many mins to keep)
//   status:         'idle' | 'active' | 'ended',
//   recordingAssetId: null | string,  — set when stream ends
// }

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
  // capped-1080p produces: capped-1080p.mp4, 720p.mp4, 480p.mp4, 360p.mp4
  const preferred = ['capped-1080p.mp4', '720p.mp4', '480p.mp4', '360p.mp4', 'high.mp4'];
  for (const name of preferred) {
    if (files.some(f => f.name === name)) {
      return `https://stream.mux.com/${playbackId}/${name}`;
    }
  }
  if (files.length > 0) {
    return `https://stream.mux.com/${playbackId}/${files[0].name}`;
  }
  // Fallback — Mux serves this once mp4_support processing finishes
  return `https://stream.mux.com/${playbackId}/capped-1080p.mp4`;
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
        video_quality:   'plus',   // enables MP4 static renditions for download
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
  console.log('[clip] Request body:', JSON.stringify(req.body));
  const { assetId, playbackId, duration, mode, startTime, endTime, minutes } = req.body;

  // ── Validate required fields ──────────────────────────────────────────────
  if (!assetId?.trim()) {
    return res.status(400).json({ error: 'assetId is required' });
  }
  if (!duration === undefined || duration === null || isNaN(parseFloat(duration))) {
    return res.status(400).json({ error: 'duration (total video seconds) is required' });
  }
  if (!['custom', 'first', 'last'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be: custom | first | last' });
  }

  // playbackId required for HLS-based clipping
  // If not sent by frontend, attempt to look it up from Mux
  let resolvedPlaybackId = playbackId;
  if (!resolvedPlaybackId?.trim()) {
    console.log('[clip] playbackId missing — looking up from assetId:', assetId);
    try {
      const asset = await video.assets.retrieve(assetId);
      resolvedPlaybackId = asset.playback_ids?.[0]?.id ?? null;
      console.log('[clip] Resolved playbackId:', resolvedPlaybackId);
    } catch (err) {
      console.error('[clip] Could not resolve playbackId:', muxErrorMessage(err));
    }
    if (!resolvedPlaybackId) {
      return res.status(400).json({ error: 'playbackId is required and could not be resolved from assetId' });
    }
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
        url:        `mux://assets/${assetId}`,
        start_time: parseFloat(start.toFixed(3)),
        end_time:   parseFloat(end.toFixed(3)),
      }],
      playback_policy: ['public'],
      video_quality:   'plus',     // 'basic' | 'plus' — needed for mp4 download
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

// ── GET /api/assets ───────────────────────────────────────────────────────────
/**
 * Lists all assets in the Mux environment that are not deleted.
 * Paginates automatically — returns up to 100 assets per call.
 * Supports optional ?limit= and ?page= query params.
 *
 * Response:
 *   {
 *     assets: [
 *       {
 *         assetId:           string,
 *         status:            'preparing' | 'ready' | 'errored',
 *         duration:          number | null,
 *         durationFormatted: string | null,
 *         durationHuman:     string | null,
 *         playbackId:        string | null,
 *         streamUrl:         string | null,
 *         thumbnailUrl:      string | null,
 *         createdAt:         string,   ← ISO timestamp
 *         mp4Support:        string | null,
 *         sourceAssetId:     string | null,  ← set if this is a clip
 *         isClip:            boolean,
 *       }
 *     ],
 *     total: number,
 *     page:  number,
 *     limit: number,
 *   }
 */
app.get('/api/assets', asyncHandler(async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit ?? '100', 10), 100);
  const page  = Math.max(parseInt(req.query.page  ?? '1',   10), 1);

  let assetList;
  try {
    assetList = await video.assets.list({ limit, page });
  } catch (err) {
    const msg = muxErrorMessage(err);
    console.error(`[assets] Mux error listing assets: ${msg}`);
    return res.status(502).json({ error: `Mux error: ${msg}` });
  }

  const assets = (assetList.data ?? assetList ?? []).map(asset => {
    const playbackId = asset.playback_ids?.[0]?.id ?? null;
    return {
      assetId:           asset.id,
      status:            asset.status,
      duration:          asset.duration          ?? null,
      durationFormatted: asset.duration != null   ? formatDuration(asset.duration) : null,
      durationHuman:     asset.duration != null   ? formatHuman(asset.duration)    : null,
      playbackId,
      streamUrl:         playbackId ? `https://stream.mux.com/${playbackId}.m3u8`          : null,
      thumbnailUrl:      playbackId ? `https://image.mux.com/${playbackId}/thumbnail.jpg`   : null,
      createdAt:         asset.created_at
                           ? new Date(parseInt(asset.created_at, 10) * 1000).toISOString()
                           : null,
      mp4Support:        asset.mp4_support        ?? null,
      sourceAssetId:     asset.source_asset_id    ?? null,
      isClip:            !!asset.source_asset_id,
    };
  });

  res.json({
    assets,
    total: assets.length,
    page,
    limit,
  });
}));



// ═════════════════════════════════════════════════════════════════════════════
// LIVE STREAM ROUTES
// ═════════════════════════════════════════════════════════════════════════════

// ── POST /api/livestream ──────────────────────────────────────────────────────
/**
 * Creates a new Mux live stream with DVR enabled.
 * DVR lets viewers rewind and clip from the live recording in real time.
 *
 * Request body (all optional):
 *   windowMins  number  — rolling window to keep in minutes (default: DVR_WINDOW_MINS)
 *   name        string  — friendly label stored in registry only
 *
 * Response:
 *   {
 *     streamId:   string,   — Mux live stream ID
 *     streamKey:  string,   — RTMP stream key
 *     ingestUrl:  string,   — full RTMPS URL for OBS / streaming software
 *     playbackId: string,   — for the HLS player
 *     streamUrl:  string,   — HLS .m3u8 URL
 *     windowMins: number,
 *     status:     'idle',
 *   }
 */
app.post('/api/livestream', asyncHandler(async (req, res) => {
  const windowMins = parseFloat(req.body.windowMins ?? DVR_WINDOW_MINS);
  const name       = req.body.name?.trim() ?? 'Live Stream';

  if (isNaN(windowMins) || windowMins <= 0) {
    return res.status(400).json({ error: 'windowMins must be a positive number' });
  }

  let stream;
  try {
    stream = await video.liveStreams.create({
      playback_policy:     ['public'],
      new_asset_settings:  {
        playback_policy: ['public'],
        video_quality:   'plus',
      },
      reduced_latency:     false,
      reconnect_window:    60,          // seconds to wait for reconnect
      max_continuous_duration: Math.min(windowMins * 60 * 10, 43200), // Mux max 12hr
    });
  } catch (err) {
    const msg = muxErrorMessage(err);
    console.error(`[livestream] Mux error creating stream: ${msg}`);
    return res.status(502).json({ error: `Mux stream creation failed: ${msg}` });
  }

  const playbackId = stream.playback_ids?.[0]?.id ?? null;
  const streamKey  = stream.stream_key;
  const ingestUrl  = `rtmps://global-live.mux.com:443/app/${streamKey}`;

  // Register in memory
  streamRegistry.set(stream.id, {
    streamId:          stream.id,
    playbackId,
    ingestUrl,
    streamKey,
    name,
    startedAt:         null,
    windowMins,
    status:            'idle',
    recordingAssetId:  null,
  });

  console.log(`[livestream] Created stream ${stream.id} — window: ${windowMins} min`);

  res.status(201).json({
    streamId:   stream.id,
    streamKey,
    ingestUrl,
    playbackId,
    streamUrl:  playbackId ? `https://stream.mux.com/${playbackId}.m3u8` : null,
    windowMins,
    status:     'idle',
    name,
  });
}));

// ── GET /api/livestream/:id ───────────────────────────────────────────────────
/**
 * Gets the current status of a live stream.
 * Merges Mux API status with in-memory registry data.
 *
 * Response:
 *   {
 *     streamId, streamKey, ingestUrl, playbackId, streamUrl,
 *     status:     'idle' | 'active' | 'ended' | 'disabled',
 *     windowMins, startedAt, elapsedSecs, elapsedFormatted,
 *     recordingAssetId, name,
 *     dvr: {
 *       windowMins,
 *       windowStart: number,   ← seconds from stream start to clip from
 *       windowEnd:   number,   ← current elapsed seconds
 *     }
 *   }
 */
app.get('/api/livestream/:id', asyncHandler(async (req, res) => {
  const { id } = req.params;

  let stream;
  try {
    stream = await video.liveStreams.retrieve(id);
  } catch (err) {
    if (err?.status === 404) {
      return res.status(404).json({ error: `Live stream not found: ${id}` });
    }
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  const reg        = streamRegistry.get(id) ?? {};
  const playbackId = stream.playback_ids?.[0]?.id ?? null;
  const now        = Date.now();
  const startedAt  = reg.startedAt ?? null;
  const elapsedSecs = startedAt ? Math.floor((now - startedAt.getTime()) / 1000) : 0;
  const windowMins  = reg.windowMins ?? parseFloat(DVR_WINDOW_MINS);

  // DVR rolling window: user can only clip within the last windowMins
  const windowSecs  = windowMins * 60;
  const windowStart = Math.max(0, elapsedSecs - windowSecs);
  const windowEnd   = elapsedSecs;

  // Sync status from Mux into registry
  if (reg.status !== 'ended') {
    const muxStatus = stream.status; // 'idle' | 'active' | 'disabled'
    if (muxStatus === 'active' && reg.status !== 'active') {
      reg.startedAt = reg.startedAt ?? new Date();
      reg.status    = 'active';
      streamRegistry.set(id, reg);
    }
  }

  res.json({
    streamId:          stream.id,
    streamKey:         stream.stream_key,
    ingestUrl:         reg.ingestUrl ?? `rtmps://global-live.mux.com:443/app/${stream.stream_key}`,
    playbackId,
    streamUrl:         playbackId ? `https://stream.mux.com/${playbackId}.m3u8` : null,
    status:            reg.status ?? stream.status,
    name:              reg.name   ?? 'Live Stream',
    windowMins,
    startedAt:         startedAt?.toISOString() ?? null,
    elapsedSecs,
    elapsedFormatted:  formatDuration(elapsedSecs),
    recordingAssetId:  reg.recordingAssetId ?? null,
    dvr: {
      windowMins,
      windowStart,
      windowEnd,
      windowStartFormatted: formatDuration(windowStart),
      windowEndFormatted:   formatDuration(windowEnd),
    },
  });
}));

// ── POST /api/livestream/:id/end ──────────────────────────────────────────────
/**
 * Ends (disables) a live stream. Mux will finalize the recording asset.
 * Poll GET /api/livestream/:id/recording until recordingAssetId is set.
 *
 * Response: { streamId, status: 'ended', message }
 */
app.post('/api/livestream/:id/end', asyncHandler(async (req, res) => {
  const { id } = req.params;

  try {
    await video.liveStreams.disable(id);
  } catch (err) {
    if (err?.status === 404) {
      return res.status(404).json({ error: `Live stream not found: ${id}` });
    }
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  // Update registry
  const reg = streamRegistry.get(id);
  if (reg) {
    reg.status = 'ended';
    streamRegistry.set(id, reg);
  }

  console.log(`[livestream] Stream ${id} ended`);

  res.json({
    streamId: id,
    status:   'ended',
    message:  'Stream ended. Recording asset will be available shortly.',
  });
}));

// ── GET /api/livestream/:id/recording ─────────────────────────────────────────
/**
 * Polls for the VOD recording asset after a stream ends.
 * Call every 5 seconds until recordingAssetId is returned.
 *
 * Response:
 *   {
 *     ready:             boolean,
 *     recordingAssetId:  string | null,
 *     playbackId:        string | null,
 *     streamUrl:         string | null,
 *     duration:          number | null,
 *     durationFormatted: string | null,
 *   }
 */
app.get('/api/livestream/:id/recording', asyncHandler(async (req, res) => {
  const { id } = req.params;

  let stream;
  try {
    stream = await video.liveStreams.retrieve(id);
  } catch (err) {
    if (err?.status === 404) {
      return res.status(404).json({ error: `Live stream not found: ${id}` });
    }
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  // Mux stores the recording asset ID on the live stream object
  const recordingAssetId = stream.active_asset_id
                        ?? stream.recent_asset_ids?.[0]
                        ?? null;

  if (!recordingAssetId) {
    return res.json({ ready: false, recordingAssetId: null });
  }

  // Update registry
  const reg = streamRegistry.get(id);
  if (reg && !reg.recordingAssetId) {
    reg.recordingAssetId = recordingAssetId;
    streamRegistry.set(id, reg);
  }

  // Fetch the recording asset for full details
  let asset;
  try {
    asset = await video.assets.retrieve(recordingAssetId);
  } catch {
    return res.json({ ready: false, recordingAssetId });
  }

  const playbackId = asset.playback_ids?.[0]?.id ?? null;

  res.json({
    ready:             asset.status === 'ready',
    status:            asset.status,
    recordingAssetId,
    playbackId,
    streamUrl:         playbackId ? `https://stream.mux.com/${playbackId}.m3u8` : null,
    duration:          asset.duration         ?? null,
    durationFormatted: asset.duration != null  ? formatDuration(asset.duration) : null,
    durationHuman:     asset.duration != null  ? formatHuman(asset.duration)    : null,
  });
}));

// ── GET /api/livestream/:id/window ────────────────────────────────────────────
/**
 * Returns the current rolling DVR window — what time range is available to clip.
 * The window shifts forward as the stream runs; only the last windowMins is clippable.
 *
 * Response:
 *   {
 *     streamId, windowMins, elapsedSecs,
 *     windowStart, windowEnd,        ← seconds from stream start
 *     windowStartFormatted,
 *     windowEndFormatted,
 *     elapsedFormatted,
 *     clippableRange: { start, end } ← same as window
 *   }
 */
app.get('/api/livestream/:id/window', asyncHandler(async (req, res) => {
  const { id } = req.params;
  const reg    = streamRegistry.get(id);

  if (!reg) {
    return res.status(404).json({ error: `Stream ${id} not found in registry. Reload the page.` });
  }

  const now         = Date.now();
  const startedAt   = reg.startedAt;
  const elapsedSecs = startedAt ? Math.floor((now - startedAt.getTime()) / 1000) : 0;
  const windowSecs  = reg.windowMins * 60;
  const windowStart = Math.max(0, elapsedSecs - windowSecs);
  const windowEnd   = elapsedSecs;

  res.json({
    streamId:             id,
    windowMins:           reg.windowMins,
    elapsedSecs,
    elapsedFormatted:     formatDuration(elapsedSecs),
    windowStart,
    windowEnd,
    windowStartFormatted: formatDuration(windowStart),
    windowEndFormatted:   formatDuration(windowEnd),
    clippableRange:       { start: windowStart, end: windowEnd },
  });
}));

// ── PATCH /api/livestream/:id/window ─────────────────────────────────────────
/**
 * Updates the rolling window size for an active stream.
 * Takes effect immediately — clips will use the new window on next request.
 *
 * Request body: { windowMins: number }
 * Response: { streamId, windowMins, message }
 */
app.patch('/api/livestream/:id/window', asyncHandler(async (req, res) => {
  const { id }     = req.params;
  const windowMins = parseFloat(req.body.windowMins);

  if (isNaN(windowMins) || windowMins <= 0) {
    return res.status(400).json({ error: 'windowMins must be a positive number' });
  }

  const reg = streamRegistry.get(id);
  if (!reg) {
    return res.status(404).json({ error: `Stream ${id} not found in registry` });
  }

  reg.windowMins = windowMins;
  streamRegistry.set(id, reg);

  res.json({
    streamId:   id,
    windowMins,
    message:    `Rolling window updated to ${windowMins} minutes`,
  });
}));

// ── POST /api/livestream/:id/clip ─────────────────────────────────────────────
/**
 * Creates a clip from a live stream recording.
 * Works during the stream (if recording asset exists) or after it ends.
 *
 * Three modes — same as VOD clipping but times are relative to stream start:
 *   custom — exact start/end in seconds from stream start
 *   last   — last X minutes of current stream
 *   first  — first X minutes of stream
 *
 * Request body:
 *   {
 *     recordingAssetId: string,   — from GET /api/livestream/:id/recording
 *     mode:             'custom' | 'last' | 'first',
 *     startTime?:       number,   — seconds from stream start (custom mode)
 *     endTime?:         number,
 *     minutes?:         number,   — for first/last mode
 *     totalDuration?:   number,   — required for first/last if stream still live
 *   }
 *
 * Response: same shape as POST /api/clip
 */
app.post('/api/livestream/:id/clip', asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { recordingAssetId, mode, startTime, endTime, minutes, totalDuration } = req.body;

  console.log(`[livestream-clip] Stream ${id} body:`, JSON.stringify(req.body));

  if (!recordingAssetId?.trim()) {
    return res.status(400).json({ error: 'recordingAssetId is required' });
  }
  if (!['custom', 'first', 'last'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be: custom | first | last' });
  }

  // Get total duration — from request body or fetch from Mux
  let duration = parseFloat(totalDuration ?? 0);
  if (!duration) {
    try {
      const asset = await video.assets.retrieve(recordingAssetId);
      duration    = asset.duration ?? 0;
    } catch (err) {
      return res.status(502).json({ error: `Could not fetch recording duration: ${muxErrorMessage(err)}` });
    }
  }

  const reg = streamRegistry.get(id);

  // For live streams still running — use elapsed time as duration
  if (!duration && reg?.startedAt) {
    duration = Math.floor((Date.now() - reg.startedAt.getTime()) / 1000);
  }

  let start, end;

  if (mode === 'custom') {
    try {
      start = parseTimeInput(startTime);
      end   = parseTimeInput(endTime);
    } catch (e) {
      return res.status(400).json({ error: e.message });
    }
  } else if (mode === 'first') {
    const mins = parseFloat(minutes);
    if (!mins || isNaN(mins) || mins <= 0) {
      return res.status(400).json({ error: 'minutes must be positive for first mode' });
    }
    start = 0;
    end   = Math.min(mins * 60, duration);
  } else {
    // last
    const mins = parseFloat(minutes);
    if (!mins || isNaN(mins) || mins <= 0) {
      return res.status(400).json({ error: 'minutes must be positive for last mode' });
    }
    // Enforce rolling window — can't clip further back than windowMins
    const windowSecs = (reg?.windowMins ?? parseFloat(DVR_WINDOW_MINS)) * 60;
    const maxLookback = Math.min(mins * 60, windowSecs);
    start = Math.max(0, duration - maxLookback);
    end   = duration;
  }

  // Validate
  if (end <= start) {
    return res.status(400).json({ error: `End (${formatDuration(end)}) must be after start (${formatDuration(start)})` });
  }
  if ((end - start) < 1) {
    return res.status(400).json({ error: 'Clip must be at least 1 second long' });
  }

  let clip;
  try {
    clip = await video.assets.create({
      input: [{
        url:        `mux://assets/${recordingAssetId}`,
        start_time: parseFloat(start.toFixed(3)),
        end_time:   parseFloat(end.toFixed(3)),
      }],
      playback_policy: ['public'],
      video_quality:   'plus',
    });
  } catch (err) {
    const msg = muxErrorMessage(err);
    console.error(`[livestream-clip] Mux error: ${msg}`);
    if (err?.status === 404) {
      return res.status(404).json({ error: `Recording asset not found: ${recordingAssetId}` });
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
    sourceStreamId:        id,
    recordingAssetId,
    message: `Live clip: ${formatDuration(start)} → ${formatDuration(end)} (${formatHuman(clipDuration)})`,
  });
}));

// ── GET /api/livestreams ──────────────────────────────────────────────────────
/**
 * Lists all live streams from Mux, merged with in-memory registry data.
 *
 * Response: { streams: [...], total: number }
 */
app.get('/api/livestreams', asyncHandler(async (req, res) => {
  let list;
  try {
    list = await video.liveStreams.list({ limit: 50 });
  } catch (err) {
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  const streams = (list.data ?? list ?? []).map(stream => {
    const reg        = streamRegistry.get(stream.id) ?? {};
    const playbackId = stream.playback_ids?.[0]?.id ?? null;
    return {
      streamId:         stream.id,
      status:           reg.status ?? stream.status,
      name:             reg.name   ?? 'Live Stream',
      playbackId,
      streamUrl:        playbackId ? `https://stream.mux.com/${playbackId}.m3u8` : null,
      ingestUrl:        reg.ingestUrl ?? null,
      windowMins:       reg.windowMins ?? parseFloat(DVR_WINDOW_MINS),
      startedAt:        reg.startedAt?.toISOString() ?? null,
      recordingAssetId: reg.recordingAssetId ?? stream.active_asset_id ?? null,
      createdAt:        stream.created_at
                          ? new Date(parseInt(stream.created_at, 10) * 1000).toISOString()
                          : null,
    };
  });

  res.json({ streams, total: streams.length });
}));

// ── DELETE /api/livestream/:id ────────────────────────────────────────────────
/**
 * Deletes a Mux live stream and removes it from the registry.
 * Does NOT delete the recording asset — use DELETE /api/asset/:assetId for that.
 *
 * Response: { deleted: true, streamId }
 */
app.delete('/api/livestream/:id', asyncHandler(async (req, res) => {
  const { id } = req.params;

  try {
    await video.liveStreams.delete(id);
  } catch (err) {
    if (err?.status === 404) {
      streamRegistry.delete(id);
      return res.json({ deleted: true, streamId: id, note: 'Already deleted or not found' });
    }
    return res.status(502).json({ error: `Mux error: ${muxErrorMessage(err)}` });
  }

  streamRegistry.delete(id);
  console.log(`[livestream] Deleted stream ${id}`);

  res.json({ deleted: true, streamId: id });
}));


  res.status(404).json({
    error: `Route not found: ${req.method} ${req.path}`,
    routes: [
      'GET    /health',
      'GET    /api/assets',
      'POST   /api/upload-url',
      'GET    /api/asset/:uploadId',
      'POST   /api/clip',
      'GET    /api/clip/:clipAssetId',
      'DELETE /api/asset/:assetId',
      'DELETE /api/assets/batch',
      'POST   /api/livestream',
      'GET    /api/livestream/:id',
      'POST   /api/livestream/:id/end',
      'GET    /api/livestream/:id/recording',
      'GET    /api/livestream/:id/window',
      'PATCH  /api/livestream/:id/window',
      'POST   /api/livestream/:id/clip',
      'DELETE /api/livestream/:id',
      'GET    /api/livestreams',
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
