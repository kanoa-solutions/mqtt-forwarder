/* eslint-disable no-console */
const mqtt = require('mqtt');
const axios = require('axios');
const pino = require('pino');
require('dotenv').config();

const {
  MQTT_HOST,
  MQTT_PORT = 8883,
  MQTT_USERNAME,
  MQTT_PASSWORD,
  MQTT_TOPIC = 'GwData/#',
  CLIENT_ID = 'forwarder-1',
  QOS = '1',
  SUPABASE_FUNCTION_URL,
  SUPABASE_AUTH,
  LOG_LEVEL = 'info',
  BEACON_WHITELIST, // optional bootstrap list (comma-separated normalized MACs)
  SEND_INTERVAL_SECONDS = '60', // throttle interval per beacon
  MIN_TEMP_DELTA = '0.3',       // immediate forward if change >= delta (in °C)
} = process.env;

// Robust log level handling with fallback to 'info'
const VALID_LEVELS = new Set(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']);
const configuredLevel = String(LOG_LEVEL || 'info').toLowerCase();
const resolvedLevel = VALID_LEVELS.has(configuredLevel) ? configuredLevel : 'info';
const log = pino({ level: resolvedLevel });

if (!MQTT_HOST || !MQTT_USERNAME || !MQTT_PASSWORD || !SUPABASE_FUNCTION_URL || !SUPABASE_AUTH) {
  log.fatal('Missing required environment variables.');
  process.exit(1);
}

const SEND_INTERVAL_MS = Math.max(0, Number(SEND_INTERVAL_SECONDS)) * 1000;
const TEMP_DELTA = Math.max(0, Number(MIN_TEMP_DELTA));

// Normalization and allowlist helpers
const normalizeMac = (mac) => String(mac || '').toLowerCase().replace(/:/g, '');
const envWhitelist = new Set(
  String(BEACON_WHITELIST || '')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);
let allowlistSet = new Set(envWhitelist); // will be replaced by DB sync when available

// Per-beacon throttle state
const lastSentByBeacon = new Map(); // normMac -> { tsMs: number, temp: number }

// Derive Supabase REST base from the Edge Function URL
const REST_BASE = (() => {
  try {
    const i = SUPABASE_FUNCTION_URL.indexOf('/functions/');
    if (i > 0) return SUPABASE_FUNCTION_URL.slice(0, i) + '/rest/v1';
  } catch (_) {}
  return null;
})();

async function syncAllowlistFromDB() {
  if (!REST_BASE) return;
  try {
    const res = await axios.get(`${REST_BASE}/beacons?select=normalized_mac,is_active`, {
      headers: {
        apikey: SUPABASE_AUTH,
        Authorization: `Bearer ${SUPABASE_AUTH}`,
      },
      timeout: 5000,
    });
    const rows = Array.isArray(res.data) ? res.data : [];
    const next = new Set(rows.filter(r => r.is_active !== false).map(r => String(r.normalized_mac || '').toLowerCase()).filter(Boolean));
    // Merge in any env whitelist entries
    for (const m of envWhitelist) next.add(m);
    allowlistSet = next;
    log.info({ size: allowlistSet.size }, 'Synced beacon allowlist from DB');
  } catch (err) {
    log.warn({ err: err.response?.data || err.message }, 'Allowlist sync failed; keeping previous set');
  }
}

function shouldForwardMac(rawMac) {
  const norm = normalizeMac(rawMac);
  if (allowlistSet.size === 0) return true; // fail-open until first sync
  return allowlistSet.has(norm);
}

function shouldSendNow(rawMac, temperature) {
  const norm = normalizeMac(rawMac);
  const now = Date.now();
  const prev = lastSentByBeacon.get(norm);
  if (!prev) return true;
  const age = now - prev.tsMs;
  const delta = typeof temperature === 'number' && typeof prev.temp === 'number'
    ? Math.abs(temperature - prev.temp)
    : 0;
  if (delta >= TEMP_DELTA) return true;
  return age >= SEND_INTERVAL_MS;
}

function markSent(rawMac, temperature) {
  const norm = normalizeMac(rawMac);
  lastSentByBeacon.set(norm, { tsMs: Date.now(), temp: temperature });
}

// Initial sync and periodic refresh
(async () => {
  await syncAllowlistFromDB();
  setInterval(syncAllowlistFromDB, 60_000);
})();

// Build MQTT URL (TLS)
const mqttUrl = `mqtts://${MQTT_HOST}:${MQTT_PORT}`;

const client = mqtt.connect(mqttUrl, {
  clientId: CLIENT_ID,
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  protocol: 'mqtts',
  reconnectPeriod: 2000,
  clean: true,
  keepalive: 60,
});

client.on('connect', () => {
  log.info({ host: MQTT_HOST, port: MQTT_PORT }, 'MQTT connected');
  client.subscribe(MQTT_TOPIC, { qos: Number(QOS) }, (err) => {
    if (err) {
      log.error({ err }, 'Subscribe error');
    } else {
      log.info({ topic: MQTT_TOPIC }, 'Subscribed');
      if (allowlistSet.size > 0) {
        log.info({ size: allowlistSet.size }, 'Beacon allowlist active');
      }
      log.info({ SEND_INTERVAL_SECONDS: Number(SEND_INTERVAL_SECONDS), MIN_TEMP_DELTA: TEMP_DELTA }, 'Throttle configured');
    }
  });
});

client.on('reconnect', () => log.warn('MQTT reconnecting'));
client.on('close', () => log.warn('MQTT connection closed'));
client.on('offline', () => log.warn('MQTT offline'));
client.on('end', () => log.warn('MQTT end'));
client.on('error', (err) => log.error({ err }, 'MQTT error'));

// Extract gateway MAC from topic "GwData/<GatewayMAC>"
function extractGatewayMac(topic) {
  const parts = topic.split('/');
  return parts.length >= 2 ? parts[1] : 'unknown';
}

// Parse temperature/battery from raw hex strings (aligns with your extension logic)
function parseReadingFromRaw(advRaw, srpRaw) {
  let temperature;
  let battery;

  if (advRaw && advRaw.length >= 14) {
    try {
      const tempInt = parseInt(advRaw.substring(10, 12), 16);
      const tempDec = parseInt(advRaw.substring(12, 14), 16);
      temperature = Number(`${tempInt}.${String(tempDec).padStart(2, '0')}`);
    } catch (e) {
      // ignore parse errors
    }
  }

  if (srpRaw && srpRaw.length >= 18) {
    try {
      const mv = parseInt(srpRaw.substring(14, 18), 16);
      battery = mv; // raw mV
    } catch (e) {
      // ignore parse errors
    }
  }

  return { temperature, battery };
}

async function forwardToSupabase(payload) {
  try {
    const res = await axios.post(SUPABASE_FUNCTION_URL, payload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${SUPABASE_AUTH}`
      },
      timeout: 5000
    });
    if (res.status !== 200) {
      log.warn({ status: res.status, data: res.data }, 'Supabase non-200 response');
    }
  } catch (err) {
    const data = err.response?.data;
    // Downgrade noise for unknown beacons
    if (data && (data.error === 'Beacon not registered' || err.response?.status === 404)) {
      log.debug({ err: data }, 'Ignored unregistered beacon');
      return;
    }
    log.error({ err: data || err.message }, 'Supabase POST error');
  }
}

client.on('message', async (topic, message) => {
  // Always log message receipt at debug level
  try {
    log.debug({ topic, length: message ? message.length : 0 }, 'Message received');
  } catch (_) {}

  const gwMac = extractGatewayMac(topic);

  let json;
  try {
    json = JSON.parse(message.toString('utf8'));
  } catch (e) {
    log.warn({ err: e.message }, 'Invalid JSON payload');
    return;
  }

  // Accept simplified JSON for testing: { beacon_mac, temperature_c|temperature|temp_c, battery_mv|battery?, ts?, pdv_id? }
  if (json?.beacon_mac) {
    if (!shouldForwardMac(json.beacon_mac)) {
      log.debug({ mac: json.beacon_mac }, 'Skipped (not in allowlist)');
      return;
    }

    const temperature =
      typeof json.temperature_c === 'number' ? json.temperature_c :
      typeof json.temperature === 'number' ? json.temperature :
      typeof json.temp_c === 'number' ? json.temp_c : undefined;

    if (typeof temperature === 'number') {
      if (!shouldSendNow(json.beacon_mac, temperature)) {
        log.debug({ mac: json.beacon_mac }, 'Throttled');
        return;
      }
      const body = {
        mac_address: json.beacon_mac,
        temperature,
        battery: typeof json.battery_mv === 'number' ? json.battery_mv : (typeof json.battery === 'number' ? json.battery : 0),
        gateway_mac: gwMac,
        ts: json.ts,
        pdv_id: json.pdv_id,
      };
      await forwardToSupabase(body);
      markSent(json.beacon_mac, temperature);
      log.info({ gwMac }, 'Forwarded simplified reading to Supabase');
    } else {
      log.debug({ json }, 'Simplified payload missing numeric temperature');
    }
    return;
  }

  if (json?.pkt_type !== 'scan_report' || !json?.data?.dev_infos || !Array.isArray(json.data.dev_infos)) 
  {
    return; // ignore non-scan-report messages
  }

  const devs = json.data.dev_infos;
  let forwarded = 0;

  for (const dev of devs) {
    const mac = dev?.addr;
    const advRaw = dev?.adv_raw;
    const srpRaw = dev?.srp_raw;

    if (!mac) continue;
    if (!shouldForwardMac(mac)) {
      log.debug({ mac }, 'Skipped (not in allowlist)');
      continue;
    }

    const { temperature, battery } = parseReadingFromRaw(advRaw, srpRaw);

    if (typeof temperature === 'number') {
      if (!shouldSendNow(mac, temperature)) {
        log.debug({ mac }, 'Throttled');
        continue;
      }
      const body = {
        mac_address: mac,
        temperature,
        battery: typeof battery === 'number' ? battery : 0,
        gateway_mac: gwMac
      };
      await forwardToSupabase(body);
      markSent(mac, temperature);
      forwarded += 1;
    }
  }

  if (forwarded > 0) {
    log.info({ forwarded, gwMac }, 'Forwarded readings to Supabase');
  }
});