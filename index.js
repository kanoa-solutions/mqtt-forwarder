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
    log.error({ err: err.response?.data || err.message }, 'Supabase POST error');
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

  // Accept simplified JSON for testing: { beacon_mac, temperature_c|temperature|temp_c, battery_mv|battery?, ts? }
  if (json?.beacon_mac) {
    const temperature =
      typeof json.temperature_c === 'number' ? json.temperature_c :
      typeof json.temperature === 'number' ? json.temperature :
      typeof json.temp_c === 'number' ? json.temp_c : undefined;

    if (typeof temperature === 'number') {
      const body = {
        mac_address: json.beacon_mac,
        temperature,
        battery: typeof json.battery_mv === 'number' ? json.battery_mv : (typeof json.battery === 'number' ? json.battery : 0),
        gateway_mac: gwMac,
        ts: json.ts
      };
      await forwardToSupabase(body);
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

    const { temperature, battery } = parseReadingFromRaw(advRaw, srpRaw);

    if (typeof temperature === 'number') {
      const body = {
        mac_address: mac,
        temperature,
        battery: typeof battery === 'number' ? battery : 0,
        gateway_mac: gwMac
      };
      await forwardToSupabase(body);
      forwarded += 1;
    }
  }

  if (forwarded > 0) {
    log.info({ forwarded, gwMac }, 'Forwarded readings to Supabase');
  }
});