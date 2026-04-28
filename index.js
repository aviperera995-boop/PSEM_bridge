const mqtt = require('mqtt');
const admin = require('firebase-admin');
const express = require('express');

// ============================================================
// CONFIGURATION - Already configured for your project!
// ============================================================

// HiveMQ Cloud
const MQTT_BROKER = 'b7d8f6ecc3e1480894a55f79a462dca6.s1.eu.hivemq.cloud';
const MQTT_PORT = 8883;
const MQTT_USERNAME = 'PSEM2026';
const MQTT_PASSWORD = 'Pdah@1002#';

// Firebase Database URL (your Singapore region URL)
const FIREBASE_DB_URL = 'https://psem2026-52929-default-rtdb.asia-southeast1.firebasedatabase.app';

const PORT = process.env.PORT || 3000;

// ============================================================
// FIREBASE INITIALIZATION
// ============================================================

let serviceAccount;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  console.log('Firebase: env credentials');
} else {
  serviceAccount = require('./firebase-key.json');
  console.log('Firebase: file credentials');
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: FIREBASE_DB_URL
});

const db = admin.database();
console.log('Firebase initialized');

// ============================================================
// MQTT SETUP
// ============================================================

const mqttOptions = {
  host: MQTT_BROKER,
  port: MQTT_PORT,
  protocol: 'mqtts',
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  clientId: 'bridge_' + Math.random().toString(16).substr(2, 8),
  rejectUnauthorized: false,
  keepalive: 60,
  reconnectPeriod: 5000
};

console.log('MQTT connecting to ' + MQTT_BROKER);
const mqttClient = mqtt.connect(mqttOptions);

const lastHistorySave = {};
const HISTORY_INTERVAL = 30000;

mqttClient.on('connect', () => {
  console.log('MQTT Connected!');
  mqttClient.subscribe('smartmeter/+/data', { qos: 1 });
  mqttClient.subscribe('smartmeter/+/status', { qos: 1 });
  mqttClient.subscribe('smartmeter/+/alert', { qos: 1 });
  console.log('Subscribed to all topics');
});

mqttClient.on('error', (err) => {
  console.error('MQTT Error:', err.message);
});

mqttClient.on('reconnect', () => {
  console.log('MQTT Reconnecting...');
});

mqttClient.on('disconnect', () => {
  console.log('MQTT Disconnected');
});

// ============================================================
// MESSAGE PROCESSING
// ============================================================

mqttClient.on('message', async (topic, message) => {
  try {
    const parts = topic.split('/');
    const meterId = parts[1];
    const messageType = parts[2];
    const data = JSON.parse(message.toString());
    
    if (messageType === 'data') {
      console.log('[' + meterId + '] V:' + data.voltage + ' P:' + data.power + 'W Bal:Rs.' + data.balance);
    } else {
      console.log('[' + meterId + '] ' + messageType);
    }
    
    if (messageType === 'data') {
      await handleData(meterId, data);
    } else if (messageType === 'status') {
      await handleStatus(meterId, data);
    } else if (messageType === 'alert') {
      await handleAlert(meterId, data);
    }
  } catch (err) {
    console.error('Error:', err.message);
  }
});

// ============================================================
// HANDLERS
// ============================================================

async function handleData(meterId, data) {
  const timestamp = admin.database.ServerValue.TIMESTAMP;
  
  await db.ref('meters/' + meterId + '/realtime').update({
    voltage: data.voltage || 0,
    current: data.current || 0,
    power: data.power || 0,
    energy: data.energy || 0,
    frequency: data.frequency || 50,
    pf: data.pf || 0,
    balance: data.balance || 0,
    period: data.period || 'DAY',
    rate: data.rate || 0,
    relay: data.relay || 'OFF',
    mode: data.mode || 'AUTO',
    daily_kwh: data.daily_kwh || 0,
    monthly_kwh: data.monthly_kwh || 0,
    wifi_rssi: data.wifi_rssi || 0,
    last_update: timestamp,
    status: 'online'
  });
  
  const now = Date.now();
  if (now - (lastHistorySave[meterId] || 0) > HISTORY_INTERVAL) {
    await db.ref('meters/' + meterId + '/history').push({
      voltage: data.voltage,
      current: data.current,
      power: data.power,
      energy: data.energy,
      balance: data.balance,
      timestamp: timestamp
    });
    lastHistorySave[meterId] = now;
    await updateDailySummary(meterId, data);
  }
}

async function updateDailySummary(meterId, data) {
  const today = new Date().toISOString().split('T')[0];
  const ref = db.ref('meters/' + meterId + '/daily_summary/' + today);
  const snap = await ref.once('value');
  let s = snap.val() || {
    start_energy: data.energy,
    end_energy: data.energy,
    peak_power: 0,
    avg_power: 0,
    total_kwh: 0,
    total_cost: 0,
    sample_count: 0,
    power_sum: 0
  };
  
  s.end_energy = data.energy;
  s.peak_power = Math.max(s.peak_power || 0, data.power || 0);
  s.power_sum = (s.power_sum || 0) + (data.power || 0);
  s.sample_count = (s.sample_count || 0) + 1;
  s.avg_power = s.power_sum / s.sample_count;
  s.total_kwh = s.end_energy - s.start_energy;
  s.total_cost = s.total_kwh * (data.rate || 50);
  
  await ref.set(s);
}

async function handleStatus(meterId, data) {
  await db.ref('meters/' + meterId + '/connection').update({
    status: data.status,
    last_seen: admin.database.ServerValue.TIMESTAMP,
    ip_address: data.ip || null
  });
}

async function handleAlert(meterId, data) {
  await db.ref('meters/' + meterId + '/alerts').push({
    type: data.type,
    message: data.message,
    balance: data.balance,
    timestamp: admin.database.ServerValue.TIMESTAMP,
    acknowledged: false
  });
  console.log('Alert [' + meterId + ']: ' + data.type);
}

// ============================================================
// CLOUD COMMANDS (Web Dashboard -> MQTT)
// ============================================================

db.ref('mqtt_commands').on('child_added', (snapshot) => {
  const cmd = snapshot.val();
  if (cmd && cmd.meter_id && cmd.type) {
    const topic = 'smartmeter/' + cmd.meter_id + '/' + cmd.type;
    const payload = JSON.stringify(cmd.payload || {});
    mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
      if (!err) {
        console.log('Sent: ' + topic);
        snapshot.ref.remove();
      }
    });
  }
});

// ============================================================
// EXPRESS SERVER (Health Check)
// ============================================================

const app = express();
app.use(express.json());

app.get('/', (req, res) => {
  res.json({
    service: 'Smart Meter Bridge',
    status: 'running',
    mqtt_connected: mqttClient.connected,
    uptime: Math.floor(process.uptime()),
    project: 'University of Sunderland - FYP'
  });
});

app.get('/health', (req, res) => {
  res.status(mqttClient.connected ? 200 : 503).json({
    status: mqttClient.connected ? 'healthy' : 'degraded'
  });
});
app.get('/stats', async (req, res) => {
  try {
    const metersSnap = await db.ref('meters').once('value');
    const meters = metersSnap.val() || {};

    const meterIds = Object.keys(meters);

    const onlineMeters = meterIds.filter(id =>
      meters[id]?.connection?.status === 'online' ||
      meters[id]?.realtime?.status === 'online'
    );

    res.json({
      total_meters: meterIds.length,
      online_meters: onlineMeters.length,
      meter_ids: meterIds,
      mqtt_status: mqttClient.connected ? 'connected' : 'disconnected',
      uptime_seconds: Math.floor(process.uptime())
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log('Server running on port ' + PORT);
});

console.log('=========================================');
console.log('  Smart Meter Bridge - University of Sunderland');
console.log('  Listening for MQTT messages...');
console.log('=========================================');