<<<<<<< HEAD
const mqtt = require('mqtt');
const admin = require('firebase-admin');
const express = require('express');

// ============================================================
// CONFIGURATION
// ============================================================

// HiveMQ Cloud
const MQTT_BROKER = 'b7d8f6ecc3e1480894a55f79a462dca6.s1.eu.hivemq.cloud';
const MQTT_PORT = 8883;
const MQTT_USERNAME = 'PSEM2026';
const MQTT_PASSWORD = 'Pdah@1002#';

// Firebase Database URL
const FIREBASE_DB_URL =
  'https://psem2026-52929-default-rtdb.asia-southeast1.firebasedatabase.app';

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
  clientId: 'bridge_' + Math.random().toString(16).substring(2, 10),
  rejectUnauthorized: false,
  keepalive: 60,
  reconnectPeriod: 5000
};

console.log('MQTT connecting to ' + MQTT_BROKER);
const mqttClient = mqtt.connect(mqttOptions);

const lastHistorySave = {};
const HISTORY_INTERVAL = 30000;

// ============================================================
// MQTT EVENTS
// ============================================================

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
      await handleData(meterId, data);
    } else if (messageType === 'status') {
      await handleStatus(meterId, data);
    } else if (messageType === 'alert') {
      await handleAlert(meterId, data);
    } else {
      console.log('Unknown message type:', messageType);
    }
  } catch (err) {
    console.error('Error:', err.message);
  }
});

// ============================================================
// HANDLERS
// ============================================================

async function handleData(meterId, data) {
  const timestamp = Date.now();

  // Use ESP32 TOU total as today's energy
  const todayEnergy = Number(data.energy ?? data.daily_kwh ?? 0);

  const peakKwh = Number(data.tou_peak ?? 0);
  const dayKwh = Number(data.tou_day ?? 0);
  const offpeakKwh = Number(data.tou_offpeak ?? 0);

  const peakCost = Number(data.cost_peak ?? 0);
  const dayCost = Number(data.cost_day ?? 0);
  const offpeakCost = Number(data.cost_offpeak ?? 0);

  await db.ref('meters/' + meterId + '/realtime').update({
    meter_id: data.meter_id || meterId,

    voltage: Number(data.voltage ?? 0),
    current: Number(data.current ?? 0),
    power: Number(data.power ?? 0),

    // Today energy = Peak + Day + Off-Peak from ESP32
    energy: todayEnergy,
    daily_kwh: todayEnergy,

    // PZEM lifetime total
    total_energy: Number(data.total_energy ?? 0),

    frequency: Number(data.frequency ?? 50),
    pf: Number(data.pf ?? 0),
    balance: Number(data.balance ?? 0),

    period: data.period || 'DAY',
    rate: Number(data.rate ?? 0),

    tou_peak: peakKwh,
    tou_day: dayKwh,
    tou_offpeak: offpeakKwh,

    cost_peak: peakCost,
    cost_day: dayCost,
    cost_offpeak: offpeakCost,

    relay: data.relay || 'OFF',
    mode: data.mode || 'AUTO',

    monthly_kwh: Number(data.monthly_kwh ?? 0),
    wifi_rssi: Number(data.wifi_rssi ?? 0),
    uptime: Number(data.uptime ?? 0),
    led_status: data.led_status || '--',

    last_update: timestamp,
    status: 'online'
  });

  await updateDailySummary(meterId, data);

  console.log(
    `[${meterId}] ` +
      `Today:${todayEnergy} ` +
      `TOU(P/D/O):${peakKwh}/${dayKwh}/${offpeakKwh} ` +
      `Cost(P/D/O):${peakCost}/${dayCost}/${offpeakCost} ` +
      `Bal:Rs.${data.balance}`
  );

  const now = Date.now();

  if (now - (lastHistorySave[meterId] || 0) > HISTORY_INTERVAL) {
    await db.ref('meters/' + meterId + '/history').push({
      voltage: Number(data.voltage ?? 0),
      current: Number(data.current ?? 0),
      power: Number(data.power ?? 0),

      energy: todayEnergy,
      daily_kwh: todayEnergy,
      total_energy: Number(data.total_energy ?? 0),

      tou_peak: peakKwh,
      tou_day: dayKwh,
      tou_offpeak: offpeakKwh,

      balance: Number(data.balance ?? 0),
      timestamp: timestamp
    });

    lastHistorySave[meterId] = now;
  }
}

async function updateDailySummary(meterId, data) {
  const today = new Date().toISOString().split('T')[0];

  const peakKwh = Number(data.tou_peak ?? 0);
  const dayKwh = Number(data.tou_day ?? 0);
  const offpeakKwh = Number(data.tou_offpeak ?? 0);

  const peakCost = Number(data.cost_peak ?? 0);
  const dayCost = Number(data.cost_day ?? 0);
  const offpeakCost = Number(data.cost_offpeak ?? 0);

  const totalKwh = peakKwh + dayKwh + offpeakKwh;
  const totalCost = peakCost + dayCost + offpeakCost;

  await db.ref('meters/' + meterId + '/daily_bills/' + today).set({
    date: today,

    peak_kwh: Number(peakKwh.toFixed(3)),
    day_kwh: Number(dayKwh.toFixed(3)),
    offpeak_kwh: Number(offpeakKwh.toFixed(3)),

    peak_cost: Number(peakCost.toFixed(2)),
    day_cost: Number(dayCost.toFixed(2)),
    offpeak_cost: Number(offpeakCost.toFixed(2)),

    total_kwh: Number(totalKwh.toFixed(3)),
    total_cost: Number(totalCost.toFixed(2)),

    balance: Number(data.balance ?? 0),
    period: data.period || '--',
    rate: Number(data.rate ?? 0),

    total_energy: Number(data.total_energy ?? 0),
    monthly_kwh: Number(data.monthly_kwh ?? 0),

    updated_at: admin.database.ServerValue.TIMESTAMP
  });
}

async function handleStatus(meterId, data) {
  await db.ref('meters/' + meterId + '/connection').update({
    status: data.status || 'unknown',
    last_seen: admin.database.ServerValue.TIMESTAMP,
    ip_address: data.ip || null,
    balance: Number(data.balance ?? 0)
  });

  await db.ref('meters/' + meterId + '/realtime').update({
    status: data.status || 'unknown',
    last_update: Date.now()
  });

  console.log('[' + meterId + '] status:', data.status);
}

async function handleAlert(meterId, data) {
  await db.ref('meters/' + meterId + '/alerts').push({
    type: data.type || 'unknown',
    message: data.message || '',
    balance: Number(data.balance ?? 0),
    timestamp: admin.database.ServerValue.TIMESTAMP,
    acknowledged: false
  });

  console.log('Alert [' + meterId + ']: ' + data.type);
}

// ============================================================
// CLOUD COMMANDS (Web Dashboard -> MQTT)
// ============================================================

db.ref('mqtt_commands').on('child_added', async (snapshot) => {
  try {
    const cmd = snapshot.val();

    if (!cmd || !cmd.meter_id || !cmd.type) {
      await snapshot.ref.remove();
      return;
    }

    const commandId =
      cmd.command_id ||
      (cmd.payload && cmd.payload.command_id) ||
      snapshot.key;

    const processedRef = db.ref('processed_commands/' + commandId);
    const processedSnap = await processedRef.once('value');

    if (processedSnap.exists()) {
      console.log('Duplicate command ignored:', commandId);
      await snapshot.ref.remove();
      return;
    }

    await processedRef.set({
      meter_id: cmd.meter_id,
      type: cmd.type,
      created_at: admin.database.ServerValue.TIMESTAMP
    });

    const topic = 'smartmeter/' + cmd.meter_id + '/' + cmd.type;

    const payload = JSON.stringify(
      cmd.payload || {
        amount: cmd.amount,
        source: 'web_dashboard',
        command_id: commandId
      }
    );

    // Remove first to prevent repeat after bridge restart/reconnect
    await snapshot.ref.remove();

    mqttClient.publish(topic, payload, { qos: 0, retain: false }, (err) => {
      if (err) {
        console.error('Command publish failed:', err.message);
      } else {
        console.log('Sent:', topic, payload);
      }
    });
  } catch (err) {
    console.error('Command error:', err.message);
  }
});

// ============================================================
// EXPRESS SERVER
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

    const onlineMeters = meterIds.filter((id) => {
      return (
        meters[id]?.connection?.status === 'online' ||
        meters[id]?.realtime?.status === 'online'
      );
    });

    res.json({
      total_meters: meterIds.length,
      online_meters: onlineMeters.length,
      meter_ids: meterIds,
      mqtt_status: mqttClient.connected ? 'connected' : 'disconnected',
      uptime_seconds: Math.floor(process.uptime())
    });
  } catch (err) {
    res.status(500).json({
      error: err.message
    });
  }
});

app.listen(PORT, () => {
  console.log('Server running on port ' + PORT);
});

console.log('=========================================');
console.log('  Smart Meter Bridge - University of Sunderland');
console.log('  Listening for MQTT messages...');
console.log('=========================================');
=======
const mqtt = require("mqtt");
const admin = require("firebase-admin");
const express = require("express");

// ============================================================
// CONFIGURATION
// ============================================================

// HiveMQ Cloud
const MQTT_BROKER = "b7d8f6ecc3e1480894a55f79a462dca6.s1.eu.hivemq.cloud";
const MQTT_PORT = 8883;

// Better: keep these in Render Environment Variables
const MQTT_USERNAME = process.env.MQTT_USERNAME || "PSEM2026";
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || "PASTE_YOUR_MQTT_PASSWORD_HERE";

// Firebase Database URL
const FIREBASE_DB_URL =
  "https://psem2026-52929-default-rtdb.asia-southeast1.firebasedatabase.app";

const PORT = process.env.PORT || 3000;

// ============================================================
// FIREBASE INITIALIZATION
// ============================================================

let serviceAccount;

if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  console.log("Firebase: env credentials");
} else {
  serviceAccount = require("./firebase-key.json");
  console.log("Firebase: file credentials");
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: FIREBASE_DB_URL,
});

const db = admin.database();
console.log("Firebase initialized");

// ============================================================
// MQTT SETUP
// ============================================================

const mqttOptions = {
  host: MQTT_BROKER,
  port: MQTT_PORT,
  protocol: "mqtts",
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  clientId: "bridge_" + Math.random().toString(16).substring(2, 10),
  rejectUnauthorized: false,
  keepalive: 60,
  reconnectPeriod: 5000,
};

console.log("MQTT connecting to " + MQTT_BROKER);
const mqttClient = mqtt.connect(mqttOptions);

const lastHistorySave = {};
const HISTORY_INTERVAL = 30000;

// ============================================================
// MQTT EVENTS
// ============================================================

mqttClient.on("connect", () => {
  console.log("MQTT Connected!");

  mqttClient.subscribe("smartmeter/+/data", { qos: 1 });
  mqttClient.subscribe("smartmeter/+/status", { qos: 1 });
  mqttClient.subscribe("smartmeter/+/alert", { qos: 1 });

  console.log("Subscribed to all smartmeter topics");
});

mqttClient.on("error", (err) => {
  console.error("MQTT Error:", err.message);
});

mqttClient.on("reconnect", () => {
  console.log("MQTT Reconnecting...");
});

mqttClient.on("disconnect", () => {
  console.log("MQTT Disconnected");
});

// ============================================================
// MESSAGE PROCESSING
// ============================================================

mqttClient.on("message", async (topic, message) => {
  try {
    const parts = topic.split("/");
    const meterId = parts[1];
    const messageType = parts[2];

    const data = JSON.parse(message.toString());

    if (messageType === "data") {
      await handleData(meterId, data);
    } else if (messageType === "status") {
      await handleStatus(meterId, data);
    } else if (messageType === "alert") {
      await handleAlert(meterId, data);
    } else {
      console.log("Unknown message type:", messageType);
    }
  } catch (err) {
    console.error("Message processing error:", err.message);
  }
});

// ============================================================
// HELPERS
// ============================================================

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function round3(value) {
  return Number(toNumber(value).toFixed(3));
}

function round2(value) {
  return Number(toNumber(value).toFixed(2));
}

// ============================================================
// DATA HANDLER
// ============================================================

async function handleData(meterId, data) {
  const timestamp = Date.now();

  // Daily / today values from ESP32
  const todayEnergy = toNumber(data.energy ?? data.daily_kwh ?? 0);

  const peakKwh = toNumber(data.tou_peak ?? 0);
  const dayKwh = toNumber(data.tou_day ?? 0);
  const offpeakKwh = toNumber(data.tou_offpeak ?? 0);

  const peakCost = toNumber(data.cost_peak ?? 0);
  const dayCost = toNumber(data.cost_day ?? 0);
  const offpeakCost = toNumber(data.cost_offpeak ?? 0);

  // Monthly values from ESP32
  const monthlyKwh = toNumber(data.monthly_kwh ?? 0);

  const monthPeakKwh = toNumber(data.month_peak ?? 0);
  const monthDayKwh = toNumber(data.month_day ?? 0);
  const monthOffpeakKwh = toNumber(data.month_offpeak ?? 0);

  const monthPeakCost = toNumber(data.month_cost_peak ?? 0);
  const monthDayCost = toNumber(data.month_cost_day ?? 0);
  const monthOffpeakCost = toNumber(data.month_cost_offpeak ?? 0);

  const realtimeData = {
    meter_id: data.meter_id || meterId,

    voltage: toNumber(data.voltage ?? 0),
    current: toNumber(data.current ?? 0),
    power: toNumber(data.power ?? 0),

    // Today energy = Peak + Day + Off-Peak from ESP32
    energy: todayEnergy,
    daily_kwh: todayEnergy,

    // Displayed project total energy from ESP32
    total_energy: toNumber(data.total_energy ?? 0),

    // Raw PZEM lifetime reading, if ESP32 sends it
    pzem_total_energy: toNumber(data.pzem_total_energy ?? 0),

    frequency: toNumber(data.frequency ?? 50),
    pf: toNumber(data.pf ?? 0),
    balance: toNumber(data.balance ?? 0),

    period: data.period || "DAY",
    rate: toNumber(data.rate ?? 0),

    // Daily TOU split
    tou_peak: peakKwh,
    tou_day: dayKwh,
    tou_offpeak: offpeakKwh,

    // Daily TOU costs
    cost_peak: peakCost,
    cost_day: dayCost,
    cost_offpeak: offpeakCost,

    // Monthly total
    monthly_kwh: monthlyKwh,

    // Monthly TOU split - IMPORTANT FIX
    month_peak: monthPeakKwh,
    month_day: monthDayKwh,
    month_offpeak: monthOffpeakKwh,

    // Monthly TOU costs - IMPORTANT FIX
    month_cost_peak: monthPeakCost,
    month_cost_day: monthDayCost,
    month_cost_offpeak: monthOffpeakCost,

    relay: data.relay || "OFF",
    mode: data.mode || "AUTO",

    wifi_rssi: toNumber(data.wifi_rssi ?? 0),
    uptime: toNumber(data.uptime ?? 0),
    led_status: data.led_status || "--",

    last_update: timestamp,
    status: "online",
  };

  await db.ref("meters/" + meterId + "/realtime").update(realtimeData);

  await updateDailySummary(meterId, data);

  console.log(
    `[${meterId}] ` +
      `Today:${todayEnergy} ` +
      `TOU(P/D/O):${peakKwh}/${dayKwh}/${offpeakKwh} ` +
      `Month:${monthlyKwh} ` +
      `MonthTOU(P/D/O):${monthPeakKwh}/${monthDayKwh}/${monthOffpeakKwh} ` +
      `Bal:Rs.${data.balance}`
  );

  const now = Date.now();

  if (now - (lastHistorySave[meterId] || 0) > HISTORY_INTERVAL) {
    await db.ref("meters/" + meterId + "/history").push({
      voltage: toNumber(data.voltage ?? 0),
      current: toNumber(data.current ?? 0),
      power: toNumber(data.power ?? 0),

      energy: todayEnergy,
      daily_kwh: todayEnergy,
      total_energy: toNumber(data.total_energy ?? 0),
      pzem_total_energy: toNumber(data.pzem_total_energy ?? 0),

      tou_peak: peakKwh,
      tou_day: dayKwh,
      tou_offpeak: offpeakKwh,

      monthly_kwh: monthlyKwh,
      month_peak: monthPeakKwh,
      month_day: monthDayKwh,
      month_offpeak: monthOffpeakKwh,

      balance: toNumber(data.balance ?? 0),
      period: data.period || "--",
      relay: data.relay || "OFF",
      mode: data.mode || "AUTO",

      timestamp: timestamp,
    });

    lastHistorySave[meterId] = now;
  }
}

// ============================================================
// DAILY SUMMARY
// ============================================================

async function updateDailySummary(meterId, data) {
  const today = new Date().toISOString().split("T")[0];

  const peakKwh = toNumber(data.tou_peak ?? 0);
  const dayKwh = toNumber(data.tou_day ?? 0);
  const offpeakKwh = toNumber(data.tou_offpeak ?? 0);

  const peakCost = toNumber(data.cost_peak ?? 0);
  const dayCost = toNumber(data.cost_day ?? 0);
  const offpeakCost = toNumber(data.cost_offpeak ?? 0);

  const totalKwh = peakKwh + dayKwh + offpeakKwh;
  const totalCost = peakCost + dayCost + offpeakCost;

  await db.ref("meters/" + meterId + "/daily_bills/" + today).set({
    date: today,

    peak_kwh: round3(peakKwh),
    day_kwh: round3(dayKwh),
    offpeak_kwh: round3(offpeakKwh),

    peak_cost: round2(peakCost),
    day_cost: round2(dayCost),
    offpeak_cost: round2(offpeakCost),

    total_kwh: round3(totalKwh),
    total_cost: round2(totalCost),

    balance: toNumber(data.balance ?? 0),
    period: data.period || "--",
    rate: toNumber(data.rate ?? 0),

    total_energy: toNumber(data.total_energy ?? 0),
    pzem_total_energy: toNumber(data.pzem_total_energy ?? 0),
    monthly_kwh: toNumber(data.monthly_kwh ?? 0),

    updated_at: admin.database.ServerValue.TIMESTAMP,
  });
}

// ============================================================
// STATUS HANDLER
// ============================================================

async function handleStatus(meterId, data) {
  await db.ref("meters/" + meterId + "/connection").update({
    status: data.status || "unknown",
    last_seen: admin.database.ServerValue.TIMESTAMP,
    ip_address: data.ip || null,
    balance: toNumber(data.balance ?? 0),
  });

  await db.ref("meters/" + meterId + "/realtime").update({
    status: data.status || "unknown",
    last_update: Date.now(),
  });

  console.log("[" + meterId + "] status:", data.status);
}

// ============================================================
// ALERT HANDLER
// ============================================================

async function handleAlert(meterId, data) {
  await db.ref("meters/" + meterId + "/alerts").push({
    type: data.type || "unknown",
    message: data.message || "",
    balance: toNumber(data.balance ?? 0),
    timestamp: admin.database.ServerValue.TIMESTAMP,
    acknowledged: false,
  });

  console.log("Alert [" + meterId + "]: " + data.type);
}

// ============================================================
// CLOUD COMMANDS: WEB DASHBOARD -> FIREBASE -> MQTT
// ============================================================

db.ref("mqtt_commands").on("child_added", async (snapshot) => {
  try {
    const cmd = snapshot.val();

    if (!cmd || !cmd.meter_id || !cmd.type) {
      await snapshot.ref.remove();
      return;
    }

    const commandId =
      cmd.command_id || (cmd.payload && cmd.payload.command_id) || snapshot.key;

    const processedRef = db.ref("processed_commands/" + commandId);
    const processedSnap = await processedRef.once("value");

    if (processedSnap.exists()) {
      console.log("Duplicate command ignored:", commandId);
      await snapshot.ref.remove();
      return;
    }

    await processedRef.set({
      meter_id: cmd.meter_id,
      type: cmd.type,
      created_at: admin.database.ServerValue.TIMESTAMP,
    });

    const topic = "smartmeter/" + cmd.meter_id + "/" + cmd.type;

    const payload = JSON.stringify(
      cmd.payload || {
        amount: cmd.amount,
        source: "web_dashboard",
        command_id: commandId,
      }
    );

    // Remove first to prevent repeat after bridge restart/reconnect
    await snapshot.ref.remove();

    mqttClient.publish(topic, payload, { qos: 0, retain: false }, (err) => {
      if (err) {
        console.error("Command publish failed:", err.message);
      } else {
        console.log("Sent:", topic, payload);
      }
    });
  } catch (err) {
    console.error("Command error:", err.message);
  }
});

// ============================================================
// EXPRESS SERVER
// ============================================================

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.json({
    service: "Smart Meter Bridge",
    status: "running",
    mqtt_connected: mqttClient.connected,
    uptime: Math.floor(process.uptime()),
    project: "University of Sunderland - FYP",
  });
});

app.get("/health", (req, res) => {
  res.status(mqttClient.connected ? 200 : 503).json({
    status: mqttClient.connected ? "healthy" : "degraded",
  });
});

app.get("/stats", async (req, res) => {
  try {
    const metersSnap = await db.ref("meters").once("value");
    const meters = metersSnap.val() || {};

    const meterIds = Object.keys(meters);

    const onlineMeters = meterIds.filter((id) => {
      return (
        meters[id]?.connection?.status === "online" ||
        meters[id]?.realtime?.status === "online"
      );
    });

    res.json({
      total_meters: meterIds.length,
      online_meters: onlineMeters.length,
      meter_ids: meterIds,
      mqtt_status: mqttClient.connected ? "connected" : "disconnected",
      uptime_seconds: Math.floor(process.uptime()),
    });
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

app.listen(PORT, () => {
  console.log("Server running on port " + PORT);
});

console.log("=========================================");
console.log("  Smart Meter Bridge - University of Sunderland");
console.log("  Listening for MQTT messages...");
console.log("=========================================");
>>>>>>> c4c6d1c (Fix monthly TOU update)
