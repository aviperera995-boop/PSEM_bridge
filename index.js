const mqtt = require("mqtt");
const admin = require("firebase-admin");
const express = require("express");

// ============================================================
// CONFIGURATION
// ============================================================

const MQTT_BROKER = "b7d8f6ecc3e1480894a55f79a462dca6.s1.eu.hivemq.cloud";
const MQTT_PORT = 8883;

const MQTT_USERNAME = process.env.MQTT_USERNAME || "PSEM2026";
const MQTT_PASSWORD =
  process.env.MQTT_PASSWORD || "Pdah@1002#";

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

// ============================================================
// HELPERS
// ============================================================

function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

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
    }
  } catch (err) {
    console.error("Message processing error:", err.message);
  }
});

// ============================================================
// DATA HANDLER
// ============================================================

async function handleData(meterId, data) {
  const todayEnergy = toNumber(data.energy ?? data.daily_kwh ?? 0);

  const peakKwh = toNumber(data.tou_peak ?? 0);
  const dayKwh = toNumber(data.tou_day ?? 0);
  const offpeakKwh = toNumber(data.tou_offpeak ?? 0);

  const monthlyKwh = toNumber(data.monthly_kwh ?? 0);

  const monthPeakKwh = toNumber(data.month_peak ?? 0);
  const monthDayKwh = toNumber(data.month_day ?? 0);
  const monthOffpeakKwh = toNumber(data.month_offpeak ?? 0);

  await db.ref("meters/" + meterId + "/realtime").update({
    meter_id: data.meter_id || meterId,

    voltage: toNumber(data.voltage ?? 0),
    current: toNumber(data.current ?? 0),
    power: toNumber(data.power ?? 0),

    energy: todayEnergy,
    daily_kwh: todayEnergy,

    total_energy: toNumber(data.total_energy ?? 0),

    frequency: toNumber(data.frequency ?? 50),
    pf: toNumber(data.pf ?? 0),
    balance: toNumber(data.balance ?? 0),

    period: data.period || "DAY",
    rate: toNumber(data.rate ?? 0),

    tou_peak: peakKwh,
    tou_day: dayKwh,
    tou_offpeak: offpeakKwh,

    monthly_kwh: monthlyKwh,

    // MONTHLY TOU
    month_peak: monthPeakKwh,
    month_day: monthDayKwh,
    month_offpeak: monthOffpeakKwh,

    relay: data.relay || "OFF",
    mode: data.mode || "AUTO",

    last_update: Date.now(),
    status: "online",
  });

  console.log(
    `[${meterId}] Today:${todayEnergy} TOU(P/D/O):${peakKwh}/${dayKwh}/${offpeakKwh} Month:${monthlyKwh} MonthTOU(P/D/O):${monthPeakKwh}/${monthDayKwh}/${monthOffpeakKwh}`
  );
}

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
  });
});

app.listen(PORT, () => {
  console.log("Server running on port " + PORT);
});

console.log("=========================================");
console.log(" Smart Meter Bridge Running ");
console.log("=========================================");
