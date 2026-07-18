

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = Number(process.env.PORT || 3000);
const BUNDLES_WS_URL = process.env.BUNDLES_WS_URL;

const MAX_SIGNALS_PER_SUBMISSION = Number(
  process.env.MAX_SIGNALS_PER_SUBMISSION || 20
);

const MAX_HISTORY_ENTRIES = Number(
  process.env.MAX_HISTORY_ENTRIES || 100
);

const AUTO_SEND_ON_START =
  String(process.env.AUTO_SEND_ON_START || "false").toLowerCase() === "true";

if (!BUNDLES_WS_URL) {
  console.error("❌ Missing required environment variable: BUNDLES_WS_URL");
  process.exit(1);
}

/*
TOKEN_ADDRESSES can be comma-separated or line-separated.

Example:
TOKEN_ADDRESSES=Ce2gx9KGXJ6C9Mp5b5x1sn9Mg87JwEbrQby4Zqo3pump,61V8vBaqAGMpgDQi4JcAwo1dmBGHsyhzodcPqnEVpump
*/
function parseConfiguredSignals() {
  const addresses = String(process.env.TOKEN_ADDRESSES || "")
    .split(/[\n,]+/)
    .map((value) => value.trim())
    .filter(Boolean);

  return addresses.map((tokenAddress, index) => ({
    tokenAddress,
    note: `Configured signal ${index + 1}`,
  }));
}

const configuredSignals = parseConfiguredSignals();
const history = [];

let ws = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let lastIncomingMessage = null;
let lastSocketError = null;
let connectedAt = null;

function websocketStatus() {
  if (!ws) return "not_created";

  switch (ws.readyState) {
    case WebSocket.CONNECTING:
      return "connecting";

    case WebSocket.OPEN:
      return "connected";

    case WebSocket.CLOSING:
      return "closing";

    case WebSocket.CLOSED:
      return "disconnected";

    default:
      return "unknown";
  }
}

function addHistory(entry) {
  history.unshift(entry);

  if (history.length > MAX_HISTORY_ENTRIES) {
    history.length = MAX_HISTORY_ENTRIES;
  }
}

function scheduleReconnect() {
  if (reconnectTimer) return;

  reconnectAttempt += 1;

  const delayMs = Math.min(
    30000,
    1000 * 2 ** Math.min(reconnectAttempt - 1, 5)
  );

  console.log(`🔁 Reconnecting to Bundles in ${delayMs} ms...`);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectWebSocket();
  }, delayMs);
}

function connectWebSocket() {
  if (
    ws &&
    (ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING)
  ) {
    return;
  }

  console.log("🔌 Connecting to Bundles WebSocket...");

  ws = new WebSocket(BUNDLES_WS_URL);

  ws.on("open", () => {
    reconnectAttempt = 0;
    connectedAt = new Date().toISOString();
    lastSocketError = null;

    console.log("✅ Connected to Bundles WebSocket");
    console.log(`🕒 Connected at: ${connectedAt}`);

    if (AUTO_SEND_ON_START && configuredSignals.length > 0) {
      console.log("🚀 AUTO_SEND_ON_START is enabled");

      sendSignals(configuredSignals)
        .then((result) => {
          console.log(
            "✅ Auto-send completed:",
            JSON.stringify(result, null, 2)
          );
        })
        .catch((error) => {
          console.error("❌ Auto-send failed:", error);
        });
    }
  });

  ws.on("message", (data) => {
    const message = data.toString();

    lastIncomingMessage = {
      receivedAt: new Date().toISOString(),
      message,
    };

    console.log("📥 Bundles message received:");
    console.log(message);

    addHistory({
      direction: "incoming",
      receivedAt: lastIncomingMessage.receivedAt,
      message,
    });
  });

  ws.on("error", (error) => {
    lastSocketError = {
      occurredAt: new Date().toISOString(),
      message: error.message,
    };

    console.error("❌ WebSocket error:", error);
  });

  ws.on("close", (code, reasonBuffer) => {
    const reason = reasonBuffer?.toString() || "No reason supplied";

    console.warn(
      `⚠️ Bundles WebSocket closed. Code: ${code}. Reason: ${reason}`
    );

    connectedAt = null;
    scheduleReconnect();
  });

  ws.on("unexpected-response", (_request, response) => {
    console.error(
      `❌ Unexpected WebSocket HTTP response: ${response.statusCode} ${response.statusMessage}`
    );
  });
}

function validateSignals(signals) {
  if (!Array.isArray(signals)) {
    throw new Error('"signals" must be an array.');
  }

  if (signals.length === 0) {
    throw new Error("At least one signal is required.");
  }

  if (signals.length > MAX_SIGNALS_PER_SUBMISSION) {
    throw new Error(
      `A maximum of ${MAX_SIGNALS_PER_SUBMISSION} signals may be submitted at once.`
    );
  }

  return signals.map((signal, index) => {
    const tokenAddress = String(signal?.tokenAddress || "").trim();
    const note = String(signal?.note || `Signal ${index + 1}`).trim();

    if (!tokenAddress) {
      throw new Error(`Signal ${index + 1} is missing tokenAddress.`);
    }

    return {
      tokenAddress,
      note,
    };
  });
}

function sendSignals(rawSignals) {
  return new Promise((resolve, reject) => {
    let signals;

    try {
      signals = validateSignals(rawSignals);
    } catch (error) {
      reject(error);
      return;
    }

    if (!ws || ws.readyState !== WebSocket.OPEN) {
      reject(
        new Error(
          `Bundles WebSocket is not open. Current status: ${websocketStatus()}`
        )
      );
      return;
    }

    const payload = {
      type: "agentic:signal:submit",
      signals,
    };

    const sentAt = new Date().toISOString();

    console.log("📤 Sending payload to Bundles:");
    console.log(JSON.stringify(payload, null, 2));

    ws.send(JSON.stringify(payload), (error) => {
      if (error) {
        console.error("❌ WebSocket send failed:", error);
        reject(error);
        return;
      }

      console.log("✅ Payload written to Bundles WebSocket");

      const historyEntry = {
        direction: "outgoing",
        sentAt,
        payload,
        socketStatusAtSend: websocketStatus(),
      };

      addHistory(historyEntry);

      resolve({
        success: true,
        message: `Submitted ${signals.length} signal(s).`,
        submissionType: payload.type,
        signals,
        sentAt,
        deliveryStatus:
          "Written to the WebSocket connection. Await Bundles response/log confirmation for acceptance.",
      });
    });
  });
}

app.get("/", (_req, res) => {
  res.json({
    service: "Second Bundles Signal Sender",
    status: "running",
    websocketStatus: websocketStatus(),
    submissionType: "agentic:signal:submit",
    payloadArray: "signals",
    configuredTokenCount: configuredSignals.length,
    configuredSignals,
    maximumSignalsPerSubmission: MAX_SIGNALS_PER_SUBMISSION,
    storedHistoryEntries: history.length,
    maximumHistoryEntries: MAX_HISTORY_ENTRIES,
    autoSendOnStart: AUTO_SEND_ON_START,
    authenticationRequired: false,
    endpoints: {
      health: "GET /health",
      sendFromBrowser: "GET /send",
      sendConfiguredSignals: "POST /send",
      sendCustomSignals: "POST /send-custom",
      viewHistory: "GET /history",
    },
  });
});

app.get("/health", (_req, res) => {
  res.json({
    service: "Second Bundles Signal Sender",
    status: "running",
    websocketStatus: websocketStatus(),
    connectedAt,
    configuredTokenCount: configuredSignals.length,
    storedHistoryEntries: history.length,
    lastIncomingMessage,
    lastSocketError,
  });
});

async function handleConfiguredSend(_req, res) {
  try {
    if (configuredSignals.length === 0) {
      return res.status(400).json({
        success: false,
        error:
          "No configured tokens found. Set TOKEN_ADDRESSES in Railway Variables.",
      });
    }

    const result = await sendSignals(configuredSignals);

    return res.json(result);
  } catch (error) {
    console.error("❌ /send failed:", error);

    return res.status(503).json({
      success: false,
      error: error.message,
      websocketStatus: websocketStatus(),
      lastSocketError,
    });
  }
}

app.get("/send", handleConfiguredSend);
app.post("/send", handleConfiguredSend);

app.post("/send-custom", async (req, res) => {
  try {
    const signals = req.body?.signals;
    const result = await sendSignals(signals);

    return res.json(result);
  } catch (error) {
    console.error("❌ /send-custom failed:", error);

    return res.status(400).json({
      success: false,
      error: error.message,
      websocketStatus: websocketStatus(),
    });
  }
});

app.get("/history", (_req, res) => {
  res.json({
    count: history.length,
    maximumHistoryEntries: MAX_HISTORY_ENTRIES,
    history,
  });
});

app.use((error, _req, res, _next) => {
  console.error("❌ Express error:", error);

  res.status(500).json({
    success: false,
    error: error.message || "Unexpected server error",
  });
});

const server = app.listen(PORT, () => {
  console.log(`🌐 HTTP server listening on port ${PORT}`);
  console.log(`🪙 Configured token count: ${configuredSignals.length}`);
  console.log("📡 Submission type: agentic:signal:submit");

  connectWebSocket();
});

function shutdown(signal) {
  console.log(`🛑 Received ${signal}. Shutting down...`);

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  if (ws) {
    try {
      ws.close(1000, "Server shutting down");
    } catch (error) {
      console.error("Error closing WebSocket:", error);
    }
  }

  server.close(() => {
    process.exit(0);
  });

  setTimeout(() => {
    process.exit(1);
  }, 10000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
