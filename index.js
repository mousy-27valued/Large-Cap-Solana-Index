"use strict";

const express = require("express");
const WebSocket = require("ws");

const app = express();

app.use(express.json({ limit: "100kb" }));

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

const PORT = Number(process.env.PORT || 3000);

const SERVICE_NAME =
  process.env.SERVICE_NAME || "Second Bundles Signal Sender";

const BUNDLES_WS_URL =
  process.env.BUNDLES_WS_URL || "wss://bundles.trade/ws/agentic";

const SUBMISSION_TYPE = "agentic:signal:submit";
const PAYLOAD_ARRAY = "signals";

const MAX_SIGNALS_PER_SUBMISSION = 20;
const MAX_HISTORY_ENTRIES = 100;

const AUTO_SEND_ON_START =
  String(process.env.AUTO_SEND_ON_START || "false").toLowerCase() === "true";

// Supports either:
// TOKEN_ADDRESSES=mint1,mint2
//
// Or:
// SIGNALS_JSON=[{"tokenAddress":"mint1","note":"Coin 1"}]
const configuredSignals = loadConfiguredSignals();

const history = [];

let socket = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
let autoSendCompleted = false;

// -----------------------------------------------------------------------------
// Configuration parsing
// -----------------------------------------------------------------------------

function loadConfiguredSignals() {
  if (process.env.SIGNALS_JSON) {
    try {
      const parsed = JSON.parse(process.env.SIGNALS_JSON);

      if (!Array.isArray(parsed)) {
        throw new Error("SIGNALS_JSON must contain a JSON array.");
      }

      return normalizeSignals(parsed);
    } catch (error) {
      console.error(
        `[CONFIG] Could not parse SIGNALS_JSON: ${error.message}`
      );

      process.exit(1);
    }
  }

  const tokenAddresses = String(process.env.TOKEN_ADDRESSES || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);

  return normalizeSignals(
    tokenAddresses.map((tokenAddress, index) => ({
      tokenAddress,
      note: `Configured signal ${index + 1}`,
    }))
  );
}

function normalizeSignals(input) {
  if (!Array.isArray(input)) {
    return [];
  }

  const uniqueSignals = new Map();

  for (const item of input) {
    let tokenAddress;
    let note;

    if (typeof item === "string") {
      tokenAddress = item.trim();
      note = "";
    } else if (item && typeof item === "object") {
      tokenAddress = String(
        item.tokenAddress ||
          item.mint ||
          item.address ||
          ""
      ).trim();

      note = String(item.note || item.symbol || "").trim();
    }

    if (!tokenAddress) {
      continue;
    }

    uniqueSignals.set(tokenAddress, {
      tokenAddress,
      ...(note ? { note } : {}),
    });
  }

  return Array.from(uniqueSignals.values()).slice(
    0,
    MAX_SIGNALS_PER_SUBMISSION
  );
}

// -----------------------------------------------------------------------------
// History
// -----------------------------------------------------------------------------

function addHistory(entry) {
  history.unshift({
    timestamp: new Date().toISOString(),
    ...entry,
  });

  if (history.length > MAX_HISTORY_ENTRIES) {
    history.length = MAX_HISTORY_ENTRIES;
  }
}

// -----------------------------------------------------------------------------
// WebSocket connection
// -----------------------------------------------------------------------------

function connectToBundles() {
  if (
    socket &&
    (socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING)
  ) {
    return;
  }

  console.log(`[WS] Connecting to ${BUNDLES_WS_URL}`);

  socket = new WebSocket(BUNDLES_WS_URL);

  socket.on("open", () => {
    reconnectAttempts = 0;

    console.log("[WS] Connected to Bundles.trade.");

    if (
      AUTO_SEND_ON_START &&
      !autoSendCompleted &&
      configuredSignals.length > 0
    ) {
      autoSendCompleted = true;

      setTimeout(async () => {
        try {
          await submitSignals(configuredSignals, "auto-send-on-start");
        } catch (error) {
          console.error(`[AUTO SEND] ${error.message}`);
        }
      }, 1000);
    }
  });

  socket.on("message", (data) => {
    const message = data.toString();

    console.log(`[WS] Received: ${message}`);

    let parsedMessage = message;

    try {
      parsedMessage = JSON.parse(message);
    } catch {
      // Keep non-JSON messages as text.
    }

    addHistory({
      direction: "received",
      message: parsedMessage,
    });
  });

  socket.on("close", (code, reasonBuffer) => {
    const reason = reasonBuffer
      ? reasonBuffer.toString()
      : "";

    console.warn(
      `[WS] Connection closed. Code: ${code}${
        reason ? ` | Reason: ${reason}` : ""
      }`
    );

    socket = null;
    scheduleReconnect();
  });

  socket.on("error", (error) => {
    console.error(`[WS] Error: ${error.message}`);
  });
}

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectAttempts += 1;

  const delay = Math.min(
    1000 * 2 ** Math.min(reconnectAttempts - 1, 5),
    30000
  );

  console.log(`[WS] Reconnecting in ${delay} ms.`);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectToBundles();
  }, delay);
}

function waitForOpenSocket(timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    if (socket && socket.readyState === WebSocket.OPEN) {
      resolve(socket);
      return;
    }

    connectToBundles();

    const startedAt = Date.now();

    const interval = setInterval(() => {
      if (socket && socket.readyState === WebSocket.OPEN) {
        clearInterval(interval);
        resolve(socket);
        return;
      }

      if (Date.now() - startedAt >= timeoutMs) {
        clearInterval(interval);

        reject(
          new Error(
            "Bundles WebSocket did not connect before the timeout."
          )
        );
      }
    }, 100);
  });
}

// -----------------------------------------------------------------------------
// Signal submission
// -----------------------------------------------------------------------------

async function submitSignals(signals, source = "api") {
  const normalizedSignals = normalizeSignals(signals);

  if (normalizedSignals.length === 0) {
    throw new Error("No valid token signals were supplied.");
  }

  if (normalizedSignals.length > MAX_SIGNALS_PER_SUBMISSION) {
    throw new Error(
      `A maximum of ${MAX_SIGNALS_PER_SUBMISSION} signals may be submitted at once.`
    );
  }

  const payload = {
    type: SUBMISSION_TYPE,
    signals: normalizedSignals,
  };

  const activeSocket = await waitForOpenSocket();

  activeSocket.send(JSON.stringify(payload));

  const historyEntry = {
    direction: "sent",
    source,
    submissionType: SUBMISSION_TYPE,
    signalCount: normalizedSignals.length,
    signals: normalizedSignals,
    payload,
  };

  addHistory(historyEntry);

  console.log(
    `[SEND] Submitted ${normalizedSignals.length} signal(s) to Bundles.trade.`
  );

  console.log(JSON.stringify(payload, null, 2));

  return historyEntry;
}

// -----------------------------------------------------------------------------
// Routes
// -----------------------------------------------------------------------------

app.get("/", (req, res) => {
  res.json(getServiceStatus());
});

app.get("/health", (req, res) => {
  res.json(getServiceStatus());
});

// Browser-friendly route.
// Opening /send submits all tokens configured in Railway.
app.get("/send", async (req, res) => {
  try {
    const result = await submitSignals(
      configuredSignals,
      "GET /send"
    );

    res.json({
      success: true,
      message: `Submitted ${result.signalCount} configured signal(s).`,
      submissionType: SUBMISSION_TYPE,
      signals: result.signals,
      timestamp: result.timestamp,
    });
  } catch (error) {
    console.error(`[GET /send] ${error.message}`);

    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Also supports POST /send.
// With no body, it sends the configured signals.
// With a signals array, it sends the supplied signals.
app.post("/send", async (req, res) => {
  try {
    const submittedSignals =
      Array.isArray(req.body?.signals) &&
      req.body.signals.length > 0
        ? req.body.signals
        : configuredSignals;

    const result = await submitSignals(
      submittedSignals,
      "POST /send"
    );

    res.json({
      success: true,
      message: `Submitted ${result.signalCount} signal(s).`,
      submissionType: SUBMISSION_TYPE,
      signals: result.signals,
      timestamp: result.timestamp,
    });
  } catch (error) {
    console.error(`[POST /send] ${error.message}`);

    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

// Accepts:
// {
//   "signals": [
//     {
//       "tokenAddress": "TOKEN_MINT",
//       "note": "Optional note"
//     }
//   ]
// }
app.post("/send-custom", async (req, res) => {
  try {
    const submittedSignals =
      req.body?.signals || req.body?.tokens;

    if (!Array.isArray(submittedSignals)) {
      return res.status(400).json({
        success: false,
        error:
          'Request body must contain a "signals" array.',
      });
    }

    const result = await submitSignals(
      submittedSignals,
      "POST /send-custom"
    );

    res.json({
      success: true,
      message: `Submitted ${result.signalCount} custom signal(s).`,
      submissionType: SUBMISSION_TYPE,
      signals: result.signals,
      timestamp: result.timestamp,
    });
  } catch (error) {
    console.error(`[POST /send-custom] ${error.message}`);

    res.status(400).json({
      success: false,
      error: error.message,
    });
  }
});

app.get("/history", (req, res) => {
  res.json({
    count: history.length,
    maximumHistoryEntries: MAX_HISTORY_ENTRIES,
    history,
  });
});

function getServiceStatus() {
  return {
    service: SERVICE_NAME,
    status: "running",
    websocketStatus: getWebSocketStatus(),
    submissionType: SUBMISSION_TYPE,
    payloadArray: PAYLOAD_ARRAY,
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
  };
}

function getWebSocketStatus() {
  if (!socket) {
    return "disconnected";
  }

  switch (socket.readyState) {
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

// -----------------------------------------------------------------------------
// Start service
// -----------------------------------------------------------------------------

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[SERVER] ${SERVICE_NAME} listening on port ${PORT}.`);
  console.log(
    `[CONFIG] Loaded ${configuredSignals.length} configured token(s).`
  );
  console.log("[AUTH] Send-secret authentication is disabled.");

  connectToBundles();
});
