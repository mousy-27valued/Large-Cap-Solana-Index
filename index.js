import "dotenv/config";
import express from "express";
import WebSocket from "ws";
import crypto from "crypto";

const PORT = Number(process.env.PORT || 3000);

const SERVICE_NAME =
  process.env.SERVICE_NAME || "Bundles.trade signal sender";

const BUNDLES_WS_URL =
  process.env.BUNDLES_WS_URL || "wss://bundles.trade/ws/agentic";

const BUNDLES_API_KEY = process.env.BUNDLES_API_KEY || "";
const BUNDLE_ADDRESS = process.env.BUNDLE_ADDRESS || "";
const SEND_SECRET = process.env.SEND_SECRET || "";

const NOTE =
  process.env.NOTE || "Candidate submitted from Railway";

const AUTO_SEND_ON_START =
  String(process.env.AUTO_SEND_ON_START || "false").toLowerCase() ===
  "true";

const REQUEST_TIMEOUT_MS = Number(
  process.env.REQUEST_TIMEOUT_MS || 15000
);

const MAX_HISTORY_ENTRIES = Math.min(
  Math.max(Number(process.env.MAX_HISTORY_ENTRIES || 100), 1),
  1000
);

const TOKEN_ADDRESSES = parseTokenAddresses(
  process.env.TOKEN_ADDRESSES || ""
);

const submissionHistory = [];

/*
|--------------------------------------------------------------------------
| Token address helpers
|--------------------------------------------------------------------------
*/

function parseTokenAddresses(input) {
  return [
    ...new Set(
      String(input)
        .split(/[\s,]+/)
        .map((address) => address.trim())
        .filter(Boolean)
    ),
  ];
}

function isLikelySolanaAddress(address) {
  return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(address);
}

function prepareTokenAddresses(tokenAddresses) {
  const uniqueAddresses = [
    ...new Set(
      tokenAddresses
        .map((address) => String(address).trim())
        .filter(Boolean)
    ),
  ];

  const validAddresses = uniqueAddresses.filter(
    isLikelySolanaAddress
  );

  const invalidAddresses = uniqueAddresses.filter(
    (address) => !isLikelySolanaAddress(address)
  );

  if (invalidAddresses.length > 0) {
    console.warn(
      "Skipping invalid-looking token addresses:",
      invalidAddresses
    );
  }

  if (validAddresses.length === 0) {
    throw new Error(
      "No valid Solana token addresses were supplied."
    );
  }

  if (validAddresses.length > 20) {
    throw new Error(
      "Bundles accepts a maximum of 20 signals per submission."
    );
  }

  return validAddresses;
}

/*
|--------------------------------------------------------------------------
| Configuration validation
|--------------------------------------------------------------------------
*/

function validateConfiguration() {
  const missingVariables = [];

  if (!BUNDLES_API_KEY) {
    missingVariables.push("BUNDLES_API_KEY");
  }

  if (!BUNDLE_ADDRESS) {
    missingVariables.push("BUNDLE_ADDRESS");
  }

  if (missingVariables.length > 0) {
    throw new Error(
      `Missing required Railway variables: ${missingVariables.join(", ")}`
    );
  }
}

/*
|--------------------------------------------------------------------------
| Submission history
|--------------------------------------------------------------------------
*/

function addHistoryEntry(entry) {
  const historyEntry = {
    id: crypto.randomUUID(),
    timestamp: new Date().toISOString(),
    ...entry,
  };

  submissionHistory.unshift(historyEntry);

  if (submissionHistory.length > MAX_HISTORY_ENTRIES) {
    submissionHistory.length = MAX_HISTORY_ENTRIES;
  }

  console.log(
    "SUBMISSION_LOG",
    JSON.stringify(historyEntry)
  );

  return historyEntry;
}

/*
|--------------------------------------------------------------------------
| Endpoint security
|--------------------------------------------------------------------------
*/

function readProvidedSecret(req) {
  return String(
    req.query.key ||
      req.get("x-send-secret") ||
      ""
  );
}

function secretsMatch(providedSecret, requiredSecret) {
  const provided = Buffer.from(String(providedSecret));
  const required = Buffer.from(String(requiredSecret));

  if (provided.length !== required.length) {
    return false;
  }

  return crypto.timingSafeEqual(provided, required);
}

function requireSendSecret(req, res, next) {
  if (!SEND_SECRET) {
    return res.status(503).json({
      success: false,
      error:
        "SEND_SECRET is not configured in Railway Variables.",
    });
  }

  const providedSecret = readProvidedSecret(req);

  if (!secretsMatch(providedSecret, SEND_SECRET)) {
    return res.status(401).json({
      success: false,
      error: "Unauthorized.",
    });
  }

  next();
}

/*
|--------------------------------------------------------------------------
| Bundles WebSocket signal submission
|--------------------------------------------------------------------------
*/

function submitSignals(tokenAddresses) {
  validateConfiguration();

  const validAddresses = prepareTokenAddresses(tokenAddresses);

  return new Promise((resolve, reject) => {
    let finished = false;
    let payloadSent = false;

    console.log(`Connecting to ${BUNDLES_WS_URL}...`);

    const socket = new WebSocket(BUNDLES_WS_URL);

    const timeout = setTimeout(() => {
      if (finished) {
        return;
      }

      finished = true;

      if (
        socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CONNECTING
      ) {
        socket.close();
      }

      if (payloadSent) {
        resolve({
          success: false,
          error:
            "Signal payload was sent, but no acknowledgement was received before timeout.",
          submittedTokens: validAddresses,
        });
      } else {
        reject(
          new Error(
            "Timed out before the signal payload could be sent."
          )
        );
      }
    }, REQUEST_TIMEOUT_MS);

    function complete(result) {
      if (finished) {
        return;
      }

      finished = true;
      clearTimeout(timeout);

      if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }

      resolve(result);
    }

    function fail(error) {
      if (finished) {
        return;
      }

      finished = true;
      clearTimeout(timeout);

      if (
        socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CONNECTING
      ) {
        socket.close();
      }

      reject(error);
    }

    socket.on("open", () => {
      console.log("Connected to Bundles.trade.");

      const payload = {
        type: "agentic:signal:submit",
        bundleAddress: BUNDLE_ADDRESS,
        apiKey: BUNDLES_API_KEY,
        signals: validAddresses.map((tokenAddress, index) => ({
          tokenAddress,
          note: `${NOTE} - candidate ${index + 1} of ${validAddresses.length}`,
        })),
      };

      console.log(
        `Submitting ${validAddresses.length} candidate signal(s).`
      );

      /*
      The API key is intentionally excluded from logs.
      */
      console.log({
        type: payload.type,
        bundleAddress: payload.bundleAddress,
        signals: payload.signals,
      });

      socket.send(JSON.stringify(payload), (error) => {
        if (error) {
          fail(error);
          return;
        }

        payloadSent = true;

        console.log(
          "Signal payload sent to Bundles.trade."
        );
      });
    });

    socket.on("message", (data) => {
      const rawMessage = data.toString();

      let bundlesResponse;

      try {
        bundlesResponse = JSON.parse(rawMessage);
      } catch {
        bundlesResponse = {
          ok: false,
          error: "Bundles returned a non-JSON response.",
          rawResponse: rawMessage,
        };
      }

      console.log(
        "Bundles response:",
        bundlesResponse
      );

      complete({
        success: bundlesResponse?.ok === true,
        submittedTokens: validAddresses,
        bundlesResponse,
      });
    });

    socket.on("error", (error) => {
      console.error(
        "WebSocket error:",
        error.message
      );

      fail(error);
    });

    socket.on("close", (code, reasonBuffer) => {
      const reason = reasonBuffer?.toString() || "";

      console.log(
        `WebSocket closed. Code: ${code}. Reason: ${reason}`
      );

      if (!finished && !payloadSent) {
        fail(
          new Error(
            `Connection closed before signal submission. Code ${code}: ${reason}`
          )
        );
      }
    });
  });
}

/*
|--------------------------------------------------------------------------
| Express application
|--------------------------------------------------------------------------
*/

const app = express();

app.disable("x-powered-by");
app.use(express.json({ limit: "100kb" }));

app.get("/", (req, res) => {
  res.json({
    service: SERVICE_NAME,
    status: "running",
    submissionType: "agentic:signal:submit",
    payloadArray: "signals",
    configuredTokenCount: TOKEN_ADDRESSES.length,
    maximumSignalsPerSubmission: 20,
    storedHistoryEntries: submissionHistory.length,
    maximumHistoryEntries: MAX_HISTORY_ENTRIES,
    autoSendOnStart: AUTO_SEND_ON_START,
    sendSecretConfigured: Boolean(SEND_SECRET),
    endpoints: {
      health: "GET /health",
      sendFromBrowser:
        "GET /send?key=YOUR_SEND_SECRET",
      sendConfiguredSignals:
        "POST /send with X-Send-Secret header",
      sendCustomSignals:
        "POST /send-custom with X-Send-Secret header",
      viewHistory:
        "GET /history?key=YOUR_SEND_SECRET",
    },
  });
});

app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: SERVICE_NAME,
    timestamp: new Date().toISOString(),
  });
});

/*
|--------------------------------------------------------------------------
| Shared configured-token submission handler
|--------------------------------------------------------------------------
*/

async function handleConfiguredSubmission(req, res) {
  const startedAt = Date.now();

  try {
    if (TOKEN_ADDRESSES.length === 0) {
      const errorMessage =
        "TOKEN_ADDRESSES is empty. Add at least one address in Railway Variables.";

      addHistoryEntry({
        requestMethod: req.method,
        endpoint: "/send",
        success: false,
        tokenAddresses: [],
        error: errorMessage,
        durationMs: Date.now() - startedAt,
      });

      return res.status(400).json({
        success: false,
        error: errorMessage,
      });
    }

    const result = await submitSignals(
      TOKEN_ADDRESSES
    );

    addHistoryEntry({
      requestMethod: req.method,
      endpoint: "/send",
      success: result.success,
      tokenAddresses: result.submittedTokens,
      acceptedCount:
        result.bundlesResponse?.acceptedCount ?? 0,
      bundlesResponse:
        result.bundlesResponse || null,
      error:
        result.error ||
        result.bundlesResponse?.error ||
        null,
      durationMs: Date.now() - startedAt,
    });

    return res
      .status(result.success ? 200 : 400)
      .json(result);
  } catch (error) {
    console.error(
      "Configured signal submission failed:",
      error
    );

    addHistoryEntry({
      requestMethod: req.method,
      endpoint: "/send",
      success: false,
      tokenAddresses: TOKEN_ADDRESSES,
      error: error.message,
      durationMs: Date.now() - startedAt,
    });

    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/*
Opening this URL in a browser submits the configured tokens:

https://your-domain.up.railway.app/send?key=YOUR_SECRET
*/
app.get(
  "/send",
  requireSendSecret,
  handleConfiguredSubmission
);

/*
POST remains available for Terminal, webhooks, and API clients.
*/
app.post(
  "/send",
  requireSendSecret,
  handleConfiguredSubmission
);

/*
Submit a custom token list without changing Railway Variables.

Request body:
{
  "tokenAddresses": [
    "TOKEN_ADDRESS_1",
    "TOKEN_ADDRESS_2"
  ]
}
*/
app.post(
  "/send-custom",
  requireSendSecret,
  async (req, res) => {
    const startedAt = Date.now();
    let cleanedAddresses = [];

    try {
      const suppliedAddresses =
        req.body?.tokenAddresses;

      if (!Array.isArray(suppliedAddresses)) {
        const errorMessage =
          "Request body must contain a tokenAddresses array.";

        addHistoryEntry({
          requestMethod: "POST",
          endpoint: "/send-custom",
          success: false,
          tokenAddresses: [],
          error: errorMessage,
          durationMs: Date.now() - startedAt,
        });

        return res.status(400).json({
          success: false,
          error: errorMessage,
          example: {
            tokenAddresses: [
              "SolanaContractAddressOne",
              "SolanaContractAddressTwo",
            ],
          },
        });
      }

      cleanedAddresses = parseTokenAddresses(
        suppliedAddresses.join(",")
      );

      const result = await submitSignals(
        cleanedAddresses
      );

      addHistoryEntry({
        requestMethod: "POST",
        endpoint: "/send-custom",
        success: result.success,
        tokenAddresses: result.submittedTokens,
        acceptedCount:
          result.bundlesResponse?.acceptedCount ?? 0,
        bundlesResponse:
          result.bundlesResponse || null,
        error:
          result.error ||
          result.bundlesResponse?.error ||
          null,
        durationMs: Date.now() - startedAt,
      });

      return res
        .status(result.success ? 200 : 400)
        .json(result);
    } catch (error) {
      console.error(
        "Custom signal submission failed:",
        error
      );

      addHistoryEntry({
        requestMethod: "POST",
        endpoint: "/send-custom",
        success: false,
        tokenAddresses: cleanedAddresses,
        error: error.message,
        durationMs: Date.now() - startedAt,
      });

      return res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  }
);

/*
View recent submission history:

https://your-domain.up.railway.app/history?key=YOUR_SECRET
*/
app.get(
  "/history",
  requireSendSecret,
  (req, res) => {
    res.json({
      success: true,
      count: submissionHistory.length,
      maximumStored: MAX_HISTORY_ENTRIES,
      warning:
        "In-memory history resets when Railway restarts or redeploys.",
      history: submissionHistory,
    });
  }
);

/*
|--------------------------------------------------------------------------
| Start the server
|--------------------------------------------------------------------------
*/

app.listen(PORT, async () => {
  console.log(
    `${SERVICE_NAME} listening on port ${PORT}.`
  );

  console.log(
    `Configured signal token count: ${TOKEN_ADDRESSES.length}.`
  );

  console.log(
    "WebSocket submission type: agentic:signal:submit"
  );

  console.log(
    "Payload array field: signals"
  );

  console.log(
    `Send secret configured: ${Boolean(SEND_SECRET)}`
  );

  console.log(
    `Maximum history entries: ${MAX_HISTORY_ENTRIES}`
  );

  if (!AUTO_SEND_ON_START) {
    return;
  }

  if (TOKEN_ADDRESSES.length === 0) {
    console.error(
      "AUTO_SEND_ON_START is true, but TOKEN_ADDRESSES is empty."
    );

    return;
  }

  const startedAt = Date.now();

  try {
    await new Promise((resolve) =>
      setTimeout(resolve, 1000)
    );

    const result = await submitSignals(
      TOKEN_ADDRESSES
    );

    addHistoryEntry({
      requestMethod: "AUTOMATIC",
      endpoint: "startup",
      success: result.success,
      tokenAddresses: result.submittedTokens,
      acceptedCount:
        result.bundlesResponse?.acceptedCount ?? 0,
      bundlesResponse:
        result.bundlesResponse || null,
      error:
        result.error ||
        result.bundlesResponse?.error ||
        null,
      durationMs: Date.now() - startedAt,
    });

    console.log(
      "Automatic signal submission complete:",
      result
    );
  } catch (error) {
    console.error(
      "Automatic signal submission failed:",
      error.message
    );

    addHistoryEntry({
      requestMethod: "AUTOMATIC",
      endpoint: "startup",
      success: false,
      tokenAddresses: TOKEN_ADDRESSES,
      error: error.message,
      durationMs: Date.now() - startedAt,
    });
  }
});
