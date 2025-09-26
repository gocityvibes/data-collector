import express from "express";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";

import { env } from "./src/config/env.js";
import { requestId } from "./src/middleware/requestId.js";
import { auth } from "./src/middleware/auth.js";
import { notFound, errorHandler } from "./src/middleware/errorHandler.js";

import candlesRoute from "./src/routes/candles.js";
import reversalsRoute from "./src/routes/reversals.js";
import indicatorsRoute from "./src/routes/indicators.js";
import pointsDailyRoute from "./src/routes/pointsDaily.js";

const app = express();

// Basic hardening
app.use(helmet());
app.use(cors({ origin: true })); // keep simple for now
app.use(express.json({ limit: "1mb" }));
app.use(requestId);
app.use(morgan("combined"));

// Health
app.get("/health", (req, res) => res.json({ ok: true, status: "healthy", ts: new Date().toISOString() }));

// Auth (simple API key if provided)
if (env.API_KEY) {
  app.use(auth);
}

// Routes
app.use(candlesRoute);
app.use(reversalsRoute);
app.use(indicatorsRoute);
app.use(pointsDailyRoute);

// 404 + error handler
app.use(notFound);
app.use(errorHandler);

// Start if run directly
if (process.env.NODE_ENV !== "test") {
  const port = Number(process.env.PORT || 3000);
  app.listen(port, () => {
    console.log(`Server listening on :${port}`);
  });
}

export default app;
