import express from "express";
import dotenv from "dotenv";
import logger from "./logger.js";
import { initDb } from "./db.js";
import { collectCandles } from "./collector.js";
import pool from "./db.js";

dotenv.config();
const app = express();
const PORT = process.env.PORT || 3000;

app.get("/db/ping", async (req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");
    res.json({ ok: true, now: result.rows[0].now });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/candles/recent", async (req, res) => {
  const { tf = "1m", limit = 100 } = req.query;
  const result = await pool.query(
    "SELECT * FROM candles_raw WHERE timeframe=$1 ORDER BY ts_utc DESC LIMIT $2",
    [tf, limit]
  );
  res.json({ ok: true, count: result.rowCount, rows: result.rows });
});

app.get("/candles/range", async (req, res) => {
  const { tf = "1m", from, to } = req.query;
  if (!from || !to) return res.status(400).json({ ok: false, error: "from and to required" });
  const result = await pool.query(
    "SELECT * FROM candles_raw WHERE timeframe=$1 AND ts_utc BETWEEN $2 AND $3 ORDER BY ts_utc ASC",
    [tf, from, to]
  );
  res.json({ ok: true, count: result.rowCount, rows: result.rows });
});

app.get("/candles/last60days", async (req, res) => {
  const { tf = "1m" } = req.query;
  const result = await pool.query(
    "SELECT * FROM candles_raw WHERE timeframe=$1 AND ts_utc > NOW() - interval '60 days' ORDER BY ts_utc ASC",
    [tf]
  );
  res.json({ ok: true, count: result.rowCount, rows: result.rows });
});

// run collector every minute
setInterval(() => {
  collectCandles();
}, 60 * 1000);

(async () => {
  await initDb();
  await collectCandles();
  app.listen(PORT, () => logger.info(`🚀 Server running on port ${PORT}`));
})();
