import express from "express";
import { normalizeSymbol, normalizeTf, validateTf, clamp } from "../utils/validate.js";
import { badRequest, internalError } from "../utils/errors.js";
import { fetchCandles } from "../services/candles.js";
import { ema, rsi, macd, atr, bollinger } from "../services/indicators.js";

const router = express.Router();

router.get("/indicators", async (req, res) => {
  try {
    const symbol = normalizeSymbol(req.query.symbol);
    const tf = normalizeTf(req.query.tf);
    const from = req.query.from ? String(req.query.from).trim() : null;
    const to = req.query.to ? String(req.query.to).trim() : null;
    const limit = clamp(req.query.limit, 1, 10000, 2000);

    if (!symbol) return badRequest(res, "symbol_required", "symbol is required");
    if (!tf) return badRequest(res, "tf_required", "tf is required");
    if (!validateTf(tf)) return badRequest(res, "bad_tf", `tf must be one of allowed timeframes`);

    const candles = await fetchCandles(symbol, tf, from, to, limit);
    if (!candles.length) return res.json({ ok: true, count: 0, rows: [] });

    const closes = candles.map(c => c.close);
    const ema20 = ema(closes, 20);
    const ema50 = ema(closes, 50);
    const rsi14 = rsi(closes, 14);
    const { macdLine, signalLine, hist } = macd(closes, 12, 26, 9);
    const atr14 = atr(candles, 14);
    const { mid: bbMid, upper: bbU, lower: bbL } = bollinger(closes, 20, 2);

    const rows = candles.map((c, i) => ({
      ts_utc: c.ts_utc,
      open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume,
      ema20: ema20[i], ema50: ema50[i],
      rsi14: rsi14[i],
      macd: macdLine[i], macd_signal: signalLine[i], macd_hist: hist[i],
      atr14: atr14[i],
      bb_mid: bbMid[i], bb_upper: bbU[i], bb_lower: bbL[i]
    }));

    return res.json({ ok: true, count: rows.length, rows });
  } catch (err) {
    return internalError(res, err);
  }
});

export default router;
