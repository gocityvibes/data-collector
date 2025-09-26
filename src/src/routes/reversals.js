import express from "express";
import { toUtcIsoOrNull, isIsoDateStrict } from "../utils/dates.js";
import { normalizeSymbol, normalizeTf, validateTf, clamp } from "../utils/validate.js";
import { badRequest, internalError } from "../utils/errors.js";
import { DEFAULT_TICK, TICK_MAP, getReversals } from "../services/reversals.js";
import { fetchCandles } from "../services/candles.js";
import { labelReversals } from "../services/labels.js";

const router = express.Router();

router.get("/reversals", async (req, res) => {
  try {
    const symbol = normalizeSymbol(req.query.symbol);
    const tf = normalizeTf(req.query.tf);
    const from = req.query.from ? String(req.query.from).trim() : null;
    const to = req.query.to ? String(req.query.to).trim() : null;
    const limit = clamp(req.query.limit, 1, 5000, 500);

    const labelsFlag = String(req.query.labels || "0") === "1";
    const labelOnly = String(req.query.label_only || "").trim();
    const tick = req.query.tick ? Number(req.query.tick) : (TICK_MAP[symbol] || DEFAULT_TICK);
    const pivot = clamp(req.query.pivot, 1, 20, 3);
    const minChange = Number(req.query.minChange || 0);
    const confirmPoints = Number(req.query.confirmPoints || 8);
    const confirmBars = Number(req.query.confirmBars || 12);
    const stopPoints = Number(req.query.stopPoints || 8);
    const minStrongPoints = Number(req.query.minStrongPoints || 10);
    const failBackPoints = Number(req.query.failBackPoints || 6);
    const failBars = Number(req.query.failBars || 8);

    if (!symbol) return badRequest(res, "symbol_required", "symbol is required");
    if (!tf) return badRequest(res, "tf_required", "tf is required");
    if (!validateTf(tf)) return badRequest(res, "bad_tf", `tf must be one of allowed timeframes`);
    if (from && !isIsoDateStrict(from)) return badRequest(res, "bad_from", "from must be ISO-8601 UTC (e.g. 2025-09-24T00:00:00Z)");
    if (to && !isIsoDateStrict(to)) return badRequest(res, "bad_to", "to must be ISO-8601 UTC (e.g. 2025-09-24T23:59:59Z)");
    if (from && to && new Date(from) > new Date(to)) return badRequest(res, "from_after_to", "from cannot be after to");

    const { source, rows, candlesAsc } = await getReversals(symbol, tf, from, to, { limit, tick, pivot, minChange });
    let out = rows;

    if (labelsFlag) {
      const candles = candlesAsc || await fetchCandles(symbol, tf, from, to, 20000);
      out = labelReversals(candles, out, {
        tick, confirmPoints, confirmBars, stopPoints, minStrongPoints, failBackPoints, failBars
      });
      if (labelOnly === "gold" || labelOnly === "hard_negative") {
        out = out.filter(r => r.label === labelOnly);
      }
    }

    return res.json({ ok: true, count: out.length, source, rows: out.slice(0, limit) });
  } catch (err) {
    return internalError(res, err);
  }
});

export default router;
