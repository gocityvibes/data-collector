import express from "express";
import { normalizeSymbol, normalizeTf } from "../utils/validate.js";
import { badRequest, internalError } from "../utils/errors.js";
import { pd_computeDaily, pd_saveRows } from "../services/pointsDaily.js";
import { DEFAULT_TICK, TICK_MAP } from "../services/reversals.js";

const router = express.Router();

router.get("/points/daily", async (req, res) => {
  try {
    const symbol = normalizeSymbol(req.query.symbol);
    const tf = normalizeTf(req.query.tf);
    const from = req.query.from ? String(req.query.from).trim() : null;
    const to = req.query.to ? String(req.query.to).trim() : null;

    if (!symbol) return badRequest(res, "symbol_required", "symbol is required");
    if (!tf)     return badRequest(res, "tf_required", "tf is required");

    const tick = req.query.tick ? Number(req.query.tick) : (TICK_MAP[symbol] || DEFAULT_TICK);
    const pivot = Math.max(1, Math.min(20, Number(req.query.pivot || 3)));
    const minChange = Number(req.query.minChange || 0);
    const confirmPoints = Number(req.query.confirmPoints || 8);
    const confirmBars = Number(req.query.confirmBars || 12);
    const stopPoints = Number(req.query.stopPoints || 8);
    const minStrongPoints = Number(req.query.minStrongPoints || 10);
    const failBackPoints = Number(req.query.failBackPoints || 6);
    const failBars = Number(req.query.failBars || 8);
    const doSave = String(req.query.save || "0") === "1";

    const rows = await pd_computeDaily(symbol, tf, from, to, {
      tick, pivot, minChange, confirmPoints, confirmBars, stopPoints, minStrongPoints, failBackPoints, failBars
    });

    let saved = 0;
    if (doSave && rows.length) {
      try { saved = await pd_saveRows(rows); } catch {}
    }

    return res.json({ ok: true, symbol, timeframe: tf, count: rows.length, saved, rows });
  } catch (err) {
    return internalError(res, err);
  }
});

export default router;
