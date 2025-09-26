import express from "express";
import { toUtcIsoOrNull, isIsoDateStrict } from "../utils/dates.js";
import { normalizeSymbol, normalizeTf, validateTf, clamp } from "../utils/validate.js";
import { badRequest, internalError } from "../utils/errors.js";
import { fetchCandles } from "../services/candles.js";

const router = express.Router();

router.get("/candles", async (req, res) => {
  try {
    const symbol = normalizeSymbol(req.query.symbol);
    const tf = normalizeTf(req.query.tf);
    const from = req.query.from ? String(req.query.from).trim() : null;
    const to = req.query.to ? String(req.query.to).trim() : null;
    const limit = clamp(req.query.limit, 1, 10000, 2000);

    if (!symbol) return badRequest(res, "symbol_required", "symbol is required");
    if (!tf) return badRequest(res, "tf_required", "tf is required");
    if (!validateTf(tf)) return badRequest(res, "bad_tf", `tf must be one of allowed timeframes`);
    if (from && !isIsoDateStrict(from)) return badRequest(res, "bad_from", "from must be ISO-8601 UTC (e.g. 2025-09-24T00:00:00Z)");
    if (to && !isIsoDateStrict(to)) return badRequest(res, "bad_to", "to must be ISO-8601 UTC (e.g. 2025-09-24T23:59:59Z)");
    if (from && to && new Date(from) > new Date(to)) return badRequest(res, "from_after_to", "from cannot be after to");

    const rows = await fetchCandles(symbol, tf, from, to, limit);
    return res.json({ ok: true, count: rows.length, rows });
  } catch (err) {
    return internalError(res, err);
  }
});

export default router;
