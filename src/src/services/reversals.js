import { query } from "../db/pool.js";
import { getColumns, pickName, quoteIdent } from "../db/introspect.js";
import { fetchCandles } from "./candles.js";

export const DEFAULT_TICK = 1;
export const TICK_MAP = {
  "ES=F": 0.25, "NQ=F": 0.25, "YM=F": 1, "RTY=F": 0.1,
  "GC=F": 0.1,  "SI=F": 0.005, "CL=F": 0.01,
  "SPY": 0.01, "AAPL": 0.01
};

export async function mapReversalsCols() {
  const cols = await getColumns("reversals");
  return {
    tsCol:   pickName(cols, ["ts_utc","ts","timestamp","time","bar_time","dt","t","created_at"]),
    symCol:  pickName(cols, ["symbol","ticker","sym"]),
    tfCol:   pickName(cols, ["timeframe","tf","interval","res","resolution"]),
    dirCol:  pickName(cols, ["direction","dir","side"]),
    scoreCol:pickName(cols, ["score","confidence","prob","probability"]),
    ptsCol:  pickName(cols, ["points","pts","score_points"]),
  };
}

// O(n * pivot) simple detector
export function computeReversals(candlesAsc, { pivot = 3, minChange = 0, tick = null } = {}) {
  const n = candlesAsc.length;
  if (n < pivot * 2 + 1) return [];
  const revs = [];
  let lastPrice = null;

  for (let i = pivot; i < n - pivot; i++) {
    const hiWin = candlesAsc.slice(i - pivot, i + pivot + 1).map((b) => Number(b.high));
    const loWin = candlesAsc.slice(i - pivot, i + pivot + 1).map((b) => Number(b.low));
    const maxH = Math.max(...hiWin);
    const minL = Math.min(...loWin);
    const isPeak = Number(candlesAsc[i].high) === maxH;
    const isTrough = Number(candlesAsc[i].low) === minL;
    if (!isPeak && !isTrough) continue;

    const ts = candlesAsc[i].ts_utc;
    const price = isPeak ? Number(candlesAsc[i].high) : Number(candlesAsc[i].low);
    if (lastPrice !== null && Math.abs(price - lastPrice) < minChange) continue;

    const direction = isPeak ? "down" : "up";
    const score = lastPrice === null ? 0 : Math.abs(price - lastPrice);
    const points = tick ? Math.round(score / tick) : Math.round(score);
    revs.push({ ts_utc: new Date(ts).toISOString(), direction, score, points });
    lastPrice = price;
  }
  return revs.reverse(); // newest first
}

export async function readReversalsFromTable(symbol, tf, from, to, limit) {
  const { tsCol, symCol, tfCol, dirCol, scoreCol, ptsCol } = await mapReversalsCols();
  if (!symCol || !tfCol || !tsCol) return []; // require ts for safe ORDER BY

  const w = [], v = []; let i = 1;
  w.push(`${quoteIdent(symCol)} = $${i++}`); v.push(symbol);
  w.push(`${quoteIdent(tfCol)} = $${i++}`); v.push(tf);
  if (from) { w.push(`${quoteIdent(tsCol)} >= $${i++}`); v.push(new Date(from).toISOString()); }
  if (to)   { w.push(`${quoteIdent(tsCol)} <= $${i++}`); v.push(new Date(to).toISOString()); }

  const dirSel = dirCol ? `${quoteIdent(dirCol)} AS direction` : `NULL::text AS direction`;
  const scoreSel = scoreCol ? `${quoteIdent(scoreCol)} AS score` : `NULL::numeric AS score`;
  const ptsSel = ptsCol ? `${quoteIdent(ptsCol)} AS points` : `NULL::int AS points`;

  const sql = `
    SELECT
      ${quoteIdent(tsCol)} AS ts_utc,
      ${quoteIdent(symCol)} AS symbol,
      ${quoteIdent(tfCol)}  AS timeframe,
      ${dirSel},
      ${scoreSel},
      ${ptsSel}
    FROM "reversals"
    WHERE ${w.join(" AND ")}
    ORDER BY 1 DESC
    LIMIT ${Math.max(1, Math.min(5000, limit || 500))}
  `;
  const { rows } = await query(sql, v);
  return rows;
}

export async function getReversals(symbol, tf, from, to, opts = {}) {
  const limit = opts.limit || 500;
  // Try table
  const tableRows = await readReversalsFromTable(symbol, tf, from, to, limit);
  if (tableRows.length) return { source: "table", rows: tableRows.slice(0, limit) };
  // Fallback compute
  const tick = opts.tick || TICK_MAP[symbol] || DEFAULT_TICK;
  const candlesAsc = await fetchCandles(symbol, tf, from, to, 20000);
  const computed = computeReversals(candlesAsc, { pivot: opts.pivot || 3, minChange: opts.minChange || 0, tick });
  return { source: "computed", rows: computed.slice(0, limit), candlesAsc };
}
