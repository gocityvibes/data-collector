import { query } from "../db/pool.js";
import { getColumns, pickName, quoteIdent } from "../db/introspect.js";

export async function mapCandlesCols() {
  const cols = await getColumns("candles_raw");
  return {
    tsCol:    pickName(cols, ["ts_utc","ts","timestamp","time","bar_time","dt","t","created_at"]),
    symCol:   pickName(cols, ["symbol","ticker","sym"]),
    tfCol:    pickName(cols, ["timeframe","tf","interval","res","resolution"]),
    openCol:  pickName(cols, ["open","o"]),
    highCol:  pickName(cols, ["high","h"]),
    lowCol:   pickName(cols, ["low","l"]),
    closeCol: pickName(cols, ["close","c"]),
    volCol:   pickName(cols, ["volume","vol","v"]),
  };
}

export async function fetchCandles(symbol, tf, from, to, limit = 20000) {
  const { tsCol, symCol, tfCol, openCol, highCol, lowCol, closeCol, volCol } = await mapCandlesCols();
  if (!symCol || !tfCol || !openCol || !highCol || !lowCol || !closeCol || !tsCol) return [];

  const w = [], v = []; let i = 1;
  w.push(`${quoteIdent(symCol)} = $${i++}`); v.push(symbol);
  w.push(`${quoteIdent(tfCol)} = $${i++}`);  v.push(tf);
  if (from) { w.push(`${quoteIdent(tsCol)} >= $${i++}`); v.push(new Date(from).toISOString()); }
  if (to)   { w.push(`${quoteIdent(tsCol)} <= $${i++}`); v.push(new Date(to).toISOString()); }

  const sql = `
    SELECT
      ${quoteIdent(tsCol)} AS ts_utc,
      ${quoteIdent(openCol)}  AS open,
      ${quoteIdent(highCol)}  AS high,
      ${quoteIdent(lowCol)}   AS low,
      ${quoteIdent(closeCol)} AS close,
      ${volCol ? `${quoteIdent(volCol)} AS volume` : `NULL::numeric AS volume`}
    FROM "candles_raw"
    WHERE ${w.join(" AND ")}
    ORDER BY 1 ASC
    LIMIT ${Math.max(1, Math.min(20000, limit))}
  `;
  const { rows } = await query(sql, v);
  return rows.map(r => ({
    ts_utc: new Date(r.ts_utc).toISOString(),
    open: Number(r.open), high: Number(r.high), low: Number(r.low), close: Number(r.close),
    volume: r.volume == null ? null : Number(r.volume)
  }));
}
