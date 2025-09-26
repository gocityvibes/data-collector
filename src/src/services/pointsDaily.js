import { query } from "../db/pool.js";
import { fetchCandles } from "./candles.js";
import { computeReversals, DEFAULT_TICK, TICK_MAP } from "./reversals.js";
import { pd_labelOne } from "./labels.js";

export async function pd_ensureTable() {
  const ddl = `
    CREATE TABLE IF NOT EXISTS points_daily(
      id BIGSERIAL PRIMARY KEY,
      date DATE NOT NULL,
      symbol TEXT NOT NULL,
      timeframe TEXT NOT NULL,
      points INT NOT NULL,
      win_rate NUMERIC NOT NULL,
      gold INT NOT NULL,
      negative INT NOT NULL,
      none INT NOT NULL,
      UNIQUE(date, symbol, timeframe)
    );
    CREATE INDEX IF NOT EXISTS idx_points_daily_date ON points_daily(date);
  `;
  await query(ddl);
}

export async function pd_computeDaily(symbol, tf, from, to, opts = {}) {
  const tick = opts.tick ?? TICK_MAP[symbol] ?? DEFAULT_TICK;
  const candlesAsc = await fetchCandles(symbol, tf, from, to, 20000);
  const revs = computeReversals(candlesAsc, { pivot: opts.pivot || 3, minChange: opts.minChange || 0, tick }); // newest-first
  const days = new Map(); // YYYY-MM-DD -> {points, gold, negative, none}

  for (const r of revs) {
    const day = new Date(r.ts_utc).toISOString().slice(0,10);
    if (!days.has(day)) days.set(day, { points: 0, gold: 0, negative: 0, none: 0 });

    const { label } = pd_labelOne(candlesAsc, r, { tick, ...opts });
    const b = days.get(day);
    if (label === "gold") { b.gold += 1; b.points += Number(r.points||0); }
    else if (label === "hard_negative") { b.negative += 1; b.points -= Number(r.points||0); }
    else { b.none += 1; }
  }

  const rows = [];
  for (const [day, b] of [...days.entries()].sort()) {
    const denom = b.gold + b.negative;
    const win_rate = denom > 0 ? b.gold / denom : 0;
    rows.push({ date: day, symbol, timeframe: tf, points: b.points, win_rate, gold: b.gold, negative: b.negative, none: b.none });
  }
  return rows;
}

export async function pd_saveRows(rows) {
  await pd_ensureTable();
  let saved = 0;
  for (const r of rows) {
    const sql = `
      INSERT INTO points_daily(date, symbol, timeframe, points, win_rate, gold, negative, none)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (date, symbol, timeframe)
      DO UPDATE SET points=EXCLUDED.points, win_rate=EXCLUDED.win_rate, gold=EXCLUDED.gold, negative=EXCLUDED.negative, none=EXCLUDED.none
    `;
    const params = [r.date, r.symbol, r.timeframe, r.points, r.win_rate, r.gold, r.negative, r.none];
    try { const res = await query(sql, params); saved += res.rowCount ? 1 : 0; } catch {}
  }
  return saved;
}
