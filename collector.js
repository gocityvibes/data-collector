const { Pool } = require('pg');
const axios = require('axios');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function fetchCandles() {
  try {
    const configs = [
      { sym: 'ES=F', tf: '1m', url: 'https://query1.finance.yahoo.com/v8/finance/chart/ES=F?interval=1m&range=1d' },
      { sym: 'ES=F', tf: '5m', url: 'https://query1.finance.yahoo.com/v8/finance/chart/ES=F?interval=5m&range=5d' }
    ];
    for (const cfg of configs) {
      const res = await axios.get(cfg.url, { timeout: 10000 });
      const result = res.data?.chart?.result?.[0];
      if (!result) continue;
      const ts = result.timestamp || [];
      const q = result.indicators?.quote?.[0] || {};
      for (let i=0; i<ts.length; i++) {
        const iso = new Date(ts[i] * 1000).toISOString();
        await pool.query(
          `INSERT INTO candles_raw(symbol,timeframe,ts_utc,open,high,low,close,volume,source)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
           ON CONFLICT DO NOTHING`,
          [cfg.sym, cfg.tf, iso, q.open?.[i] ?? null, q.high?.[i] ?? null, q.low?.[i] ?? null, q.close?.[i] ?? null, q.volume?.[i] ?? null, 'yahoo']
        );
      }
    }
    console.log("Collector: batch insert complete.");
  } catch (e) {
    console.error("Collector error:", e.message);
  }
}

async function loop() {
  while (true) {
    await fetchCandles();
    await new Promise(r => setTimeout(r, 60000));
  }
}
loop();