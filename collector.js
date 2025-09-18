const { Pool } = require('pg');
const axios = require('axios');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function fetchCandle() {
  try {
    // Example: 1m candle for ES=F
    const url = 'https://query1.finance.yahoo.com/v8/finance/chart/ES=F?interval=1m&range=1d';
    const res = await axios.get(url);
    const data = res.data.chart.result[0];
    const ts = new Date(data.meta.regularMarketTime * 1000).toISOString();
    const close = data.meta.regularMarketPrice;

    await pool.query(
      'INSERT INTO candles_raw(symbol,timeframe,ts_utc,close,source) VALUES($1,$2,$3,$4,$5)',
      ['ES=F','1m',ts,close,'yahoo']
    );
    console.log("Inserted candle:", ts, close);
  } catch (err) {
    console.error("Collector error:", err.message);
  }
}

async function loop() {
  while (true) {
    await fetchCandle();
    await new Promise(r => setTimeout(r, 60000)); // every 1 min
  }
}

loop();