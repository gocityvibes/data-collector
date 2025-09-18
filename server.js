import express from 'express';
import cors from 'cors';
import pino from 'pino-http';
import pg from 'pg';

const { Pool } = pg;

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(cors());
app.use(pino());

const ADMIN_KEY = process.env.ADMIN_KEY || '';
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('DATABASE_URL is required');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function ping() {
  const { rows } = await pool.query('select 1 as ok');
  return rows[0].ok === 1;
}

app.get('/', (_, res) => res.json({ ok: true, service: 'es-phase2-auto', ts: new Date().toISOString() }));
app.get('/health', async (_, res) => {
  try {
    await ping();
    res.json({ status: 'ok', ts: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ status: 'unhealthy', error: String(e) });
  }
});

function requireAdmin(req, res, next) {
  const key = req.query.key || req.headers['x-admin-key'];
  if (!ADMIN_KEY || key !== ADMIN_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// Apply schema (idempotent)
app.post('/admin/apply-schema', requireAdmin, async (req, res) => {
  try {
    const sql = await (await import('fs')).promises.readFile('./db/schema.sql', 'utf8');
    await pool.query(sql);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// Refresh MV + auto label
app.post('/admin/refresh-and-label', requireAdmin, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM refresh_and_auto_label();');
    res.json({ ok: true, result: rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// Quick stats
app.get('/stats', async (_, res) => {
  try {
    const q = `
      SELECT 
        (SELECT count(*) FROM reversals) as reversals_total,
        (SELECT count(*) FROM reversals WHERE outcome <> 'pending') as reversals_resolved,
        (SELECT count(*) FROM reversals_labels) as labels_total,
        (SELECT count(*) FROM training_fingerprints) as tf_rows
    `;
    const { rows } = await pool.query(q);
    res.json({ ok: true, ...rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`ES Phase2 API listening on :${PORT}`);
});
