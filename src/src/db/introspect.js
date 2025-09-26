import { query } from "./pool.js";

const colCache = new Map();

export async function getColumns(table) {
  const key = table.toLowerCase();
  if (colCache.has(key)) return colCache.get(key);
  const { rows } = await query(
    `SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=$1`,
    [key]
  );
  const cols = rows.map((r) => r.column_name);
  const set = new Set(cols.map((c) => c.toLowerCase()));
  const map = {}; cols.forEach((c) => (map[c.toLowerCase()] = c));
  const out = { list: cols, set, map };
  colCache.set(key, out);
  return out;
}

export function pickName(colset, candidates) {
  for (const c of candidates) if (colset.set.has(c)) return colset.map[c];
  return null;
}

export function quoteIdent(id) {
  return `"` + String(id).replace(/"/g, '""') + `"`;
}
