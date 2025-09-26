import { env } from "../config/env.js";

export function normalizeSymbol(sym) {
  return String(sym || "").trim().toUpperCase();
}
export function normalizeTf(tf) {
  return String(tf || "").trim();
}
export function validateTf(tf) {
  return env.ALLOWED_TFS.includes(tf);
}
export function clamp(n, lo, hi, dflt) {
  const x = Number(n);
  if (!Number.isFinite(x)) return dflt;
  return Math.max(lo, Math.min(hi, x));
}
