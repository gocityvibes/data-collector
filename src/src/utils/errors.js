export function badRequest(res, code, message, meta = {}) {
  return res.status(400).json({ ok: false, code, message, meta });
}
export function internalError(res, err, meta = {}) {
  const detail = err?.message || String(err);
  console.error("internal_error:", detail);
  return res.status(500).json({ ok: false, code: "internal_error", message: "Internal server error", detail, meta });
}
