export function notFound(req, res, next) {
  res.status(404).json({ ok: false, code: "not_found", message: `No route for ${req.method} ${req.originalUrl}` });
}

export function errorHandler(err, req, res, next) {
  const detail = err?.message || String(err);
  res.status(500).json({ ok: false, code: "internal_error", message: "Internal server error", detail });
}
