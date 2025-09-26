import { env } from "../config/env.js";

export function auth(req, res, next) {
  const hdr = req.headers["x-api-key"] || (req.headers["authorization"] || "").replace(/^Bearer\s+/i, "");
  if (!hdr || hdr !== env.API_KEY) {
    return res.status(401).json({ ok: false, code: "unauthorized", message: "Invalid or missing API key" });
  }
  next();
}
