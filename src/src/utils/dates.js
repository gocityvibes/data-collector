export function isIsoDateStrict(s) {
  if (typeof s !== "string") return false;
  // Require trailing Z for UTC, e.g., 2025-09-24T23:59:59Z
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(s);
}

export function toUtcIsoOrNull(s) {
  if (!s) return null;
  const d = new Date(s);
  if (isNaN(d.getTime())) return null;
  return new Date(d.toISOString()).toISOString();
}
