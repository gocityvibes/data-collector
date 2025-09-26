const required = ["DATABASE_URL", "PG_CONNECTION_STRING", "POSTGRES_URL", "PG_URL"];
const haveAnyDb = required.some(k => !!process.env[k]);
if (!haveAnyDb) {
  console.warn("[env] No database connection string found in DATABASE_URL/POSTGRES_URL/PG_CONNECTION_STRING/PG_URL");
}

export const env = {
  NODE_ENV: process.env.NODE_ENV || "development",
  API_KEY: process.env.API_KEY || process.env.ADMIN_KEY || "", // allow either
  DATABASE_URL: process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.PG_CONNECTION_STRING || process.env.PG_URL || "",
  ALLOWED_TFS: (process.env.ALLOWED_TFS || "1m,5m,15m").split(",").map(s => s.trim()),
};
