const sql = require("mssql");

// Environment-driven, production-safe configuration
const toBool = (v, def) => {
  if (v === undefined) return def;
  return String(v).toLowerCase() === "true";
};

const baseConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  pool: {
    max: Number(process.env.DB_POOL_MAX || 10),
    min: Number(process.env.DB_POOL_MIN || 0),
    idleTimeoutMillis: Number(process.env.DB_IDLE_TIMEOUT || 30000),
  },
  options: {
    encrypt: toBool(process.env.DB_ENCRYPT, true),
    // Use false in production unless you specifically need to trust a self-signed cert
    trustServerCertificate: toBool(process.env.DB_TRUST_SERVER_CERT, false),
  },
  connectionTimeout: Number(process.env.DB_CONNECTION_TIMEOUT || 15000),
  requestTimeout: Number(process.env.DB_REQUEST_TIMEOUT || 30000),
};

let pool; // singleton instance

const createPool = async () => {
  const connection = new sql.ConnectionPool(baseConfig);
  // Attach listeners to mark pool unusable; next getPool call will recreate
  connection.on("error", (err) => {
    console.error("[DB] Pool error:", err?.message || err);
    try { connection.close(); } catch (_) {}
    pool = undefined;
  });
  connection.on("close", () => {
    console.warn("[DB] Pool closed");
    pool = undefined;
  });
  await connection.connect();
  console.log("[DB] Connected to MSSQL with pooling");
  return connection;
};

// Minimal retry for transient startup failures
const getPool = async () => {
  if (pool && pool.connected) return pool;
  const maxAttempts = 3;
  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      pool = await createPool();
      return pool;
    } catch (err) {
      lastErr = err;
      console.error(`[DB] Connect attempt ${attempt} failed:`, err?.message || err);
      if (attempt < maxAttempts) {
        const backoff = 500 * attempt; // simple linear backoff
        await new Promise((res) => setTimeout(res, backoff));
      }
    }
  }
  throw lastErr || new Error("Failed to initialize DB pool");
};

// Common request for SQLquery
const SQLquery = async (queryString, params = {}) => {
  try {
    const activePool = await getPool();
    const request = activePool.request();

    // Bind params as param0, param1, ...; consumer must reference in query
    Object.keys(params).forEach((key, index) => {
      request.input(`param${index}`, params[key] ?? null);
    });

    const result = await request.query(queryString);
    return result.recordset;
  } catch (error) {
    console.error("[DB] Query execution error:", error?.message || error);
    throw error;
  }
};

module.exports = {
  SQLquery,
  getPool,
};
