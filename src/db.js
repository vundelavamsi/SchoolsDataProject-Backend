const { Pool } = require("pg");
const { SCHOOL_FIELDS } = require("./fields");

const connectionString = process.env.DATABASE_URL;
const isRemote = connectionString && !connectionString.includes("localhost") && !connectionString.includes("127.0.0.1");

const pool = new Pool({
  connectionString,
  ssl: isRemote ? { rejectUnauthorized: false } : false,
  max: 20,
});

async function initDb() {
  const allColumns = SCHOOL_FIELDS.map((field) => `"${field}" TEXT`).join(",\n    ");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS schools (
      "sourceKey" TEXT PRIMARY KEY,
      ${allColumns}
    )
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_districtId ON schools("districtId")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_blockId ON schools("blockId")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_villageId ON schools("villageId")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_schoolStatus ON schools("schoolStatus")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_schType ON schools("schType")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_schMgmtId ON schools("schMgmtId")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_schCategoryId ON schools("schCategoryId")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_udiseschCode ON schools("udiseschCode")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_schools_stateId ON schools("stateId")`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS school_edits (
      id SERIAL PRIMARY KEY,
      "sourceKey" TEXT NOT NULL,
      "fieldName" TEXT NOT NULL,
      "oldValue" TEXT,
      "newValue" TEXT NOT NULL,
      "submittedBy" TEXT NOT NULL,
      "submittedPhone" TEXT,
      "submittedAt" TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      "reviewedAt" TEXT
    )
  `);

  await pool.query(`ALTER TABLE school_edits ADD COLUMN IF NOT EXISTS "submittedPhone" TEXT`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_edits_sourceKey ON school_edits("sourceKey")`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_edits_status ON school_edits(status)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_edits_submittedPhone ON school_edits("submittedPhone")`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS phone_access (
      phone TEXT PRIMARY KEY,
      role TEXT NOT NULL CHECK (role IN ('edit', 'review')),
      "blockIds" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
      "createdAt" TEXT NOT NULL,
      "updatedAt" TEXT NOT NULL
    )
  `);
}

module.exports = { pool, initDb };
