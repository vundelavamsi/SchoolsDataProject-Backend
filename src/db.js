const fs = require("node:fs");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const { SCHOOL_FIELDS } = require("./fields");

const DATA_DIR = path.resolve(__dirname, "..", "data");
const DB_PATH = path.join(DATA_DIR, "schools.db");

let db;

function initDb() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode = WAL;");

  const allColumns = SCHOOL_FIELDS.map((field) => `"${field}" TEXT`).join(",\n");
  db.exec(`
    CREATE TABLE IF NOT EXISTS schools (
      sourceKey TEXT PRIMARY KEY,
      ${allColumns}
    );
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_schools_districtId ON schools(districtId);
    CREATE INDEX IF NOT EXISTS idx_schools_blockId ON schools(blockId);
    CREATE INDEX IF NOT EXISTS idx_schools_villageId ON schools(villageId);
    CREATE INDEX IF NOT EXISTS idx_schools_schoolStatus ON schools(schoolStatus);
    CREATE INDEX IF NOT EXISTS idx_schools_schType ON schools(schType);
    CREATE INDEX IF NOT EXISTS idx_schools_schMgmtId ON schools(schMgmtId);
    CREATE INDEX IF NOT EXISTS idx_schools_schCategoryId ON schools(schCategoryId);
    CREATE INDEX IF NOT EXISTS idx_schools_udiseschCode ON schools(udiseschCode);
    CREATE INDEX IF NOT EXISTS idx_schools_stateId ON schools(stateId);
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS school_edits (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      sourceKey   TEXT NOT NULL,
      fieldName   TEXT NOT NULL,
      oldValue    TEXT,
      newValue    TEXT NOT NULL,
      submittedBy TEXT NOT NULL,
      submittedAt TEXT NOT NULL,
      status      TEXT NOT NULL DEFAULT 'pending',
      reviewedAt  TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_edits_sourceKey ON school_edits(sourceKey);
    CREATE INDEX IF NOT EXISTS idx_edits_status ON school_edits(status);
  `);

  return db;
}

function getDb() {
  if (!db) {
    return initDb();
  }
  return db;
}

module.exports = { getDb, DB_PATH };
