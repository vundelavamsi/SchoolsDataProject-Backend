const fs = require("node:fs");
const path = require("node:path");
const xlsx = require("xlsx");
const { getDb } = require("../src/db");
const { SCHOOL_FIELDS } = require("../src/fields");

const inputPath = path.resolve(process.cwd(), process.argv[2] || "../28_2811.xlsx");

if (!fs.existsSync(inputPath)) {
  // eslint-disable-next-line no-console
  console.error(`Input file not found: ${inputPath}`);
  process.exit(1);
}

const workbook = xlsx.readFile(inputPath);
const firstSheetName = workbook.SheetNames[0];
const sheet = workbook.Sheets[firstSheetName];
const sourceRows = xlsx.utils.sheet_to_json(sheet, { defval: null, raw: false });
const db = getDb();

function normalizeKey(key) {
  return String(key || "").trim();
}

function toRecord(row) {
  const normalized = {};
  for (const [key, value] of Object.entries(row)) {
    normalized[normalizeKey(key)] = value;
  }

  const record = {};
  for (const field of SCHOOL_FIELDS) {
    const value = normalized[field];
    record[field] = value == null ? null : String(value).trim();
  }
  return record;
}

function getSourceKey(record, index) {
  if (record.schoolId) {
    return `schoolId:${record.schoolId}`;
  }
  if (record.udiseschCode) {
    return `udise:${record.udiseschCode}`;
  }
  return `row:${index + 1}`;
}

const columns = ["sourceKey", ...SCHOOL_FIELDS];
const insertSql = `
  INSERT INTO schools (${columns.map((c) => `"${c}"`).join(", ")})
  VALUES (${columns.map((c) => `@${c}`).join(", ")})
  ON CONFLICT(sourceKey) DO UPDATE SET
  ${SCHOOL_FIELDS.map((c) => `"${c}"=excluded."${c}"`).join(",\n  ")};
`;
const insertStmt = db.prepare(insertSql);

let processed = 0;
db.exec("BEGIN;");
try {
  sourceRows.forEach((row, index) => {
    const record = toRecord(row);
    const payload = {
      sourceKey: getSourceKey(record, index),
      ...record
    };
    insertStmt.run(payload);
    processed += 1;
  });
  db.exec("COMMIT;");
} catch (error) {
  db.exec("ROLLBACK;");
  throw error;
}
const count = db.prepare("SELECT COUNT(*) AS count FROM schools").get().count;

// eslint-disable-next-line no-console
console.log(`Imported ${processed} rows from ${path.basename(inputPath)}. Total rows in DB: ${count}`);
