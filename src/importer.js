const xlsx = require("xlsx");
const { getDb } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");

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

function importWorkbookBuffer(buffer) {
  const workbook = xlsx.read(buffer, { type: "buffer" });
  const firstSheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[firstSheetName];
  const sourceRows = xlsx.utils.sheet_to_json(sheet, { defval: null, raw: false });

  const columns = ["sourceKey", ...SCHOOL_FIELDS];
  const insertSql = `
    INSERT INTO schools (${columns.map((c) => `"${c}"`).join(", ")})
    VALUES (${columns.map((c) => `@${c}`).join(", ")})
    ON CONFLICT(sourceKey) DO UPDATE SET
    ${SCHOOL_FIELDS.map((c) => `"${c}"=excluded."${c}"`).join(",\n    ")};
  `;
  const insertStmt = db.prepare(insertSql);

  let processed = 0;
  db.exec("BEGIN;");
  try {
    sourceRows.forEach((row, index) => {
      const record = toRecord(row);
      insertStmt.run({
        sourceKey: getSourceKey(record, index),
        ...record
      });
      processed += 1;
    });
    db.exec("COMMIT;");
  } catch (error) {
    db.exec("ROLLBACK;");
    throw error;
  }

  const total = db.prepare("SELECT COUNT(*) AS count FROM schools").get().count;
  return {
    processed,
    total,
    sheetName: firstSheetName
  };
}

module.exports = { importWorkbookBuffer };
