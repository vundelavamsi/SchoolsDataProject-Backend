const xlsx = require("xlsx");
const { getDb } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");

const db = getDb();

function getSourceKey(record, index) {
  if (record.schoolId) {
    return `schoolId:${record.schoolId}`;
  }
  if (record.udiseschCode) {
    return `udise:${record.udiseschCode}`;
  }
  return `row:${index + 1}`;
}

function cellValue(sheet, r, c) {
  const addr = xlsx.utils.encode_cell({ r, c });
  const cell = sheet[addr];
  if (!cell) return null;
  const v = cell.w !== undefined ? cell.w : (cell.v !== undefined ? String(cell.v) : null);
  return v == null ? null : String(v).trim();
}

function importWorkbookBuffer(buffer) {
  const workbook = xlsx.read(buffer, { type: "buffer", cellStyles: false, cellFormula: false });
  const firstSheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[firstSheetName];
  const range = xlsx.utils.decode_range(sheet["!ref"]);

  // Read headers from first row
  const headers = [];
  for (let c = range.s.c; c <= range.e.c; c++) {
    const v = cellValue(sheet, range.s.r, c);
    headers.push(v ? v.trim() : `col_${c}`);
  }

  // Map header index to field name
  const headerToCol = {};
  for (let c = 0; c < headers.length; c++) {
    if (SCHOOL_FIELDS.includes(headers[c])) {
      headerToCol[headers[c]] = c;
    }
  }

  const columns = ["sourceKey", ...SCHOOL_FIELDS];
  const insertSql = `
    INSERT INTO schools (${columns.map((c) => `"${c}"`).join(", ")})
    VALUES (${columns.map((c) => `@${c}`).join(", ")})
    ON CONFLICT(sourceKey) DO UPDATE SET
    ${SCHOOL_FIELDS.map((c) => `"${c}"=excluded."${c}"`).join(",\n    ")};
  `;
  const insertStmt = db.prepare(insertSql);

  let processed = 0;
  const BATCH_SIZE = 5000;

  db.exec("BEGIN;");
  try {
    for (let r = range.s.r + 1; r <= range.e.r; r++) {
      const record = {};
      for (const field of SCHOOL_FIELDS) {
        const col = headerToCol[field];
        record[field] = col !== undefined ? cellValue(sheet, r, col) : null;
      }

      insertStmt.run({
        sourceKey: getSourceKey(record, r - 1),
        ...record,
      });
      processed += 1;

      // Commit in batches to avoid holding too much in the WAL
      if (processed % BATCH_SIZE === 0) {
        db.exec("COMMIT;");
        db.exec("BEGIN;");
      }
    }
    db.exec("COMMIT;");
  } catch (error) {
    db.exec("ROLLBACK;");
    throw error;
  }

  const total = db.prepare("SELECT COUNT(*) AS count FROM schools").get().count;
  return {
    processed,
    total,
    sheetName: firstSheetName,
  };
}

module.exports = { importWorkbookBuffer };
