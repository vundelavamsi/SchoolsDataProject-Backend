const { Readable } = require("node:stream");
const { createInterface } = require("node:readline");
const xlsx = require("xlsx");
const { getDb } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");

const db = getDb();

function getSourceKey(record, index) {
  if (record.schoolId) return `schoolId:${record.schoolId}`;
  if (record.udiseschCode) return `udise:${record.udiseschCode}`;
  return `row:${index + 1}`;
}

// Minimal CSV field parser that handles quoted fields with commas/newlines
function parseCSVLine(line) {
  const fields = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      // Quoted field
      let val = "";
      i++; // skip opening quote
      while (i < line.length) {
        if (line[i] === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            val += '"';
            i += 2;
          } else {
            i++; // skip closing quote
            break;
          }
        } else {
          val += line[i];
          i++;
        }
      }
      fields.push(val);
      if (i < line.length && line[i] === ",") i++; // skip comma
    } else {
      // Unquoted field
      const next = line.indexOf(",", i);
      if (next === -1) {
        fields.push(line.slice(i));
        break;
      }
      fields.push(line.slice(i, next));
      i = next + 1;
    }
  }
  return fields;
}

async function importWorkbookBuffer(buffer) {
  // Step 1: Convert Excel to CSV string using xlsx (lightweight conversion)
  const workbook = xlsx.read(buffer, {
    type: "buffer",
    cellFormula: false,
    cellStyles: false,
    cellHTML: false,
    cellDates: false,
  });
  const firstSheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[firstSheetName];
  const csvString = xlsx.utils.sheet_to_csv(sheet, { blankrows: false });

  // Free the workbook from memory immediately
  workbook.SheetNames.length = 0;
  Object.keys(workbook.Sheets).forEach((k) => delete workbook.Sheets[k]);

  // Step 2: Stream-parse the CSV line by line
  const stream = Readable.from(csvString);
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  let headerToCol = {};
  let rowIndex = 0;
  let processed = 0;
  const BATCH_SIZE = 5000;

  const columns = ["sourceKey", ...SCHOOL_FIELDS];
  const insertSql = `
    INSERT INTO schools (${columns.map((c) => `"${c}"`).join(", ")})
    VALUES (${columns.map((c) => `@${c}`).join(", ")})
    ON CONFLICT(sourceKey) DO UPDATE SET
    ${SCHOOL_FIELDS.map((c) => `"${c}"=excluded."${c}"`).join(",\n    ")};
  `;
  const insertStmt = db.prepare(insertSql);

  db.exec("BEGIN;");

  try {
    for await (const line of rl) {
      if (!line.trim()) continue;

      const fields = parseCSVLine(line);

      if (!headers) {
        // First line = headers
        headers = fields.map((h) => h.trim());
        for (let c = 0; c < headers.length; c++) {
          if (SCHOOL_FIELDS.includes(headers[c])) {
            headerToCol[headers[c]] = c;
          }
        }
        continue;
      }

      // Build record from this row
      const record = {};
      for (const field of SCHOOL_FIELDS) {
        const col = headerToCol[field];
        const val = col !== undefined && col < fields.length ? fields[col] : null;
        record[field] = val != null && String(val).trim() !== "" ? String(val).trim() : null;
      }

      rowIndex++;
      insertStmt.run({
        sourceKey: getSourceKey(record, rowIndex),
        ...record,
      });
      processed++;

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
  return { processed, total, sheetName: firstSheetName };
}

module.exports = { importWorkbookBuffer };
