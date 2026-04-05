const { Readable } = require("node:stream");
const { createInterface } = require("node:readline");
const xlsx = require("xlsx");
const { pool } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");

function getSourceKey(record, index) {
  if (record.schoolId) return `schoolId:${record.schoolId}`;
  if (record.udiseschCode) return `udise:${record.udiseschCode}`;
  return `row:${index + 1}`;
}

function parseCSVLine(line) {
  const fields = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      let val = "";
      i++;
      while (i < line.length) {
        if (line[i] === '"') {
          if (i + 1 < line.length && line[i + 1] === '"') {
            val += '"';
            i += 2;
          } else {
            i++;
            break;
          }
        } else {
          val += line[i];
          i++;
        }
      }
      fields.push(val);
      if (i < line.length && line[i] === ",") i++;
    } else {
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

async function flushBatch(db, batch, columns) {
  if (batch.length === 0) return;

  const values = [];
  const rowPlaceholders = [];
  let paramIndex = 1;

  for (const row of batch) {
    const placeholders = columns.map(() => `$${paramIndex++}`);
    rowPlaceholders.push(`(${placeholders.join(", ")})`);
    for (const col of columns) {
      values.push(row[col] ?? null);
    }
  }

  const sql = `
    INSERT INTO schools (${columns.map((c) => `"${c}"`).join(", ")})
    VALUES ${rowPlaceholders.join(",\n")}
    ON CONFLICT ("sourceKey") DO UPDATE SET
    ${SCHOOL_FIELDS.map((c) => `"${c}" = EXCLUDED."${c}"`).join(",\n")}
  `;

  await db.query(sql, values);
}

async function importWorkbookBuffer(buffer) {
  console.log(`[import] Received buffer: ${(buffer.length / 1024 / 1024).toFixed(1)} MB`);
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

  console.log(`[import] CSV generated: ${(csvString.length / 1024 / 1024).toFixed(1)} MB, sheet: ${firstSheetName}`);

  // Free workbook from memory
  workbook.SheetNames.length = 0;
  Object.keys(workbook.Sheets).forEach((k) => delete workbook.Sheets[k]);

  const stream = Readable.from(csvString);
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  let headerToCol = {};
  let rowIndex = 0;
  let processed = 0;
  const BATCH_SIZE = 50;
  const columns = ["sourceKey", ...SCHOOL_FIELDS];

  console.log("[import] Starting row processing...");

  let batch = [];

  for await (const line of rl) {
    if (!line.trim()) continue;

    const fields = parseCSVLine(line);

    if (!headers) {
      headers = fields.map((h) => h.trim());
      for (let c = 0; c < headers.length; c++) {
        if (SCHOOL_FIELDS.includes(headers[c])) {
          headerToCol[headers[c]] = c;
        }
      }
      console.log(`[import] Found ${Object.keys(headerToCol).length} matching columns`);
      continue;
    }

    const record = {};
    for (const field of SCHOOL_FIELDS) {
      const col = headerToCol[field];
      const val = col !== undefined && col < fields.length ? fields[col] : null;
      record[field] = val != null && String(val).trim() !== "" ? String(val).trim() : null;
    }

    rowIndex++;
    batch.push({ sourceKey: getSourceKey(record, rowIndex), ...record });

    if (batch.length >= BATCH_SIZE) {
      const t = Date.now();
      await flushBatch(pool, batch, columns);
      processed += batch.length;
      console.log(`[import] Processed ${processed} rows (${Date.now() - t}ms)`);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await flushBatch(pool, batch, columns);
    processed += batch.length;
    console.log(`[import] Processed ${processed} rows (final batch)`);
  }

  const countResult = await pool.query("SELECT COUNT(*) AS count FROM schools");
  const total = parseInt(countResult.rows[0].count, 10);
  return { processed, total, sheetName: firstSheetName };
}

module.exports = { importWorkbookBuffer };
