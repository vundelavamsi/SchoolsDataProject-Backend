const xlsx = require("xlsx");
const { pool } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");

function getSourceKey(record, index) {
  if (record.schoolId) return `schoolId:${record.schoolId}`;
  if (record.udiseschCode) return `udise:${record.udiseschCode}`;
  return `row:${index + 1}`;
}

function cellValue(sheet, r, c) {
  const addr = xlsx.utils.encode_cell({ r, c });
  const cell = sheet[addr];
  if (!cell) return null;
  const v = cell.w !== undefined ? cell.w : (cell.v !== undefined ? String(cell.v) : null);
  return v == null ? null : String(v).trim();
}

async function flushBatch(batch, columns) {
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

  await pool.query(sql, values);
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
  const range = xlsx.utils.decode_range(sheet["!ref"]);

  console.log(`[import] Sheet: ${firstSheetName}, rows: ${range.e.r - range.s.r}`);

  // Read headers
  const headers = [];
  for (let c = range.s.c; c <= range.e.c; c++) {
    const v = cellValue(sheet, range.s.r, c);
    headers.push(v ? v.trim() : `col_${c}`);
  }

  const headerToCol = {};
  for (let c = 0; c < headers.length; c++) {
    if (SCHOOL_FIELDS.includes(headers[c])) {
      headerToCol[headers[c]] = c;
    }
  }
  console.log(`[import] Found ${Object.keys(headerToCol).length} matching columns`);

  const columns = ["sourceKey", ...SCHOOL_FIELDS];
  const BATCH_SIZE = 50;
  let processed = 0;
  let batch = [];

  for (let r = range.s.r + 1; r <= range.e.r; r++) {
    const record = {};
    for (const field of SCHOOL_FIELDS) {
      const col = headerToCol[field];
      record[field] = col !== undefined ? cellValue(sheet, r, col) : null;
    }

    const hasData = SCHOOL_FIELDS.some((f) => record[f] != null && record[f] !== "");
    if (!hasData) continue;

    batch.push({ sourceKey: getSourceKey(record, r), ...record });

    if (batch.length >= BATCH_SIZE) {
      const t = Date.now();
      await flushBatch(batch, columns);
      processed += batch.length;
      console.log(`[import] Processed ${processed} rows (${Date.now() - t}ms)`);
      batch = [];
    }
  }

  if (batch.length > 0) {
    await flushBatch(batch, columns);
    processed += batch.length;
    console.log(`[import] Processed ${processed} rows (final batch)`);
  }

  const countResult = await pool.query("SELECT COUNT(*) AS count FROM schools");
  const total = parseInt(countResult.rows[0].count, 10);
  console.log(`[import] Complete: ${processed} processed, ${total} total in DB`);
  return { processed, total, sheetName: firstSheetName };
}

module.exports = { importWorkbookBuffer };
