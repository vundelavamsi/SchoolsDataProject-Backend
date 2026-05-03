#!/usr/bin/env node

require("dotenv").config();
const { Pool } = require("pg");
const { SCHOOL_FIELDS } = require("../src/fields");

const SOURCE_DATABASE_URL = process.env.DATABASE_URL;
const TARGET_DATABASE_URL = process.env.DATABASE_URL_v1;
const BATCH_SIZE = Number(process.env.DB_MIGRATION_BATCH_SIZE || 250);

const SCHOOLS_COLUMNS = ["sourceKey", ...SCHOOL_FIELDS];
const SCHOOL_EDITS_COLUMNS = [
  "id",
  "sourceKey",
  "fieldName",
  "oldValue",
  "newValue",
  "submittedBy",
  "submittedPhone",
  "submittedAt",
  "status",
  "reviewedAt",
];
const PHONE_ACCESS_COLUMNS = ["phone", "role", "blockIds", "createdAt", "updatedAt"];

function quoteIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, "\"\"")}"`;
}

function createPool(connectionString) {
  const isRemote =
    connectionString &&
    !connectionString.includes("localhost") &&
    !connectionString.includes("127.0.0.1");

  return new Pool({
    connectionString,
    ssl: isRemote ? { rejectUnauthorized: false } : false,
    max: 10,
  });
}

function buildInsertSql(tableName, columns, rowCount) {
  const quotedColumns = columns.map((column) => quoteIdentifier(column)).join(", ");
  let parameterIndex = 1;
  const valueRows = [];

  for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
    const placeholders = [];
    for (let columnIndex = 0; columnIndex < columns.length; columnIndex += 1) {
      placeholders.push(`$${parameterIndex}`);
      parameterIndex += 1;
    }
    valueRows.push(`(${placeholders.join(", ")})`);
  }

  return `INSERT INTO ${quoteIdentifier(tableName)} (${quotedColumns}) VALUES ${valueRows.join(", ")}`;
}

function flattenRows(rows, columns) {
  const values = [];
  for (const row of rows) {
    for (const column of columns) {
      values.push(row[column] ?? null);
    }
  }
  return values;
}

async function ensureSchema(queryable) {
  const allSchoolColumns = SCHOOL_FIELDS.map((field) => `${quoteIdentifier(field)} TEXT`).join(",\n      ");

  await queryable.query(`
    CREATE TABLE IF NOT EXISTS schools (
      "sourceKey" TEXT PRIMARY KEY,
      ${allSchoolColumns}
    )
  `);

  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_districtId ON schools("districtId")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_blockId ON schools("blockId")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_villageId ON schools("villageId")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_schoolStatus ON schools("schoolStatus")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_schType ON schools("schType")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_schMgmtId ON schools("schMgmtId")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_schCategoryId ON schools("schCategoryId")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_udiseschCode ON schools("udiseschCode")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_schools_stateId ON schools("stateId")`);

  await queryable.query(`
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

  await queryable.query(`ALTER TABLE school_edits ADD COLUMN IF NOT EXISTS "submittedPhone" TEXT`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_edits_sourceKey ON school_edits("sourceKey")`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_edits_status ON school_edits(status)`);
  await queryable.query(`CREATE INDEX IF NOT EXISTS idx_edits_submittedPhone ON school_edits("submittedPhone")`);

  await queryable.query(`
    CREATE TABLE IF NOT EXISTS phone_access (
      phone TEXT PRIMARY KEY,
      role TEXT NOT NULL CHECK (role IN ('edit', 'review')),
      "blockIds" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
      "createdAt" TEXT NOT NULL,
      "updatedAt" TEXT NOT NULL
    )
  `);
}

async function copyTableInBatches({
  sourceQueryable,
  targetQueryable,
  tableName,
  columns,
  keyColumn,
  batchSize,
}) {
  const quotedColumns = columns.map((column) => quoteIdentifier(column)).join(", ");
  const quotedTable = quoteIdentifier(tableName);
  const quotedKey = quoteIdentifier(keyColumn);
  let lastKey = null;
  let copiedRows = 0;

  while (true) {
    let selectSql = `SELECT ${quotedColumns} FROM ${quotedTable}`;
    const params = [];

    if (lastKey !== null) {
      params.push(lastKey);
      selectSql += ` WHERE ${quotedKey} > $1`;
    }

    params.push(batchSize);
    selectSql += ` ORDER BY ${quotedKey} ASC LIMIT $${params.length}`;

    const { rows } = await sourceQueryable.query(selectSql, params);
    if (rows.length === 0) {
      break;
    }

    const insertSql = buildInsertSql(tableName, columns, rows.length);
    const insertValues = flattenRows(rows, columns);
    await targetQueryable.query(insertSql, insertValues);

    copiedRows += rows.length;
    lastKey = rows[rows.length - 1][keyColumn];
    console.log(`[${tableName}] copied ${copiedRows} rows`);
  }

  return copiedRows;
}

async function syncSchoolEditsSequence(queryable) {
  const {
    rows: [result],
  } = await queryable.query(`SELECT MAX(id) AS max_id FROM school_edits`);
  const maxId = result?.max_id ? Number(result.max_id) : null;

  if (maxId) {
    await queryable.query(
      `SELECT setval(pg_get_serial_sequence('school_edits', 'id'), $1, true)`,
      [maxId]
    );
    return;
  }

  await queryable.query(`SELECT setval(pg_get_serial_sequence('school_edits', 'id'), 1, false)`);
}

async function runMigration() {
  if (!SOURCE_DATABASE_URL) {
    throw new Error("Missing DATABASE_URL in environment.");
  }
  if (!TARGET_DATABASE_URL) {
    throw new Error("Missing DATABASE_URL_v1 in environment.");
  }
  if (SOURCE_DATABASE_URL === TARGET_DATABASE_URL) {
    throw new Error("DATABASE_URL and DATABASE_URL_v1 point to the same value. Aborting.");
  }
  if (!Number.isInteger(BATCH_SIZE) || BATCH_SIZE <= 0) {
    throw new Error("DB_MIGRATION_BATCH_SIZE must be a positive integer.");
  }

  const sourcePool = createPool(SOURCE_DATABASE_URL);
  const targetPool = createPool(TARGET_DATABASE_URL);
  const targetClient = await targetPool.connect();

  try {
    await targetClient.query("BEGIN");
    await ensureSchema(targetClient);
    await targetClient.query(`TRUNCATE TABLE "phone_access", "school_edits", "schools" RESTART IDENTITY`);

    const schoolsCount = await copyTableInBatches({
      sourceQueryable: sourcePool,
      targetQueryable: targetClient,
      tableName: "schools",
      columns: SCHOOLS_COLUMNS,
      keyColumn: "sourceKey",
      batchSize: BATCH_SIZE,
    });

    const editsCount = await copyTableInBatches({
      sourceQueryable: sourcePool,
      targetQueryable: targetClient,
      tableName: "school_edits",
      columns: SCHOOL_EDITS_COLUMNS,
      keyColumn: "id",
      batchSize: BATCH_SIZE,
    });

    const phoneAccessCount = await copyTableInBatches({
      sourceQueryable: sourcePool,
      targetQueryable: targetClient,
      tableName: "phone_access",
      columns: PHONE_ACCESS_COLUMNS,
      keyColumn: "phone",
      batchSize: BATCH_SIZE,
    });

    await syncSchoolEditsSequence(targetClient);
    await targetClient.query("COMMIT");

    console.log("Database migration completed successfully.");
    console.log(`schools: ${schoolsCount}`);
    console.log(`school_edits: ${editsCount}`);
    console.log(`phone_access: ${phoneAccessCount}`);
  } catch (error) {
    await targetClient.query("ROLLBACK");
    throw error;
  } finally {
    targetClient.release();
    await Promise.all([sourcePool.end(), targetPool.end()]);
  }
}

runMigration().catch((error) => {
  console.error("Database migration failed.");
  console.error(error.message);
  process.exitCode = 1;
});
