require("dotenv").config();
const { Pool } = require("pg");
const cs = process.env.DATABASE_URL;
const isRemote = cs && !cs.includes("localhost");
const pool = new Pool({ connectionString: cs, ssl: isRemote ? { rejectUnauthorized: false } : false });

const dims = [
  ["stateId", "stateName"],
  ["districtId", "districtName"],
  ["blockId", "blockName"],
  ["villageId", "villageName"],
  ["schCategoryId", "schCatDesc"],
  ["schType", "schTypeDesc"],
  ["schMgmtId", "schMgmtDesc"],
  ["schoolStatus", "schoolStatusName"],
  ["schLocRuralUrban", "schLocDesc"],
];

(async () => {
  console.log("Distinct labels per dimension (unfiltered):");
  console.log("dim\t\t\tdistinct\thits 500?\thits 10000?");
  for (const [v, l] of dims) {
    const sql = `
      SELECT COUNT(*) AS c FROM (
        SELECT COALESCE(NULLIF(TRIM("${l}"), ''), "${v}") AS label
        FROM schools
        WHERE "${v}" IS NOT NULL AND TRIM("${v}") <> ''
        GROUP BY 1
      ) t
    `;
    const { rows } = await pool.query(sql);
    const c = Number(rows[0].c);
    console.log(`${v.padEnd(20)}\t${c}\t\t${c > 500 ? "YES" : "no"}\t\t${c > 10000 ? "YES" : "no"}`);
  }

  // Also check max distinct villages within a single (districtId, blockId) — the only scoped call
  const { rows: vrows } = await pool.query(`
    SELECT MAX(c) AS max_villages_per_block FROM (
      SELECT "districtId", "blockId", COUNT(DISTINCT COALESCE(NULLIF(TRIM("villageName"), ''), "villageId")) AS c
      FROM schools
      WHERE "villageId" IS NOT NULL AND TRIM("villageId") <> ''
      GROUP BY "districtId", "blockId"
    ) t
  `);
  console.log("\nMax distinct villages within a single (districtId, blockId):", vrows[0].max_villages_per_block);

  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
