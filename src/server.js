require("dotenv").config();
const express = require("express");
const cors = require("cors");
const morgan = require("morgan");
const multer = require("multer");
const { pool, initDb } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");
const EDIT_FIELD_ALLOWLIST = ["villageName", "gmapLocationLink"];
const { importWorkbookBuffer } = require("./importer");
const {
  VALID_ROLES,
  normalizePhone,
  parseBlockIds,
  resolveAccess,
  addBlockScopeFilter,
  assertSchoolInScope,
} = require("./access");

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 },
});

app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/api/reset", async (_req, res) => {
  await pool.query("DELETE FROM school_edits");
  await pool.query("DELETE FROM schools");
  res.json({ message: "All data cleared" });
});

// ── Import ────────────────────────────────────────────────────
app.post("/api/import", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      error: "Missing file. Send multipart/form-data with file field name 'file'.",
    });
  }
  try {
    const result = await importWorkbookBuffer(req.file.buffer);
    return res.json({
      message: "Import completed",
      processed: result.processed,
      total: result.total,
      sheetName: result.sheetName,
    });
  } catch (error) {
    return res.status(400).json({ error: "Import failed", details: error.message });
  }
});

// ── Options helpers ───────────────────────────────────────────
async function getValueLabelOptions(valueField, labelField, filters = {}, access = null) {
  const where = [`"${valueField}" IS NOT NULL`, `TRIM("${valueField}") <> ''`];
  const values = [];
  const paramIndexRef = { value: 1 };

  Object.entries(filters).forEach(([key, value]) => {
    if (value) {
      where.push(`"${key}" = $${paramIndexRef.value++}`);
      values.push(String(value));
    }
  });

  if (access) {
    addBlockScopeFilter(access, where, values, paramIndexRef);
  }

  const { rows } = await pool.query(
    `SELECT
       STRING_AGG(DISTINCT "${valueField}", ',') AS value,
       COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}") AS label
     FROM schools
     WHERE ${where.join(" AND ")}
     GROUP BY COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}")
     ORDER BY LOWER(COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}"))
     LIMIT 500`,
    values
  );
  return rows;
}

// ── Options endpoints ─────────────────────────────────────────
app.get("/api/options", async (_req, res) => {
  const access = await resolveAccess(_req);
  const [schCategoryId, schType, schMgmtId, schoolStatus, schLocRuralUrban] = await Promise.all([
    getValueLabelOptions("schCategoryId", "schCatDesc", {}, access),
    getValueLabelOptions("schType", "schTypeDesc", {}, access),
    getValueLabelOptions("schMgmtId", "schMgmtDesc", {}, access),
    getValueLabelOptions("schoolStatus", "schoolStatusName", {}, access),
    getValueLabelOptions("schLocRuralUrban", "schLocDesc", {}, access),
  ]);
  res.json({
    schCategoryId,
    schType,
    schMgmtId,
    schoolStatus,
    schLocRuralUrban: schLocRuralUrban.filter((o) => ["Rural", "Urban"].includes(o.label)),
  });
});

app.get("/api/options/blocks", async (req, res) => {
  const access = await resolveAccess(req);
  const stateId = req.query.stateId ? String(req.query.stateId) : "";
  const districtId = req.query.districtId ? String(req.query.districtId) : "";
  res.json({
    blockId: await getValueLabelOptions("blockId", "blockName", { stateId, districtId }, access),
  });
});

app.get("/api/options/villages", async (req, res) => {
  const access = await resolveAccess(req);
  const districtId = req.query.districtId ? String(req.query.districtId) : "";
  const blockId = req.query.blockId ? String(req.query.blockId) : "";
  res.json({
    villageId: await getValueLabelOptions("villageId", "villageName", { districtId, blockId }, access),
  });
});

app.get("/api/options/all", async (req, res) => {
  const access = await resolveAccess(req);
  async function getLegacy(valueField, labelField) {
    const where = [`"${valueField}" IS NOT NULL`, `TRIM("${valueField}") <> ''`];
    const values = [];
    const paramIndexRef = { value: 1 };
    addBlockScopeFilter(access, where, values, paramIndexRef);
    const { rows } = await pool.query(
      `SELECT
         STRING_AGG(DISTINCT "${valueField}", ',') AS value,
         COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}") AS label
        FROM schools
       WHERE ${where.join(" AND ")}
        GROUP BY COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}")
        ORDER BY LOWER(COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}"))
       LIMIT 500`,
      values
    );
    return rows;
  }

  const [districtId, blockId, villageId, schCategoryId, schType, schMgmtId, schoolStatus] =
    await Promise.all([
      getLegacy("districtId", "districtName"),
      getLegacy("blockId", "blockName"),
      getLegacy("villageId", "villageName"),
      getLegacy("schCategoryId", "schCatDesc"),
      getLegacy("schType", "schTypeDesc"),
      getLegacy("schMgmtId", "schMgmtDesc"),
      getLegacy("schoolStatus", "schoolStatusName"),
    ]);

  res.json({ districtId, blockId, villageId, schCategoryId, schType, schMgmtId, schoolStatus });
});

app.get("/api/options/classRanges", async (req, res) => {
  const access = await resolveAccess(req);
  const where = [
    `"classFrm" IS NOT NULL`,
    `TRIM("classFrm") <> ''`,
    `"classTo" IS NOT NULL`,
    `TRIM("classTo") <> ''`,
  ];
  const values = [];
  const paramIndexRef = { value: 1 };
  addBlockScopeFilter(access, where, values, paramIndexRef);
  const { rows } = await pool.query(
    `SELECT "classFrm", "classTo", COUNT(*) AS cnt
     FROM schools
     WHERE ${where.join(" AND ")}
     GROUP BY "classFrm", "classTo"
     ORDER BY cnt DESC`,
    values
  );

  const classRange = rows.map((r) => ({
    value: `${r.classFrm}-${r.classTo}`,
    label: `Class ${r.classFrm} – ${r.classTo}`,
  }));

  res.json({ classRange });
});

app.get("/api/options/states", async (_req, res) => {
  const access = await resolveAccess(_req);
  res.json({ stateId: await getValueLabelOptions("stateId", "stateName", {}, access) });
});

app.get("/api/options/districts", async (req, res) => {
  const stateId = req.query.stateId ? String(req.query.stateId) : "";
  const access = await resolveAccess(req);
  res.json({ districtId: await getValueLabelOptions("districtId", "districtName", { stateId }, access) });
});

// ── Mock Access ────────────────────────────────────────────────
app.get("/api/access", async (req, res) => {
  const access = await resolveAccess(req);
  return res.json({
    phone: access.phone,
    authenticated: access.authenticated,
    role: access.role,
    canEdit: access.canEdit,
    canReview: access.canReview,
    blockIds: access.blockIds,
    scope: access.scope,
  });
});

app.get("/api/access/phones", async (_req, res) => {
  const { rows } = await pool.query(
    `SELECT phone, role, COALESCE("blockIds", ARRAY[]::TEXT[]) AS "blockIds", "createdAt", "updatedAt"
     FROM phone_access
     ORDER BY "updatedAt" DESC`
  );
  return res.json(rows);
});

app.post("/api/access/phones", async (req, res) => {
  const rawPhone = req.body?.phone;
  const role = String(req.body?.role || "").trim();
  const phone = normalizePhone(rawPhone);
  const blockIds = parseBlockIds(req.body?.blockIds);

  if (!phone) {
    return res.status(400).json({ error: "Valid phone is required" });
  }
  if (!VALID_ROLES.includes(role)) {
    return res.status(400).json({ error: "role must be one of: edit, review" });
  }

  const now = new Date().toISOString();
  const { rows } = await pool.query(
    `INSERT INTO phone_access (phone, role, "blockIds", "createdAt", "updatedAt")
     VALUES ($1, $2, $3, $4, $4)
     ON CONFLICT (phone) DO UPDATE SET
       role = EXCLUDED.role,
       "blockIds" = EXCLUDED."blockIds",
       "updatedAt" = EXCLUDED."updatedAt"
     RETURNING phone, role, "blockIds", "createdAt", "updatedAt"`,
    [phone, role, blockIds, now]
  );
  return res.status(201).json(rows[0]);
});

app.delete("/api/access/phones/:phone", async (req, res) => {
  const phone = normalizePhone(req.params.phone);
  if (!phone) {
    return res.status(400).json({ error: "Valid phone is required" });
  }
  const { rows } = await pool.query(
    `DELETE FROM phone_access WHERE phone = $1 RETURNING phone, role, "blockIds"`,
    [phone]
  );
  if (rows.length === 0) {
    return res.status(404).json({ error: "Phone access entry not found" });
  }
  return res.json(rows[0]);
});

// ── Edits ─────────────────────────────────────────────────────
app.post("/api/edits", async (req, res) => {
  const access = await resolveAccess(req);
  if (!access.canEdit || !access.phone) {
    return res.status(403).json({ error: "Edit access is not enabled for this phone number" });
  }

  const { sourceKey, fieldName, newValue, submittedBy } = req.body || {};
  if (!sourceKey || !fieldName || !newValue || !submittedBy) {
    return res.status(400).json({ error: "Missing required fields" });
  }
  if (!EDIT_FIELD_ALLOWLIST.includes(fieldName)) {
    return res.status(400).json({ error: "Invalid fieldName" });
  }

  const scopedSchool = await assertSchoolInScope(sourceKey, access);
  if (!scopedSchool.ok) {
    return res.status(403).json({ error: scopedSchool.reason });
  }

  const schoolResult = await pool.query(
    `SELECT "${fieldName}" FROM schools WHERE "sourceKey" = $1`,
    [sourceKey]
  );
  if (schoolResult.rows.length === 0) {
    return res.status(400).json({ error: "School not found" });
  }

  const oldValue = schoolResult.rows[0][fieldName] ?? null;
  const submittedAt = new Date().toISOString();

  const pendingResult = await pool.query(
    `SELECT id
     FROM school_edits
     WHERE "sourceKey" = $1
       AND "fieldName" = $2
       AND "submittedPhone" = $3
       AND status = 'pending'
     ORDER BY "submittedAt" DESC
     LIMIT 1`,
    [sourceKey, fieldName, access.phone]
  );

  if (pendingResult.rows.length > 0) {
    const pendingId = pendingResult.rows[0].id;
    const { rows } = await pool.query(
      `UPDATE school_edits
       SET "newValue" = $1, "submittedBy" = $2, "submittedAt" = $3
       WHERE id = $4
       RETURNING *`,
      [String(newValue), String(submittedBy), submittedAt, pendingId]
    );
    return res.json(rows[0]);
  }

  const { rows } = await pool.query(
    `INSERT INTO school_edits ("sourceKey", "fieldName", "oldValue", "newValue", "submittedBy", "submittedPhone", "submittedAt", status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
     RETURNING *`,
    [sourceKey, fieldName, oldValue, String(newValue), String(submittedBy), access.phone, submittedAt]
  );
  return res.status(201).json(rows[0]);
});

app.get("/api/edits", async (req, res) => {
  const access = await resolveAccess(req);
  if (!access.phone || (!access.canEdit && !access.canReview)) {
    return res.status(403).json({ error: "Edit history access is not enabled for this phone number" });
  }

  const status = req.query.status ? String(req.query.status) : null;
  const where = [];
  const values = [];
  let paramIndex = 1;

  if (status) {
    where.push(`e.status = $${paramIndex++}`);
    values.push(status);
  }

  if (access.canReview) {
    if (access.scope === "blocks" && access.blockIds.length > 0) {
      where.push(`s."blockId" = ANY($${paramIndex++})`);
      values.push(access.blockIds);
    }
  } else {
    where.push(`e."submittedPhone" = $${paramIndex++}`);
    values.push(access.phone);
    if (access.scope === "blocks" && access.blockIds.length > 0) {
      where.push(`s."blockId" = ANY($${paramIndex++})`);
      values.push(access.blockIds);
    }
  }

  const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const sql = `
    SELECT e.*, s."schoolName", s."udiseschCode", s."blockCd"
    FROM school_edits e
    LEFT JOIN schools s ON e."sourceKey" = s."sourceKey"
    ${whereClause}
    ORDER BY e."submittedAt" DESC
  `;
  const { rows } = await pool.query(sql, values);
  return res.json(rows);
});

app.get("/api/edits/school/:sourceKey", async (req, res) => {
  const access = await resolveAccess(req);
  if (!access.canEdit || !access.phone) {
    return res.status(403).json({ error: "Edit access is not enabled for this phone number" });
  }

  const scopedSchool = await assertSchoolInScope(req.params.sourceKey, access);
  if (!scopedSchool.ok) {
    return res.status(403).json({ error: scopedSchool.reason });
  }

  const values = [req.params.sourceKey];
  let extraWhere = "";
  if (!access.canReview) {
    values.push(access.phone);
    extraWhere = ` AND "submittedPhone" = $2`;
  }

  const { rows } = await pool.query(
    `SELECT * FROM school_edits WHERE "sourceKey" = $1 AND status = 'pending'${extraWhere}`,
    values
  );
  return res.json(rows);
});

app.post("/api/edits/:id/approve", async (req, res) => {
  const access = await resolveAccess(req);
  if (!access.canReview || !access.phone) {
    return res.status(403).json({ error: "Review access is not enabled for this phone number" });
  }

  const id = Number.parseInt(req.params.id, 10);

  const editResult = await pool.query(`SELECT * FROM school_edits WHERE id = $1`, [id]);
  if (editResult.rows.length === 0) return res.status(404).json({ error: "Edit not found" });

  const edit = editResult.rows[0];
  if (edit.status !== "pending") return res.status(400).json({ error: "Edit is not pending" });
  if (!EDIT_FIELD_ALLOWLIST.includes(edit.fieldName)) {
    return res.status(400).json({ error: "Invalid fieldName" });
  }

  const scopedSchool = await assertSchoolInScope(edit.sourceKey, access);
  if (!scopedSchool.ok) {
    return res.status(403).json({ error: scopedSchool.reason });
  }

  const reviewedAt = new Date().toISOString();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `UPDATE schools SET "${edit.fieldName}" = $1 WHERE "sourceKey" = $2`,
      [edit.newValue, edit.sourceKey]
    );
    await client.query(
      `UPDATE school_edits SET status = 'approved', "reviewedAt" = $1 WHERE id = $2`,
      [reviewedAt, id]
    );
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    return res.status(500).json({ error: "Approval failed", details: err.message });
  } finally {
    client.release();
  }

  const { rows } = await pool.query(`SELECT * FROM school_edits WHERE id = $1`, [id]);
  return res.json(rows[0]);
});

app.post("/api/edits/:id/reject", async (req, res) => {
  const access = await resolveAccess(req);
  if (!access.canReview || !access.phone) {
    return res.status(403).json({ error: "Review access is not enabled for this phone number" });
  }

  const id = Number.parseInt(req.params.id, 10);

  const editResult = await pool.query(`SELECT * FROM school_edits WHERE id = $1`, [id]);
  if (editResult.rows.length === 0) return res.status(404).json({ error: "Edit not found" });

  const edit = editResult.rows[0];
  if (edit.status !== "pending") return res.status(400).json({ error: "Edit is not pending" });

  const scopedSchool = await assertSchoolInScope(edit.sourceKey, access);
  if (!scopedSchool.ok) {
    return res.status(403).json({ error: scopedSchool.reason });
  }

  const reviewedAt = new Date().toISOString();
  await pool.query(
    `UPDATE school_edits SET status = 'rejected', "reviewedAt" = $1 WHERE id = $2`,
    [reviewedAt, id]
  );

  const { rows } = await pool.query(`SELECT * FROM school_edits WHERE id = $1`, [id]);
  return res.json(rows[0]);
});

// ── Schools listing ───────────────────────────────────────────
app.get("/api/schools", async (req, res) => {
  const access = await resolveAccess(req);
  const page = Math.max(Number.parseInt(req.query.page || "1", 10), 1);
  const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize || "25", 10), 1), 200);
  const offset = (page - 1) * pageSize;

  const where = [];
  const values = [];
  let paramIndex = 1;

  const equalsFilters = [
    "stateId", "districtId", "blockId", "villageId",
    "schCategoryId", "schType", "schMgmtId", "schoolStatus", "schLocRuralUrban",
  ];

  for (const key of equalsFilters) {
    const value = req.query[key];
    if (value) {
      const vals = String(value).split(",").map((v) => v.trim()).filter(Boolean);
      if (vals.length === 1) {
        where.push(`"${key}" = $${paramIndex++}`);
        values.push(vals[0]);
      } else if (vals.length > 1) {
        const placeholders = vals.map(() => `$${paramIndex++}`);
        where.push(`"${key}" IN (${placeholders.join(", ")})`);
        values.push(...vals);
      }
    }
  }

  if (access.scope === "blocks" && access.blockIds.length > 0) {
    where.push(`"blockId" = ANY($${paramIndex++})`);
    values.push(access.blockIds);
  }

  if (req.query.classRange) {
    const parts = String(req.query.classRange).split("-");
    if (parts.length === 2) {
      where.push(`"classFrm" = $${paramIndex++} AND "classTo" = $${paramIndex++}`);
      values.push(parts[0].trim(), parts[1].trim());
    }
  }

  if (req.query.search) {
    const searchParam = `%${String(req.query.search).trim()}%`;
    where.push(
      `("schoolName" ILIKE $${paramIndex} OR "villageName" ILIKE $${paramIndex} OR "blockName" ILIKE $${paramIndex} OR "districtName" ILIKE $${paramIndex} OR CONCAT("blockCd", REPLACE("udiseschCode", '******', '')) ILIKE $${paramIndex})`
    );
    values.push(searchParam);
    paramIndex++;
  }

  const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const listColumns = `"sourceKey", ${SCHOOL_FIELDS.map((f) => `"${f}"`).join(", ")}`;

  const countValues = [...values];
  const countResult = await pool.query(
    `SELECT COUNT(*) AS count FROM schools ${whereClause}`,
    countValues
  );
  const total = parseInt(countResult.rows[0].count, 10);

  values.push(pageSize, offset);
  const dataResult = await pool.query(
    `SELECT ${listColumns}
     FROM schools
     ${whereClause}
     ORDER BY "schoolName", "udiseschCode"
     LIMIT $${paramIndex++} OFFSET $${paramIndex++}`,
    values
  );

  res.json({
    page,
    pageSize,
    total,
    totalPages: Math.ceil(total / pageSize),
    data: dataResult.rows,
  });
});

// ── Start server ──────────────────────────────────────────────
const port = Number.parseInt(process.env.PORT || "3001", 10);

(async () => {
  await initDb();
  app.listen(port, () => {
    console.log(`Backend running on http://localhost:${port}`);
  });
})();

process.on("SIGTERM", async () => {
  await pool.end();
  process.exit(0);
});
