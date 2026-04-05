const express = require("express");
const cors = require("cors");
const morgan = require("morgan");
const multer = require("multer");
const { getDb } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");
const EDIT_FIELD_ALLOWLIST = ["villageName", "gmapLocationLink"];
const { importWorkbookBuffer } = require("./importer");

const app = express();
const db = getDb();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 }
});

app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/api/import", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({
      error: "Missing file. Send multipart/form-data with file field name 'file'."
    });
  }

  try {
    const result = await importWorkbookBuffer(req.file.buffer);
    return res.json({
      message: "Import completed",
      processed: result.processed,
      total: result.total,
      sheetName: result.sheetName
    });
  } catch (error) {
    return res.status(400).json({
      error: "Import failed",
      details: error.message
    });
  }
});

function getValueLabelOptions(valueField, labelField, filters = {}) {
  const where = [`"${valueField}" IS NOT NULL`, `TRIM("${valueField}") <> ''`];
  const params = {};

  Object.entries(filters).forEach(([key, value]) => {
    if (value) {
      where.push(`"${key}" = @${key}`);
      params[key] = String(value);
    }
  });

  return db
    .prepare(
      `SELECT
        GROUP_CONCAT(DISTINCT "${valueField}") AS value,
        COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}") AS label
      FROM schools
      WHERE ${where.join(" AND ")}
      GROUP BY COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}")
      ORDER BY label COLLATE NOCASE
      LIMIT 500`
    )
    .all(params);
}

app.get("/api/options", (_req, res) => {
  res.json({
    schCategoryId: getValueLabelOptions("schCategoryId", "schCatDesc"),
    schType: getValueLabelOptions("schType", "schTypeDesc"),
    schMgmtId: getValueLabelOptions("schMgmtId", "schMgmtDesc"),
    schoolStatus: getValueLabelOptions("schoolStatus", "schoolStatusName"),
    schLocRuralUrban: getValueLabelOptions("schLocRuralUrban", "schLocDesc").filter((o) => ["Rural", "Urban"].includes(o.label))
  });
});

app.get("/api/options/blocks", (req, res) => {
  const stateId = req.query.stateId ? String(req.query.stateId) : "";
  const districtId = req.query.districtId ? String(req.query.districtId) : "";
  res.json({
    blockId: getValueLabelOptions("blockId", "blockName", { stateId, districtId })
  });
});

app.get("/api/options/villages", (req, res) => {
  const districtId = req.query.districtId ? String(req.query.districtId) : "";
  const blockId = req.query.blockId ? String(req.query.blockId) : "";
  res.json({
    villageId: getValueLabelOptions("villageId", "villageName", { districtId, blockId })
  });
});

app.get("/api/options/all", (_req, res) => {
  const getLegacyValueLabelOptions = (valueField, labelField) =>
    db
      .prepare(
        `SELECT
          GROUP_CONCAT(DISTINCT "${valueField}") AS value,
          COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}") AS label
        FROM schools
        WHERE "${valueField}" IS NOT NULL AND TRIM("${valueField}") <> ''
        GROUP BY COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}")
        ORDER BY label COLLATE NOCASE
        LIMIT 500`
      )
      .all();

  res.json({
    districtId: getLegacyValueLabelOptions("districtId", "districtName"),
    blockId: getLegacyValueLabelOptions("blockId", "blockName"),
    villageId: getLegacyValueLabelOptions("villageId", "villageName"),
    schCategoryId: getLegacyValueLabelOptions("schCategoryId", "schCatDesc"),
    schType: getLegacyValueLabelOptions("schType", "schTypeDesc"),
    schMgmtId: getLegacyValueLabelOptions("schMgmtId", "schMgmtDesc"),
    schoolStatus: getLegacyValueLabelOptions("schoolStatus", "schoolStatusName")
  });
});

app.get("/api/options/classRanges", (_req, res) => {
  const rows = db
    .prepare(
      `SELECT classFrm, classTo, COUNT(*) AS cnt
       FROM schools
       WHERE classFrm IS NOT NULL AND TRIM(classFrm) <> ''
         AND classTo IS NOT NULL AND TRIM(classTo) <> ''
       GROUP BY classFrm, classTo
       ORDER BY cnt DESC`
    )
    .all();

  const classRange = rows.map((r) => ({
    value: `${r.classFrm}-${r.classTo}`,
    label: `Class ${r.classFrm} – ${r.classTo}`,
  }));

  res.json({ classRange });
});

app.get("/api/options/states", (_req, res) => {
  res.json({ stateId: getValueLabelOptions("stateId", "stateName") });
});

app.get("/api/options/districts", (req, res) => {
  const stateId = req.query.stateId ? String(req.query.stateId) : "";
  res.json({ districtId: getValueLabelOptions("districtId", "districtName", { stateId }) });
});

app.post("/api/edits", (req, res) => {
  const { sourceKey, fieldName, newValue, submittedBy } = req.body || {};
  if (!sourceKey || !fieldName || !newValue || !submittedBy) {
    return res.status(400).json({ error: "Missing required fields" });
  }
  if (!EDIT_FIELD_ALLOWLIST.includes(fieldName)) {
    return res.status(400).json({ error: "Invalid fieldName" });
  }
  const school = db.prepare(`SELECT "${fieldName}" FROM schools WHERE sourceKey = ?`).get(sourceKey);
  if (!school) {
    return res.status(400).json({ error: "School not found" });
  }
  const oldValue = school[fieldName] ?? null;
  const submittedAt = new Date().toISOString();
  const result = db.prepare(
    `INSERT INTO school_edits (sourceKey, fieldName, oldValue, newValue, submittedBy, submittedAt, status)
     VALUES (?, ?, ?, ?, ?, ?, 'pending')`
  ).run(sourceKey, fieldName, oldValue, String(newValue), String(submittedBy), submittedAt);
  const created = db.prepare(`SELECT * FROM school_edits WHERE id = ?`).get(result.lastInsertRowid);
  return res.status(201).json(created);
});

app.get("/api/edits", (req, res) => {
  const status = req.query.status ? String(req.query.status) : null;
  const sql = `
    SELECT e.*, s.schoolName, s.udiseschCode, s.blockCd
    FROM school_edits e
    LEFT JOIN schools s ON e.sourceKey = s.sourceKey
    ${status ? "WHERE e.status = ?" : ""}
    ORDER BY e.submittedAt DESC
  `;
  const rows = status
    ? db.prepare(sql).all(status)
    : db.prepare(sql).all();
  return res.json(rows);
});

app.get("/api/edits/school/:sourceKey", (req, res) => {
  const sourceKey = req.params.sourceKey;
  const rows = db.prepare(
    `SELECT * FROM school_edits WHERE sourceKey = ? AND status = 'pending'`
  ).all(sourceKey);
  return res.json(rows);
});

app.post("/api/edits/:id/approve", (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  const edit = db.prepare(`SELECT * FROM school_edits WHERE id = ?`).get(id);
  if (!edit) return res.status(404).json({ error: "Edit not found" });
  if (edit.status !== "pending") return res.status(400).json({ error: "Edit is not pending" });
  if (!EDIT_FIELD_ALLOWLIST.includes(edit.fieldName)) {
    return res.status(400).json({ error: "Invalid fieldName" });
  }
  const reviewedAt = new Date().toISOString();
  db.exec("BEGIN");
  try {
    db.prepare(`UPDATE schools SET "${edit.fieldName}" = ? WHERE sourceKey = ?`)
      .run(edit.newValue, edit.sourceKey);
    db.prepare(`UPDATE school_edits SET status = 'approved', reviewedAt = ? WHERE id = ?`)
      .run(reviewedAt, id);
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    return res.status(500).json({ error: "Approval failed", details: err.message });
  }
  const updated = db.prepare(`SELECT * FROM school_edits WHERE id = ?`).get(id);
  return res.json(updated);
});

app.post("/api/edits/:id/reject", (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  const edit = db.prepare(`SELECT * FROM school_edits WHERE id = ?`).get(id);
  if (!edit) return res.status(404).json({ error: "Edit not found" });
  if (edit.status !== "pending") return res.status(400).json({ error: "Edit is not pending" });
  const reviewedAt = new Date().toISOString();
  db.prepare(`UPDATE school_edits SET status = 'rejected', reviewedAt = ? WHERE id = ?`)
    .run(reviewedAt, id);
  const updated = db.prepare(`SELECT * FROM school_edits WHERE id = ?`).get(id);
  return res.json(updated);
});

app.get("/api/schools", (req, res) => {
  const page = Math.max(Number.parseInt(req.query.page || "1", 10), 1);
  const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize || "25", 10), 1), 200);
  const offset = (page - 1) * pageSize;

  const where = [];
  const params = {};
  const equalsFilters = [
    "stateId",
    "districtId",
    "blockId",
    "villageId",
    "schCategoryId",
    "schType",
    "schMgmtId",
    "schoolStatus",
    "schLocRuralUrban"
  ];

  for (const key of equalsFilters) {
    const value = req.query[key];
    if (value) {
      const values = String(value).split(",").map((v) => v.trim()).filter(Boolean);
      if (values.length === 1) {
        where.push(`"${key}" = @${key}`);
        params[key] = values[0];
      } else if (values.length > 1) {
        const placeholders = values.map((v, i) => `@${key}_${i}`);
        where.push(`"${key}" IN (${placeholders.join(", ")})`);
        values.forEach((v, i) => { params[`${key}_${i}`] = v; });
      }
    }
  }

  if (req.query.classRange) {
    const parts = String(req.query.classRange).split("-");
    if (parts.length === 2) {
      where.push(`classFrm = @classFrm AND classTo = @classTo`);
      params.classFrm = parts[0].trim();
      params.classTo = parts[1].trim();
    }
  }

  if (req.query.search) {
    where.push(
      `(schoolName LIKE @search OR udiseschCode LIKE @search OR schoolId LIKE @search OR villageName LIKE @search OR blockName LIKE @search OR districtName LIKE @search)`
    );
    params.search = `%${String(req.query.search).trim()}%`;
  }

  const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const listColumns = `"sourceKey", ${SCHOOL_FIELDS.map((field) => `"${field}"`).join(", ")}`;

  const total = db.prepare(`SELECT COUNT(*) AS count FROM schools ${whereClause}`).get(params).count;
  const rows = db
    .prepare(
      `SELECT ${listColumns}
       FROM schools
       ${whereClause}
       ORDER BY schoolName COLLATE NOCASE, udiseschCode
       LIMIT @pageSize OFFSET @offset`
    )
    .all({ ...params, pageSize, offset });

  res.json({
    page,
    pageSize,
    total,
    totalPages: Math.ceil(total / pageSize),
    data: rows
  });
});

const port = Number.parseInt(process.env.PORT || "3001", 10);
app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Backend running on http://localhost:${port}`);
});
