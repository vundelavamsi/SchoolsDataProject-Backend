const express = require("express");
const cors = require("cors");
const morgan = require("morgan");
const { getDb } = require("./db");
const { SCHOOL_FIELDS } = require("./fields");

const app = express();
const db = getDb();

app.use(cors());
app.use(express.json());
app.use(morgan("dev"));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
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
        "${valueField}" AS value,
        COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}") AS label
      FROM schools
      WHERE ${where.join(" AND ")}
      GROUP BY "${valueField}", COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}")
      ORDER BY label COLLATE NOCASE
      LIMIT 500`
    )
    .all(params);
}

app.get("/api/options", (_req, res) => {
  res.json({
    districtId: getValueLabelOptions("districtId", "districtName"),
    schCategoryId: getValueLabelOptions("schCategoryId", "schCatDesc"),
    schType: getValueLabelOptions("schType", "schTypeDesc"),
    schMgmtId: getValueLabelOptions("schMgmtId", "schMgmtDesc"),
    schoolStatus: getValueLabelOptions("schoolStatus", "schoolStatusName")
  });
});

app.get("/api/options/blocks", (req, res) => {
  const districtId = req.query.districtId ? String(req.query.districtId) : "";
  res.json({
    blockId: getValueLabelOptions("blockId", "blockName", { districtId })
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
          "${valueField}" AS value,
          COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}") AS label
        FROM schools
        WHERE "${valueField}" IS NOT NULL AND TRIM("${valueField}") <> ''
        GROUP BY "${valueField}", COALESCE(NULLIF(TRIM("${labelField}"), ''), "${valueField}")
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

app.get("/api/schools", (req, res) => {
  const page = Math.max(Number.parseInt(req.query.page || "1", 10), 1);
  const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize || "25", 10), 1), 200);
  const offset = (page - 1) * pageSize;

  const where = [];
  const params = {};
  const equalsFilters = [
    "districtId",
    "blockId",
    "villageId",
    "schCategoryId",
    "schType",
    "schMgmtId",
    "schoolStatus"
  ];

  for (const key of equalsFilters) {
    const value = req.query[key];
    if (value) {
      where.push(`"${key}" = @${key}`);
      params[key] = String(value);
    }
  }

  if (req.query.classFromMin) {
    where.push(`CAST(classFrm AS INTEGER) >= @classFromMin`);
    params.classFromMin = Number.parseInt(req.query.classFromMin, 10);
  }

  if (req.query.classToMax) {
    where.push(`CAST(classTo AS INTEGER) <= @classToMax`);
    params.classToMax = Number.parseInt(req.query.classToMax, 10);
  }

  if (req.query.search) {
    where.push(
      `(schoolName LIKE @search OR udiseschCode LIKE @search OR schoolId LIKE @search OR villageName LIKE @search OR blockName LIKE @search OR districtName LIKE @search)`
    );
    params.search = `%${String(req.query.search).trim()}%`;
  }

  const whereClause = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const listColumns = SCHOOL_FIELDS.map((field) => `"${field}"`).join(", ");

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
