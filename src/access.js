const { pool } = require("./db");

const ROLE_EDIT = "edit";
const ROLE_REVIEW = "review";
const VALID_ROLES = [ROLE_EDIT, ROLE_REVIEW];

function normalizePhone(input) {
  const digits = String(input || "").replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length < 10) return "";
  return digits.slice(-10);
}

function parseBlockIds(input) {
  if (Array.isArray(input)) {
    return [...new Set(input.map((v) => String(v || "").trim()).filter(Boolean))];
  }
  if (typeof input === "string") {
    return [...new Set(input.split(",").map((v) => v.trim()).filter(Boolean))];
  }
  return [];
}

function getPhoneFromRequest(req) {
  const headerPhone = req.get("x-user-phone");
  if (headerPhone) return headerPhone;
  if (req.query && req.query.phone) return String(req.query.phone);
  if (req.body && req.body.phone) return String(req.body.phone);
  return "";
}

async function getAccessByPhone(rawPhone) {
  const phone = normalizePhone(rawPhone);
  if (!phone) return null;
  const { rows } = await pool.query(
    `SELECT phone, role, COALESCE("blockIds", ARRAY[]::TEXT[]) AS "blockIds"
     FROM phone_access
     WHERE phone = $1`,
    [phone]
  );
  return rows[0] || null;
}

async function resolveAccess(req) {
  const phone = normalizePhone(getPhoneFromRequest(req));
  if (!phone) {
    return {
      phone: "",
      authenticated: false,
      role: null,
      canEdit: false,
      canReview: false,
      blockIds: [],
      scope: "global",
    };
  }

  const profile = await getAccessByPhone(phone);
  if (!profile) {
    return {
      phone,
      authenticated: false,
      role: null,
      canEdit: false,
      canReview: false,
      blockIds: [],
      scope: "global",
    };
  }

  const role = profile.role;
  const blockIds = Array.isArray(profile.blockIds)
    ? profile.blockIds.map((v) => String(v)).filter(Boolean)
    : [];

  return {
    phone,
    authenticated: true,
    role,
    canEdit: role === ROLE_EDIT || role === ROLE_REVIEW,
    canReview: role === ROLE_REVIEW,
    blockIds,
    scope: blockIds.length > 0 ? "blocks" : "global",
  };
}

function addBlockScopeFilter(access, where, values, paramIndexRef) {
  if (access.scope === "blocks" && access.blockIds.length > 0) {
    where.push(`"blockId" = ANY($${paramIndexRef.value++})`);
    values.push(access.blockIds);
  }
}

async function assertSchoolInScope(sourceKey, access) {
  const { rows } = await pool.query(
    `SELECT "sourceKey", "blockId" FROM schools WHERE "sourceKey" = $1`,
    [sourceKey]
  );
  if (rows.length === 0) return { ok: false, reason: "School not found", school: null };
  const school = rows[0];
  if (access.scope === "blocks" && !access.blockIds.includes(String(school.blockId || ""))) {
    return { ok: false, reason: "School is out of your block scope", school };
  }
  return { ok: true, reason: null, school };
}

module.exports = {
  ROLE_EDIT,
  ROLE_REVIEW,
  VALID_ROLES,
  normalizePhone,
  parseBlockIds,
  resolveAccess,
  addBlockScopeFilter,
  assertSchoolInScope,
};
