const { pool } = require("./db");

const ROLE_EDIT = "edit";
const ROLE_REVIEW = "review";
const ROLE_ADMIN = "admin";
const VALID_ROLES = [ROLE_EDIT, ROLE_REVIEW, ROLE_ADMIN];

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
    `SELECT phone, name, role, status, COALESCE("blockIds", ARRAY[]::TEXT[]) AS "blockIds"
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
      name: "",
      authenticated: false,
      role: null,
      canEdit: false,
      canReview: false,
      canAdmin: false,
      status: null,
      blockIds: [],
      scope: "global",
    };
  }

  const profile = await getAccessByPhone(phone);
  if (!profile) {
    return {
      phone,
      name: "",
      authenticated: false,
      role: null,
      canEdit: false,
      canReview: false,
      canAdmin: false,
      status: null,
      blockIds: [],
      scope: "global",
    };
  }

  const status = profile.status === "active" ? "active" : "inactive";
  if (status !== "active") {
    return {
      phone,
      name: profile.name || "",
      authenticated: false,
      role: profile.role || null,
      canEdit: false,
      canReview: false,
      canAdmin: false,
      status,
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
    name: profile.name || "",
    authenticated: true,
    role,
    canEdit: role === ROLE_EDIT || role === ROLE_REVIEW || role === ROLE_ADMIN,
    canReview: role === ROLE_REVIEW || role === ROLE_ADMIN,
    canAdmin: role === ROLE_ADMIN,
    status,
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
  ROLE_ADMIN,
  VALID_ROLES,
  normalizePhone,
  parseBlockIds,
  resolveAccess,
  addBlockScopeFilter,
  assertSchoolInScope,
};
