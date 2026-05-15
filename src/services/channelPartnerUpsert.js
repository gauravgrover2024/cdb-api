import Channel from "../models/Channel.js";

export const normalizeMobileForChannel = (value) => {
  const digits = String(value ?? "").replace(/\D/g, "");
  return digits.length >= 10 ? digits.slice(-10) : "";
};

const escapeRegex = (value) =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const guessCityForChannel = (payload = {}, address = "") => {
  const explicit = String(
    payload.city ||
      payload.permanentCity ||
      payload.companyCity ||
      payload.registrationCity ||
      payload.postfile_regd_city ||
      payload.dealerCity ||
      "",
  ).trim();
  if (explicit) return explicit;

  const match = String(address || "").match(
    /([A-Za-z\s]+),\s*[A-Za-z\s]+(?:\s+\d{6})?$/,
  );
  if (match?.[1]) return match[1].trim();

  return "Unknown";
};

const pickMobile = (...values) => {
  for (const value of values) {
    const mobile = normalizeMobileForChannel(value);
    if (mobile) return mobile;
  }
  return "";
};

const resolvePartnerSpec = (payload = {}) => {
  const channelId = String(payload.channelDealerNo || "").trim();
  if (channelId) {
    return { mode: "lookup", channelId };
  }

  const source = String(
    payload.recordSource || payload.source || payload.sourceOrigin || "",
  )
    .trim()
    .toLowerCase();
  const policyDoneBy = String(payload.policyDoneBy || "").trim().toLowerCase();

  if (source === "indirect") {
    const name = String(
      payload.dealerChannelName ||
        payload.dealerName ||
        payload.sourceName ||
        "",
    ).trim();
    if (!name) return null;

    return {
      mode: "upsert",
      type: "Dealer",
      name,
      mobile: pickMobile(
        payload.dealerMobile,
        payload.dealerChannelMobile,
        payload.sourceMobile,
        payload.referencePhone,
        payload.alternatePhone,
        payload.mobile,
        payload.primaryMobile,
      ),
      address: String(
        payload.dealerChannelAddress || payload.dealerAddress || "",
      ).trim(),
      city: guessCityForChannel(payload, payload.dealerChannelAddress),
    };
  }

  if (policyDoneBy.includes("broker")) {
    const name = String(payload.brokerName || "").trim();
    if (!name) return null;

    return {
      mode: "upsert",
      type: "Broker",
      name,
      mobile: pickMobile(
        payload.brokerMobile,
        payload.referencePhone,
        payload.alternatePhone,
        payload.mobile,
        payload.primaryMobile,
      ),
      address: String(
        payload.dealerChannelAddress ||
          payload.residenceAddress ||
          payload.address ||
          "",
      ).trim(),
      city: guessCityForChannel(payload, payload.residenceAddress),
    };
  }

  return null;
};

const findExistingChannel = async ({ channelId, name, mobile, type }) => {
  if (channelId) {
    const byCode = await Channel.findOne({ channelId });
    if (byCode) return byCode;
  }

  const clauses = [];
  if (mobile) clauses.push({ mobile });
  if (name) {
    clauses.push({
      name: new RegExp(`^${escapeRegex(name)}$`, "i"),
    });
  }
  if (!clauses.length) return null;

  const query = { $or: clauses };
  if (type) query.type = type;

  return Channel.findOne(query);
};

const patchExistingChannel = async (existing, patch) => {
  let dirty = false;
  const assign = (key, value) => {
    if (value == null || value === "") return;
    if (existing[key] !== value) {
      existing[key] = value;
      dirty = true;
    }
  };

  assign("name", patch.name);
  assign("businessName", patch.name);
  assign("contactPerson", patch.name);
  assign("mobile", patch.mobile);
  assign("address", patch.address);
  assign("city", patch.city);
  assign("type", patch.type);
  if (!existing.status || existing.status !== "Active") {
    existing.status = "Active";
    dirty = true;
  }

  if (dirty) await existing.save();
  return existing;
};

/**
 * Find or create channel partner for Loan + Insurance payloads.
 * Never throws — callers should wrap if they need hard failures.
 */
export const upsertChannelPartner = async (payload = {}) => {
  const spec = resolvePartnerSpec(payload);
  if (!spec) return null;

  if (spec.mode === "lookup") {
    return findExistingChannel({ channelId: spec.channelId });
  }

  const { type, name } = spec;
  let { mobile, address, city } = spec;

  address =
    address ||
    String(payload.residenceAddress || payload.dealerChannelAddress || "").trim() ||
    "Address Not Provided";
  city = city || guessCityForChannel(payload, address) || "Unknown";

  const existing = await findExistingChannel({ name, mobile, type });
  if (existing) {
    return patchExistingChannel(existing, {
      name,
      mobile: mobile || existing.mobile,
      address,
      city,
      type,
    });
  }

  if (!mobile) return null;

  const year = new Date().getFullYear();
  const channelCount = await Channel.countDocuments();
  const channelId = `CH-${year}-${String(channelCount + 1).padStart(4, "0")}`;

  return Channel.create({
    channelId,
    name,
    businessName: name,
    type,
    contactPerson: name,
    mobile,
    address,
    city,
    status: "Active",
    commissionRate: Number(payload.commissionRate || payload.payoutPercent || 0) || 0,
    payoutPercentage: 0,
  });
};

/** @deprecated Use upsertChannelPartner — kept for loan import alias */
export const upsertChannelPartnerFromLoan = upsertChannelPartner;
