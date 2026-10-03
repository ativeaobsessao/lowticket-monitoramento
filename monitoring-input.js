const MONITORING_TYPES = new Map([
  ["pagina", "pagina"],
  ["dominio", "dominio"],
  ["keyword", "keyword"],
  ["palavra", "keyword"],
  ["palavra-chave", "keyword"],
  ["palavra chave", "keyword"],
]);

export function normalizeMonitoringType(value) {
  return MONITORING_TYPES.get(String(value ?? "").trim().toLowerCase()) ?? null;
}

export function buildKeywordSearchUrl(keyword) {
  const cleaned = String(keyword ?? "").trim().replace(/^["'\s]+|["'\s]+$/g, "");
  if (!cleaned) return null;
  return `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&q=${encodeURIComponent(cleaned)}&search_type=keyword_unordered`;
}

export function inferMonitoringType(rawUrl) {
  try {
    const url = new URL(rawUrl);
    if (url.searchParams.has("view_all_page_id") || url.searchParams.has("id")) return "pagina";
    if (
      url.searchParams.get("search_type") === "keyword_unordered"
      && url.searchParams.has("q")
    ) {
      const query = url.searchParams.get("q").trim();
      return /^[a-z0-9-]+(\.[a-z0-9-]+)+$/i.test(query) ? "dominio" : "keyword";
    }
  } catch {
    return "dominio";
  }
  return "dominio";
}

export function parseBatchLine(line) {
  const parts = String(line).split("|").map((part) => part.trim()).filter(Boolean);
  if (parts.length < 2) return null;

  const forcedType = normalizeMonitoringType(parts[0]);
  const nome = forcedType ? parts[1] : parts[0];
  const fields = forcedType ? parts.slice(2) : parts.slice(1);
  let instagram_url = null;
  const last = fields.at(-1);
  if (fields.length >= 2 && /instagram\.com/i.test(last)) {
    instagram_url = fields.pop();
  }

  let url = fields.join("|");
  if (!url && forcedType === "keyword") url = nome;
  if (!url) return null;

  const tipo = forcedType ?? inferMonitoringType(url);
  return { nome, url, tipo, instagram_url };
}
