import test from "node:test";
import assert from "node:assert/strict";
import {
  buildKeywordSearchUrl,
  inferMonitoringType,
  parseBatchLine,
} from "../monitoring-input.js";

test("builds a Meta keyword search URL from a phrase", () => {
  assert.equal(
    buildKeywordSearchUrl("jejum intermitente"),
    "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&q=jejum%20intermitente&search_type=keyword_unordered",
  );
});

test("recognizes keyword searches and keeps domain searches classified as domains", () => {
  assert.equal(
    inferMonitoringType("https://www.facebook.com/ads/library/?q=jejum&search_type=keyword_unordered"),
    "keyword",
  );
  assert.equal(
    inferMonitoringType("https://www.facebook.com/ads/library/?q=loja.com&search_type=keyword_unordered"),
    "dominio",
  );
  assert.equal(
    inferMonitoringType("https://www.facebook.com/ads/library/?view_all_page_id=123"),
    "pagina",
  );
});

test("parses keyword batch entries with generated or explicit Meta search URLs", () => {
  assert.deepEqual(parseBatchLine("keyword | Jejum Intermitente"), {
    nome: "Jejum Intermitente",
    url: "Jejum Intermitente",
    tipo: "keyword",
    instagram_url: null,
  });
  assert.deepEqual(
    parseBatchLine("palavra-chave | Jejum | https://www.facebook.com/ads/library/?q=jejum&search_type=keyword_unordered"),
    {
      nome: "Jejum",
      url: "https://www.facebook.com/ads/library/?q=jejum&search_type=keyword_unordered",
      tipo: "keyword",
      instagram_url: null,
    },
  );
  assert.equal(
    parseBatchLine("Jejum | https://www.facebook.com/ads/library/?q=jejum%20intermitente&search_type=keyword_unordered").tipo,
    "keyword",
  );
});
