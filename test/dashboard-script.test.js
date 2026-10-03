import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

test("dashboard inline script remains valid JavaScript", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  const renderMarker = source.indexOf("function render(D,HD,P){");
  const scriptStart = source.lastIndexOf("<script>", renderMarker);
  const scriptEnd = source.indexOf("<\\/script>", renderMarker);

  assert.notEqual(renderMarker, -1);
  assert.notEqual(scriptStart, -1);
  assert.notEqual(scriptEnd, -1);
  const script = source
    .slice(scriptStart + 8, scriptEnd)
    .replace("const IG_SVG=${JSON.stringify(IG_SVG)};", 'const IG_SVG="<svg></svg>";');
  assert.doesNotThrow(() => new Function(script));
});

test("summary tables retain Instagram only for libraries", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  for (const prefix of ["pag", "dom"]) {
    const tbodyIndex = source.indexOf(`<tbody id="${prefix}_tbody">`);
    const tableStart = source.lastIndexOf("<table", tbodyIndex);
    const tableEnd = source.indexOf("</table>", tbodyIndex);
    assert.notEqual(tbodyIndex, -1);
    assert.notEqual(tableStart, -1);
    assert.notEqual(tableEnd, -1);
    const table = source.slice(tableStart, tableEnd + "</table>".length);
    const [, headerHtml] = table.match(/<thead><tr>([\s\S]*?)<\/tr><\/thead>/) || [];
    assert.ok(headerHtml);
    const headers = [...headerHtml.matchAll(/<th>(.*?)<\/th>/g)].map(([, title]) => title);
    const expected = [
      "#", prefix === "pag" ? "Bibliotecas" : "Domínios",
      "Gráfico", "Descoberta", "Inicial", "Atual",
      "Última Checagem", "Δ Total", "Tendência", "Participação", "3 dias",
      ...(prefix === "pag" ? ["Instagram"] : []),
    ];
    assert.deepEqual(headers, expected);
  }
  assert.match(source, /const instagramCell=P==="pag_"/);
  assert.match(source, /instagram_url/);
});

test("dashboard includes a separate keyword monitoring section and slot history", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  for (const marker of [
    'id="key_scaling"',
    'id="key_cRosca"',
    'id="key_legend"',
    'id="key_cHist"',
    'id="key_busca"',
    'id="key_tbody"',
    'id="key_hist-section"',
    'render(D_KEY,HD_KEY,"key_")',
    '.replace("__DADOS_KEY__", () => dadosKey)',
    '.replace("__HIST_KEY__", () => histDadosKey)',
  ]) {
    assert.ok(source.includes(marker), `dashboard should include ${marker}`);
  }
});

test("admin exposes keyword registration with the requested help text", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  assert.match(source, /<option value="keyword">🔑 Palavra-chave<\/option>/);
  assert.match(source, /Digite apenas a palavra ou frase/);
  assert.match(source, /Ex: jejum intermitente, biblia explicada, ansiedade/);
});

test("cron separates domains, keywords and pages into their scheduled windows", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  const start = source.indexOf('app.get("/api/cron/tick"');
  const end = source.indexOf("async function saveExtensionInitial", start);
  const cron = source.slice(start, end);

  assert.match(cron, /brtHour >= 5 && brtHour < 6/);
  assert.match(cron, /brtHour >= 6 && brtHour < 7/);
  assert.match(cron, /p\.tipo = 'dominio'[\s\S]*?sh\.slot = 5/);
  assert.match(cron, /p\.tipo = 'keyword'[\s\S]*?sh\.slot = 6/);
  assert.match(cron, /p\.tipo = 'pagina'[\s\S]*?sh\.slot = \$1/);
  assert.match(cron, /\[TICK-KEYWORD\] Iniciando lote de palavras-chave para slot=6/);
  assert.match(source, /CHECK \(tipo IN \('pagina', 'dominio', 'keyword'\)\)/);
});

test("completed manual reports have persistent dismissal controls keyed by run", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  assert.match(source, /id="manual-run-dismiss"/);
  assert.match(source, /aria-label="Fechar relatório e quadro de checagem"/);
  assert.match(source, /function dismissManualReport\(runId\)/);
  assert.match(source, /localStorage\.setItem\(MANUAL_REPORT_DISMISSED_KEY,runId\)/);
  assert.match(source, /localStorage\.getItem\(MANUAL_REPORT_DISMISSED_KEY\)/);
  assert.match(source, /dismissedRunId===state\.runId/);
  assert.match(source, /if\(finalizado&&state\.runId\)/);
  assert.match(source, /manualRunDismiss\.hidden=!finalizado/);
  assert.match(source, /manualDisplayedRunId=state\.runId/);
  assert.match(source, /manualReportDialog\.addEventListener\("cancel"/);
});
