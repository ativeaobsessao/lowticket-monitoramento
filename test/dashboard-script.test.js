import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";

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

test("last-check date uses only successful collection and failures are separate six-hour alerts", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  const routeStart = source.indexOf('app.get("/api/ultima-checagem"');
  const routeEnd = source.indexOf('app.get("/api/cron/tick"', routeStart);
  const route = source.slice(routeStart, routeEnd);
  const dashboardStart = source.indexOf('app.get("/dashboard"');
  const inlineScriptStart = source.lastIndexOf("<script>", source.indexOf("function render(D,HD,P){", dashboardStart));
  const inlineScriptEnd = source.indexOf("<\\/script>", inlineScriptStart);
  const dashboardScript = source.slice(inlineScriptStart, inlineScriptEnd);
  const renderStart = dashboardScript.indexOf("function render(D,HD,P){");
  const renderAndRefresh = dashboardScript.slice(renderStart);
  const tableAlertStart = renderAndRefresh.indexOf("const checkLabels=");
  const tableAlertEnd = renderAndRefresh.indexOf("tr.dataset.search=", tableAlertStart);
  const tableAlert = renderAndRefresh.slice(tableAlertStart, tableAlertEnd);
  const refreshStart = renderAndRefresh.indexOf("async function refreshLastChecks(){");
  const refreshEnd = renderAndRefresh.indexOf("\nasync function updateManualCheck(){", refreshStart);
  const refreshLastChecks = renderAndRefresh.slice(refreshStart, refreshEnd);

  assert.match(route, /l\.collected_at AS ultima_coleta_ok/);
  assert.match(route, /jsonb_build_object\([\s\S]*'status', a\.status,[\s\S]*'error', a\.error,[\s\S]*'at', a\.checked_at,[\s\S]*'relevante'/);
  assert.match(route, /a\.checked_at > NOW\(\) - INTERVAL '6 hours'/);
  assert.match(route, /a\.status IN \('falha_timeout','falha_bloqueio','falha_parse','falha_url_invalida','falha_gravacao'\)/);
  assert.match(route, /a\.source LIKE 'cron%' OR a\.source LIKE 'manual%'/);
  assert.match(route, /status, error, source/);
  assert.doesNotMatch(route, /falha_execucao|em_andamento/);
  assert.doesNotMatch(route, /CASE\s+WHEN[\s\S]*last_attempt_at/);

  assert.match(renderAndRefresh, /const checkAt=ultima\[pag\]\?\.ultimaColeta/);
  assert.match(renderAndRefresh, /tentativa\?\.relevante/);
  assert.match(tableAlert, /checkLabels=\{falha_timeout:"timeout",falha_bloqueio:"bloqueio Meta",falha_parse:"não lido",falha_url_invalida:"URL inválida",falha_gravacao:"falha ao salvar"\}/);
  assert.doesNotMatch(tableAlert, /falha_execucao|em_andamento|execução interrompida/);
  assert.match(refreshLastChecks, /const labels=\{falha_timeout:"timeout",falha_bloqueio:"bloqueio Meta",falha_parse:"não lido",falha_url_invalida:"URL inválida",falha_gravacao:"falha ao salvar"\}/);
  assert.doesNotMatch(refreshLastChecks, /falha_execucao|em_andamento|execução interrompida/);
  assert.match(renderAndRefresh, /check\.ultima_coleta_ok/);
  assert.match(renderAndRefresh, /check\.tentativa\?\.relevante&&labels\[check\.tentativa\?\.status\]/);
  assert.match(renderAndRefresh, /labels\[check\.tentativa\?\.status\]/);
  assert.doesNotMatch(renderAndRefresh, /check\.checked_at|ultima\[pag\]\?\.tentativa\|\|ultima\[pag\]\?\.ultimaColeta/);
});

test("summary tables retain Instagram only for libraries", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  for (const prefix of ["pag", "dom", "key"]) {
    const tbodyIndex = source.indexOf(`<tbody id="${prefix}_tbody">`);
    const tableStart = source.lastIndexOf("<table", tbodyIndex);
    const tableEnd = source.indexOf("</table>", tbodyIndex);
    assert.notEqual(tbodyIndex, -1);
    assert.notEqual(tableStart, -1);
    assert.notEqual(tableEnd, -1);
    const table = source.slice(tableStart, tableEnd + "</table>".length);
    const [, headerHtml] = table.match(/<thead><tr>([\s\S]*?)<\/tr><\/thead>/) || [];
    assert.ok(headerHtml);
    const headers = [...headerHtml.matchAll(/<th(?:\s+[^>]*)?>(.*?)<\/th>/g)].map(([, title]) => title);
    const expected = [
      "#", prefix === "pag" ? "Bibliotecas" : prefix === "dom" ? "Domínios" : "Palavras-chave",
      "Gráfico", "Descoberta", "Inicial", "Atual",
      "Última Checagem", "Δ Total", "Tendência", ...(prefix === "pag" ? ["SCORE ↕"] : []), "Participação", prefix === "pag" ? "3D" : "3 dias",
      ...(prefix === "pag" ? ["Instagram"] : []),
    ];
    assert.deepEqual(headers, expected);
  }
  assert.match(source, /const instagramCell=P==="pag_"/);
  assert.match(source, /instagram_url/);
});

test("library summary has a persistent date-window selector limited to pag_", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  const selectorIndex = source.indexOf('id="pag_window_selector"');
  const searchIndex = source.indexOf('id="pag_busca"');
  assert.ok(selectorIndex >= 0 && selectorIndex < searchIndex);
  for (const value of ["3", "7", "14", "30", "custom"]) {
    assert.ok(source.includes(`data-w="${value}"`), `window selector should include ${value}`);
  }
  assert.match(source, /function computeWindowStats\(pagName,windowDays,customRange\)/);
  assert.match(source, /function updateResumoBibliotecas\(\)/);
  assert.match(source, /localStorage\.setItem\("pag_window"/);
  assert.match(source, /localStorage\.setItem\("pag_custom"/);
  assert.match(source, /setupResumoBibliotecas\(\);\s*updateResumoBibliotecas\(\);/);
  assert.match(source, /if\(P!=="pag_"\)return;/);
});

test("library scores use the selected window and never add score columns to other groups", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  const renderMarker = source.indexOf("function render(D,HD,P){");
  const scriptStart = source.lastIndexOf("<script>", renderMarker);
  const scriptEnd = source.indexOf("<\\/script>", renderMarker);
  const script = source.slice(scriptStart + 8, scriptEnd)
    .replace("const IG_SVG=${JSON.stringify(IG_SVG)};", 'const IG_SVG="<svg></svg>";');
  const slopeStart = script.indexOf("function slope(datas,valores){");
  const slopeEnd = script.indexOf("\nfunction slopeHistorico", slopeStart);
  const dateShiftStart = script.indexOf("function shiftDateKey(dateKey,days){");
  const scoreStart = script.indexOf("function computeScoreAndFase(pagName){");
  const scoreEnd = script.indexOf("\nlet scoreSortDirection", scoreStart);

  assert.ok(slopeStart >= 0 && slopeEnd > slopeStart);
  assert.ok(scoreStart >= 0 && scoreEnd > scoreStart);
  const scoreCode = script.slice(slopeStart, slopeEnd)
    + "\n" + script.slice(dateShiftStart, scoreStart)
    + "\n" + script.slice(scoreStart, scoreEnd);
  const dates = Array.from({ length: 14 }, (_, index) => {
    const date = new Date(Date.UTC(2026, 8, index + 1));
    return date.toISOString().slice(0, 10);
  });
  const context = {
    datas: dates,
    pags: {
      Ester: Object.fromEntries(dates.map((date, index) => [date, [index + 2]])),
      MeuFluxo: Object.fromEntries(dates.map((date, index) => [date, [index < 7 ? 250 + index * 20 : [340, 343, 346, 349, 352, 356, 360][index - 7]]])),
    },
    ultima: { Ester: { ads: 15 }, MeuFluxo: { ads: 360 } },
    mon: { Ester: { ini: 5 }, MeuFluxo: { ini: 140 } },
    pagWindow: 7,
    pagWindowCustom: false,
    pagCustom: null,
    med: values => values.reduce((sum, value) => sum + value, 0) / values.length,
  };
  vm.runInNewContext(scoreCode, context);
  const ester = vm.runInNewContext("computeScoreAndFase('Ester')", context);
  const meuFluxo = vm.runInNewContext("computeScoreAndFase('MeuFluxo')", context);
  const inactiveContext = { ...context, ultima: { Dead: { ads: 0 } }, pags: { Dead: {} }, mon: {} };
  vm.runInNewContext(scoreCode, inactiveContext);
  const inactive = vm.runInNewContext("computeScoreAndFase('Dead')", inactiveContext);

  assert.equal(ester.score, 100);
  assert.equal(ester.fase, "🚀 ESCALANDO");
  assert.equal(ester.veredito, "✅ Vale modelar");
  assert.equal(meuFluxo.score, 43);
  assert.equal(meuFluxo.fase, "⚠️ EM DECLÍNIO");
  assert.equal(meuFluxo.veredito, "⚠️ Cuidado - em declínio");
  assert.equal(inactive.score, 0);
  assert.equal(inactive.fase, "💀 INATIVO");
  assert.equal(inactive.veredito, "Morto");
  assert.match(source, /id="pag_th_score"/);
  assert.match(script, /function scoreCellContent\(result,pagName\)/);
  assert.match(script, /Desacelerando/);
  assert.match(script, /score-escalando/);
  assert.match(script, /score-declinio/);
  assert.match(script, /const porAds=\[\.\.\.LP\]\.sort\(\(a,b\)=>P==="pag_"/);
  assert.match(script, /let scoreSortDirection=-1/);
  assert.match(script, /return \(scoreA-scoreB\)\*scoreSortDirection/);
  assert.doesNotMatch(script.slice(script.indexOf('id="dom_tbody"'), script.indexOf('id="key_tbody"')), /data-role="pag-score"/);
});

test("summary table participation stays clipped inside a fixed-width scrolling table", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  assert.match(source, /\.tbl-panel\{[^}]*overflow-x:auto/);
  assert.match(source, /\.tbl-panel table\{[^}]*min-width:1250px;table-layout:fixed/);
  assert.match(source, /th\.participacao-header,td\.participacao-cell\{width:140px;min-width:140px;max-width:140px;overflow:hidden/);
  assert.match(source, /\.participacao-wrapper \.scalebar-bg\{width:70px;min-width:70px;max-width:70px[^}]*overflow:hidden/);
  assert.match(source, /class="participacao-cell" data-label="Participação"><div class="participacao-wrapper">/);
  assert.match(source, /class="pct" style="[^"]*flex-shrink:0"/);
  assert.doesNotMatch(source, /\.tbl-panel table thead\{display:none\}/);
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
