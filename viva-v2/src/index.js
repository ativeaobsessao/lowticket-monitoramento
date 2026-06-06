import express from "express";
import cors from "cors";
import { chromium } from "playwright";
import { execSync } from "child_process";
import cron from "node-cron";
import pg from "pg";

const { Pool } = pg;
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Database ────────────────────────────────────────────────────────────────

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL environment variable is required.");
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

async function initDb() {
  await query(`
    CREATE TABLE IF NOT EXISTS pages (
      slug        TEXT PRIMARY KEY,
      nome        TEXT NOT NULL,
      url         TEXT NOT NULL,
      created_at  TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS scrape_history (
      id           SERIAL PRIMARY KEY,
      slug         TEXT NOT NULL,
      ads_count    INTEGER NOT NULL,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug)
  `);
  console.log("[DB] Tables ready.");
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function toSlug(nome) {
  return nome
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function getChromiumPath() {
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    return process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }
  try {
    return execSync("which chromium || which chromium-browser || which google-chrome", {
      encoding: "utf8",
    }).trim().split("\n")[0];
  } catch {
    return undefined;
  }
}

// ─── Scraper ──────────────────────────────────────────────────────────────────

// Scrapes a single URL reusing an already-open browser context.
// Returns the ad count, or null if all strategies failed.
async function scrapeWithContext(context, url) {
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForTimeout(15000);

    const content = await page.content();

    // Strategy 1: HTML regex
    const htmlMatch = content.match(/([\d.,]+)\s*(resultados|results)/i);
    if (htmlMatch) {
      const parsed = parseInt(htmlMatch[1].replace(/[,.]/g, ""), 10);
      if (!isNaN(parsed)) return parsed;
    }

    // Strategy 2: visible element
    for (const kw of ["resultados", "results"]) {
      try {
        const el = page.locator(`text=/${kw}/i`).first();
        await el.waitFor({ timeout: 3000 });
        const texto = await el.innerText();
        const match = texto.replace(/[,.]/g, "").match(/\d+/);
        if (match) return parseInt(match[0], 10);
      } catch {
        continue;
      }
    }

    // Strategy 3: body text
    const bodyText = (await page.textContent("body")) ?? "";
    const textMatch = bodyText.match(/([\d.,]+)\s*(resultados|results)/i);
    if (textMatch) {
      const parsed = parseInt(textMatch[1].replace(/[,.]/g, ""), 10);
      if (!isNaN(parsed)) return parsed;
    }

    return null;
  } finally {
    await page.close();
  }
}

// Single-URL scrape (opens its own browser). Used by the manual /api/coletar route.
async function scrapeAdCount(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
      ],
    });
    try {
      const context = await browser.newContext({
        locale: "pt-BR",
        userAgent:
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        extraHTTPHeaders: { "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8" },
      });
      const count = await scrapeWithContext(context, url);
      if (count !== null) {
        console.log(`[SCRAPE] attempt=${attempt} count=${count}`);
        return count;
      }
      console.warn(`[SCRAPE] attempt=${attempt} — count not found, retrying...`);
    } catch (err) {
      console.error(`[SCRAPE] attempt=${attempt} error: ${err.message}`);
    } finally {
      await browser.close();
    }
    if (attempt < retries) await new Promise((r) => setTimeout(r, 5000));
  }
  console.error(`[SCRAPE] all ${retries} attempts failed, returning 0`);
  return 0;
}

// Save a count to history, skipping duplicates within 60s.
async function saveCount(slug, count) {
  const { rows: recent } = await query(
    `SELECT id FROM scrape_history
     WHERE slug = $1 AND collected_at >= NOW() - INTERVAL '60 seconds'
     LIMIT 1`,
    [slug]
  );
  if (recent.length === 0) {
    await query("INSERT INTO scrape_history (slug, ads_count) VALUES ($1, $2)", [slug, count]);
    console.log(`[HISTORY] slug=${slug} count=${count} saved`);
  } else {
    console.log(`[HISTORY] slug=${slug} skipped duplicate`);
  }
}

// ─── Scheduled run: scrape ALL pages reusing one browser ────────────────────────

let isRunning = false;

async function runAllScrapes(trigger = "cron") {
  if (isRunning) {
    console.warn(`[RUN] skipped (${trigger}) — a run is already in progress`);
    return { skipped: true };
  }
  isRunning = true;
  const startedAt = new Date();
  console.log(`[RUN] ===== started (${trigger}) at ${startedAt.toISOString()} =====`);

  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");
  if (!pages.length) {
    console.log("[RUN] no pages registered, nothing to do");
    isRunning = false;
    return { pages: 0 };
  }

  let browser;
  const results = [];
  try {
    browser = await chromium.launch({
      executablePath: getChromiumPath(),
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
      ],
    });
    const context = await browser.newContext({
      locale: "pt-BR",
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      extraHTTPHeaders: { "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8" },
    });

    for (const p of pages) {
      let count = null;
      // up to 2 attempts per page within the shared browser
      for (let attempt = 1; attempt <= 2 && count === null; attempt++) {
        try {
          count = await scrapeWithContext(context, p.url);
        } catch (err) {
          console.error(`[RUN] slug=${p.slug} attempt=${attempt} error: ${err.message}`);
        }
      }
      const final = count ?? 0;
      await saveCount(p.slug, final);
      results.push({ slug: p.slug, count: final });
    }
  } catch (err) {
    console.error(`[RUN] fatal error: ${err.message}`);
  } finally {
    if (browser) await browser.close();
    isRunning = false;
  }

  const secs = Math.round((Date.now() - startedAt.getTime()) / 1000);
  console.log(`[RUN] ===== finished (${trigger}) — ${results.length} pages in ${secs}s =====`);
  return { pages: results.length, durationSec: secs, results };
}

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get("/api/healthz", (_req, res) => {
  res.json({ status: "ok", ts: new Date().toISOString() });
});

app.post("/api/salvar", async (req, res) => {
  const { nome, url } = req.body;
  if (!nome || !url) {
    return res.status(400).json({ error: "Fields 'nome' and 'url' are required." });
  }
  const slug = toSlug(nome);
  if (!slug) {
    return res.status(400).json({ error: "Could not generate a valid slug." });
  }
  await query(
    `INSERT INTO pages (slug, nome, url)
     VALUES ($1, $2, $3)
     ON CONFLICT (slug) DO UPDATE SET nome = $2, url = $3`,
    [slug, nome, url]
  );
  console.log(`[SALVAR] registered slug=${slug}`);
  res.json({ slug, coletarPath: `/api/coletar/${slug}` });
});

// Manual single-page scrape (kept for compatibility / manual testing)
app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
  const row = rows[0];
  if (!row) {
    return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
  }
  try {
    const count = await scrapeAdCount(row.url);
    res.type("text/plain").send(String(count));
    await saveCount(slug, count);
  } catch (err) {
    console.error(`[COLETAR] error slug=${slug}: ${err.message}`);
    res.type("text/plain").send("0");
  }
});

// Manual trigger to scrape everything now (handy for testing the scheduler logic)
app.get("/api/coletar-tudo", async (_req, res) => {
  res.json({ status: "started" });
  runAllScrapes("manual").catch((e) => console.error("[RUN] manual error:", e.message));
});

app.get("/api/historico/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT id, slug, ads_count, collected_at
     FROM scrape_history
     WHERE slug = $1
     ORDER BY collected_at DESC`,
    [slug]
  );
  res.json(rows);
});

app.get("/api/resumo/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT ads_count, collected_at
     FROM scrape_history
     WHERE slug = $1
     ORDER BY collected_at ASC`,
    [slug]
  );
  if (rows.length === 0) return res.json({ slug, message: "No data yet." });
  const counts = rows.map((r) => r.ads_count);
  const min = Math.min(...counts);
  const max = Math.max(...counts);
  const avg = Math.round(counts.reduce((a, b) => a + b, 0) / counts.length);
  const first = counts[0];
  const last = counts[counts.length - 1];
  const trend = last > first ? "crescendo" : last < first ? "caindo" : "estável";
  res.json({ slug, total_coletas: rows.length, min, max, avg, trend, first, last });
});

app.get("/api/status", async (_req, res) => {
  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");
  const result = await Promise.all(
    pages.map(async (p) => {
      const { rows } = await query(
        `SELECT ads_count, collected_at
         FROM scrape_history
         WHERE slug = $1
         ORDER BY collected_at DESC
         LIMIT 1`,
        [p.slug]
      );
      const latest = rows[0];
      return {
        slug: p.slug,
        nome: p.nome,
        url: p.url,
        ads_ativos: latest?.ads_count ?? null,
        ultima_coleta: latest?.collected_at ?? null,
      };
    })
  );
  res.json(result);
});

app.get("/api/paginas", async (_req, res) => {
  const { rows } = await query("SELECT slug, nome, url FROM pages");
  res.json(rows);
});

// ─── Dashboard ────────────────────────────────────────────────────────────────

app.get("/dashboard", async (_req, res) => {
  try {
    const { rows: pages } = await query("SELECT slug, nome FROM pages");
    const ultimaLeitura = {};
    const primeiraData = {};
    const paginas = {};
    const mon = {};

    for (const p of pages) {
      const { rows: hist } = await query(
        `SELECT ads_count, collected_at FROM scrape_history WHERE slug=$1 ORDER BY collected_at ASC`,
        [p.slug]
      );
      if (!hist.length) continue;
      ultimaLeitura[p.nome] = { ads: hist[hist.length - 1].ads_count };
      primeiraData[p.nome] = hist[0].collected_at.toISOString().slice(0, 10);
      // BASELINE: primeiro valor de ads coletado (faz a coluna "Inicial" e a tendencia funcionarem)
      mon[p.nome] = { ini: hist[0].ads_count };
      paginas[p.nome] = {};
      for (const h of hist) {
        const dk = h.collected_at.toISOString().slice(0, 10);
        const hour = h.collected_at.getHours();
        const slot = [3, 12, 22].reduce((b, s) => Math.abs(hour - s) < Math.abs(hour - b) ? s : b, 3);
        if (!paginas[p.nome][dk]) paginas[p.nome][dk] = {};
        paginas[p.nome][dk][slot] = h.ads_count;
      }
    }

    const dados = JSON.stringify({ pags: paginas, ultima: ultimaLeitura, primeira: primeiraData, mon });

    res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VIVA Labs — Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#080810;--surface:#0f0f1e;--border:#1e1e38;--text:#e8e8f8;--muted:#555570;--accent:#6c63ff;--green:#3ecfcf;--red:#ff6b6b;--yellow:#ffd166}
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;overflow:hidden}
body{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans',sans-serif;display:grid;grid-template-rows:48px 72px 1fr 1fr 120px;gap:10px;padding:10px;height:100vh}
.hdr{display:flex;align-items:center;gap:10px;background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:0 14px}
.hdr-icon{font-size:18px}
.hdr h1{font-size:14px;font-weight:700;color:#fff;letter-spacing:.3px}
.hdr-sub{font-size:10px;color:var(--muted);margin-left:auto;font-family:'IBM Plex Mono',monospace}
.cards-row{display:flex;gap:8px;overflow-x:auto;padding:2px 0;scrollbar-width:thin;scrollbar-color:var(--border) transparent}
.cards-row::-webkit-scrollbar{height:3px}
.cards-row::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px 14px;min-width:130px;flex-shrink:0;display:flex;flex-direction:column;gap:2px;transition:border-color .2s}
.card:hover{border-color:var(--accent)}
.card-name{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:110px}
.card-val{font-size:20px;font-weight:700;color:#fff;font-family:'IBM Plex Mono',monospace;line-height:1.2}
.card-trend{font-size:10px;line-height:1}
.card-delta{font-size:9px;color:var(--muted);font-family:'IBM Plex Mono',monospace}
.rank-bar-bg{height:3px;background:var(--border);border-radius:2px}
.rank-bar{height:3px;border-radius:2px;background:var(--accent)}
.row-charts{display:grid;grid-template-columns:340px 1fr;gap:10px}
.box{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 14px;display:flex;flex-direction:column;gap:6px;overflow:hidden}
.box-title{font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;flex-shrink:0}
.chart-wrap{flex:1;position:relative;min-height:0}
.chart-wrap canvas{position:absolute;inset:0;width:100%!important;height:100%!important}
.box-hist{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 14px;display:flex;flex-direction:column;gap:6px;overflow:hidden}
#legend-rosca{display:flex;flex-direction:column;gap:4px;overflow-y:auto;flex:1;scrollbar-width:thin;scrollbar-color:var(--border) transparent}
#legend-rosca::-webkit-scrollbar{width:3px}
#legend-rosca::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.leg-item{display:flex;align-items:center;gap:7px;padding:2px 0}
.leg-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0}
.leg-label{font-size:11px;color:#d0d0e8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.leg-val{font-size:11px;font-family:'IBM Plex Mono',monospace;color:#fff;font-weight:600;flex-shrink:0}
.tbl-wrap{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;display:flex;flex-direction:column}
.tbl-scroll{overflow-y:auto;flex:1;scrollbar-width:thin;scrollbar-color:var(--border) transparent}
.tbl-scroll::-webkit-scrollbar{width:4px}
.tbl-scroll::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
table{width:100%;border-collapse:collapse;font-size:11px}
thead th{background:#13132a;color:#8888aa;font-size:9px;text-transform:uppercase;letter-spacing:.5px;padding:7px 12px;text-align:left;position:sticky;top:0;z-index:1}
td{padding:6px 12px;border-top:1px solid var(--border);color:#ccc;white-space:nowrap}
tr:hover td{background:#13132a}
.num{font-family:'IBM Plex Mono',monospace;font-weight:600}
.badge{display:inline-block;padding:2px 7px;border-radius:20px;font-size:9px;font-weight:600}
.bu{background:#0a2a2a;color:#3ecfcf}.bd{background:#2a0a0a;color:#ff6b6b}.bs{background:#18182e;color:#777}.br{background:#18082e;color:#a78bfa}.bi{background:#1a0808;color:#ff4444}.bst{background:#081a10;color:#60d394}
</style>
</head>
<body>
<div class="hdr">
  <span class="hdr-icon">📡</span>
  <h1>VIVA Labs — Monitor de Bibliotecas</h1>
  <span class="hdr-sub" id="upd"></span>
</div>
<div class="cards-row" id="cards"></div>
<div class="row-charts">
  <div class="box" style="flex-direction:row;gap:12px">
    <div style="display:flex;flex-direction:column;gap:6px;width:160px;flex-shrink:0">
      <div class="box-title">🍩 Distribuicao atual</div>
      <div style="position:relative;flex:1;min-height:0">
        <canvas id="cRosca" style="position:absolute;inset:0;width:100%!important;height:100%!important"></canvas>
      </div>
    </div>
    <div style="display:flex;flex-direction:column;gap:4px;flex:1;overflow:hidden">
      <div class="box-title" style="margin-bottom:2px">Ranking</div>
      <div id="legend-rosca"></div>
    </div>
  </div>
  <div class="box">
    <div class="box-title">📈 Variacao intradiaria — hoje (03h · 12h · 22h)</div>
    <div class="chart-wrap"><canvas id="cIntra"></canvas></div>
  </div>
</div>
<div class="box-hist">
  <div class="box-title">📊 Evolucao historica — media diaria &nbsp;<span style="color:var(--red);font-weight:400;text-transform:none;letter-spacing:0">● dia de descoberta</span></div>
  <div class="chart-wrap"><canvas id="cHist"></canvas></div>
</div>
<div class="tbl-wrap">
  <div class="box-title" style="padding:8px 12px 4px;flex-shrink:0">📋 Resumo completo</div>
  <div class="tbl-scroll">
    <table>
      <thead><tr><th>#</th><th>Biblioteca</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Δ Total</th><th>Tendencia</th><th>Escala</th><th>3 dias</th></tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>
<script>
const D=__DADOS_PLACEHOLDER__;
const COR=["#6c63ff","#3ecfcf","#ff6b9d","#ffd166","#60d394","#a78bfa","#f77f00","#4cc9f0","#ff4d6d","#b5e48c","#e040fb","#00bcd4","#ff7043","#8bc34a","#ffc107","#03a9f4","#e91e63","#009688","#cddc39","#ff5722"];
const HR=[3,12,22];
function med(s){const v=Object.values(s).filter(x=>!isNaN(x));return v.length?Math.round(v.reduce((a,b)=>a+b,0)/v.length):null}
function fd(dk){const[y,m,d]=dk.split("-");return d+"/"+m}
function fdFull(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y}
const{pags,ultima,primeira,mon}=D;
const LP=Object.keys(pags).sort();
const dSet=new Set();LP.forEach(p=>Object.keys(pags[p]).forEach(d=>dSet.add(d)));
const datas=Array.from(dSet).sort();
const ult=datas[datas.length-1];
document.getElementById("upd").textContent="Atualizado: "+new Date().toLocaleString("pt-BR")+" · 03h · 12h · 22h";
const porAds=[...LP].sort((a,b)=>(ultima[b]?.ads||0)-(ultima[a]?.ads||0));
const maxAds=ultima[porAds[0]]?.ads||1;
function tendInfo(pag){
  const m=mon[pag]||{};const at=ultima[pag]?.ads??0;const ini=m.ini??at;
  const vn=at-ini,pct=ini>0?((at-ini)/ini)*100:0;
  let tend,tc,bc;
  if(at===0){tend="⛔ Inativo";tc="#ff4444";bc="bi"}
  else if(pct>50){tend="🚀 Escalando forte";tc="#a78bfa";bc="br"}
  else if(pct>10){tend="⬆ Crescendo";tc="#3ecfcf";bc="bu"}
  else if(pct<-30){tend="⬇ Cortando";tc="#ff6b6b";bc="bd"}
  else if(pct<-5){tend="↘ Diminuindo";tc="#ff6b6b";bc="bd"}
  else if(vn===0){tend="➡ Estavel";tc="#666";bc="bs"}
  else{tend="📈 Comecando";tc="#60d394";bc="bst"}
  return{at,ini,vn,pct,tend,tc,bc};
}
porAds.forEach(pag=>{
  const t=tendInfo(pag);
  const pct=Math.round((t.at/maxAds)*100);
  const el=document.createElement("div");el.className="card";
  el.innerHTML='<div class="card-name">'+pag+'</div><div class="card-val">'+t.at.toLocaleString("pt-BR")+'</div><div class="card-trend" style="color:'+t.tc+'">'+t.tend+'</div><div class="card-delta">'+(t.vn>=0?"+":"")+t.vn+' desde inicio</div><div class="rank-bar-bg"><div class="rank-bar" style="width:'+pct+'%"></div></div>';
  document.getElementById("cards").appendChild(el);
});
const ro=porAds.filter(p=>(ultima[p]?.ads||0)>0);
const totalRo=ro.reduce((s,p)=>s+(ultima[p]?.ads||0),0);
porAds.forEach(pag=>{
  const t=tendInfo(pag);
  const did=primeira[pag]?fdFull(primeira[pag]):"—";
  const d3=datas.slice(-3).map(d=>pags[pag]?.[d]?med(pags[pag][d]):null).filter(v=>v!==null);
  const s3=d3.length>=2?(d3[d3.length-1]>d3[0]?"▲ sub":d3[d3.length-1]<d3[0]?"▼ cai":"= est"):"—";
  const pct=Math.round((t.at/maxAds)*100);
  const idxCor=porAds.indexOf(pag);
  const tr=document.createElement("tr");
  tr.innerHTML='<td class="num" style="color:var(--muted)">'+(porAds.indexOf(pag)+1)+'</td><td style="font-weight:600;color:#fff">'+pag+'</td><td style="color:var(--muted)">'+did+'</td><td class="num">'+t.ini+'</td><td class="num" style="color:#fff">'+t.at+'</td><td class="num" style="color:'+(t.vn>=0?"#3ecfcf":"#ff6b6b")+'">'+(t.vn>=0?"+":"")+t.vn+'</td><td><span class="badge '+t.bc+'">'+t.tend+'</span></td><td><div style="display:flex;align-items:center;gap:6px"><div style="width:60px;height:4px;background:var(--border);border-radius:2px"><div style="width:'+pct+'%;height:4px;background:'+COR[idxCor%COR.length]+';border-radius:2px"></div></div><span style="font-size:9px;color:var(--muted);font-family:IBM Plex Mono,monospace">'+pct+'%</span></div></td><td style="color:var(--muted)">'+s3+'</td>';
  document.getElementById("tbody").appendChild(tr);
});
const legContainer=document.getElementById("legend-rosca");
ro.forEach((p,i)=>{
  const t=tendInfo(p);
  const pct=totalRo>0?Math.round((t.at/totalRo)*100):0;
  const item=document.createElement("div");item.className="leg-item";
  item.innerHTML='<div class="leg-dot" style="background:'+COR[i%COR.length]+'"></div><span class="leg-label" title="'+p+'">'+p+'</span><span class="leg-val">'+t.at.toLocaleString("pt-BR")+'</span><span style="font-size:9px;color:var(--muted);font-family:IBM Plex Mono,monospace;margin-left:4px">'+pct+'%</span>';
  legContainer.appendChild(item);
});
new Chart(document.getElementById("cRosca"),{type:"doughnut",data:{labels:ro,datasets:[{data:ro.map(p=>ultima[p]?.ads||0),backgroundColor:ro.map((_,i)=>COR[i%COR.length]),borderWidth:2,borderColor:"#0f0f1e"}]},options:{responsive:true,maintainAspectRatio:false,cutout:"60%",plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>" "+ctx.label+": "+ctx.parsed.toLocaleString("pt-BR")+" ads"}}}}});
const LP_intra=porAds.slice(0,10);
new Chart(document.getElementById("cIntra"),{type:"line",data:{labels:["03h","12h","22h"],datasets:LP_intra.map((p,i)=>({label:p,data:HR.map(s=>pags[p]?.[ult]?.[s]??null),borderColor:COR[i%COR.length],backgroundColor:"transparent",borderWidth:2,pointRadius:4,pointHoverRadius:6,tension:.3,spanGaps:true}))},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"bottom",labels:{color:"#d0d0e8",font:{size:10},padding:8,boxWidth:8,usePointStyle:true}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#8888aa",font:{size:10}},grid:{color:"#1e1e38"}},y:{ticks:{color:"#8888aa",font:{size:10}},grid:{color:"#1e1e38"},beginAtZero:false}}}});
const LP_hist=porAds.slice(0,8);
new Chart(document.getElementById("cHist"),{type:"line",data:{labels:datas.map(fd),datasets:LP_hist.map((p,i)=>{const didK=primeira[p]||null;return{label:p,data:datas.map(dk=>pags[p]?.[dk]?med(pags[p][dk]):null),borderColor:COR[i%COR.length],backgroundColor:"transparent",borderWidth:2,pointBackgroundColor:datas.map(dk=>dk===didK?"#ff4444":COR[i%COR.length]),pointRadius:datas.map(dk=>dk===didK?5:2),pointHoverRadius:6,tension:.35,spanGaps:true};})},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"bottom",labels:{color:"#d0d0e8",font:{size:10},padding:8,boxWidth:8,usePointStyle:true}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#8888aa",font:{size:10},maxTicksLimit:14,maxRotation:0},grid:{color:"#1e1e38"}},y:{ticks:{color:"#8888aa",font:{size:10}},grid:{color:"#1e1e38"},beginAtZero:false}}}});
<\/script>
</body>
</html>`.replace("__DADOS_PLACEHOLDER__", dados));
  } catch (err) {
    res.status(500).send("Erro: " + err.message);
  }
});

// ─── Scheduler (internal cron — replaces Make.com) ──────────────────────────────
// Three runs per day in Brazil time: 03h, 12h, 22h.

const TZ = "America/Sao_Paulo";
cron.schedule("0 3 * * *", () => runAllScrapes("cron-03h"), { timezone: TZ });
cron.schedule("0 12 * * *", () => runAllScrapes("cron-12h"), { timezone: TZ });
cron.schedule("0 22 * * *", () => runAllScrapes("cron-22h"), { timezone: TZ });
console.log(`[CRON] scheduled 03h/12h/22h (${TZ})`);

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;

initDb().then(() => {
  app.listen(PORT, () => {
    console.log(`[SERVER] Running on port ${PORT}`);
  });
});