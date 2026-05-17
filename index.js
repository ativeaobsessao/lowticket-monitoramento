import express from "express";
import cors from "cors";
import { chromium } from "playwright";
import { execSync } from "child_process";
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

// ─── Scraper (with retry) ─────────────────────────────────────────────────────

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

      const page = await context.newPage();
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      await page.waitForTimeout(15000);

      const content = await page.content();

      // Strategy 1: HTML regex
      const htmlMatch = content.match(/([\d.,]+)\s*(resultados|results)/i);
      if (htmlMatch) {
        const parsed = parseInt(htmlMatch[1].replace(/[,.]/g, ""), 10);
        if (!isNaN(parsed)) {
          console.log(`[SCRAPE] attempt=${attempt} count=${parsed} via HTML regex`);
          return parsed;
        }
      }

      // Strategy 2: visible element
      for (const kw of ["resultados", "results"]) {
        try {
          const el = page.locator(`text=/${kw}/i`).first();
          await el.waitFor({ timeout: 3000 });
          const texto = await el.innerText();
          const match = texto.replace(/[,.]/g, "").match(/\d+/);
          if (match) {
            const parsed = parseInt(match[0], 10);
            console.log(`[SCRAPE] attempt=${attempt} count=${parsed} via locator`);
            return parsed;
          }
        } catch {
          continue;
        }
      }

      // Strategy 3: body text
      const bodyText = (await page.textContent("body")) ?? "";
      const textMatch = bodyText.match(/([\d.,]+)\s*(resultados|results)/i);
      if (textMatch) {
        const parsed = parseInt(textMatch[1].replace(/[,.]/g, ""), 10);
        if (!isNaN(parsed)) {
          console.log(`[SCRAPE] attempt=${attempt} count=${parsed} via body text`);
          return parsed;
        }
      }

      console.warn(`[SCRAPE] attempt=${attempt} — could not find count, retrying...`);
    } catch (err) {
      console.error(`[SCRAPE] attempt=${attempt} error: ${err.message}`);
    } finally {
      await browser.close();
    }

    // wait before retry
    if (attempt < retries) await new Promise((r) => setTimeout(r, 5000));
  }

  console.error(`[SCRAPE] all ${retries} attempts failed, returning 0`);
  return 0;
}

// ─── Routes ───────────────────────────────────────────────────────────────────

// Health check
app.get("/api/healthz", (_req, res) => {
  res.json({ status: "ok", ts: new Date().toISOString() });
});

// Register page
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

// Scrape and return count (used by Make.com)
app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
  const row = rows[0];

  if (!row) {
    return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
  }

  try {
    const count = await scrapeAdCount(row.url);

    // Respond immediately with the count (Make.com needs just this)
    res.type("text/plain").send(String(count));

    // Dedup: skip if same slug was inserted in the last 60s
    const { rows: recent } = await query(
      `SELECT id FROM scrape_history
       WHERE slug = $1 AND collected_at >= NOW() - INTERVAL '60 seconds'
       LIMIT 1`,
      [slug]
    );

    if (recent.length === 0) {
      await query(
        "INSERT INTO scrape_history (slug, ads_count) VALUES ($1, $2)",
        [slug, count]
      );
      console.log(`[HISTORY] slug=${slug} count=${count} saved`);
    } else {
      console.log(`[HISTORY] slug=${slug} skipped duplicate`);
    }
  } catch (err) {
    console.error(`[COLETAR] error slug=${slug}: ${err.message}`);
    res.type("text/plain").send("0");
  }
});

// Full history for a page
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

// Summary: min/max/avg/trend for a page
app.get("/api/resumo/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT ads_count, collected_at
     FROM scrape_history
     WHERE slug = $1
     ORDER BY collected_at ASC`,
    [slug]
  );

  if (rows.length === 0) {
    return res.json({ slug, message: "No data yet." });
  }

  const counts = rows.map((r) => r.ads_count);
  const min = Math.min(...counts);
  const max = Math.max(...counts);
  const avg = Math.round(counts.reduce((a, b) => a + b, 0) / counts.length);
  const first = counts[0];
  const last = counts[counts.length - 1];
  const trend = last > first ? "crescendo" : last < first ? "caindo" : "estável";

  res.json({ slug, total_coletas: rows.length, min, max, avg, trend, first, last });
});

// All registered pages with their latest count
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

// All registered pages (simple list)
app.get("/api/paginas", async (_req, res) => {
  const { rows } = await query("SELECT slug, nome, url FROM pages");
  res.json(rows);
});

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;

initDb().then(() => {
  app.listen(PORT, () => {
    console.log(`[SERVER] Running on port ${PORT}`);
  });
});
