import express from "express";
import cors from "cors";
import { chromium } from "playwright";
import { execSync } from "child_process";
import { createHash, randomUUID } from "node:crypto";
import pg from "pg";
import {
  getBusinessSlot,
  getBusinessSlotForDomain,
  getBusinessSlotForKeyword,
  processWithRetries,
  withTimeout,
} from "./scrape-orchestration.js";
import {
  buildKeywordSearchUrl,
  normalizeKeywordIdentity,
  normalizeMonitoringType,
  parseBatchLine,
} from "./monitoring-input.js";

const { Pool } = pg;
const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));
app.use(express.urlencoded({ extended: true }));

// ─── Database ────────────────────────────────────────────────────────────────

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL environment variable is required.");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 10_000,
  query_timeout: 15_000,
  statement_timeout: 15_000,
});

// O Neon (serverless) derruba conexões ociosas de tempos em tempos (autosuspend/reciclagem
// do pooler). Sem este handler, um erro num cliente idle do pool vira uma exceção não
// tratada que derruba o processo inteiro. Aqui só logamos e seguimos — o pg cria uma
// conexão nova automaticamente na próxima query.
pool.on("error", (err) => {
  console.error("[PG POOL] erro em conexão ociosa (ignorado, processo continua):", err.message);
});

// Rede de segurança geral: qualquer erro assíncrono que escape dos try/catch normais
// (ex: uma Promise rejeitada sem .catch) também não deve derrubar o serviço inteiro.
process.on("unhandledRejection", (err) => {
  console.error("[UNHANDLED REJECTION] (ignorado, processo continua):", err);
});
process.on("uncaughtException", (err) => {
  console.error("[UNCAUGHT EXCEPTION] (ignorado, processo continua):", err);
});

async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

// Garante que toda URL salva tenha esquema (http/https). Sem isso, o Playwright falha
// ao tentar navegar ("Cannot navigate to invalid URL") e, em links renderizados no
// front-end (<a href="...">), o navegador interpreta como caminho relativo e abre
// dentro do próprio domínio do app (ex: "Cannot GET /dominio.com").
function normalizeUrl(url) {
  if (!url) return url;
  const trimmed = url.trim();
  if (!trimmed) return trimmed;
  return /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
}

// Valida se a URL é da Biblioteca de Anúncios da Meta (host facebook.com + caminho /ads/library).
function isMetaLibraryUrl(url) {
  try {
    const u = new URL(url);
    return /(^|\.)facebook\.com$/i.test(u.hostname) && u.pathname.startsWith("/ads/library");
  } catch {
    return false;
  }
}

// Devolve uma URL válida da Biblioteca a partir do que foi enviado, ou null (rejeitar).
// - Já é URL da Biblioteca: devolve como está (limpa aspas/espaços e garante https://).
// - tipo "dominio" e veio só o domínio (ou URL do site): monta a busca por palavra-chave com country=ALL.
// - Qualquer outro caso: null.
function resolveMetaUrl(raw, tipo) {
  if (!raw) return null;
  const limpo = String(raw).trim().replace(/^["'\s]+|["'\s]+$/g, "");
  if (!limpo) return null;
  const comEsquema = normalizeUrl(limpo);
  if (isMetaLibraryUrl(comEsquema)) return comEsquema;
  if (tipo === "keyword") return buildKeywordSearchUrl(limpo);
  if (tipo === "dominio") {
    const dominio = limpo
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "")
      .split(/[\/?#]/)[0]
      .trim()
      .toLowerCase();
    if (/^[a-z0-9-]+(\.[a-z0-9-]+)+$/.test(dominio)) {
      return `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&q=${encodeURIComponent(dominio)}&search_type=keyword_unordered`;
    }
  }
  return null;
}

async function initDb() {
  await query(`
    CREATE TABLE IF NOT EXISTS pages (
      slug          TEXT PRIMARY KEY,
      nome          TEXT NOT NULL,
      url           TEXT NOT NULL,
      tipo          TEXT NOT NULL DEFAULT 'pagina',
      keyword_key   TEXT,
      instagram_url TEXT,
      geo           TEXT,
      nicho         TEXT,
      created_at    TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS scrape_history (
      id           SERIAL PRIMARY KEY,
      slug         TEXT NOT NULL,
      ads_count    INTEGER NOT NULL,
      slot         SMALLINT,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_history_collected_at
    ON scrape_history(collected_at DESC)
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS scrape_latest (
      slug         TEXT PRIMARY KEY,
      ads_count    INTEGER NOT NULL,
      collected_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await query(`ALTER TABLE scrape_history ADD COLUMN IF NOT EXISTS business_date DATE`);
  await query(`
    UPDATE scrape_history
    SET business_date = (
      (collected_at - INTERVAL '3 hours')
      - CASE
          WHEN EXTRACT(HOUR FROM collected_at - INTERVAL '3 hours') < 3
          THEN INTERVAL '1 day'
          ELSE INTERVAL '0 days'
        END
    )::date
    WHERE slot IS NOT NULL AND business_date IS NULL
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_history_slot_business_date
    ON scrape_history(slug, slot, business_date)
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS scrape_attempts (
      id           TEXT PRIMARY KEY,
      slug         TEXT NOT NULL,
      source       TEXT NOT NULL,
      slot         SMALLINT,
      business_date DATE,
      status       TEXT NOT NULL,
      ads_count    INTEGER,
      error        TEXT,
      started_at   TIMESTAMP NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMP,
      lease_owner  TEXT NOT NULL
    )
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_scrape_attempts_slug_started
    ON scrape_attempts(slug, started_at DESC)
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS scrape_worker_lease (
      lease_key    TEXT PRIMARY KEY,
      owner_id     TEXT,
      expires_at   TIMESTAMP NOT NULL,
      blocked_until TIMESTAMP
    )
  `);
  await query(`
    INSERT INTO scrape_worker_lease (lease_key, expires_at)
    VALUES ('scraper', NOW() - INTERVAL '1 day')
    ON CONFLICT (lease_key) DO NOTHING
  `);

  // Migrações: garante colunas novas em banco antigo (seguro rodar sempre)
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS tipo TEXT NOT NULL DEFAULT 'pagina'`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS keyword_key TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS inicial_count INTEGER`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS instagram_url TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS geo TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS nicho TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS funil TEXT`);
  await query(`
    DO $$
    DECLARE
      c_name text;
    BEGIN
      FOR c_name IN
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_attribute a
          ON a.attrelid = c.conrelid
         AND a.attname = 'tipo'
        WHERE c.conrelid = 'pages'::regclass
          AND c.contype = 'c'
          AND a.attnum = ANY(c.conkey)
      LOOP
        EXECUTE format('ALTER TABLE pages DROP CONSTRAINT %I', c_name);
      END LOOP;
    END $$;
  `);
  await query(`
    ALTER TABLE pages ADD CONSTRAINT pages_tipo_check
    CHECK (tipo IN ('pagina', 'dominio', 'keyword'))
  `);
  await query(`
    WITH identities AS (
      SELECT slug,
             COALESCE(
               NULLIF(keyword_key, ''),
               lower(regexp_replace(btrim(nome), '[[:space:]]+', ' ', 'g'))
             ) AS identity,
             row_number() OVER (
               PARTITION BY COALESCE(
                 NULLIF(keyword_key, ''),
                 lower(regexp_replace(btrim(nome), '[[:space:]]+', ' ', 'g'))
               )
               ORDER BY slug
             ) AS identity_rank
      FROM pages
      WHERE tipo = 'keyword'
    )
    UPDATE pages p
    SET keyword_key = CASE
      WHEN identities.identity_rank = 1 THEN identities.identity
      ELSE identities.identity || '::legacy::' || identities.slug
    END
    FROM identities
    WHERE p.slug = identities.slug
      AND p.keyword_key IS DISTINCT FROM CASE
        WHEN identities.identity_rank = 1 THEN identities.identity
        ELSE identities.identity || '::legacy::' || identities.slug
      END
  `);
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pages_keyword_key_unique
    ON pages(keyword_key) WHERE tipo = 'keyword'
  `);

  // FIX (fila travada / starvation do cron): coluna que registra a última tentativa
  // de coleta de cada página, sucesso OU falha. Sem isso, uma página com falha
  // persistente nunca sai da lista de "pendentes" do slot (porque falha não grava em
  // scrape_history) e, sem ORDER BY, a query do /api/cron/tick devolvia sempre as
  // mesmas 5 páginas quebradas em todo tick — monopolizando o LIMIT 5 e impedindo
  // qualquer outra página do slot de ser tentada. Ver uso em processBatch() e na
  // query de /api/cron/tick.
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_status TEXT`);
  await query(`ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_error TEXT`);

  await query(`
    CREATE TABLE IF NOT EXISTS funnel_nodes (
      id         SERIAL PRIMARY KEY,
      slug       TEXT NOT NULL REFERENCES pages(slug) ON DELETE CASCADE,
      tipo       TEXT NOT NULL CHECK (tipo IN ('advertorial','tsl','vsl','quiz','whatsapp','checkout')),
      rotulo     TEXT NOT NULL,
      url        TEXT NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS funnel_edges (
      id           SERIAL PRIMARY KEY,
      from_node_id INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
      to_node_id   INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
      created_at   TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await query(`CREATE INDEX IF NOT EXISTS idx_funnel_nodes_slug ON funnel_nodes(slug)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_funnel_edges_from ON funnel_edges(from_node_id)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_funnel_edges_to ON funnel_edges(to_node_id)`);

  // Migração: amplia o CHECK de funnel_nodes.tipo para incluir 'ads' e 'presell'.
  // Em vez de depender de adivinhar o nome do constraint (que o Postgres gera
  // automaticamente), o bloco DO $$ abaixo PROCURA dinamicamente qual é o CHECK
  // constraint da coluna `tipo` e o remove, seja qual for o nome — depois recria
  // com um nome fixo e conhecido (`funnel_nodes_tipo_check`). Isso é 100% seguro
  // de rodar em todo restart: se não encontrar nenhum constraint, não faz nada;
  // se encontrar, remove e recria com a lista ampliada.
  await query(`
    DO $$
    DECLARE
      c_name text;
    BEGIN
      SELECT conname INTO c_name
      FROM pg_constraint
      WHERE conrelid = 'funnel_nodes'::regclass
        AND contype = 'c'
        AND pg_get_constraintdef(oid) ILIKE '%tipo%';
      IF c_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE funnel_nodes DROP CONSTRAINT %I', c_name);
      END IF;
    END $$;
  `);
  await query(`
    ALTER TABLE funnel_nodes ADD CONSTRAINT funnel_nodes_tipo_check
    CHECK (tipo IN ('ads','advertorial','presell','tsl','vsl','quiz','whatsapp','checkout'))
  `);

  console.log("[DB] Tabelas ready");
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

async function saveMonitoringRecord({
  nome,
  url,
  tipo,
  instagram_url = null,
  geo = null,
  nicho = null,
  funil = null,
}) {
  const baseSlug = toSlug(nome);
  if (!baseSlug) throw new Error("Could not generate a valid slug.");
  const keywordKey = tipo === "keyword" ? normalizeKeywordIdentity(nome) : null;
  if (tipo === "keyword" && !keywordKey) throw new Error("A palavra-chave não pode estar vazia.");

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (keywordKey) {
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`keyword:${keywordKey}`]);
    }

    let existing;
    if (keywordKey) {
      const result = await client.query(
        "SELECT slug FROM pages WHERE tipo = 'keyword' AND keyword_key = $1 FOR UPDATE",
        [keywordKey],
      );
      existing = result.rows[0];
    } else {
      const result = await client.query(
        "SELECT slug FROM pages WHERE slug = $1 AND tipo = $2 FOR UPDATE",
        [baseSlug, tipo],
      );
      existing = result.rows[0];
      if (!existing) {
        const typedResult = await client.query(
          "SELECT slug FROM pages WHERE slug = $1 AND tipo = $2 FOR UPDATE",
          [`${tipo}-${baseSlug}`, tipo],
        );
        existing = typedResult.rows[0];
      }
    }

    let slug = existing?.slug;
    if (slug) {
      await client.query(
        `UPDATE pages
         SET nome = $2, url = $3, instagram_url = COALESCE($4, instagram_url),
             geo = COALESCE($5, geo), nicho = COALESCE($6, nicho),
             funil = COALESCE($7, funil), keyword_key = $8
         WHERE slug = $1`,
        [slug, nome, url, instagram_url, geo, nicho, funil, keywordKey],
      );
    } else {
      for (let suffix = 0; suffix < 1000; suffix++) {
        const candidate = suffix === 0
          ? baseSlug
          : `${tipo}-${baseSlug}${suffix === 1 ? "" : `-${suffix}`}`;
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`slug:${candidate}`]);
        const occupied = await client.query(
          "SELECT slug, tipo, keyword_key FROM pages WHERE slug = $1 FOR UPDATE",
          [candidate],
        );
        if (occupied.rowCount) {
          const row = occupied.rows[0];
          if (row.tipo === tipo && (tipo !== "keyword" || row.keyword_key === keywordKey)) {
            slug = row.slug;
            await client.query(
              `UPDATE pages
               SET nome = $2, url = $3, instagram_url = COALESCE($4, instagram_url),
                   geo = COALESCE($5, geo), nicho = COALESCE($6, nicho),
                   funil = COALESCE($7, funil), keyword_key = $8
               WHERE slug = $1`,
              [slug, nome, url, instagram_url, geo, nicho, funil, keywordKey],
            );
            break;
          }
          continue;
        }
        slug = candidate;
        await client.query(
          `INSERT INTO pages (slug, nome, url, tipo, instagram_url, geo, nicho, funil, keyword_key)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
          [slug, nome, url, tipo, instagram_url, geo, nicho, funil, keywordKey],
        );
        break;
      }
      if (!slug) throw new Error("Could not allocate a unique monitoring identifier.");
    }

    await client.query("COMMIT");
    return { slug, keyword_key: keywordKey };
  } catch (err) {
    await client.query("ROLLBACK").catch((rollbackError) => {
      console.error(`[SALVAR] rollback falhou: ${rollbackError.message}`);
    });
    throw err;
  } finally {
    client.release();
  }
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

// ─── Scraper ─────────────────────────────────────────────────────────────────

function getBrowserLaunchArgs() {
  return [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process",
    "--window-size=1366,768",
  ];
}

async function createStealthContext(browser) {
  const context = await browser.newContext({
    locale: "pt-BR",
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    viewport: { width: 1366, height: 768 },
    extraHTTPHeaders: {
      "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
      "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
    },
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, "languages", { get: () => ["pt-BR", "pt", "en-US", "en"] });
    window.chrome = { runtime: {} };
  });

  // Injeta cookies de sessão do Facebook se configurados
  const fbCookiesRaw = process.env.FB_COOKIES;
  if (fbCookiesRaw) {
    try {
      const raw = JSON.parse(fbCookiesRaw);
      // Suporta formato Cookie-Editor (array de objetos) e formato Netscape simplificado
      const cookies = raw.map((c) => ({
        name: c.name,
        value: c.value,
        domain: c.domain || ".facebook.com",
        path: c.path || "/",
        httpOnly: c.httpOnly ?? false,
        secure: c.secure ?? true,
        sameSite: c.sameSite === "no_restriction" ? "None"
                : c.sameSite === "lax" ? "Lax"
                : c.sameSite === "strict" ? "Strict"
                : "None",
        ...(c.expirationDate ? { expires: Math.floor(c.expirationDate) } : {}),
      }));
      await context.addCookies(cookies);
      console.log(`[AUTH] ${cookies.length} cookies do Facebook injetados no contexto.`);
    } catch (err) {
      console.warn(`[AUTH] Falha ao parsear FB_COOKIES: ${err.message} — continuando sem autenticação.`);
    }
  } else {
    console.warn("[AUTH] FB_COOKIES não definido — Playwright rodará sem sessão (pode falhar na Meta).");
  }

  return context;
}

async function extractCount(page) {
  return await page.evaluate(() => {
    const normalizeText = (text) => text.normalize("NFKC").replace(/\p{Cf}/gu, "").replace(/\s+/g, " ");
    const bodyText = normalizeText(document.body ? document.body.innerText : "");
    let m = bodyText.match(/(?:~\s*)?([\d.,]+)\s*(?:resultados?|results?)/i);
    if (m) {
      const n = parseInt(m[1].replace(/[,.]/g, ""), 10);
      if (!Number.isNaN(n)) return n;
    }

    const elements = Array.from(document.querySelectorAll("div, span, h1, h2, h3, p, strong, b"));
    for (const el of elements) {
      const txt = normalizeText(el.innerText || "");
      if (txt.length < 60 && /(?:~\s*)?[\d.,]+\s*(?:resultados?|results?)/i.test(txt)) {
        const match = txt.match(/(?:~\s*)?([\d.,]+)\s*(?:resultados?|results?)/i);
        if (match) {
          const n = parseInt(match[1].replace(/[,.]/g, ""), 10);
          if (!Number.isNaN(n)) return n;
        }
      }
    }

    const docText = normalizeText(document.documentElement ? document.documentElement.innerText : "");
    m = docText.match(/(?:~\s*)?([\d.,]+)\s*(?:resultados?|results?)/i);
    if (m) {
      const n = parseInt(m[1].replace(/[,.]/g, ""), 10);
      if (!Number.isNaN(n)) return n;
    }

    return null;
  });
}

async function waitForCounter(page, maxWaitMs = 18000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    const count = await extractCount(page);
    if (count !== null) return count;
    await page.evaluate(() => window.scrollBy(0, 100)).catch(() => {});
    await page.waitForTimeout(1000);
  }
  return null;
}

function cleanMetaUrl(rawUrl) {
  try {
    const u = new URL(rawUrl);
    if (u.hostname.includes("facebook.com") && u.searchParams.has("view_all_page_id") && u.searchParams.has("id")) {
      u.searchParams.delete("id");
      return u.toString();
    }
  } catch {}
  return rawUrl;
}

// Preenche o objeto diag (quando recebido) com o status classificado da coleta.
function setDiag(diag, status, detalhe) {
  if (!diag) return;
  diag.status = status;
  diag.detalhe = detalhe ? String(detalhe).slice(0, 500) : null;
}

// Lê o estado atual da página: frase de vazio, título da Biblioteca, sinais de bloqueio e texto para diagnóstico.
async function detectPageState(page) {
  return await page.evaluate(() => {
    const t = (document.body ? document.body.innerText : "").replace(/\s+/g, " ").trim();
    return {
      vazio: /Nenhum an[uú]ncio corresponde|No ads match/i.test(t),
      biblioteca: /Biblioteca de An[uú]ncios|Ad Library/i.test(t),
      bloqueio: /captcha|checkpoint|confirme que voc[eê] [ée] humano|confirm (that )?you.?re (a )?human|security check|verifica[cç][aã]o de seguran[cç]a/i.test(t) || /\/login|checkpoint/i.test(location.pathname),
      texto: t.slice(0, 1500),
    };
  });
}

// Espera o contador OU a tela de vazio. O vazio só vale se a frase + título da Biblioteca
// se mantiverem por 3s seguidos SEM nenhum contador aparecer (evita pegar estado transitório).
async function waitForCounterOrEmpty(page, maxWaitMs = 18000) {
  const start = Date.now();
  let vazioDesde = null;
  while (Date.now() - start < maxWaitMs) {
    const count = await extractCount(page);
    if (count !== null) return { count, vazio: false };
    const st = await detectPageState(page);
    if (st && st.vazio && st.biblioteca) {
      if (vazioDesde === null) vazioDesde = Date.now();
      else if (Date.now() - vazioDesde >= 3000) return { count: null, vazio: true };
    } else {
      vazioDesde = null;
    }
    await page.evaluate(() => window.scrollBy(0, 100)).catch(() => {});
    await page.waitForTimeout(1000);
  }
  return { count: null, vazio: false };
}

async function scrapeWithContext(context, url, diag = null) {
  const page = await context.newPage();
  try {
    // Bloqueia APENAS imagens e mídias pesadas — NUNCA bloqueia CSS nem scripts
    await page.route("**/*", (route) => {
      const type = route.request().resourceType();
      if (["image", "media"].includes(type)) {
        route.abort();
      } else {
        route.continue();
      }
    });

    // Navegação com classificação: erro de rede/timeout vira falha_timeout e continua propagando o erro.
    const irPara = async (alvo, timeout) => {
      try {
        await page.goto(alvo, { waitUntil: "domcontentloaded", timeout });
      } catch (err) {
        setDiag(diag, "falha_timeout", err.message);
        throw err;
      }
    };

    // Resultado positivo: contador encontrado (ok) ou tela de vazio confirmada (ok_zero).
    const concluir = (r) => {
      if (r.count !== null) { setDiag(diag, "ok", null); return true; }
      if (r.vazio) { setDiag(diag, "ok_zero", "Meta: Nenhum anúncio corresponde aos critérios de pesquisa"); return true; }
      return false;
    };

    const targetUrl = cleanMetaUrl(url);
    await irPara(targetUrl, 35000);

    let r = await waitForCounterOrEmpty(page, 18000);
    if (concluir(r)) return r.count ?? 0;

    // Se o contador não apareceu e a URL foi limpa, tenta a original também
    if (targetUrl !== url) {
      console.log(`[SCRAPE] tentando URL alternativa: ${url}`);
      await irPara(url, 35000);
      r = await waitForCounterOrEmpty(page, 12000);
      if (concluir(r)) return r.count ?? 0;
    }

    // Se ainda não encontrou, checa se tem redirect para outra URL que NÃO SEJA _fb_noscript
    const html = await page.content();
    const m = html.match(/<meta[^>]+http-equiv=["']refresh["'][^>]*content=["'][^"']*url\s*=\s*([^"'>]+)["']/i);
    if (m && !m[1].includes("_fb_noscript")) {
      const target = m[1].trim().replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#39;/g, "'");
      const nextUrl = new URL(target, page.url()).toString();
      console.log(`[SCRAPE] seguindo redirect válido: ${nextUrl}`);
      await irPara(nextUrl, 30000);
      r = await waitForCounterOrEmpty(page, 12000);
      if (concluir(r)) return r.count ?? 0;
    }

    // Falha: classifica em bloqueio ou parse e registra 1500 caracteres do que o robô viu.
    const estado = await detectPageState(page);
    const pageTitle = await page.title().catch(() => "");
    const texto = estado ? estado.texto : "";
    console.warn(`[SCRAPE-DIAG] Falha na extração. Title: "${pageTitle}" | Conteúdo visto: "${texto}"`);
    setDiag(diag, estado && estado.bloqueio ? "falha_bloqueio" : "falha_parse", `Título: ${pageTitle} | ${texto}`);

    return null;
  } finally {
    await page.close();
  }
}

// Captura inicial no momento do cadastro (individual)
async function captureInicial(slug, url) {
  let lease;
  try {
    lease = await acquireScrapeLease("cadastro");
    if (!lease) {
      console.warn(`[DESCOBERTA] slug=${slug} captura inicial adiada: outra coleta está em andamento`);
      return null;
    }
    const [result] = await processBatch(
      [{ slug, nome: slug, url }],
      null,
      null,
      null,
      { source: "cadastro", lease, persistInitial: true },
    );
    if (result.count === null) console.warn(`[DESCOBERTA] slug=${slug} falhou, valor inicial não capturado`);
    else console.log(`[DESCOBERTA] slug=${slug} inicial=${result.count} capturado no cadastro`);
    return result.count;
  } catch (err) {
    console.error(`[DESCOBERTA] falha ao capturar inicial de slug=${slug}: ${err.message}`);
    return null;
  } finally {
    if (lease) await lease.release();
  }
}

// Processa lote em background (fire-and-forget)
async function runLote(itens) {
  let lease;
  try {
    lease = await acquireScrapeLease("cadastro_lote");
  } catch (err) {
    console.error(`[LOTE] não foi possível reservar worker: ${err.message}`);
    loteStatus.erros.push("Não foi possível reservar o worker de coleta.");
    loteStatus.emAndamento = false;
    return;
  }
  if (!lease) {
    console.warn("[LOTE] abortado — já existe uma coleta em andamento (cron ou outro lote)");
    loteStatus.erros.push("Abortado: já havia uma coleta (cron ou outro lote) em andamento. Tente de novo em alguns minutos.");
    loteStatus.emAndamento = false;
    return;
  }
  loteStatus = {
    emAndamento: true,
    total: itens.length,
    concluidos: 0,
    atual: null,
    erros: [],
    iniciadoEm: new Date().toISOString(),
    finalizadoEm: null,
  };
  console.log(`[LOTE] ===== iniciado — ${itens.length} itens =====`);

  try {
    const pages = [];
    for (const item of itens) {
      if (!toSlug(item.nome)) {
        loteStatus.erros.push(`"${item.nome}" — nome inválido, ignorado`);
        loteStatus.concluidos++;
        continue;
      }
      try {
        const record = await saveMonitoringRecord({
          nome: item.nome,
          url: item.url,
          tipo: item.tipo,
          instagram_url: item.instagram_url || null,
        });
        pages.push({ slug: record.slug, nome: item.nome, url: item.url });
      } catch (err) {
        console.error(`[LOTE] erro no item "${item.nome}": ${err.message}`);
        loteStatus.erros.push(`"${item.nome}" — erro: ${err.message}`);
        loteStatus.concluidos++;
      }
    }
    const reportedSlugs = new Set();
    const results = await processBatch(
      pages,
      null,
      (result) => {
        if (!reportedSlugs.has(result.slug)) {
          reportedSlugs.add(result.slug);
          loteStatus.concluidos++;
        }
      },
      (page) => { loteStatus.atual = page.nome; },
      { source: "cadastro_lote", lease, persistInitial: true },
    );
    for (const result of results) {
      if (result.count === null) {
        loteStatus.erros.push(`"${result.nome}" — ${result.falha || "falha na captura inicial"}`);
      }
      console.log(`[LOTE] slug=${result.slug} cadastro e captura finalizados (${loteStatus.concluidos}/${itens.length})`);
    }
  } catch (err) {
    console.error(`[LOTE] erro fatal: ${err.message}`);
    loteStatus.erros.push(`Erro fatal: ${err.message}`);
  } finally {
    await lease.release();
    loteStatus.emAndamento = false;
    loteStatus.atual = null;
    loteStatus.finalizadoEm = new Date().toISOString();
    console.log(`[LOTE] ===== finalizado — ${loteStatus.concluidos}/${loteStatus.total} processados, ${loteStatus.erros.length} erros =====`);
  }
}

async function mirrorToSheet(rows) {
  const url = process.env.SHEET_WEBHOOK_URL;
  if (!url) return;
  try {
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ collected_at: new Date().toISOString(), rows }),
    });
    console.log("[SHEET] mirrored to backup webhook");
  } catch (err) {
    console.error(`[SHEET] mirror failed: ${err.message}`);
  }
}

let isRunning = false;
const SCRAPE_LEASE_KEY = "scraper";
const SCRAPE_LEASE_MS = 2 * 60 * 1000;
const SCRAPE_HEARTBEAT_MS = 30 * 1000;
const PAGE_TIMEOUT_MS = 120 * 1000;
const PAGE_ATTEMPT_BUDGET_MS = 6 * 60 * 1000;
const BATCH_TIMEOUT_MS = 45 * 60 * 1000;
const BROWSER_START_TIMEOUT_MS = 30 * 1000;
const BROWSER_CONNECT_TIMEOUT_MS = 15 * 1000;
const PAGE_PAUSE_MS = 1500;
const BROWSER_CLOSE_TIMEOUT_MS = 5000;
let leaseHeartbeat = null;

let loteStatus = {
  emAndamento: false,
  total: 0,
  concluidos: 0,
  atual: null,
  erros: [],
  iniciadoEm: null,
  finalizadoEm: null,
};

let manualCheckStatus = {
  runId: null,
  requestId: null,
  status: "idle",
  total: 0,
  concluidos: 0,
  sucesso: 0,
  falha: 0,
  atual: null,
  resultados: [],
  iniciadoEm: null,
  finalizadoEm: null,
  erro: null,
};
const manualCheckRequestLedger = new Map();
const MANUAL_CHECK_REQUEST_LEDGER_LIMIT = 500;

// Parser de lote — aceita 3 formatos:
//   Nome | URL
//   tipo | Nome | URL
//   Nome | URL | https://instagram.com/...   (Instagram no final — opcional)
//   tipo | Nome | URL | https://instagram.com/...
function parseLoteInput(texto) {
  const linhas = texto.split("\n").map((l) => l.trim()).filter(Boolean);
  const itens = [];
  for (const linha of linhas) {
    const item = parseBatchLine(linha);
    if (!item?.nome || !item.url) continue;
    const urlValida = resolveMetaUrl(item.url, item.tipo);
    if (!urlValida) {
      console.warn(`[LOTE] linha ignorada, URL inválida para "${item.nome}": ${item.url}`);
      continue;
    }
    itens.push({ ...item, url: urlValida });
  }
  return itens;
}

function getCurrentSlot(now = new Date()) {
  return getBusinessSlot(now);
}

async function withScrapeLease(lease, callback) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `SELECT owner_id FROM scrape_worker_lease
       WHERE lease_key = $1 AND expires_at > NOW()
       FOR UPDATE`,
      [SCRAPE_LEASE_KEY],
    );
    if (lease.lost || rows[0]?.owner_id !== lease.ownerId) {
      throw new Error("A reserva do worker expirou ou foi assumida por outra execução.");
    }
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK").catch((rollbackError) => {
      console.error(`[LEASE] rollback falhou: ${rollbackError.message}`);
    });
    throw err;
  } finally {
    client.release();
  }
}

async function acquireScrapeLease(source) {
  if (isRunning) return null;
  isRunning = true;
  const ownerId = randomUUID();
  try {
    const { rows } = await query(
      `INSERT INTO scrape_worker_lease (lease_key, owner_id, expires_at)
       VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 millisecond'))
       ON CONFLICT (lease_key) DO UPDATE
         SET owner_id = EXCLUDED.owner_id,
             expires_at = EXCLUDED.expires_at
         WHERE scrape_worker_lease.expires_at <= NOW()
       RETURNING owner_id`,
      [SCRAPE_LEASE_KEY, ownerId, SCRAPE_LEASE_MS],
    );
    if (rows.length === 0) {
      isRunning = false;
      console.warn(`[LEASE] ${source} não iniciou: worker reservado por outra instância`);
      return null;
    }

    const lease = { ownerId, lost: false, release: null };
    await withScrapeLease(lease, async (client) => {
      const { rows: staleAttempts } = await client.query(
        `UPDATE scrape_attempts
         SET status = 'falha_execucao',
             error = 'A execução anterior perdeu a reserva do worker.',
             completed_at = NOW()
         WHERE status = 'em_andamento' AND lease_owner <> $1
         RETURNING slug, started_at`,
        [ownerId],
      );
      for (const attempt of staleAttempts) {
        await client.query(
          `UPDATE pages SET last_status = 'falha_execucao', last_error = $2
           WHERE slug = $1 AND last_attempt_at <= $3`,
          [attempt.slug, "A execução anterior foi interrompida.", attempt.started_at],
        );
      }
    });

    leaseHeartbeat = setInterval(async () => {
      try {
        const { rowCount } = await query(
          `UPDATE scrape_worker_lease
           SET expires_at = NOW() + ($3 * INTERVAL '1 millisecond')
           WHERE lease_key = $1 AND owner_id = $2`,
          [SCRAPE_LEASE_KEY, ownerId, SCRAPE_LEASE_MS],
        );
        if (rowCount !== 1) {
          lease.lost = true;
          console.error(`[LEASE] ${source} perdeu a reserva do worker`);
        }
      } catch (err) {
        console.error(`[LEASE] heartbeat de ${source} falhou: ${err.message}`);
      }
    }, SCRAPE_HEARTBEAT_MS);
    leaseHeartbeat.unref();

    lease.release = async () => {
      if (leaseHeartbeat) clearInterval(leaseHeartbeat);
      leaseHeartbeat = null;
      try {
        await query(
          `UPDATE scrape_worker_lease
           SET owner_id = NULL, expires_at = NOW() - INTERVAL '1 second'
           WHERE lease_key = $1 AND owner_id = $2`,
          [SCRAPE_LEASE_KEY, ownerId],
        );
      } catch (err) {
        console.error(`[LEASE] não foi possível liberar reserva de ${source}: ${err.message}; ela expirará automaticamente`);
      } finally {
        isRunning = false;
      }
    };
    console.log(`[LEASE] ${source} reservou o worker owner=${ownerId}`);
    return lease;
  } catch (err) {
    try {
      await query(
        `UPDATE scrape_worker_lease
         SET owner_id = NULL, expires_at = NOW() - INTERVAL '1 second'
         WHERE lease_key = $1 AND owner_id = $2`,
        [SCRAPE_LEASE_KEY, ownerId],
      );
    } catch (releaseError) {
      console.error(`[LEASE] não foi possível liberar reserva após erro: ${releaseError.message}`);
    }
    isRunning = false;
    throw err;
  }
}

async function recordAttemptStart(page, source, slotInfo, lease) {
  const attemptId = randomUUID();
  await withScrapeLease(lease, async (client) => {
    await client.query(
      `INSERT INTO scrape_attempts
         (id, slug, source, slot, business_date, status, lease_owner)
       VALUES ($1, $2, $3, $4, $5, 'em_andamento', $6)`,
      [attemptId, page.slug, source, slotInfo?.slot ?? null, slotInfo?.businessDate ?? null, lease.ownerId],
    );
    await client.query(
      `UPDATE pages SET last_attempt_at = NOW(), last_status = 'em_andamento', last_error = NULL
       WHERE slug = $1`,
      [page.slug],
    );
  });
  return attemptId;
}

async function saveSuccessfulCount(client, slug, count, slotInfo) {
  await client.query(
    `INSERT INTO scrape_latest (slug, ads_count, collected_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (slug) DO UPDATE
       SET ads_count = EXCLUDED.ads_count, collected_at = EXCLUDED.collected_at`,
    [slug, count],
  );

  if (!slotInfo) {
    console.log(`[LATEST] slug=${slug} count=${count} (manual; sem novo ponto no histórico automático)`);
    return;
  }

  const { rows: existing } = await client.query(
    `SELECT id FROM scrape_history
     WHERE slug = $1 AND slot = $2 AND business_date = $3
     LIMIT 1`,
    [slug, slotInfo.slot, slotInfo.businessDate],
  );
  if (existing.length === 0) {
    await client.query(
      `INSERT INTO scrape_history (slug, ads_count, slot, business_date)
       VALUES ($1, $2, $3, $4)`,
      [slug, count, slotInfo.slot, slotInfo.businessDate],
    );
    console.log(`[HISTORY] slug=${slug} slot=${slotInfo.slot} date=${slotInfo.businessDate} count=${count} saved`);
  } else {
    console.log(`[HISTORY] slug=${slug} slot=${slotInfo.slot} date=${slotInfo.businessDate} skipped duplicate`);
  }
}

async function completeAttempt(attemptId, page, result, options) {
  const { lease, slotInfo, persistInitial } = options;
  await withScrapeLease(lease, async (client) => {
    if (result.count !== null && !result.dbError) {
      await saveSuccessfulCount(client, page.slug, result.count, slotInfo);
      if (persistInitial) {
        await client.query(
          `UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`,
          [page.slug, result.count],
        );
      }
    }
    const { rowCount } = await client.query(
      `UPDATE scrape_attempts
       SET status = $2, ads_count = $3, error = $4, completed_at = NOW()
       WHERE id = $1 AND status = 'em_andamento' AND lease_owner = $5`,
      [attemptId, result.status, result.count, result.falha ?? null, lease.ownerId],
    );
    if (rowCount !== 1) throw new Error(`Não foi possível finalizar tentativa ${attemptId}.`);
    await client.query(
      `UPDATE pages SET last_attempt_at = NOW(), last_status = $2, last_error = $3 WHERE slug = $1`,
      [page.slug, result.status, result.falha ?? null],
    );
  });
}

async function markAttemptWriteFailure(attemptId, page, error, lease) {
  const message = "A coleta terminou, mas não foi possível persistir o resultado.";
  try {
    await withScrapeLease(lease, async (client) => {
      await client.query(
        `WITH failed AS (
           UPDATE scrape_attempts
           SET status = 'falha_gravacao', error = $2, completed_at = NOW()
           WHERE id = $1 AND status = 'em_andamento' AND lease_owner = $3
           RETURNING slug
         )
         UPDATE pages p SET last_attempt_at = NOW(), last_status = 'falha_gravacao', last_error = $4
         FROM failed f WHERE p.slug = f.slug`,
        [attemptId, `${message} ${error.message}`.slice(0, 500), lease.ownerId, message],
      );
    });
  } catch (recordError) {
    console.error(`[BATCH] slug=${page.slug} não foi possível persistir falha de gravação: ${recordError.message}`);
  }
}

async function processBatch(pages, slotInfo, onResult = null, onPageStart = null, options = {}) {
  const { source = slotInfo ? "cron" : "manual", lease, persistInitial = false } = options;
  if (!lease) throw new Error("A coleta exige uma reserva durável do worker.");
  const batchTimeoutMs = Math.ceil(pages.length / 5) * BATCH_TIMEOUT_MS;
  const batchDeadline = Date.now() + batchTimeoutMs;
  let browser;
  let browserServer;
  let context;
  let pagesSinceLaunch = 0;

  async function launchBrowser() {
    const server = await chromium.launchServer({
      executablePath: getChromiumPath(),
      headless: true,
      timeout: BROWSER_START_TIMEOUT_MS,
      args: getBrowserLaunchArgs(),
    });
    browserServer = server;
    try {
      browser = await chromium.connect(server.wsEndpoint(), { timeout: BROWSER_CONNECT_TIMEOUT_MS });
      context = await createStealthContext(browser);
    } catch (err) {
      await closeBrowser("falha de inicialização");
      throw err;
    }
  }

  async function closeBrowser(reason) {
    const currentServer = browserServer;
    browser = null;
    browserServer = null;
    context = null;
    pagesSinceLaunch = 0;
    if (!currentServer) return;
    try {
      await withTimeout(() => currentServer.close(), BROWSER_CLOSE_TIMEOUT_MS);
    } catch (err) {
      console.error(`[BATCH] fechamento do Chromium (${reason}) falhou: ${err.message}`);
      const child = currentServer.process();
      if (child.exitCode === null && child.signalCode === null) {
        try {
          const exited = new Promise((resolve) => {
            if (child.exitCode !== null || child.signalCode !== null) resolve();
            else child.once("exit", resolve);
          });
          child.kill("SIGKILL");
          await withTimeout(() => exited, BROWSER_CLOSE_TIMEOUT_MS);
        } catch (killError) {
          console.error(`[BATCH] encerramento forçado do Chromium (${reason}) falhou: ${killError.message}`);
        }
      }
    }
  }

  async function attemptPage(page, pass) {
    if (onPageStart) onPageStart(page);
    const slotInfoForAttempt = slotInfo
      ? { slot: slotInfo.slot, businessDate: slotInfo.businessDate }
      : null;
    const attemptId = await recordAttemptStart(page, source, slotInfoForAttempt, lease);
    const diag = { status: null, detalhe: null };
    let count = null;
    let falha = null;

    if (!isMetaLibraryUrl(page.url || "")) {
      falha = `URL inválida (não é da Biblioteca da Meta): "${page.url}"`;
      diag.status = "falha_url_invalida";
      diag.detalhe = falha;
      console.error(`[BATCH] slug=${page.slug} ${falha}`);
    } else {
      try {
        count = await withTimeout(
          async () => {
            if (!browser || !browser.isConnected()) await launchBrowser();
            return scrapeWithContext(context, page.url, diag);
          },
          PAGE_TIMEOUT_MS,
          async () => {
            console.error(`[BATCH] slug=${page.slug} pass=${pass} excedeu timeout de ${PAGE_TIMEOUT_MS}ms; fechando Chromium`);
            await closeBrowser("timeout de página");
          },
        );
        pagesSinceLaunch++;
      } catch (err) {
        falha = err.message;
        if (err.name === "TimeoutError" || err.errors?.some((nested) => nested.name === "TimeoutError")) {
          diag.status = "falha_timeout";
          diag.detalhe = err.message;
        } else if (!diag.status) {
          diag.status = "falha_execucao";
          diag.detalhe = err.message;
        }
        console.error(`[BATCH] slug=${page.slug} pass=${pass} erro: ${err.message}`);
      }
    }

    const status = count !== null
      ? diag.status === "ok_zero" ? "ok_zero" : "ok"
      : diag.status?.startsWith("falha_") ? diag.status : "falha_timeout";
    const result = {
      slug: page.slug,
      nome: page.nome,
      count,
      status,
      falha: count === null ? (String(diag.detalhe || falha || "falha desconhecida").slice(0, 500)) : null,
      retryable: status !== "falha_url_invalida",
    };

    try {
      await completeAttempt(attemptId, page, result, { lease, slotInfo: slotInfoForAttempt, persistInitial });
    } catch (dbErr) {
      console.error(`[BATCH] slug=${page.slug} count=${count} falha ao gravar a tentativa: ${dbErr.message}`);
      await markAttemptWriteFailure(attemptId, page, dbErr, lease);
      result.count = null;
      result.status = "falha_gravacao";
      result.falha = "A coleta foi feita, mas não foi possível salvar o resultado.";
      result.dbError = true;
      result.retryable = true;
    }

    if (onResult) onResult(result);
    if (pagesSinceLaunch >= 5) await closeBrowser("limite de cinco páginas");
    await new Promise((resolve) => setTimeout(resolve, PAGE_PAUSE_MS));
    return { ...result, ok: result.count !== null && !result.dbError };
  }

  let results;
  try {
    results = await processWithRetries(pages, attemptPage, {
      maxPasses: 2,
      isSuccess: (result) => result.ok,
      shouldRetry: (result) => result.retryable,
      shouldStartRetryPass: ({ pending, nextPass }) => {
        const retryBudgetMs = pending.length * (PAGE_ATTEMPT_BUDGET_MS + PAGE_PAUSE_MS);
        if (Date.now() + retryBudgetMs <= batchDeadline) return true;
        console.warn(
          `[BATCH] pass=${nextPass} não iniciada: orçamento restante insuficiente para ${pending.length} retentativas; ` +
          "todas as bibliotecas já têm o resultado da primeira passagem registrado",
        );
        return false;
      },
    });
    for (const result of results) {
      if (!result.ok) {
        console.warn(`[BATCH] slug=${result.slug} falhou após até duas passagens [${result.status}] ${result.falha || ""}`);
      }
    }
  } finally {
    await closeBrowser("fim do lote");
  }

  const resultsOk = results.filter((result) => result.count !== null && !result.dbError);
  if (resultsOk.length > 0) {
    await mirrorToSheet(resultsOk);
  }
  return results.map(({ ok, retryable, ...result }) => result);
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.get("/api/healthz", (_req, res) => res.json({ status: "ok", ts: new Date().toISOString() }));

app.get("/api/ultima-checagem", async (_req, res) => {
  try {
    const { rows } = await query(`
      SELECT p.slug,
             l.collected_at AS ultima_coleta_ok,
             jsonb_build_object(
               'status', a.status,
               'error', a.error,
               'at', a.checked_at,
               'relevante', COALESCE(
                 a.checked_at > NOW() - INTERVAL '6 hours'
                 AND a.status IN ('falha_timeout','falha_bloqueio','falha_parse','falha_url_invalida','falha_gravacao')
                 AND (a.source LIKE 'cron%' OR a.source LIKE 'manual%'),
                 FALSE
               )
             ) AS tentativa
      FROM pages p
      LEFT JOIN LATERAL (
        SELECT COALESCE(completed_at, started_at) AS checked_at, status, error, source
        FROM scrape_attempts
        WHERE slug = p.slug
        ORDER BY started_at DESC, id DESC
        LIMIT 1
      ) a ON TRUE
      LEFT JOIN scrape_latest l ON l.slug = p.slug
      ORDER BY p.slug
    `);
    res.set("Cache-Control", "no-store").json(rows);
  } catch (err) {
    console.error(`[API] falha ao consultar última checagem: ${err.message}`);
    res.status(500).json({ error: "Não foi possível consultar a última checagem." });
  }
});

app.get("/api/cron/tick", async (req, res) => {
  res.json({ status: "alive", message: "Tick received" });
  let lease;
  try {
    const now = new Date();
    const brtHour = new Date(now.getTime() - 3 * 60 * 60 * 1000).getUTCHours();
    const mode = brtHour >= 5 && brtHour < 6
      ? "dominio"
      : brtHour >= 6 && brtHour < 7
        ? "keyword"
        : "pagina";
    const isScheduledSearchMode = mode !== "pagina";
    console.log(`[TICK] modo ${mode} identificado`);

    const { rows: cooldownRows } = await query(
      `SELECT blocked_until FROM scrape_worker_lease WHERE lease_key = $1`,
      [SCRAPE_LEASE_KEY],
    );
    const blockedUntil = cooldownRows[0]?.blocked_until
      ? new Date(cooldownRows[0].blocked_until)
      : null;
    if (!isScheduledSearchMode && blockedUntil && blockedUntil.getTime() > Date.now()) {
      console.warn(`[TICK] ignorado durante cooldown até ${blockedUntil.toISOString()}`);
      return;
    }

    if (isRunning) {
      console.warn("[TICK] ignorado: já existe execução ativa nesta instância");
      return;
    }
    lease = await acquireScrapeLease("cron");
    if (!lease) {
      console.warn("[TICK] ignorado: outra instância mantém a reserva durável do worker");
      return;
    }

    const slotInfo = mode === "dominio"
      ? getBusinessSlotForDomain(now)
      : mode === "keyword"
        ? getBusinessSlotForKeyword(now)
        : getCurrentSlot(now);
    const { rows: pages } = mode === "dominio"
      ? await query(`
          SELECT p.slug, p.nome, p.url
          FROM pages p
          WHERE p.tipo = 'dominio'
            AND NOT EXISTS (
              SELECT 1 FROM scrape_history sh
              WHERE sh.slug = p.slug
                AND sh.slot = 5
                AND sh.business_date = $1
            )
          ORDER BY p.last_attempt_at ASC NULLS FIRST
          LIMIT 5;
        `, [slotInfo.businessDate])
      : mode === "keyword"
        ? await query(`
            SELECT p.slug, p.nome, p.url
            FROM pages p
            WHERE p.tipo = 'keyword'
              AND NOT EXISTS (
                SELECT 1 FROM scrape_history sh
                WHERE sh.slug = p.slug
                  AND sh.slot = 6
                  AND sh.business_date = $1
              )
            ORDER BY p.last_attempt_at ASC NULLS FIRST
            LIMIT 5;
          `, [slotInfo.businessDate])
        : await query(`
          SELECT p.slug, p.nome, p.url
          FROM pages p
          WHERE p.tipo = 'pagina'
            AND NOT EXISTS (
              SELECT 1 FROM scrape_history sh
              WHERE sh.slug = p.slug
                AND sh.slot = $1
                AND sh.business_date = $2
            )
          ORDER BY p.last_attempt_at ASC NULLS FIRST
          LIMIT 5;
        `, [slotInfo.slot, slotInfo.businessDate]);

    if (pages.length === 0) {
      console.log(
        mode === "dominio"
          ? `[TICK-DOMINIO] nenhum domínio pendente para slot=5 date=${slotInfo.businessDate}`
          : mode === "keyword"
            ? `[TICK-KEYWORD] nenhuma palavra-chave pendente para slot=6 date=${slotInfo.businessDate}`
            : `[TICK] nenhuma biblioteca pendente para slot=${slotInfo.slot} date=${slotInfo.businessDate}`,
      );
      return;
    }

    console.log(
      mode === "dominio"
        ? `[TICK-DOMINIO] Iniciando lote de domínios para slot=5 date=${slotInfo.businessDate}`
        : mode === "keyword"
          ? `[TICK-KEYWORD] Iniciando lote de palavras-chave para slot=6 date=${slotInfo.businessDate}`
          : `[TICK] Iniciando lote de ${pages.length} páginas para slot=${slotInfo.slot} date=${slotInfo.businessDate}...`,
    );
    const results = await processBatch(pages, slotInfo, null, null, {
      source: mode === "dominio" ? "cron_dominio" : mode === "keyword" ? "cron_keyword" : "cron",
      lease,
    });
    const metaSlugs = new Set(pages.filter(p => /facebook\.com\/ads\/library/.test(p.url)).map(p => p.slug));
    const metaRes = results.filter(r => metaSlugs.has(r.slug));
    const FALHAS_TECNICAS = ["falha_bloqueio", "falha_timeout", "falha_parse"];
    if (mode === "pagina" && metaRes.length >= 3 && metaRes.every(r => r.count === null && !r.dbError && FALHAS_TECNICAS.includes(r.status))) {
      const { rows } = await query(
        `UPDATE scrape_worker_lease
         SET blocked_until = NOW() + INTERVAL '90 minutes'
         WHERE lease_key = $1 AND owner_id = $2
         RETURNING blocked_until`,
        [SCRAPE_LEASE_KEY, lease.ownerId],
      );
      if (rows[0]) {
        console.warn(`[TICK] ${metaRes.length}/${metaRes.length} páginas da Meta falharam (${metaRes.map(r => r.status).join(", ")}) — cooldown de 90 min até ${new Date(rows[0].blocked_until).toISOString()}`);
      }
    }
    console.log(`[TICK] Lote do slot=${slotInfo.slot} finalizado.`);
  } catch (err) {
    console.error("[TICK] Erro geral:", err);
  } finally {
    if (lease) await lease.release();
  }
});

async function saveExtensionInitial(slug, count) {
  const client = await pool.connect();
  const attemptId = randomUUID();
  try {
    await client.query("BEGIN");
    await client.query(
      `UPDATE pages SET inicial_count = COALESCE(inicial_count, $2) WHERE slug = $1`,
      [slug, count],
    );
    await client.query(
      `INSERT INTO scrape_latest (slug, ads_count, collected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (slug) DO UPDATE
         SET ads_count = EXCLUDED.ads_count, collected_at = EXCLUDED.collected_at`,
      [slug, count],
    );
    await client.query(
      `INSERT INTO scrape_history (slug, ads_count, slot)
       VALUES ($1, $2, NULL)`,
      [slug, count],
    );
    await client.query(
      `INSERT INTO scrape_attempts
         (id, slug, source, status, ads_count, started_at, completed_at, lease_owner)
       VALUES ($1, $2, 'cadastro_extensao', 'ok', $3, NOW(), NOW(), 'extension')`,
      [attemptId, slug, count],
    );
    await client.query(
      `UPDATE pages SET last_attempt_at = NOW(), last_status = 'ok', last_error = NULL
       WHERE slug = $1`,
      [slug],
    );
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch((rollbackError) => {
      console.error(`[DESCOBERTA] rollback de contagem da extensão falhou: ${rollbackError.message}`);
    });
    throw err;
  } finally {
    client.release();
  }
}

app.post("/api/salvar", async (req, res) => {
  const { nome, url: urlRaw, tipo, instagram_url, geo, nicho, funil, ads_count_inicial } = req.body;
  if (!nome || !urlRaw) return res.status(400).json({ error: "Fields 'nome' and 'url' are required." });
  const tipoFinal = normalizeMonitoringType(tipo) || "pagina";
  if (!toSlug(nome)) return res.status(400).json({ error: "Could not generate a valid slug." });
  const url = resolveMetaUrl(urlRaw, tipoFinal);
  if (!url) return res.status(400).json({ error: "Entrada inválida: informe a URL da Meta Ad Library, um domínio ou uma palavra-chave conforme o tipo selecionado." });
  try {
    const record = await saveMonitoringRecord({
      nome,
      url,
      tipo: tipoFinal,
      instagram_url: instagram_url || null,
      geo: geo || null,
      nicho: nicho || null,
      funil: funil || null,
    });
    console.log(`[SALVAR] registered slug=${record.slug} tipo=${tipoFinal}`);

    if (tipoFinal === "keyword") {
      captureInicial(record.slug, url).catch((err) => {
        console.error(`[DESCOBERTA] falha ao iniciar captura de keyword ${record.slug}: ${err.message}`);
      });
      return res.status(202).json({
        slug: record.slug,
        tipo: tipoFinal,
        keyword_key: record.keyword_key,
        inicial: null,
        coletarPath: `/api/coletar/${record.slug}`,
      });
    }

    let inicial = ads_count_inicial;
    if (inicial !== undefined && inicial !== null) {
      const countNum = parseInt(inicial, 10) || 0;
      await saveExtensionInitial(record.slug, countNum);
      inicial = countNum;
      console.log(`[DESCOBERTA] slug=${record.slug} inicial=${countNum} salvo via Extensão (Sem Playwright)`);
    } else {
      inicial = await captureInicial(record.slug, url);
    }
    return res.json({ slug: record.slug, tipo: tipoFinal, inicial, coletarPath: `/api/coletar/${record.slug}` });
  } catch (err) {
    console.error(`[SALVAR] erro ao registrar tipo=${tipoFinal}: ${err.message}`);
    return res.status(500).json({ error: "Não foi possível salvar o monitoramento." });
  }
});

app.get("/api/coletar/:slug", async (req, res) => {
  const { slug } = req.params;
  let lease;
  try {
    lease = await acquireScrapeLease("manual_individual");
    if (!lease) return res.status(409).type("text/plain").send("OCUPADO");
    const { rows } = await query("SELECT * FROM pages WHERE slug = $1 LIMIT 1", [slug]);
    const row = rows[0];
    if (!row) return res.status(404).type("text/plain").send(`Page '${slug}' not registered.`);
    const [result] = await processBatch(
      [{ slug: row.slug, nome: row.nome, url: row.url }],
      null,
      null,
      null,
      { source: "manual_individual", lease },
    );
    if (result.count === null) return res.status(502).type("text/plain").send("FALHA");
    return res.type("text/plain").send(String(result.count));
  } catch (err) {
    console.error(`[COLETAR] error slug=${slug}: ${err.message}`);
    return res.status(500).type("text/plain").send("FALHA");
  } finally {
    if (lease) await lease.release();
  }
});

function manualCheckFailureMessage(status) {
  const messages = {
    falha_bloqueio: "A Meta não permitiu acessar a biblioteca durante a checagem.",
    falha_timeout: "A página não respondeu dentro do tempo limite. Tente novamente.",
    falha_parse: "Não foi possível identificar a contagem de anúncios.",
    falha_url_invalida: "A URL cadastrada não é uma biblioteca válida.",
    falha_gravacao: "A coleta foi feita, mas não foi possível salvar o resultado.",
    falha_execucao: "A execução foi interrompida antes de concluir a coleta.",
  };
  return messages[status] || "Não foi possível concluir a coleta. Tente novamente.";
}

function fingerprintManualPages(pages) {
  const canonical = pages
    .map(({ slug, nome, url }) => [slug, nome, url])
    .sort(([leftSlug], [rightSlug]) => leftSlug < rightSlug ? -1 : leftSlug > rightSlug ? 1 : 0);
  return createHash("sha256").update(JSON.stringify(canonical), "utf8").digest("hex");
}

function addManualCheckResult(result) {
  const sucesso = result.count !== null && !result.dbError;
  const resultado = {
    slug: result.slug,
    nome: result.nome,
    count: sucesso ? result.count : null,
    status: result.status || (sucesso ? "ok" : "falha_execucao"),
    falha: sucesso ? null : manualCheckFailureMessage(result.status),
  };
  const results = [...manualCheckStatus.resultados];
  const previousIndex = results.findIndex((item) => item.slug === result.slug);
  if (previousIndex === -1) results.push(resultado);
  else results[previousIndex] = resultado;
  const sucessoCount = results.filter((item) => item.count !== null).length;
  manualCheckStatus = {
    ...manualCheckStatus,
    resultados: results,
    concluidos: results.length,
    sucesso: sucessoCount,
    falha: results.length - sucessoCount,
    atual: result.nome,
  };
}

async function startManualCheck(req, res) {
  const requestId = req.method === "POST" && req.body ? req.body.requestId : null;
  if (req.method === "POST" && (typeof requestId !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(requestId))) {
    return res.status(400).json({ status: "invalid_request", message: "Identificador de execução inválido. Confirme novamente a checagem." });
  }

  const hasSelection = req.method === "POST" && req.body && Object.prototype.hasOwnProperty.call(req.body, "slugs");
  const hasSnapshot = req.method === "POST" && req.body
    && Object.prototype.hasOwnProperty.call(req.body, "snapshotHash")
    && Object.prototype.hasOwnProperty.call(req.body, "snapshotCount");
  let selectedSlugs = null;
  let expectedSnapshotHash = null;
  let expectedSnapshotCount = null;
  if (hasSelection) {
    const requestedSlugs = req.body.slugs;
    if (!Array.isArray(requestedSlugs) || requestedSlugs.length === 0 || requestedSlugs.length > 500
      || requestedSlugs.some((slug) => typeof slug !== "string" || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(slug))) {
      return res.status(400).json({ status: "invalid_selection", message: "Selecione ao menos uma biblioteca válida." });
    }
    selectedSlugs = [...new Set(requestedSlugs)];
  }
  if (req.method === "POST" && !hasSnapshot) {
    return res.status(400).json({ status: "invalid_selection", message: "Confirme novamente a lista antes de iniciar a checagem." });
  }
  if (hasSnapshot) {
    if (typeof req.body.snapshotHash !== "string" || !/^[a-f0-9]{64}$/i.test(req.body.snapshotHash)
      || !Number.isSafeInteger(req.body.snapshotCount) || req.body.snapshotCount < 1) {
      return res.status(400).json({ status: "invalid_selection", message: "Não foi possível validar a lista confirmada. Revise a checagem." });
    }
    expectedSnapshotHash = req.body.snapshotHash.toLowerCase();
    expectedSnapshotCount = req.body.snapshotCount;
  }
  const requestSignature = createHash("sha256").update(JSON.stringify({
    slugs: selectedSlugs ? [...selectedSlugs].sort() : null,
    snapshotHash: expectedSnapshotHash,
    snapshotCount: expectedSnapshotCount,
  }), "utf8").digest("hex");
  const previousRequest = manualCheckRequestLedger.get(requestId);
  if (previousRequest) {
    if (previousRequest.signature !== requestSignature) {
      return res.status(409).json({ status: "request_conflict", message: "Este identificador já foi usado com outra lista. Reconfirme a checagem." });
    }
    return res.status(202).json({ status: "started", runId: previousRequest.runId, escopo: previousRequest.escopo });
  }
  let lease;
  try {
    lease = await acquireScrapeLease("manual_lote");
  } catch (err) {
    console.error(`[RUN] não foi possível reservar worker: ${err.message}`);
    return res.status(503).json({ status: "error", message: "Não foi possível iniciar a checagem. Tente novamente." });
  }
  if (!lease) {
    return res.status(409).json({ status: "busy", message: "Já existe uma coleta em andamento (cron ou lote). Tente em alguns minutos." });
  }

  let pages;
  try {
    const result = selectedSlugs
      ? await query("SELECT slug, nome, url FROM pages WHERE slug = ANY($1) ORDER BY tipo, nome", [selectedSlugs])
      : await query("SELECT slug, nome, url FROM pages ORDER BY tipo, nome");
    pages = result.rows;
  } catch (err) {
    await lease.release();
    console.error(`[RUN] não foi possível carregar bibliotecas: ${err.message}`);
    return res.status(500).json({ status: "error", message: "Não foi possível carregar as bibliotecas." });
  }

  if (selectedSlugs && pages.length !== selectedSlugs.length) {
    await lease.release();
    return res.status(400).json({ status: "invalid_selection", message: "Uma ou mais bibliotecas selecionadas não existem." });
  }
  if (expectedSnapshotHash && (pages.length !== expectedSnapshotCount || fingerprintManualPages(pages) !== expectedSnapshotHash)) {
    await lease.release();
    return res.status(409).json({ status: "selection_changed", message: "A lista de bibliotecas mudou após a confirmação. Revise a lista completa e confirme outra vez." });
  }

  const runId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const escopo = selectedSlugs ? "selecionadas" : "todas";
  manualCheckRequestLedger.set(requestId, { signature: requestSignature, runId, escopo, state: null });
  if (manualCheckRequestLedger.size > MANUAL_CHECK_REQUEST_LEDGER_LIMIT) {
    manualCheckRequestLedger.delete(manualCheckRequestLedger.keys().next().value);
  }
  manualCheckStatus = {
    runId,
    requestId,
    status: "running",
    escopo,
    total: 0,
    concluidos: 0,
    sucesso: 0,
    falha: 0,
    atual: null,
    resultados: [],
    iniciadoEm: new Date().toISOString(),
    finalizadoEm: null,
    erro: null,
  };
  res.status(req.method === "POST" ? 202 : 200).json({ status: "started", runId, escopo: manualCheckStatus.escopo });

  (async () => {
    try {
      manualCheckStatus = { ...manualCheckStatus, total: pages.length };
      if (pages.length === 0) {
        console.log("[RUN] checagem manual sem bibliotecas selecionadas");
        manualCheckStatus = { ...manualCheckStatus, status: "completed" };
        return;
      }

      const BATCH_SIZE = 5;
      console.log(`[RUN] checagem manual iniciada — ${pages.length} bibliotecas, blocos de ${BATCH_SIZE}; retentativas somente após a primeira passagem completa`);
      await processBatch(
        pages,
        null,
        addManualCheckResult,
        (page) => { manualCheckStatus = { ...manualCheckStatus, atual: page.nome }; },
        { source: "manual_lote", lease },
      );

      console.log(`[RUN] coleta-tudo manual finalizada — ${pages.length} páginas`);
      manualCheckStatus = {
        ...manualCheckStatus,
        status: manualCheckStatus.falha > 0 ? "completed_with_errors" : "completed",
      };
    } catch (e) {
      console.error("[RUN] manual error:", e.message);
      manualCheckStatus = {
        ...manualCheckStatus,
        status: manualCheckStatus.concluidos > 0 ? "completed_with_errors" : "failed",
        erro: "Falha ao consultar ou processar as bibliotecas. Consulte os logs do serviço.",
      };
    } finally {
      await lease.release();
      manualCheckStatus = {
        ...manualCheckStatus,
        atual: null,
        finalizadoEm: new Date().toISOString(),
      };
      const completedRequest = manualCheckRequestLedger.get(manualCheckStatus.requestId);
      if (completedRequest) completedRequest.state = { ...manualCheckStatus, ocupado: false };
    }
  })();
}

app.post("/api/coletar-tudo", startManualCheck);
app.get("/api/coletar-tudo", (_req, res) => res.status(405).json({ status: "confirmation_required", message: "Confirme a lista de bibliotecas antes de iniciar a checagem." }));
app.get("/api/coletar-tudo/status", (req, res) => {
  const requestedId = typeof req.query.requestId === "string" ? req.query.requestId : null;
  const requestEntry = requestedId ? manualCheckRequestLedger.get(requestedId) : null;
  if (requestedId && !requestEntry) {
    return res.status(404).json({ status: "unknown_run", message: "A execução solicitada não está mais disponível." });
  }
  const isCurrentRun = !!requestEntry && manualCheckStatus.requestId === requestedId;
  const status = requestEntry
    ? (isCurrentRun ? manualCheckStatus : requestEntry.state || manualCheckStatus)
    : manualCheckStatus;
  const emExecucao = status.status === "running";
  res.json({
    ...status,
    resultados: emExecucao ? status.resultados.slice(-15) : status.resultados,
    totalResultados: status.resultados.length,
    ocupado: isCurrentRun ? isRunning : requestedId ? false : isRunning,
  });
});

app.get("/api/historico/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT id, slug, ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at DESC`,
    [slug]
  );
  res.json(rows);
});

app.get("/api/resumo/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows } = await query(
    `SELECT ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at ASC`,
    [slug]
  );
  if (rows.length === 0) return res.json({ slug, message: "No data yet." });
  const counts = rows.map((r) => r.ads_count);
  const min = Math.min(...counts), max = Math.max(...counts);
  const avg = Math.round(counts.reduce((a, b) => a + b, 0) / counts.length);
  const first = counts[0], last = counts[counts.length - 1];
  const trend = last > first ? "crescendo" : last < first ? "caindo" : "estável";
  res.json({ slug, total_coletas: rows.length, min, max, avg, trend, first, last });
});

app.get("/api/status", async (_req, res) => {
  const { rows: pages } = await query("SELECT slug, nome, url FROM pages");
  const result = await Promise.all(pages.map(async (p) => {
    const { rows } = await query(
      `SELECT ads_count, collected_at FROM scrape_history WHERE slug = $1 ORDER BY collected_at DESC LIMIT 1`,
      [p.slug]
    );
    const latest = rows[0];
    return { slug: p.slug, nome: p.nome, url: p.url, ads_ativos: latest?.ads_count ?? null, ultima_coleta: latest?.collected_at ?? null };
  }));
  res.json(result);
});

app.get("/api/paginas", async (_req, res) => {
  const { rows } = await query("SELECT slug, nome, url, tipo, keyword_key, instagram_url, geo, nicho, funil FROM pages");
  res.set("Cache-Control", "no-store").json(rows);
});

// ─── Admin ───────────────────────────────────────────────────────────────────

app.get("/admin", async (_req, res) => {
  const { rows: pages } = await query(
    "SELECT slug, nome, url, tipo, instagram_url, geo, nicho, funil, created_at FROM pages ORDER BY tipo, created_at DESC"
  );

  // JSON de cada item, embutido no atributo data-item, usado pelo JS para preencher o formulário ao clicar em Editar
  function escAttr(str) {
    return String(str ?? "").replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  const lista = pages.map(p => `
    <tr data-search="${escAttr((p.nome + " " + p.url + " " + (p.geo||"") + " " + (p.nicho||"")).toLowerCase())}">
      <td><span class="badge ${p.tipo === "dominio" ? "b-dom" : p.tipo === "keyword" ? "b-key" : "b-pag"}">${p.tipo === "dominio" ? "🌐 Domínio" : p.tipo === "keyword" ? "🔑 Palavra-chave" : "📡 Biblioteca"}</span></td>
      <td class="nome-cell">
        <div class="nome">${p.nome}</div>
        <div class="meta-badges">
          ${p.geo ? `<span class="meta-tag">🌍 ${p.geo}</span>` : ""}
          ${p.nicho ? `<span class="meta-tag">🏷️ ${p.nicho}</span>` : ""}
          ${p.funil ? `<span class="meta-tag">🎯 ${p.funil}</span>` : ""}
          ${p.instagram_url ? `<a href="${p.instagram_url}" target="_blank" class="ig-tag">
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" style="display:inline;vertical-align:middle;margin-right:3px"><rect width="24" height="24" rx="6" fill="url(#ig_admin)"/><circle cx="12" cy="12" r="4.5" stroke="white" stroke-width="1.8" fill="none"/><circle cx="17" cy="7" r="1.2" fill="white"/><rect x="3" y="3" width="18" height="18" rx="5" stroke="white" stroke-width="1.8" fill="none"/><defs><linearGradient id="ig_admin" x1="0" y1="24" x2="24" y2="0"><stop offset="0%" stop-color="#f09433"/><stop offset="25%" stop-color="#e6683c"/><stop offset="50%" stop-color="#dc2743"/><stop offset="75%" stop-color="#cc2366"/><stop offset="100%" stop-color="#bc1888"/></linearGradient></defs></svg>Instagram</a>` : ""}
        </div>
      </td>
      <td><a href="${p.url}" target="_blank" class="url-link">Ver na Meta ↗</a></td>
      <td>${new Date(p.created_at).toLocaleDateString("pt-BR")}</td>
      <td style="white-space:nowrap">
        <button type="button" class="btn-edit"
          data-slug="${escAttr(p.slug)}"
          data-nome="${escAttr(p.nome)}"
          data-url="${escAttr(p.url)}"
          data-tipo="${escAttr(p.tipo)}"
          data-instagram="${escAttr(p.instagram_url)}"
          data-geo="${escAttr(p.geo)}"
          data-nicho="${escAttr(p.nicho)}"
          data-funil="${escAttr(p.funil)}"
          onclick="editarItem(this)">✏️ Editar</button>
        <a href="/admin/funis/${p.slug}" class="btn-funis">🔀 Funis</a>
        <form id="form-remover-${escAttr(p.slug)}" method="POST" action="/admin/remover" style="display:none">
          <input type="hidden" name="slug" value="${p.slug}">
        </form>
        <button type="button" class="btn-del" data-slug="${escAttr(p.slug)}" data-nome="${escAttr(p.nome)}" onclick="abrirModalRemoverAdmin(this)">Remover</button>
      </td>
    </tr>`).join("");

  const msgOk = (() => {
    const q = res.req?.query || {};
    if (q.ok === "1") return '<div class="msg ok">✅ Rastreamento cadastrado com sucesso.</div>';
    if (q.ok === "editado") return '<div class="msg ok">✏️ Rastreamento atualizado com sucesso.</div>';
    if (q.ok === "removido") return '<div class="msg ok">🗑️ Rastreamento removido.</div>';
    if (q.erro === "campos-obrigatorios") return '<div class="msg err">⚠️ Nome e URL são obrigatórios.</div>';
    if (q.erro === "nome-invalido") return '<div class="msg err">⚠️ Nome inválido.</div>';
    if (q.erro === "url-invalida") return '<div class="msg err">⚠️ Entrada inválida. Use a URL da Meta Ad Library, um domínio ou uma palavra-chave conforme o tipo selecionado.</div>';
    if (q.erro === "lote-vazio") return '<div class="msg err">⚠️ Nenhum item enviado no lote.</div>';
    if (q.erro === "lote-invalido") return '<div class="msg err">⚠️ Nenhuma linha válida encontrada no lote.</div>';
    if (q.erro === "lote-em-andamento") return '<div class="msg err">⚠️ Já existe um lote em andamento. Aguarde terminar.</div>';
    if (q.erro === "erro-interno") return '<div class="msg err">⚠️ Erro interno. Tente novamente.</div>';
    return "";
  })();

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lowticket Monitor — Admin</title>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:1000px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:28px;padding-bottom:16px;border-bottom:1px solid var(--border)}
.hdr h1{font-size:18px;font-weight:700}
.hdr a{margin-left:auto;font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px}
.hdr a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:22px 24px;margin-bottom:20px}
.card h2{font-size:14px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:18px}
.form-row{display:grid;grid-template-columns:160px 1fr 1fr;gap:12px;align-items:end;margin-bottom:12px}
.form-row-2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}
.form-row-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:12px}
@media(max-width:700px){.form-row,.form-row-2,.form-row-3{grid-template-columns:1fr}}
.field{display:flex;flex-direction:column;gap:6px}
label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.label-optional{font-size:10px;color:var(--muted);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px;opacity:.7}
input,select{background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:14px;padding:10px 14px;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:600;padding:10px 22px;cursor:pointer;transition:opacity .2s}
.btn:hover{opacity:.85}
.btn-del{background:transparent;color:var(--down);border:1px solid var(--down);border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s;margin-left:6px}
.btn-edit{background:transparent;color:var(--accent);border:1px solid var(--accent);border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s}
.btn-edit:hover{background:var(--accent);color:#fff}
.btn-funis{display:inline-block;background:transparent;color:#34d399;border:1px solid #34d399;border-radius:6px;font-family:'Space Grotesk',sans-serif;font-size:11px;padding:4px 10px;cursor:pointer;transition:all .2s;text-decoration:none;margin-left:6px}
.btn-funis:hover{background:#34d399;color:#0a0a14}
.btn-del:hover{background:var(--down);color:#fff}
.tip{font-size:12px;color:var(--muted);margin-top:14px;line-height:1.6;background:#0f0f1e;border-radius:8px;padding:12px 14px;border-left:3px solid var(--accent)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:10px 14px;text-align:left;border-bottom:1px solid var(--border)}
td{padding:11px 14px;border-bottom:1px solid var(--border);vertical-align:middle}
.nome{font-weight:600;color:#fff}
.nome-cell{vertical-align:middle}
.meta-badges{display:flex;flex-wrap:wrap;gap:5px;margin-top:5px}
.meta-tag{font-size:10px;background:rgba(124,111,255,.12);color:#a78bfa;padding:2px 7px;border-radius:5px;font-weight:500}
.ig-tag{font-size:10px;background:rgba(220,39,67,.12);color:#fb7185;padding:2px 7px;border-radius:5px;font-weight:500;text-decoration:none;display:inline-flex;align-items:center;gap:3px}
.ig-tag:hover{background:rgba(220,39,67,.25)}
.url-link{color:var(--accent);font-size:12px;text-decoration:none;font-family:'Space Mono',monospace}
.url-link:hover{text-decoration:underline}
.badge{display:inline-block;padding:3px 9px;border-radius:6px;font-size:11px;font-weight:600}
.b-dom{background:rgba(124,111,255,.15);color:#a78bfa}
.b-key{background:rgba(251,191,36,.14);color:#fbbf24}
.b-pag{background:rgba(52,211,153,.12);color:#34d399}
.msg{padding:12px 16px;border-radius:8px;font-size:13px;margin-bottom:18px}
.msg.ok{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.msg.err{background:rgba(251,113,133,.12);color:#fb7185;border:1px solid rgba(251,113,133,.25)}
.empty{color:var(--muted);font-size:13px;text-align:center;padding:24px}
.divider{border:none;border-top:1px solid var(--border);margin:16px 0}
.search-wrap{position:relative;margin-bottom:16px}
.search-wrap svg{position:absolute;left:13px;top:50%;transform:translateY(-50%);color:var(--muted);pointer-events:none}
.search-wrap input{width:100%;padding:10px 14px 10px 38px;border-radius:10px;background:#0f0f1e;border:1px solid var(--border);color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;outline:none;transition:.18s;box-sizing:border-box}
.search-wrap input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(124,111,255,.15)}
.search-wrap input::placeholder{color:var(--muted)}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);display:flex;align-items:center;justify-content:center;z-index:9999;opacity:0;pointer-events:none;transition:opacity .2s ease}
.modal-overlay.open{opacity:1;pointer-events:all}
.modal-card{background:#1c1c2e;border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:30px 28px 24px;max-width:360px;width:calc(100% - 40px);box-shadow:0 40px 100px rgba(0,0,0,.7),0 0 0 1px rgba(255,255,255,.06);transform:scale(.92) translateY(12px);transition:transform .28s cubic-bezier(.34,1.4,.64,1),opacity .22s ease;opacity:0;text-align:center}
.modal-overlay.open .modal-card{transform:scale(1) translateY(0);opacity:1}
.modal-icon{font-size:38px;margin-bottom:14px;line-height:1}
.modal-title{font-size:16px;font-weight:700;color:#fff;margin-bottom:8px;letter-spacing:-.2px}
.modal-desc{font-size:13px;color:#8888aa;margin-bottom:24px;line-height:1.6;word-break:break-word}
.modal-desc b{color:#fb7185}
.modal-actions{display:flex;gap:10px}
.modal-btn-cancel{flex:1;background:rgba(255,255,255,.07);color:#e0e0f0;border:1px solid rgba(255,255,255,.1);border-radius:12px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:600;padding:12px 0;cursor:pointer;transition:background .15s}
.modal-btn-cancel:hover{background:rgba(255,255,255,.13)}
.modal-btn-confirm{flex:1;background:#fb7185;color:#fff;border:none;border-radius:12px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:700;padding:12px 0;cursor:pointer;transition:background .15s,transform .1s}
.modal-btn-confirm:hover{background:#f43f5e}
.modal-btn-confirm:active{transform:scale(.97)}
</style>
</head>
<body>
<div class="modal-overlay" id="modal-remover-admin" onclick="fecharModalRemoverOverlayAdmin(event)">
  <div class="modal-card">
    <div class="modal-icon">🗑️</div>
    <h3 class="modal-title">Remover rastreamento</h3>
    <p class="modal-desc" id="modal-remover-admin-desc"></p>
    <div class="modal-actions">
      <button class="modal-btn-cancel" onclick="fecharModalRemoverAdmin()">Cancelar</button>
      <button class="modal-btn-confirm" onclick="confirmarRemoverAdminFinal()">Remover</button>
    </div>
  </div>
</div>
<div class="hdr">
  <h1>⚙️ Admin — Lowticket Monitor</h1>
  <div style="margin-left:auto;display:flex;gap:8px;flex-wrap:wrap">
    <a href="/dashboard" style="font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px">← Ver Dashboard</a>
    <a href="/funis" style="font-size:13px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:7px 16px;border-radius:8px">🔀 Ver Mapa de Funis</a>
  </div>
</div>

${msgOk}

<div class="card" id="form-card">
  <h2 id="form-title">➕ Cadastrar novo rastreamento</h2>
  <form method="POST" action="/admin/salvar" id="mainForm">
    <input type="hidden" name="original_slug" id="originalSlug" value="">
    <div class="form-row">
      <div class="field">
        <label>Tipo</label>
        <select name="tipo" id="tipoSelect" onchange="atualizarDica()">
          <option value="pagina">📡 Biblioteca (página)</option>
          <option value="dominio">🌐 Domínio (URL)</option>
          <option value="keyword">🔑 Palavra-chave</option>
        </select>
      </div>
      <div class="field">
        <label>Nome</label>
        <input type="text" name="nome" id="nomeInput" placeholder="Ex: FlowForce Max ou FLOWFORCE.COM" required>
      </div>
      <div class="field">
        <label id="urlLabel">URL da Meta Ad Library</label>
        <input type="text" name="url" id="urlInput" placeholder="https://www.facebook.com/ads/library/..." required>
      </div>
    </div>

    <hr class="divider">
    <div style="font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Informações adicionais <span style="font-weight:400;text-transform:none;letter-spacing:0;opacity:.6">(opcionais)</span></div>

    <div class="form-row-3">
      <div class="field">
        <label>Instagram <span class="label-optional">opcional</span></label>
        <input type="url" name="instagram_url" id="instagramInput" placeholder="https://www.instagram.com/perfil">
      </div>
      <div class="field">
        <label>Geo <span class="label-optional">opcional</span></label>
        <input type="text" name="geo" id="geoInput" placeholder="Ex: US, BR, UK">
      </div>
      <div class="field">
        <label>Nicho <span class="label-optional">opcional</span></label>
        <input type="text" name="nicho" id="nichoInput" placeholder="Ex: Próstata, Weight Loss, ED">
      </div>
    </div>

    <div style="display:flex;gap:10px;margin-top:4px">
      <button type="submit" class="btn" id="submitBtn">Cadastrar</button>
      <button type="button" class="btn" id="cancelBtn" style="display:none;background:transparent;border:1px solid var(--border);color:var(--text2)" onclick="cancelarEdicao()">Cancelar edição</button>
    </div>

    <div class="tip" id="dica">
      💡 <strong>Biblioteca:</strong> Cole a URL da página do anunciante na Meta Ad Library com filtro "Anúncios ativos".<br>
      Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&ad_type=all&id=XXXXXXXXX</code>
    </div>
  </form>
</div>

<div class="card">
  <h2>📦 Cadastro em lote</h2>
  <form method="POST" action="/admin/lote">
    <div class="field">
      <label>Uma linha por item</label>
      <textarea name="itens" rows="7"
        placeholder="FlowForce Max | https://www.facebook.com/ads/library/?view_all_page_id=123456 | https://instagram.com/flowforcemax&#10;FLOWFORCE.COM | https://www.facebook.com/ads/library/?q=FLOWFORCE.COM...&#10;dominio | AnotherOffer | https://www.facebook.com/ads/library/?q=ANOTHEROFFER.COM | https://instagram.com/anotheroffer"
        style="background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Mono',monospace;font-size:12px;padding:12px 14px;outline:none;resize:vertical;width:100%"></textarea>
    </div>
    <button type="submit" class="btn" style="margin-top:12px">Cadastrar lote</button>
    <div class="tip">
      💡 Formatos aceitos por linha:<br>
      <code>Nome | URL da Meta Ad Library</code><br>
      <code>Nome | URL da Meta Ad Library | https://instagram.com/perfil</code><br>
      <code>tipo | Nome | URL | https://instagram.com/perfil</code><br>
      <code>keyword | Jejum Intermitente</code><br>
      <code>palavra | Jejum | URL da Meta Ad Library</code><br><br>
      O tipo (Biblioteca, Domínio ou Palavra-chave) é detectado automaticamente pela URL quando possível. O Instagram é opcional — basta omitir.<br>
      Geo e Nicho só podem ser preenchidos após o cadastro, editando o item individualmente no admin.<br>
      Cada item leva ~15-20s pra processar. A página não precisa ficar aberta.
    </div>
  </form>
  <div id="lote-progresso" style="display:none;margin-top:16px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:14px 16px">
    <div id="lote-texto" style="font-size:13px;color:var(--text2)"></div>
    <div style="background:var(--border);border-radius:6px;height:8px;margin-top:10px;overflow:hidden">
      <div id="lote-barra" style="background:var(--accent);height:100%;width:0%;transition:width .3s"></div>
    </div>
    <div id="lote-erros" style="font-size:12px;color:var(--down);margin-top:10px"></div>
  </div>
</div>

<div class="card">
  <h2>📋 Rastreamentos cadastrados (${pages.length})</h2>
  ${pages.length === 0 ? '<div class="empty">Nenhum rastreamento cadastrado ainda.</div>' : `
  <div class="search-wrap">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3-3"/></svg>
    <input type="text" id="buscaRastreios" placeholder="Buscar por nome, URL, geo ou nicho..." oninput="filtrarRastreios()">
  </div>
  <table>
    <thead><tr><th>Tipo</th><th>Nome / Metadados</th><th>Link Meta</th><th>Cadastrado</th><th></th></tr></thead>
    <tbody id="tbody-rastreios">${lista}</tbody>
  </table>
  <div class="empty" id="busca-rastreios-vazio" style="display:none">Nenhum resultado encontrado.</div>`}
</div>

<script>
function atualizarDica(){
  const tipo=document.getElementById('tipoSelect').value;
  const dica=document.getElementById('dica');
  const url=document.getElementById('urlInput');
  const urlLabel=document.getElementById('urlLabel');
  if(tipo==='dominio'){
    dica.innerHTML='💡 <strong>Domínio:</strong> Cole a URL de busca por palavra-chave/domínio na Meta Ad Library.<br>Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&q=SEUDOMINIO.COM&search_type=keyword_unordered</code>';
    urlLabel.textContent='URL da Meta Ad Library ou domínio';
    url.placeholder='https://www.facebook.com/ads/library/?active_status=active&q=SEUDOMINIO.COM...';
  }else if(tipo==='keyword'){
    dica.textContent="💡 Palavra-chave: Digite apenas a palavra ou frase. O sistema monta a busca na Biblioteca automaticamente. Ex: 'ansiedade' vira busca por todos os anúncios ativos com essa palavra.";
    urlLabel.textContent='Palavra-chave ou URL da Meta Ad Library';
    url.placeholder='Ex: jejum intermitente, biblia explicada, ansiedade';
  }else{
    dica.innerHTML='💡 <strong>Biblioteca:</strong> Cole a URL da página do anunciante na Meta Ad Library com filtro "Anúncios ativos".<br>Exemplo: <code>https://www.facebook.com/ads/library/?active_status=active&ad_type=all&id=XXXXXXXXX</code>';
    urlLabel.textContent='URL da Meta Ad Library';
    url.placeholder='https://www.facebook.com/ads/library/?active_status=active&id=...';
  }
}

function editarItem(btn){
  document.getElementById('originalSlug').value=btn.dataset.slug;
  document.getElementById('nomeInput').value=btn.dataset.nome;
  document.getElementById('urlInput').value=btn.dataset.url;
  document.getElementById('tipoSelect').value=btn.dataset.tipo;
  document.getElementById('instagramInput').value=btn.dataset.instagram;
  document.getElementById('geoInput').value=btn.dataset.geo;
  document.getElementById('nichoInput').value=btn.dataset.nicho;
  document.getElementById('form-title').textContent='✏️ Editando: '+btn.dataset.nome;
  document.getElementById('submitBtn').textContent='Salvar alterações';
  document.getElementById('cancelBtn').style.display='inline-block';
  atualizarDica();
  document.getElementById('form-card').scrollIntoView({behavior:'smooth',block:'start'});
}

function cancelarEdicao(){
  document.getElementById('mainForm').reset();
  document.getElementById('originalSlug').value='';
  document.getElementById('form-title').textContent='➕ Cadastrar novo rastreamento';
  document.getElementById('submitBtn').textContent='Cadastrar';
  document.getElementById('cancelBtn').style.display='none';
  atualizarDica();
}
  
function filtrarRastreios(){
  const termo=document.getElementById('buscaRastreios').value.trim().toLowerCase();
  const linhas=document.querySelectorAll('#tbody-rastreios tr');
  let visiveis=0;
  linhas.forEach(function(tr){
    const match=!termo||(tr.dataset.search||'').includes(termo);
    tr.style.display=match?'':'none';
    if(match)visiveis++;
  });
  document.getElementById('busca-rastreios-vazio').style.display=visiveis===0?'block':'none';
}

var _slugParaRemoverAdmin=null;
function abrirModalRemoverAdmin(btn){
  _slugParaRemoverAdmin=btn.dataset.slug;
  document.getElementById('modal-remover-admin-desc').innerHTML=
    'Tem certeza que deseja remover <b>'+btn.dataset.nome+'</b>? Todo o histórico de coletas e dados desta biblioteca serão perdidos permanentemente.';
  document.getElementById('modal-remover-admin').classList.add('open');
  document.body.style.overflow='hidden';
}
function fecharModalRemoverAdmin(){
  document.getElementById('modal-remover-admin').classList.remove('open');
  document.body.style.overflow='';
  _slugParaRemoverAdmin=null;
}
function fecharModalRemoverOverlayAdmin(e){
  if(e.target===document.getElementById('modal-remover-admin'))fecharModalRemoverAdmin();
}
function confirmarRemoverAdminFinal(){
  if(!_slugParaRemoverAdmin)return;
  const frm=document.getElementById('form-remover-'+_slugParaRemoverAdmin);
  if(frm)frm.submit();
}

(function iniciarPollingLote(){
  const params=new URLSearchParams(window.location.search);
  const painel=document.getElementById('lote-progresso');
  const texto=document.getElementById('lote-texto');
  const barra=document.getElementById('lote-barra');
  const errosEl=document.getElementById('lote-erros');
  if(!painel)return;
  async function checarStatus(){
    try{
      const r=await fetch('/api/lote/status');
      const s=await r.json();
      if(!s.emAndamento&&!s.total)return;
      painel.style.display='block';
      const pct=s.total?Math.round((s.concluidos/s.total)*100):0;
      barra.style.width=pct+'%';
      if(s.emAndamento){
        texto.textContent='Processando '+s.concluidos+' de '+s.total+'... atual: '+(s.atual||'—');
        setTimeout(checarStatus,3000);
      }else{
        texto.textContent='Lote finalizado — '+s.concluidos+' de '+s.total+' itens processados.';
        if(s.erros&&s.erros.length){errosEl.innerHTML=s.erros.map(e=>'⚠️ '+e).join('<br>');}
      }
    }catch(e){console.error('Falha ao consultar status do lote',e);}
  }
  if(params.get('lote')==='iniciado'){checarStatus();}else{checarStatus();}
})();
</script>
</body>
</html>`);
});

app.post("/admin/salvar", async (req, res) => {
    const { nome, url: urlRaw, tipo, instagram_url, geo, nicho, funil, original_slug } = req.body;
  if (!nome || !urlRaw) return res.redirect("/admin?erro=campos-obrigatorios");
  const tipoFinal = normalizeMonitoringType(tipo) || "pagina";
  const url = resolveMetaUrl(urlRaw, tipoFinal);
  if (!url) return res.redirect("/admin?erro=url-invalida");

  // Modo edição: atualiza o registro existente pelo slug original — o slug NUNCA muda,
  // mesmo que o nome de exibição mude, para preservar o vínculo com scrape_history/scrape_latest.
  if (original_slug && original_slug.trim()) {
    try {
      const keywordKey = tipoFinal === "keyword" ? normalizeKeywordIdentity(nome) : null;
      const { rowCount } = await query(
        `UPDATE pages SET nome=$1, url=$2, tipo=$3, instagram_url=$4, geo=$5, nicho=$6, funil=$7, keyword_key=$8 WHERE slug=$9`,
        [nome, url, tipoFinal, instagram_url || null, geo || null, nicho || null, funil || null, keywordKey, original_slug.trim()]
      );
      if (rowCount === 0) {
        console.warn(`[ADMIN] edição falhou — slug=${original_slug} não encontrado`);
        return res.redirect("/admin?erro=erro-interno");
      }
      console.log(`[ADMIN] editou slug=${original_slug}`);
      return res.redirect("/admin?ok=editado");
    } catch (err) {
      console.error("[ADMIN] erro ao editar:", err.message);
      return res.redirect("/admin?erro=erro-interno");
    }
  }

  // Modo cadastro (novo item)
  if (!toSlug(nome)) return res.redirect("/admin?erro=nome-invalido");
  try {
    const record = await saveMonitoringRecord({
      nome,
      url,
      tipo: tipoFinal,
      instagram_url: instagram_url || null,
      geo: geo || null,
      nicho: nicho || null,
      funil: funil || null,
    });
    console.log(`[ADMIN] cadastrou slug=${record.slug} tipo=${tipoFinal}`);
    await captureInicial(record.slug, url);
    res.redirect("/admin?ok=1");
  } catch (err) {
    console.error("[ADMIN] erro:", err.message);
    res.redirect("/admin?erro=erro-interno");
  }
});

app.post("/admin/remover", async (req, res) => {
  const { slug } = req.body;
  if (!slug) return res.redirect("/admin");
  await query("DELETE FROM pages WHERE slug=$1", [slug]);
  await query("DELETE FROM scrape_history WHERE slug=$1", [slug]);
  await query("DELETE FROM scrape_latest WHERE slug=$1", [slug]);
  console.log(`[ADMIN] removeu slug=${slug}`);
  res.redirect("/admin?ok=removido");
});

app.post("/admin/lote", async (req, res) => {
  const { itens: textoItens } = req.body;
  if (!textoItens || !textoItens.trim()) return res.redirect("/admin?erro=lote-vazio");
  const itens = parseLoteInput(textoItens);
  if (!itens.length) return res.redirect("/admin?erro=lote-invalido");
  if (loteStatus.emAndamento) return res.redirect("/admin?erro=lote-em-andamento");
  res.redirect("/admin?lote=iniciado");
  runLote(itens).catch((err) => console.error("[LOTE] erro não tratado:", err.message));
});

app.get("/api/lote/status", (_req, res) => {
  res.json(loteStatus);
});

// ─── Funis (modelo de grafo: nós + conexões) ────────────────────────────────

const TIPO_INFO = {
  ads:         { icon: "📢", label: "ADS" },
  advertorial: { icon: "📄", label: "Advertorial" },
  presell:     { icon: "🧲", label: "Presell" },
  tsl:         { icon: "📝", label: "TSL" },
  vsl:         { icon: "🎬", label: "VSL" },
  quiz:        { icon: "🧩", label: "Quiz" },
  whatsapp:    { icon: "💬", label: "WhatsApp" },
  checkout:    { icon: "💳", label: "Checkout" },
};
const TIPOS_ORDEM = ["ads", "advertorial", "presell", "tsl", "vsl", "quiz", "whatsapp", "checkout"];

// Computa todos os caminhos (raiz → folha) de um grafo de nós/conexões.
// Raiz = nó sem conexão de entrada. Folha = nó sem conexão de saída.
// Guarda contra ciclos interrompendo o caminho se o nó já apareceu nele.
function computarCaminhos(nodes, edges) {
  const nodesById = {};
  nodes.forEach(n => { nodesById[n.id] = n; });

  const adjOut = {};
  const adjIn = {};
  const adjUndir = {};
  nodes.forEach(n => {
    adjOut[n.id] = [];
    adjIn[n.id] = [];
    adjUndir[n.id] = [];
  });
  edges.forEach(e => {
    if (adjOut[e.from_node_id] && adjIn[e.to_node_id]) {
      adjOut[e.from_node_id].push(e.to_node_id);
      adjIn[e.to_node_id].push(e.from_node_id);
      adjUndir[e.from_node_id].push(e.to_node_id);
      adjUndir[e.to_node_id].push(e.from_node_id);
    }
  });

  const visited = new Set();
  const componentes = [];
  nodes.forEach(n => {
    if (visited.has(n.id)) return;
    if (adjUndir[n.id].length === 0) return;
    const comp = [];
    const queue = [n.id];
    visited.add(n.id);
    while (queue.length > 0) {
      const curr = queue.shift();
      comp.push(curr);
      adjUndir[curr].forEach(nxt => {
        if (!visited.has(nxt)) {
          visited.add(nxt);
          queue.push(nxt);
        }
      });
    }
    if (comp.length >= 2) componentes.push(comp);
  });

  const funisMapeados = [];
  componentes.forEach(comp => {
    const compSet = new Set(comp);
    let raizes = comp.filter(id => adjIn[id].filter(x => compSet.has(x)).length === 0);
    if (raizes.length === 0) raizes = [comp[0]];

    const levels = [];
    const assigned = new Set();
    let currentLevel = [...raizes];
    currentLevel.forEach(id => assigned.add(id));

    while (currentLevel.length > 0) {
      levels.push(currentLevel.map(id => nodesById[id]).filter(Boolean));
      const nextLevelSet = new Set();
      currentLevel.forEach(id => {
        adjOut[id].forEach(nxt => {
          if (compSet.has(nxt) && !assigned.has(nxt)) {
            nextLevelSet.add(nxt);
          }
        });
      });
      currentLevel = Array.from(nextLevelSet);
      currentLevel.forEach(id => assigned.add(id));
    }

    const remaining = comp.filter(id => !assigned.has(id)).map(id => nodesById[id]).filter(Boolean);
    if (remaining.length > 0) {
      if (levels.length > 0) levels[levels.length - 1].push(...remaining);
      else levels.push(remaining);
    }

    function buildTree(id, treeVisited = new Set()) {
      if (treeVisited.has(id)) return null;
      treeVisited.add(id);
      const node = nodesById[id];
      if (!node) return null;
      const childrenIds = (adjOut[id] || []).filter(nxt => compSet.has(nxt));
      const children = childrenIds
        .map(cid => buildTree(cid, new Set(treeVisited)))
        .filter(Boolean);
      return { node, children };
    }

    const rootTrees = raizes.map(rid => buildTree(rid)).filter(Boolean);
    const mainTree = rootTrees.length === 1 ? rootTrees[0] : { node: null, children: rootTrees };

    funisMapeados.push({
      tree: mainTree,
      levels: levels,
      allNodes: comp.map(id => nodesById[id]).filter(Boolean)
    });
  });

  return funisMapeados;
}

async function getNodesEdges(slug) {
  const { rows: nodes } = await query(
    `SELECT id, tipo, rotulo, url FROM funnel_nodes WHERE slug=$1 ORDER BY created_at ASC`, [slug]
  );
  let edges = [];
  if (nodes.length) {
    const ids = nodes.map(n => n.id);
    const { rows } = await query(
      `SELECT id, from_node_id, to_node_id FROM funnel_edges WHERE from_node_id = ANY($1) OR to_node_id = ANY($1)`,
      [ids]
    );
    edges = rows;
  }
  return { nodes, edges };
}

function renderChip(node) {
  const info = TIPO_INFO[node.tipo] || { icon: "🔗", label: node.tipo };
  return `<a href="${node.url}" target="_blank" rel="noopener" class="chip" title="Abrir ${node.rotulo} em nova guia">
    <span class="chip-icon">${info.icon}</span>
    <span class="chip-label">${node.rotulo}</span>
  </a>`;
}

function renderNotionLinearConnector() {
  return `<span class="flow-notion-arrow" style="display:inline-flex;align-items:center;margin:0 6px;flex-shrink:0">
    <svg width="24" height="12" viewBox="0 0 24 12" fill="none">
      <line x1="0" y1="6" x2="18" y2="6" stroke="#7c6fff" stroke-width="2" stroke-linecap="round"/>
      <path d="M15 2L20 6L15 10" stroke="#7c6fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </span>`;
}

function renderNotionBranchConnector() {
  return `<span class="flow-branch-arrow" style="display:inline-flex;align-items:center;margin-right:6px;flex-shrink:0">
    <svg width="10" height="12" viewBox="0 0 10 12" fill="none">
      <path d="M1 2L6 6L1 10" stroke="#a78bfa" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  </span>`;
}

function renderTreeNode(treeNode) {
  if (!treeNode) return "";
  if (!treeNode.node && treeNode.children) {
    return treeNode.children.map(renderTreeNode).join("");
  }
  const chipHtml = renderChip(treeNode.node);
  if (!treeNode.children || treeNode.children.length === 0) {
    return `<div class="flow-tree-node">${chipHtml}</div>`;
  }
  if (treeNode.children.length === 1) {
    return `<div class="flow-tree-node">${chipHtml}<div class="flow-linear-branch">${renderNotionLinearConnector()}${renderTreeNode(treeNode.children[0])}</div></div>`;
  }
  const branchesHtml = treeNode.children.map(child => `
    <div class="flow-tree-branch">
      ${renderNotionBranchConnector()}
      <div class="flow-branch-content">
        ${renderTreeNode(child)}
      </div>
    </div>
  `).join("");

  return `<div class="flow-tree-node">${chipHtml}<div class="flow-tree-branches">${branchesHtml}</div></div>`;
}

function renderCaminho(funilItem, idx = 0, isEditMode = false, explicitSlug = "") {
  let tree, levels, allNodes;
  if (funilItem && funilItem.tree) {
    tree = funilItem.tree;
    levels = funilItem.levels;
    allNodes = funilItem.allNodes;
  } else if (Array.isArray(funilItem)) {
    levels = Array.isArray(funilItem[0]) ? funilItem : funilItem.map(n => [n]);
    allNodes = levels.flat();
    tree = { node: allNodes[0], children: [] };
  } else {
    return "";
  }

  const slug = explicitSlug || allNodes[0]?.slug || "";
  const label = allNodes.map(n => n.rotulo).join(' \u2192 ');
  const levelsJson = JSON.stringify(levels.map(lvl => lvl.map(n => n.id)));
  const allIdsJson = JSON.stringify(allNodes.map(n => n.id));

  const treeHtml = renderTreeNode(tree);

  if (!isEditMode) {
    return `<div class="caminho-row flow-row-container" style="display:flex;align-items:center;overflow-x:auto;padding:14px 16px">
      ${treeHtml}
    </div>`;
  }

  return `<div class="caminho-row flow-row-container" style="display:flex;align-items:center;justify-content:space-between;gap:16px;overflow-x:auto;padding:14px 16px">
    <div style="display:flex;align-items:center">
      ${treeHtml}
    </div>
    <div style="display:flex;align-items:center;gap:8px;margin-left:auto;flex-shrink:0">
      <button type="button" class="btn-edit-sm" onclick='editarFunil(${levelsJson})' title="Editar este funil no construtor">\u270F\uFE0F Editar</button>
      <form id="form-rem-caminho-${idx}" method="POST" action="/admin/funis/remover-caminho" style="display:none">
        <input type="hidden" name="slug" value="${slug}">
        <input type="hidden" name="funil_node_ids" value='${allIdsJson}'>
      </form>
      <button type="button" class="btn-del-sm" onclick="abrirModalRemover('caminho', ${idx}, 'Excluir funil mapeado', '${label.replace(/'/g, "\\'")}')">\u2715 Excluir</button>
    </div>
  </div>`;
}

// Página de gerenciamento de nós/conexões de um player
app.get("/admin/funis/:slug", async (req, res) => {
  const { slug } = req.params;
  const { rows: pages } = await query("SELECT nome, url FROM pages WHERE slug=$1 LIMIT 1", [slug]);
  if (!pages.length) return res.status(404).send("Player não encontrado.");
  const nomePage = pages[0].nome;

  const { nodes, edges } = await getNodesEdges(slug);
  const nodesById = {};
  nodes.forEach(n => { nodesById[n.id] = n; });

  const optionsNodes = nodes.map(n => {
    const info = TIPO_INFO[n.tipo] || { icon: "🔗", label: n.tipo };
    return `<option value="${n.id}">${info.icon} ${n.rotulo} (${info.label})</option>`;
  }).join("");

  const optionsTipos = TIPOS_ORDEM.map(t => `<option value="${t}">${TIPO_INFO[t].icon} ${TIPO_INFO[t].label}</option>`).join("");

  const listaNodes = nodes.length ? nodes.map(n => {
    const info = TIPO_INFO[n.tipo] || { icon: "🔗", label: n.tipo };
    return `<div class="node-row">
      <span class="node-tipo">${info.icon} ${info.label}</span>
      <span class="node-rotulo">${n.rotulo}</span>
      <a href="${n.url}" target="_blank" class="node-url">${n.url.length > 45 ? n.url.slice(0,45)+'...' : n.url}</a>
      <form id="form-rem-node-${n.id}" method="POST" action="/admin/funis/remover-node" style="display:none">
        <input type="hidden" name="node_id" value="${n.id}">
        <input type="hidden" name="slug" value="${slug}">
      </form>
      <button type="button" class="btn-del-sm" onclick="abrirModalRemover('node', ${n.id}, 'Remover etapa', '${n.rotulo.replace(/'/g, "\\'")}')">✕</button>
    </div>`;
  }).join("") : '<div class="empty-hint-sm">Nenhuma etapa cadastrada ainda. Crie a primeira acima.</div>';

  const listaEdges = edges.length ? edges.map(e => {
    const de = nodesById[e.from_node_id], para = nodesById[e.to_node_id];
    if (!de || !para) return "";
    const label = de.rotulo + ' \u2192 ' + para.rotulo;
    return `<div class="edge-row">
      ${renderChip(de)}<span class="chip-arrow">→</span>${renderChip(para)}
      <form id="form-rem-${e.id}" method="POST" action="/admin/funis/remover-edge" style="display:none">
        <input type="hidden" name="edge_id" value="${e.id}">
        <input type="hidden" name="slug" value="${slug}">
      </form>
      <button type="button" class="btn-del-sm" style="margin-left:auto"
        onclick="abrirModalRemover(${e.id},'${label.replace(/'/g, "\\'")}')">&#x2715;</button>
    </div>`;
  }).join("") : '<div class="empty-hint-sm">Nenhuma conexão criada ainda.</div>';

  const caminhos = computarCaminhos(nodes, edges);
  const previaCaminhos = caminhos.length
    ? caminhos.map((c, idx) => renderCaminho(c, idx, true, slug)).join("")
    : '<div class="empty-hint-sm">Cadastre etapas e conecte-as para ver os funis mapeados aqui.</div>';

  const msgOk = (() => {
    const q = req.query;
    if (q.ok === "node-add") return '<div class="msg ok">✅ Etapa criada.</div>';
    if (q.ok === "node-rem") return '<div class="msg ok">🗑️ Etapa removida.</div>';
    if (q.ok === "edge-add") return '<div class="msg ok">✅ Conexão criada.</div>';
    if (q.ok === "edge-rem") return '<div class="msg ok">🗑️ Conexão removida.</div>';
    if (q.erro === "sem-etapas") return '<div class="msg err">⚠️ Cadastre pelo menos 2 etapas antes de conectar.</div>';
    if (q.erro === "mesma-etapa") return '<div class="msg err">⚠️ Uma etapa não pode se conectar a ela mesma.</div>';
    return "";
  })();

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Funil — ${nomePage}</title>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:900px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.hdr h1{font-size:17px;font-weight:700}
.hdr-sub{font-size:12px;color:var(--muted);margin-top:3px}
.hdr-nav{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.hdr-nav a{font-size:12px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;white-space:nowrap}
.hdr-nav a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:20px 22px;margin-bottom:18px}
.card h2{font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:16px}
.field{display:flex;flex-direction:column;gap:6px}
label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
input,select{background:#0f0f1e;border:1px solid var(--border);border-radius:8px;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;padding:9px 13px;outline:none;transition:border-color .2s}
input:focus,select:focus{border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.form-row{display:grid;grid-template-columns:180px 160px 1fr auto;gap:10px;align-items:end}
.form-row-edge{display:grid;grid-template-columns:1fr auto 1fr auto;gap:10px;align-items:end}
.chain-builder{display:flex;align-items:flex-end;flex-wrap:wrap;gap:8px}
.chain-elo{display:flex;flex-direction:column;gap:6px;min-width:165px;flex:1}
.elo-selects{display:flex;flex-direction:column;gap:6px}
.btn-elo-bifurcar{background:transparent;color:#a78bfa;border:1px dashed rgba(167,139,250,0.35);border-radius:6px;font-size:11px;font-weight:600;padding:5px 8px;cursor:pointer;margin-top:2px;width:100%;transition:all .15s}
.btn-elo-bifurcar:hover{background:rgba(167,139,250,0.12);border-color:#a78bfa;color:#fff}
.funil-seta-apple{display:inline-flex;align-items:center;color:#a78bfa;margin:0 5px;opacity:0.85}
.funil-seta-apple svg{display:block;filter:drop-shadow(0 0 4px rgba(167,139,250,0.4))}
.chain-arrow{color:var(--muted);font-size:16px;padding-bottom:10px;flex-shrink:0}
.btn-chain-add{background:transparent;color:var(--accent);border:1px solid var(--accent);border-radius:6px;font-size:18px;font-weight:700;width:32px;height:36px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;line-height:1;transition:all .15s;padding:0;margin-bottom:1px}
.btn-chain-add:hover{background:var(--accent);color:#fff}
.btn-chain-rem{background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:12px;width:32px;height:36px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center;line-height:1;transition:all .15s;padding:0;margin-bottom:1px}
.btn-chain-rem:hover{color:var(--down);border-color:var(--down)}
@media(max-width:700px){.form-row,.form-row-edge{grid-template-columns:1fr}.chain-builder{flex-direction:column}}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;font-family:'Space Grotesk',sans-serif;font-size:13px;font-weight:600;padding:9px 20px;cursor:pointer;white-space:nowrap}
.btn:hover{opacity:.85}
.btn-del-sm{background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:6px;font-size:11px;padding:5px 10px;cursor:pointer;line-height:1.4;transition:all .15s}
.btn-del-sm:hover{color:var(--down);border-color:var(--down);background:rgba(251,113,133,.08)}
.btn-edit-sm{background:rgba(167,139,250,.12);color:#a78bfa;border:1px solid rgba(167,139,250,.3);border-radius:6px;font-size:11px;padding:5px 10px;cursor:pointer;line-height:1.4;transition:all .15s;font-weight:600}
.btn-edit-sm:hover{background:var(--accent);color:#fff}
.btn-insert-mid{background:transparent;color:var(--accent);border:1px dashed var(--accent);border-radius:5px;font-size:11px;font-weight:700;padding:2px 7px;cursor:pointer;line-height:1.2;transition:all .15s}
.btn-insert-mid:hover{background:var(--accent);color:#fff}
.node-row{display:flex;align-items:center;gap:10px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:9px 12px;margin-bottom:8px;flex-wrap:wrap}
.node-tipo{font-size:11px;font-weight:600;color:#a78bfa;background:rgba(167,139,250,.12);padding:3px 9px;border-radius:5px;white-space:nowrap}
.node-rotulo{font-size:13px;font-weight:700;color:#fff}
.node-url{font-size:11px;color:var(--accent);text-decoration:none;font-family:'Space Mono',monospace;margin-left:auto}
.node-url:hover{text-decoration:underline}
.edge-row{display:flex;align-items:center;gap:6px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:9px 12px;margin-bottom:8px;flex-wrap:wrap}
.chip{display:inline-flex;align-items:center;gap:5px;background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:5px 10px;text-decoration:none;font-size:12px;font-weight:600;color:#fff}
.chip:hover{border-color:var(--accent)}
.chip-icon{font-size:13px}
.chip-arrow{color:var(--muted);font-size:15px;font-weight:700;margin:0 2px}
.caminho-row{display:flex;align-items:center;flex-wrap:wrap;gap:2px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:8px}
.flow-tree-node{display:inline-flex;align-items:center}
.flow-linear-branch{display:inline-flex;align-items:center}
.flow-tree-branches{display:flex;flex-direction:column;justify-content:center;gap:12px;position:relative;margin-left:8px;padding-left:18px}
.flow-tree-branches::before{content:'';position:absolute;left:0;top:16px;bottom:16px;width:2px;background:rgba(167,139,250,0.45);border-radius:2px}
.flow-tree-branch{display:inline-flex;align-items:center;position:relative}
.flow-tree-branch::before{content:'';position:absolute;left:-18px;top:50%;width:12px;height:2px;background:rgba(167,139,250,0.45)}
.flow-branch-content{display:inline-flex;align-items:center}
.flow-row-container{min-height:54px}
.empty-hint-sm{color:var(--muted);font-size:12px;text-align:center;padding:16px;border:1px dashed var(--border);border-radius:8px}
.msg{padding:11px 14px;border-radius:8px;font-size:13px;margin-bottom:16px}
.msg.ok{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.25)}
.msg.err{background:rgba(251,113,133,.12);color:#fb7185;border:1px solid rgba(251,113,133,.25)}
.divider{border:none;border-top:1px solid var(--border);margin:14px 0}
/* Modal Apple-style */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);display:flex;align-items:center;justify-content:center;z-index:9999;opacity:0;pointer-events:none;transition:opacity .2s ease}
.modal-overlay.open{opacity:1;pointer-events:all}
.modal-card{background:#1c1c2e;border:1px solid rgba(255,255,255,.1);border-radius:20px;padding:30px 28px 24px;max-width:340px;width:calc(100% - 40px);box-shadow:0 40px 100px rgba(0,0,0,.7),0 0 0 1px rgba(255,255,255,.06);transform:scale(.92) translateY(12px);transition:transform .28s cubic-bezier(.34,1.4,.64,1),opacity .22s ease;opacity:0;text-align:center}
.modal-overlay.open .modal-card{transform:scale(1) translateY(0);opacity:1}
.modal-icon{font-size:38px;margin-bottom:14px;line-height:1}
.modal-title{font-size:16px;font-weight:700;color:#fff;margin-bottom:8px;letter-spacing:-.2px}
.modal-desc{font-size:13px;color:#8888aa;margin-bottom:24px;line-height:1.6;word-break:break-all}
.modal-actions{display:flex;gap:10px}
.modal-btn-cancel{flex:1;background:rgba(255,255,255,.07);color:#e0e0f0;border:1px solid rgba(255,255,255,.1);border-radius:12px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:600;padding:12px 0;cursor:pointer;transition:background .15s}
.modal-btn-cancel:hover{background:rgba(255,255,255,.13)}
.modal-btn-confirm{flex:1;background:#fb7185;color:#fff;border:none;border-radius:12px;font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:700;padding:12px 0;cursor:pointer;transition:background .15s,transform .1s}
.modal-btn-confirm:hover{background:#f43f5e}
.modal-btn-confirm:active{transform:scale(.97)}
</style>
</head>
<body>
<!-- Modal Apple-style universal -->
<div class="modal-overlay" id="modal-remover-apple" onclick="fecharModalAppleOverlay(event)">
  <div class="modal-card">
    <div class="modal-icon">🗑️</div>
    <h3 class="modal-title" id="modal-apple-title">Confirmar exclusão</h3>
    <p class="modal-desc" id="modal-apple-desc"></p>
    <div class="modal-actions">
      <button class="modal-btn-cancel" onclick="fecharModalApple()">Cancelar</button>
      <button class="modal-btn-confirm" onclick="confirmarRemoverApple()">Excluir</button>
    </div>
  </div>
</div>
<div class="hdr">
  <div>
    <h1>🔀 Funil — ${nomePage}</h1>
    <div class="hdr-sub">Mapeamento de etapas e conexões</div>
  </div>
  <div class="hdr-nav">
    <a href="/admin">⚙️ Admin</a>
    <a href="/dashboard">📊 Dashboard</a>
    <a href="/funis">🔀 Ver Mapa de Funis</a>
  </div>
</div>

${msgOk}

<div class="card">
  <h2>➕ Nova etapa</h2>
  <form method="POST" action="/admin/funis/add-node">
    <input type="hidden" name="slug" value="${slug}">
    <div class="form-row">
      <div class="field"><label>Tipo</label><select name="tipo">${optionsTipos}</select></div>
      <div class="field"><label>Rótulo</label><input type="text" name="rotulo" placeholder="Ex: TSL2, Checkout1" required></div>
      <div class="field"><label>URL</label><input type="url" name="url" placeholder="https://..." required></div>
      <button type="submit" class="btn">Adicionar</button>
    </div>
  </form>
</div>

<div class="card">
  <h2>📋 Etapas cadastradas (${nodes.length})</h2>
  ${listaNodes}
</div>

<div class="card" id="card-conectar-etapas">
  <h2>🔗 Conectar etapas</h2>
  ${nodes.length < 2
    ? '<div class="empty-hint-sm">Cadastre pelo menos 2 etapas para poder conectá-las.</div>'
    : `<form method="POST" action="/admin/funis/add-edge" id="form-add-edge">
        <input type="hidden" name="slug" value="${slug}">
        <div style="font-size:11px;color:var(--muted);margin-bottom:12px;line-height:1.5">
          Monte a sequência completa do funil. Cada etapa conecta à próxima em cadeia.
        </div>
        <input type="hidden" name="chain_steps_json" id="chain_steps_json">
        <div class="chain-builder" id="chain-builder">
          <div class="chain-elo">
            <label>Etapa 1</label>
            <div class="elo-selects">
              <select name="chain_ids[]">${optionsNodes}</select>
            </div>
            <button type="button" class="btn-elo-bifurcar" onclick="bifurcarElo(this)" title="Adicionar bifurcação nesta etapa">🔀 Bifurcar</button>
          </div>
          <div class="chain-arrow" id="chain-arrow-1">→</div>
          <div class="chain-elo" id="chain-elo-2">
            <label>Etapa 2</label>
            <div class="elo-selects">
              <select name="chain_ids[]">${optionsNodes}</select>
            </div>
            <button type="button" class="btn-elo-bifurcar" onclick="bifurcarElo(this)" title="Adicionar bifurcação nesta etapa">🔀 Bifurcar</button>
          </div>
          <button type="button" class="btn-chain-add" id="btn-chain-add" onclick="adicionarElo()" title="Adicionar próxima etapa na cadeia">+</button>
        </div>
        <div style="margin-top:14px;display:flex;justify-content:flex-end">
          <button type="submit" class="btn">Conectar Cadeia</button>
        </div>
      </form>`}
</div>

<div class="card">
  <h2>✨ FUNIS MAPEADOS (${caminhos.length})</h2>
  ${previaCaminhos}
</div>

<script>
var _chainCount=2;
var _optionsHtml=(function(){
  var sel=document.querySelector('#chain-builder select');
  return sel?sel.innerHTML:'';
})();
function adicionarElo(val){
  _chainCount++;
  var builder=document.getElementById('chain-builder');
  var addBtn=document.getElementById('btn-chain-add');
  var arrow=document.createElement('div');
  arrow.className='chain-arrow';
  arrow.textContent='\u2192';
  var elo=document.createElement('div');
  elo.className='chain-elo';
  elo.id='chain-elo-'+_chainCount;
  var lbl=document.createElement('label');
  lbl.textContent='Etapa '+_chainCount;
  var selWrap=document.createElement('div');
  selWrap.className='elo-selects';
  var sel=document.createElement('select');
  sel.name='chain_ids[]';
  sel.innerHTML=_optionsHtml;
  if(val) sel.value=val;
  selWrap.appendChild(sel);
  var bifBtn=document.createElement('button');
  bifBtn.type='button';
  bifBtn.className='btn-elo-bifurcar';
  bifBtn.textContent='🔀 Bifurcar';
  bifBtn.title='Adicionar bifurcação nesta etapa';
  bifBtn.onclick=function(){bifurcarElo(bifBtn);};
  var remBtn=document.createElement('button');
  remBtn.type='button';
  remBtn.className='btn-chain-rem';
  remBtn.textContent='\u2715';
  remBtn.title='Remover esta etapa';
  remBtn.onclick=function(){arrow.remove();elo.remove();renumerarCadeia();};
  elo.appendChild(lbl);
  elo.appendChild(selWrap);
  elo.appendChild(bifBtn);
  builder.insertBefore(arrow,addBtn);
  builder.insertBefore(elo,addBtn);
  builder.insertBefore(remBtn,addBtn);
}
function bifurcarElo(btnEl){
  var elo=btnEl.closest('.chain-elo');
  var wrap=elo.querySelector('.elo-selects');
  var row=document.createElement('div');
  row.style.cssText='display:flex;align-items:center;gap:4px;margin-top:4px';
  var sel=document.createElement('select');
  sel.name='chain_ids[]';
  sel.innerHTML=_optionsHtml;
  var rm=document.createElement('button');
  rm.type='button';
  rm.className='btn-chain-rem';
  rm.style.padding='2px 6px';
  rm.textContent='\u2715';
  rm.onclick=function(){row.remove();};
  row.appendChild(sel);
  row.appendChild(rm);
  wrap.appendChild(row);
}
function renumerarCadeia(){
  var elos=document.querySelectorAll('#chain-builder .chain-elo label');
  elos.forEach(function(lbl, i){ lbl.textContent='Etapa '+(i+1); });
}
function editarFunil(levels){
  var builder=document.getElementById('chain-builder');
  if(!builder) return;
  if(!Array.isArray(levels[0])) levels = levels.map(function(id){ return [id]; });
  
  builder.innerHTML = '';
  _chainCount = 0;
  levels.forEach(function(stepIds, idx){
    if(idx > 0){
      var arrow=document.createElement('div');
      arrow.className='chain-arrow';
      arrow.textContent='\u2192';
      builder.appendChild(arrow);
    }
    _chainCount++;
    var elo=document.createElement('div');
    elo.className='chain-elo';
    elo.id='chain-elo-'+_chainCount;
    var lbl=document.createElement('label');
    lbl.textContent='Etapa '+_chainCount;
    var selWrap=document.createElement('div');
    selWrap.className='elo-selects';
    
    stepIds.forEach(function(nodeId, sIdx){
      var row=document.createElement('div');
      row.style.cssText=sIdx===0 ? '' : 'display:flex;align-items:center;gap:4px;margin-top:4px';
      var sel=document.createElement('select');
      sel.name='chain_ids[]';
      sel.innerHTML=_optionsHtml;
      sel.value=nodeId;
      if(sIdx===0){
        selWrap.appendChild(sel);
      } else {
        var rm=document.createElement('button');
        rm.type='button';
        rm.className='btn-chain-rem';
        rm.style.padding='2px 6px';
        rm.textContent='\u2715';
        rm.onclick=function(){row.remove();};
        row.appendChild(sel);
        row.appendChild(rm);
        selWrap.appendChild(row);
      }
    });

    var bifBtn=document.createElement('button');
    bifBtn.type='button';
    bifBtn.className='btn-elo-bifurcar';
    bifBtn.textContent='🔀 Bifurcar';
    bifBtn.onclick=function(){bifurcarElo(bifBtn);};

    elo.appendChild(lbl);
    elo.appendChild(selWrap);
    elo.appendChild(bifBtn);

    if(_chainCount > 2){
      var remEloBtn=document.createElement('button');
      remEloBtn.type='button';
      remEloBtn.className='btn-chain-rem';
      remEloBtn.textContent='\u2715';
      remEloBtn.title='Remover esta etapa';
      remEloBtn.onclick=function(){elo.previousSibling?.remove();elo.remove();renumerarCadeia();};
      elo.appendChild(remEloBtn);
    }

    builder.appendChild(elo);
  });

  var addBtn=document.createElement('button');
  addBtn.type='button';
  addBtn.className='btn-chain-add';
  addBtn.id='btn-chain-add';
  addBtn.onclick=function(){adicionarElo();};
  addBtn.textContent='+';
  addBtn.title='Adicionar próxima etapa na cadeia';
  builder.appendChild(addBtn);

  var card=document.getElementById('card-conectar-etapas');
  if(card){
    card.scrollIntoView({behavior:'smooth',block:'center'});
    card.style.transition='box-shadow 0.4s ease, border-color 0.4s ease';
    card.style.borderColor='var(--accent)';
    card.style.boxShadow='0 0 0 2px rgba(167,139,250,0.4)';
    setTimeout(function(){ card.style.borderColor=''; card.style.boxShadow=''; }, 1500);
  }
}
function editarCaminho(ids){ editarFunil(ids); }

/* ── Modal Apple-style para remover ── */
var _itemToRemove=null;
function abrirModalRemover(tipo,id,titulo,descricao){
  _itemToRemove={tipo:tipo, id:id};
  document.getElementById('modal-apple-title').textContent=titulo;
  document.getElementById('modal-apple-desc').textContent=descricao;
  var m=document.getElementById('modal-remover-apple');
  m.classList.add('open');
  document.body.style.overflow='hidden';
}
function fecharModalApple(){
  var m=document.getElementById('modal-remover-apple');
  m.classList.remove('open');
  document.body.style.overflow='';
}
function fecharModalAppleOverlay(e){
  if(e.target===document.getElementById('modal-remover-apple')) fecharModalApple();
}
function confirmarRemoverApple(){
  if(!_itemToRemove) return;
  sessionStorage.setItem('funil_scroll_y', window.scrollY);
  var frm = document.getElementById('form-rem-'+_itemToRemove.tipo+'-'+_itemToRemove.id);
  if(frm) frm.submit();
}
/* Restaurar posição do scroll após redirect sem pular para o topo */
function restoreScrollPosition(){
  var y=sessionStorage.getItem('funil_scroll_y');
  if(y!==null){
    window.scrollTo({top:parseInt(y,10),behavior:'instant'});
    sessionStorage.removeItem('funil_scroll_y');
  }
}
restoreScrollPosition();
window.addEventListener('DOMContentLoaded', function(){
  restoreScrollPosition();
  var frm = document.getElementById('form-add-edge');
  if(frm){
    frm.addEventListener('submit', function(){
      var elos = document.querySelectorAll('#chain-builder .chain-elo');
      var steps = [];
      elos.forEach(function(elo){
        var selects = elo.querySelectorAll('select');
        var ids = [];
        selects.forEach(function(s){
          if(s.value && ids.indexOf(s.value) === -1) ids.push(s.value);
        });
        if(ids.length > 0) steps.push(ids);
      });
      var hidden = document.getElementById('chain_steps_json');
      if(hidden) hidden.value = JSON.stringify(steps);
    });
  }
});
</script>
</body>
</html>`);
});

app.post("/admin/funis/add-node", async (req, res) => {
  const { slug, tipo, rotulo, url } = req.body;
  if (!slug || !tipo || !rotulo || !url) return res.redirect(`/admin/funis/${slug || ""}`);
  if (!TIPOS_ORDEM.includes(tipo)) return res.redirect(`/admin/funis/${slug}`);
  await query(`INSERT INTO funnel_nodes (slug, tipo, rotulo, url) VALUES ($1,$2,$3,$4)`, [slug, tipo, rotulo, url]);
  console.log(`[FUNIS] add-node slug=${slug} tipo=${tipo} rotulo=${rotulo}`);
  res.redirect(`/admin/funis/${slug}?ok=node-add`);
});

app.post("/admin/funis/remover-node", async (req, res) => {
  const { node_id, slug } = req.body;
  if (!node_id) return res.redirect("/admin");
  await query(`DELETE FROM funnel_nodes WHERE id=$1`, [node_id]);
  console.log(`[FUNIS] remover-node node_id=${node_id}`);
  res.redirect(`/admin/funis/${slug}?ok=node-rem`);
});

app.post("/admin/funis/add-edge", async (req, res) => {
  const { slug, chain_steps_json } = req.body;
  if (!slug) return res.redirect("/admin");

  let steps = [];
  try {
    steps = JSON.parse(chain_steps_json || "[]");
  } catch (e) {}

  if (!steps.length && req.body.chain_ids) {
    let chain = req.body.chain_ids;
    if (!Array.isArray(chain)) chain = chain ? [chain] : [];
    steps = chain.map(id => [String(id)]).filter(arr => arr[0]);
  }

  if (steps.length < 2) return res.redirect(`/admin/funis/${slug}?erro=sem-etapas`);

  for (let i = 0; i < steps.length - 1; i++) {
    const fromNodes = steps[i];
    const toNodes = steps[i + 1];
    for (const from of fromNodes) {
      for (const to of toNodes) {
        if (from === to) continue;
        const { rows: exist } = await query(
          `SELECT id FROM funnel_edges WHERE from_node_id=$1 AND to_node_id=$2 LIMIT 1`,
          [from, to]
        );
        if (!exist.length) {
          await query(`INSERT INTO funnel_edges (from_node_id, to_node_id) VALUES ($1,$2)`, [from, to]);
          console.log(`[FUNIS] add-edge ${from} -> ${to}`);
        }
      }
    }
  }
  res.redirect(`/admin/funis/${slug}?ok=edge-add`);
});

app.post("/admin/funis/remover-edge", async (req, res) => {
  const { edge_id, slug } = req.body;
  if (!edge_id) return res.redirect("/admin");
  await query(`DELETE FROM funnel_edges WHERE id=$1`, [edge_id]);
  console.log(`[FUNIS] remover-edge edge_id=${edge_id}`);
  res.redirect(`/admin/funis/${slug}?ok=edge-rem`);
});

app.post("/admin/funis/remover-caminho", async (req, res) => {
  const { slug, funil_node_ids } = req.body;
  let ids = [];
  try {
    ids = JSON.parse(funil_node_ids || "[]");
  } catch (e) {}

  if (!ids.length && req.body.chain_ids) {
    let chain = req.body.chain_ids;
    if (!Array.isArray(chain)) chain = chain ? [chain] : [];
    ids = chain.map(String).filter(Boolean);
  }

  if (!slug || ids.length < 2) return res.redirect(`/admin/funis/${slug || ""}`);

  for (const from of ids) {
    for (const to of ids) {
      if (from === to) continue;
      await query(`DELETE FROM funnel_edges WHERE from_node_id=$1 AND to_node_id=$2`, [from, to]);
    }
  }
  console.log(`[FUNIS] remover-caminho funil unificado:`, ids);
  res.redirect(`/admin/funis/${slug}?ok=edge-rem`);
});

// AUDITORIA (salvar-anúncio via botão "Ações" do card, extensão): antes, este endpoint
// exigia SEMPRE um `rotulo` explícito no corpo da requisição (400 se ausente), porque os
// únicos chamadores eram fluxos que já pediam o rótulo ao operador (o construtor de funil
// multi-etapas). O novo botão "📢 Salvar Anúncio" do dropdown "Ações" de cada card salva em
// 1 clique, sem abrir modal nenhum — não existe rótulo pra pedir. Regra nova: `rotulo`
// continua obrigatório para todo `tipo`, EXCETO 'ads': quando `tipo === 'ads'` e nenhum
// `rotulo` é enviado, o PRÓPRIO SERVIDOR gera "ads01", "ads02", "ads03"... contando quantos
// nós `tipo='ads'` já existem para aquele `slug`. A geração fica no servidor (nunca no
// content.js) de propósito: evita que dois cliques rápidos em anúncios diferentes gerem o
// mesmo número por uma corrida no cliente — a contagem e o INSERT acontecem em sequência
// dentro da mesma requisição no servidor.
// Também: quando o node já existe (mesmo slug+url — ex: o operador clica "Salvar Anúncio"
// duas vezes no mesmo card), só atualiza tipo/rótulo se um rótulo EXPLÍCITO foi enviado —
// nunca renumera um "adsNN" que já foi salvo antes, e a resposta devolve o rótulo final
// usado (`rotulo`) para o pop-up de confirmação da extensão poder exibi-lo.
app.post("/api/funis/salvar-node", async (req, res) => {
  const { slug, tipo, rotulo: rotuloRaw, url: urlRaw, checkout_url: checkoutRaw } = req.body;
  if (!slug || !tipo || !urlRaw) {
    return res.status(400).json({ error: "Missing required fields" });
  }
  if (!rotuloRaw && tipo !== "ads") {
    return res.status(400).json({ error: "Missing required fields" });
  }
  const url = normalizeUrl(urlRaw);
  const checkout_url = checkoutRaw ? normalizeUrl(checkoutRaw) : checkoutRaw;

  try {
    // 1. Resolve ou cria o nó da Landing Page (ou do Anúncio, quando tipo='ads')
    let landingNodeId;
    let rotuloResolvido = rotuloRaw || null;
    const { rows: existingLanding } = await query(
      "SELECT id, rotulo FROM funnel_nodes WHERE slug = $1 AND url = $2 LIMIT 1",
      [slug, url]
    );

    if (existingLanding.length > 0) {
      landingNodeId = existingLanding[0].id;
      rotuloResolvido = existingLanding[0].rotulo;
      // Só atualiza tipo/rótulo se um rótulo EXPLÍCITO veio na requisição — para um
      // salvamento rápido de 'ads' sem rótulo (o caso normal do botão da extensão), o node
      // já existente mantém o rótulo original, nunca é renumerado.
      if (rotuloRaw) {
        await query(
          "UPDATE funnel_nodes SET tipo = $1, rotulo = $2 WHERE id = $3",
          [tipo, rotuloRaw, landingNodeId]
        );
        rotuloResolvido = rotuloRaw;
      }
    } else {
      let rotuloFinal = rotuloRaw;
      if (!rotuloFinal && tipo === "ads") {
        const { rows: countRows } = await query(
          "SELECT COUNT(*)::int AS n FROM funnel_nodes WHERE slug = $1 AND tipo = 'ads'",
          [slug]
        );
        const proximoNumero = (countRows[0]?.n || 0) + 1;
        rotuloFinal = `ads${String(proximoNumero).padStart(2, "0")}`;
      }
      const { rows: newLanding } = await query(
        "INSERT INTO funnel_nodes (slug, tipo, rotulo, url) VALUES ($1, $2, $3, $4) RETURNING id",
        [slug, tipo, rotuloFinal, url]
      );
      landingNodeId = newLanding[0].id;
      rotuloResolvido = rotuloFinal;
    }

    // 2. Se houver checkout preenchido, resolve o nó do checkout e conecta
    if (checkout_url && checkout_url.trim()) {
      let checkoutNodeId;
      const cleanCheckout = checkout_url.trim();

      const { rows: existingCheckout } = await query(
        "SELECT id FROM funnel_nodes WHERE slug = $1 AND url = $2 LIMIT 1",
        [slug, cleanCheckout]
      );

      if (existingCheckout.length > 0) {
        checkoutNodeId = existingCheckout[0].id;
      } else {
        const { rows: newCheckout } = await query(
          "INSERT INTO funnel_nodes (slug, tipo, rotulo, url) VALUES ($1, 'checkout', 'Checkout', $2) RETURNING id",
          [slug, cleanCheckout]
        );
        checkoutNodeId = newCheckout[0].id;
      }

      // 3. Cria a conexão (edge) entre a Landing Page e o Checkout se não existir
      const { rows: existingEdge } = await query(
        "SELECT id FROM funnel_edges WHERE from_node_id = $1 AND to_node_id = $2 LIMIT 1",
        [landingNodeId, checkoutNodeId]
      );

      if (existingEdge.length === 0) {
        await query(
          "INSERT INTO funnel_edges (from_node_id, to_node_id) VALUES ($1, $2)",
          [landingNodeId, checkoutNodeId]
        );
        console.log(`[FUNIL] Conectou nó ${landingNodeId} ao checkout ${checkoutNodeId}`);
      }
    }

    res.json({ success: true, landingNodeId, rotulo: rotuloResolvido });
  } catch (err) {
    console.error("[API] Error saving funnel node:", err.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Página de visão geral — mapa de funis de todos os players
app.get("/funis", async (_req, res) => {
  const { rows: pages } = await query(
    "SELECT slug, nome, url, tipo FROM pages ORDER BY created_at DESC"
  );

  const { rows: allNodes } = await query(`SELECT id, slug, tipo, rotulo, url FROM funnel_nodes`);
  const { rows: allEdges } = await query(`SELECT id, from_node_id, to_node_id FROM funnel_edges`);

  const nodesBySlug = {};
  allNodes.forEach(n => { (nodesBySlug[n.slug] ||= []).push(n); });

  const nodeIdToSlug = {};
  allNodes.forEach(n => { nodeIdToSlug[n.id] = n.slug; });
  const edgesBySlug = {};
  allEdges.forEach(e => {
    const s = nodeIdToSlug[e.from_node_id];
    if (s) (edgesBySlug[s] ||= []).push(e);
  });

  const comMapa = [];
  const semMapa = [];
  for (const p of pages) {
    const nodes = nodesBySlug[p.slug] || [];
    if (nodes.length === 0) { semMapa.push(p); continue; }
    const edges = edgesBySlug[p.slug] || [];
    const caminhos = computarCaminhos(nodes, edges);
    comMapa.push({ ...p, caminhos });
  }

  const cardsHtml = comMapa.map(p => `
    <div class="player-card">
      <div class="player-hdr">
        <span class="player-tipo-badge ${p.tipo === "dominio" ? "b-dom" : p.tipo === "keyword" ? "b-key" : "b-pag"}">${p.tipo === "dominio" ? "🌐" : p.tipo === "keyword" ? "🔑" : "📡"}</span>
        <a href="${p.url}" target="_blank" rel="noopener" class="player-nome">${p.nome}</a>
        <a href="/admin/funis/${p.slug}" class="player-edit-link">✏️ Editar</a>
      </div>
      <div class="player-caminhos">
        ${p.caminhos.map((c, idx) => renderCaminho(c, idx, false)).join("")}
      </div>
    </div>`).join("");

  const semMapaHtml = semMapa.length ? `
    <div class="card" style="margin-top:8px">
      <h2>💤 Sem funil mapeado ainda (${semMapa.length})</h2>
      <div class="sem-mapa-list">
        ${semMapa.map(p => `<a href="/admin/funis/${p.slug}" class="sem-mapa-item">${p.tipo === "dominio" ? "🌐" : p.tipo === "keyword" ? "🔑" : "📡"} ${p.nome}</a>`).join("")}
      </div>
    </div>` : "";

  res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mapa de Funis — Lowticket Monitor</title>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--down:#fb7185}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',sans-serif;padding:24px;max-width:1100px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:14px;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.hdr h1{font-size:19px;font-weight:700}
.hdr-sub{font-size:12px;color:var(--muted);margin-top:3px}
.hdr-nav{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.hdr-nav a{font-size:12px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;white-space:nowrap}
.hdr-nav a:hover{background:var(--accent);color:#fff}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:20px 22px;margin-bottom:18px}
.card h2{font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:14px}
.player-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px 20px;margin-bottom:14px}
.player-hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.player-tipo-badge{font-size:15px}
.player-nome{font-size:15px;font-weight:700;color:#fff;text-decoration:none}
.player-nome:hover{color:var(--accent)}
.player-edit-link{margin-left:auto;font-size:11px;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:4px 10px;border-radius:6px;white-space:nowrap}
.player-edit-link:hover{background:var(--accent);color:#fff}
.player-caminhos{display:flex;flex-direction:column;gap:8px}
.caminho-row{display:flex;align-items:center;flex-wrap:wrap;gap:2px;background:#0f0f1e;border:1px solid var(--border);border-radius:8px;padding:10px 12px}
.flow-tree-node{display:inline-flex;align-items:center}
.flow-linear-branch{display:inline-flex;align-items:center}
.flow-tree-branches{display:flex;flex-direction:column;justify-content:center;gap:12px;position:relative;margin-left:8px;padding-left:18px}
.flow-tree-branches::before{content:'';position:absolute;left:0;top:16px;bottom:16px;width:2px;background:rgba(167,139,250,0.45);border-radius:2px}
.flow-tree-branch{display:inline-flex;align-items:center;position:relative}
.flow-tree-branch::before{content:'';position:absolute;left:-18px;top:50%;width:12px;height:2px;background:rgba(167,139,250,0.45)}
.flow-branch-content{display:inline-flex;align-items:center}
.flow-row-container{min-height:54px}
.chip{display:inline-flex;align-items:center;gap:5px;background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:5px 10px;text-decoration:none;font-size:12px;font-weight:600;color:#fff}
.chip:hover{border-color:var(--accent)}
.chip-icon{font-size:13px}
.chip-arrow{color:var(--muted);font-size:15px;font-weight:700;margin:0 2px}
.empty-state{color:var(--muted);font-size:13px;text-align:center;padding:32px;border:1px dashed var(--border);border-radius:12px}
.sem-mapa-list{display:flex;flex-wrap:wrap;gap:8px}
.sem-mapa-item{font-size:12px;color:var(--muted);text-decoration:none;background:#0f0f1e;border:1px solid var(--border);border-radius:7px;padding:6px 12px}
.sem-mapa-item:hover{color:var(--accent);border-color:var(--accent)}
@media(max-width:640px){.hdr-nav{width:100%}.hdr-nav a{flex:1;text-align:center}}
</style>
</head>
<body>
<div class="hdr">
  <div>
    <h1>🔀 Mapa de Funis</h1>
    <div class="hdr-sub">Visão geral de todos os caminhos mapeados por biblioteca/domínio</div>
  </div>
  <div class="hdr-nav">
    <a href="/admin">⚙️ Admin</a>
    <a href="/dashboard">📊 Dashboard</a>
  </div>
</div>

${comMapa.length === 0
  ? '<div class="empty-state">Nenhum funil mapeado ainda. Vá em Admin → 🔀 Funis num player para começar.</div>'
  : cardsHtml}

${semMapaHtml}

</body>
</html>`);
});

// ─── Dashboard ───────────────────────────────────────────────────────────────

app.get("/dashboard", async (_req, res) => {
  try {
    const { rows: allPages } = await query(
      "SELECT slug, nome, url, tipo, created_at, inicial_count, instagram_url, geo, nicho, funil FROM pages"
    );

    const BR_OFFSET_MS = 3 * 60 * 60 * 1000;
    function toBrDate(utcNaiveTimestamp) {
      return new Date(new Date(utcNaiveTimestamp).getTime() - BR_OFFSET_MS);
    }

    async function processarGrupo(pagesDoGrupo) {
      const ultimaLeitura = {};
      const primeiraData = {};
      const paginas = {};
      const mon = {};
      const meta = {}; // geo, nicho, funil por nome
      const slugsDoGrupo = pagesDoGrupo.map((page) => page.slug);
      const attemptsBySlug = new Map();
      if (slugsDoGrupo.length > 0) {
        const { rows: attempts } = await query(`
          SELECT DISTINCT ON (slug)
                 slug, status, error,
                 COALESCE(completed_at, started_at) AS checked_at,
                 COALESCE(
                   COALESCE(completed_at, started_at) > NOW() - INTERVAL '6 hours'
                   AND status LIKE 'falha_%',
                   FALSE
                 ) AS alert_relevant
          FROM scrape_attempts
          WHERE slug = ANY($1)
          ORDER BY slug, started_at DESC, id DESC
        `, [slugsDoGrupo]);
        for (const attempt of attempts) attemptsBySlug.set(attempt.slug, attempt);
      }

      for (const p of pagesDoGrupo) {
        const { rows: hist } = await query(
          `SELECT ads_count, slot, collected_at
           FROM scrape_history WHERE slug=$1 ORDER BY collected_at ASC`,
          [p.slug]
        );

        const { rows: latest } = await query(
          `SELECT ads_count, collected_at FROM scrape_latest WHERE slug = $1 LIMIT 1`,
          [p.slug]
        );

        const latestRow = latest[0];
        const latestAttempt = attemptsBySlug.get(p.slug);

        ultimaLeitura[p.nome] = {
          slug:          p.slug,
          ads:          latestRow ? latestRow.ads_count : (hist.length ? hist[hist.length - 1].ads_count : null),
          url:          p.url,
          ultimaColeta: latestRow
            ? new Date(latestRow.collected_at).toISOString()
            : null,
          tentativa:    latestAttempt ? {
            status: latestAttempt.status,
            error: latestAttempt.error,
            at: latestAttempt.checked_at ? new Date(latestAttempt.checked_at).toISOString() : null,
            relevante: Boolean(latestAttempt.alert_relevant),
          } : null,
        };

        primeiraData[p.nome] = toBrDate(p.created_at).toISOString().slice(0, 10);

        mon[p.nome] = {
          ini: p.inicial_count ?? (hist.length ? hist[0].ads_count : (latestRow ? latestRow.ads_count : null))
        };

        // Metadados para a dashboard
        meta[p.nome] = {
          instagram_url: p.instagram_url || null,
          geo:           p.geo || null,
          nicho:         p.nicho || null,
          funil:         p.funil || null,
        };

        paginas[p.nome] = {};
        for (const h of hist) {
          const brDt = toBrDate(h.collected_at);
          const dk = brDt.toISOString().slice(0, 10);
          const legacySlots = p.tipo === "keyword" ? [6] : p.tipo === "dominio" ? [3, 5, 12, 22] : [3, 12, 22];
          const slot = (h.slot !== null && h.slot !== undefined)
            ? Number(h.slot)
            : legacySlots.reduce((b, s) => Math.abs(brDt.getUTCHours() - s) < Math.abs(brDt.getUTCHours() - b) ? s : b, legacySlots[0]);
          if (!paginas[p.nome][dk]) paginas[p.nome][dk] = {};
          paginas[p.nome][dk][slot] = h.ads_count;
        }
      }

      const slugs = slugsDoGrupo;
      let histMap = {}, histDates = [];
      if (slugs.length) {
        const { rows: histAll } = await query(`
          SELECT p.nome, p.tipo, sh.ads_count, sh.slot, sh.collected_at
          FROM scrape_history sh
          JOIN pages p ON p.slug = sh.slug
          WHERE sh.slug = ANY($1) AND sh.collected_at >= NOW() - INTERVAL '60 days'
          ORDER BY sh.collected_at DESC
        `, [slugs]);
        for (const r of histAll) {
          const nome = r.nome;
          const brDt = toBrDate(r.collected_at);
          const dk = brDt.toISOString().slice(0, 10);
          const legacySlots = r.tipo === "keyword" ? [6] : r.tipo === "dominio" ? [3, 5, 12, 22] : [3, 12, 22];
          const slot = (r.slot !== null && r.slot !== undefined)
            ? Number(r.slot)
            : legacySlots.reduce((b, s) => Math.abs(brDt.getUTCHours() - s) < Math.abs(brDt.getUTCHours() - b) ? s : b, legacySlots[0]);
          if (!histMap[nome]) histMap[nome] = {};
          if (!histMap[nome][dk]) histMap[nome][dk] = {};
          if (histMap[nome][dk][slot] === undefined) histMap[nome][dk][slot] = r.ads_count;
        }
        histDates = [...new Set(histAll.map(r => toBrDate(r.collected_at).toISOString().slice(0, 10)))]
          .sort((a, b) => b.localeCompare(a));
      }
      const histLibs = Object.keys(histMap).sort((a, b) => (ultimaLeitura[b]?.ads || 0) - (ultimaLeitura[a]?.ads || 0));

      return {
        geral: { pags: paginas, ultima: ultimaLeitura, primeira: primeiraData, mon, meta },
        hist:  { map: histMap, dates: histDates, libs: histLibs },
        count: Object.keys(paginas).length,
      };
    }

    const grupoPaginas  = await processarGrupo(allPages.filter(p => p.tipo === "pagina"));
    const grupoDominios = await processarGrupo(allPages.filter(p => p.tipo === "dominio"));
    const grupoKeywords = await processarGrupo(allPages.filter(p => p.tipo === "keyword"));

    const dados       = JSON.stringify(grupoPaginas.geral);
    const histDados   = JSON.stringify(grupoPaginas.hist);
    const dadosDom    = JSON.stringify(grupoDominios.geral);
    const histDadosDom = JSON.stringify(grupoDominios.hist);
    const dadosKey    = JSON.stringify(grupoKeywords.geral);
    const histDadosKey = JSON.stringify(grupoKeywords.hist);
    const IG_SVG = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><rect x="3" y="3" width="18" height="18" rx="5"/><circle cx="12" cy="12" r="4"/><circle cx="17.5" cy="6.5" r="1" fill="currentColor"/></svg>';

    // "📢 Mapeamento ADS": lê todo nó tipo='ads', conectado ou não, agrupado por página.
    // Não depende de computarCaminhos() (que exige componente com 2+ nós) — um ADS
    // cadastrado sozinho, sem conexão nenhuma, já aparece aqui.
    const { rows: adsRows } = await query(`
      SELECT fn.id, fn.rotulo, fn.url AS ad_url, p.slug, p.nome, p.tipo
      FROM funnel_nodes fn
      JOIN pages p ON p.slug = fn.slug
      WHERE fn.tipo = 'ads'
      ORDER BY p.nome, fn.created_at ASC
    `);

    const adsBySlug = {};
    adsRows.forEach(r => {
      (adsBySlug[r.slug] ||= { nome: r.nome, tipo: r.tipo, itens: [] })
        .itens.push({ id: r.id, rotulo: r.rotulo, url: r.ad_url });
    });
    const adsPaginas = Object.values(adsBySlug).sort((a, b) => a.nome.localeCompare(b.nome));

    const adsCardsHtml = adsPaginas.map(pg => `
      <div class="player-card">
        <div class="player-hdr">
          <span class="player-tipo-badge ${pg.tipo === "dominio" ? "b-dom" : pg.tipo === "keyword" ? "b-key" : "b-pag"}">${pg.tipo === "dominio" ? "🌐" : pg.tipo === "keyword" ? "🔑" : "📡"}</span>
          <span class="player-nome">${pg.nome}</span>
          <span class="ads-count-badge">📢 ${pg.itens.length} ADS</span>
        </div>
        <div class="ads-chip-row">
          ${pg.itens.map(a => `<a href="${a.url}" target="_blank" rel="noopener" class="chip" title="${a.rotulo}"><span class="chip-icon">📢</span><span class="chip-label">${a.rotulo}</span></a>`).join("")}
        </div>
      </div>
    `).join("");

    res.send(`<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lowticket Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>
<style>
:root{--bg:#0a0a14;--surface:#12121f;--surface2:#171728;--border:#23233f;--text:#f0f0fa;--text2:#b8b8d0;--muted:#7a7a98;--accent:#7c6fff;--up:#34d399;--up2:#10b981;--down:#fb7185;--flat:#8888aa;--hot:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Grotesk',system-ui,sans-serif;padding:18px;max-width:1600px;margin:0 auto}
.hdr{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.hdr h1{font-size:19px;font-weight:700;color:#fff;letter-spacing:.2px}
.hdr-sub{font-size:12px;color:var(--text2);margin-top:3px;font-family:'Space Mono',monospace}
.hdr-live{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:12px;color:var(--text2);background:var(--surface);border:1px solid var(--border);padding:7px 14px;border-radius:8px}
.hdr-admin-btn{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:6px 14px;border-radius:8px;transition:all .15s;white-space:nowrap}
.hdr-admin-btn:hover{background:var(--accent);color:#fff}
.manual-check-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;background:#f5f5f7;color:#17171b;border:0;border-radius:8px;padding:7px 13px;font:600 12px 'Space Grotesk',sans-serif;cursor:pointer;transition:background .15s,opacity .15s;white-space:nowrap}
.manual-check-btn:hover{background:#fff}
.manual-check-btn:disabled{opacity:.6;cursor:wait}
.manual-check-btn:focus-visible{outline:2px solid #fff;outline-offset:3px}
.manual-customize-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;background:transparent;color:var(--text2);border:1px solid var(--border);border-radius:8px;padding:7px 12px;font:600 12px 'Space Grotesk',sans-serif;cursor:pointer;transition:background .15s,border-color .15s,color .15s;white-space:nowrap}
.manual-customize-btn:hover{background:var(--surface2);border-color:#85859a;color:#fff}
.manual-customize-btn:disabled{opacity:.55;cursor:wait}
.manual-customize-btn:focus-visible,.dialog-close:focus-visible,.dialog-primary:focus-visible,.dialog-secondary:focus-visible,.manual-report-open:focus-visible{outline:3px solid #7c6fff;outline-offset:3px}
.selection-dialog,.confirm-dialog,.report-dialog{width:min(94vw,980px);max-height:calc(100dvh - 36px);padding:0;overflow:hidden;border:1px solid rgba(15,23,42,.12);border-radius:18px;background:#f5f6f7;color:#181b20;box-shadow:0 28px 90px rgba(0,0,0,.38);font-family:'Space Grotesk',sans-serif}
.selection-dialog::backdrop,.confirm-dialog::backdrop,.report-dialog::backdrop{background:rgba(5,7,12,.68);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px)}
.selection-dialog[open],.confirm-dialog[open],.report-dialog[open]{animation:dialog-enter .2s cubic-bezier(.2,.8,.2,1)}
@keyframes dialog-enter{from{opacity:0;transform:translateY(8px) scale(.99)}to{opacity:1;transform:translateY(0) scale(1)}}
.selection-dialog-shell,.confirm-dialog-shell,.report-dialog-shell{display:flex;flex-direction:column;max-height:calc(100dvh - 36px)}
.selection-dialog-head,.report-head{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;padding:26px 30px 20px;background:#fff;border-bottom:1px solid #e4e6e9}
.dialog-eyebrow{margin-bottom:8px;color:#69717b;font:600 10px 'Space Mono',monospace;letter-spacing:1.15px}
.selection-dialog-head h2,.report-head h2{font-size:24px;line-height:1.2;font-weight:650;letter-spacing:0}
.selection-dialog-head p,.report-head p{margin-top:7px;color:#68717b;font-size:13px;line-height:1.5}
.dialog-close{display:grid;place-items:center;flex:0 0 34px;width:34px;height:34px;border:0;border-radius:50%;background:#f0f1f2;color:#555e68;font:400 25px/1 'Space Grotesk',sans-serif;cursor:pointer;transition:background .15s,color .15s}
.dialog-close:hover{background:#e3e5e8;color:#111}
.selection-dialog-body{min-height:0;padding:20px 30px;overflow:auto}
.manual-selection-search-wrap{display:flex;align-items:center;gap:10px;height:42px;margin-bottom:18px;padding:0 13px;border:1px solid #d9dde1;border-radius:10px;background:#fff;color:#7a838d}
.manual-selection-search{width:100%;height:100%;border:0;outline:0;background:transparent;color:#20252a;font:13px 'Space Grotesk',sans-serif}
.manual-selection-search-wrap:focus-within{border-color:#397c4b;box-shadow:0 0 0 3px rgba(57,124,75,.16)}
.manual-selection-search::placeholder{color:#9098a1}
.selection-groups{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
.selection-group{min-width:0;border:1px solid #dfe2e5;border-radius:12px;background:#fff;overflow:hidden}
.selection-group>header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:13px 15px;border-bottom:1px solid #eceef0}
.selection-group>header>div{display:flex;align-items:center;gap:10px;min-width:0}
.selection-group h3{font-size:13px;font-weight:650;letter-spacing:0}
.selection-group-icon{display:grid;place-items:center;width:27px;height:27px;border-radius:8px;background:#eaf2ec;color:#426d4d;font:700 11px 'Space Mono',monospace}
.selection-group-icon-url{background:#edf0f4;color:#596775}
.selection-group-count{color:#737c85;font:11px 'Space Mono',monospace}
.manual-selection-list{display:flex;flex-direction:column;gap:5px;max-height:310px;min-height:104px;padding:9px;overflow:auto}
.manual-selection-option{display:flex;align-items:center;gap:11px;min-width:0;padding:10px;border:1px solid transparent;border-radius:8px;background:#f7f8f8;color:#252a30;cursor:pointer;transition:background .14s,border-color .14s}
.manual-selection-option:hover{background:#f0f3f0}
.manual-selection-option:has(input:checked){border-color:#b8d0bf;background:#eff6f0}
.manual-selection-option input{width:17px;height:17px;flex:0 0 17px;margin:0;accent-color:#397c4b}
.manual-selection-option-copy{display:flex;flex-direction:column;gap:3px;min-width:0}
.manual-selection-option-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;font-weight:600}
.manual-selection-option-url{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#79828b;font:10px 'Space Mono',monospace}
.selection-empty{display:grid;place-items:center;min-height:86px;padding:14px;color:#89919a;font-size:12px;text-align:center}
.manual-selection-option[hidden]{display:none}
.selection-dialog-footer,.report-footer{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 30px;background:#fff;border-top:1px solid #e4e6e9}
.manual-selection-message,.confirm-note{color:#68717b;font-size:12px}
.selection-dialog-footer>div,.report-footer>div{display:flex;align-items:center;justify-content:flex-end;gap:9px}
.dialog-primary,.dialog-secondary{min-height:38px;padding:0 15px;border:1px solid transparent;border-radius:8px;font:600 12px 'Space Grotesk',sans-serif;cursor:pointer;transition:background .15s,border-color .15s,opacity .15s}
.dialog-primary{background:#20262c;color:#fff}
.dialog-primary:hover:not(:disabled){background:#394149}
.dialog-primary:disabled{opacity:.42;cursor:not-allowed}
.dialog-secondary{border-color:#d9dde1;background:#fff;color:#515a64}
.dialog-secondary:hover{background:#f2f3f4;border-color:#c6cbd0}
.confirm-dialog{width:min(94vw,760px)}
.confirm-selection-summary{display:flex;align-items:center;gap:10px;padding:14px 30px;color:#333b43;font-size:12px;font-weight:600}
.confirm-selection-summary span{display:inline-flex;gap:5px;align-items:center;padding:5px 9px;border-radius:6px;background:#e9ecee;color:#505a63;font:11px 'Space Mono',monospace}
.confirm-selection-list{display:flex;flex-direction:column;gap:6px;max-height:min(48vh,420px);margin:0 30px 18px;padding:2px 4px 2px 0;overflow:auto}
.confirm-selection-item{display:flex;flex-direction:column;gap:4px;padding:10px 12px;border:1px solid #e1e4e6;border-radius:8px;background:#fff}
.confirm-selection-item strong{color:#252b31;font-size:12px}
.confirm-selection-item code{overflow-wrap:anywhere;color:#717b85;font:10px/1.55 'Space Mono',monospace}
.report-dialog{width:min(94vw,920px)}
.report-head{align-items:center;padding-bottom:18px}
.report-head h2{font-size:22px}
.report-status{display:flex;align-items:center;gap:9px;margin:20px 30px 14px;color:#315f3e;font-size:12px;font-weight:650}
.report-status:before{content:"";width:8px;height:8px;border-radius:50%;background:#438c55;box-shadow:0 0 0 4px #e2efe4}
.report-status.has-failures{color:#895f2d}
.report-status.has-failures:before{background:#c58a36;box-shadow:0 0 0 4px #f6eddd}
.report-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:0 30px 22px}
.report-metric{min-width:0;padding:14px;border:1px solid #e0e3e5;border-radius:10px;background:#fff}
.report-metric-label{color:#77808a;font-size:10px;font-weight:600;text-transform:uppercase}
.report-metric-value{margin-top:7px;color:#222930;font:600 23px/1 'Space Mono',monospace}
.report-results-section{margin:0 30px 20px;border:1px solid #dfe2e5;border-radius:11px;background:#fff;overflow:hidden}
.report-results-section>header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:13px 15px;border-bottom:1px solid #e8eaec}
.report-results-section h3{font-size:13px;font-weight:650}
.report-results-section>header span{color:#79828b;font:10px 'Space Mono',monospace}
.report-results{display:flex;flex-direction:column;max-height:min(40vh,330px);overflow:auto}
.report-result-row{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:12px;padding:10px 15px;border-bottom:1px solid #eef0f1}
.report-result-row:last-child{border-bottom:0}
.report-result-copy{display:flex;flex-direction:column;gap:4px;min-width:0}
.report-result-name{overflow:hidden;color:#2b3137;font-size:12px;font-weight:600;text-overflow:ellipsis;white-space:nowrap}
.report-result-detail{overflow-wrap:anywhere;color:#77808a;font-size:10px;line-height:1.45}
.report-result-value{white-space:nowrap;color:#397449;font:600 12px 'Space Mono',monospace}
.report-result-value.is-failed{color:#a85c4f}
.report-empty{padding:22px;color:#78818b;font-size:12px;text-align:center}
.report-footer{font-size:10px;color:#7c858e}
.manual-report-open{margin-top:12px;padding:0;border:0;background:transparent;color:#c8c7ff;font:600 11px 'Space Grotesk',sans-serif;cursor:pointer}
.manual-report-open[hidden]{display:none}
.manual-report-open:hover{text-decoration:underline}
.manual-run-panel{margin:0 0 20px;padding:14px 16px;background:rgba(255,255,255,.035);border:1px solid var(--border);border-radius:12px}
.manual-run-panel[hidden]{display:none}
.manual-run-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;font-size:12px}
.manual-run-dismiss{flex:0 0 auto;width:30px;height:30px;border:1px solid var(--border);border-radius:8px;background:transparent;color:var(--text2);font-size:20px;line-height:1;cursor:pointer}
.manual-run-dismiss:hover{border-color:var(--down);color:var(--down)}
.manual-run-dismiss[hidden]{display:none}
.manual-run-title{font-weight:600;color:var(--text)}
.manual-run-summary,.manual-run-current{color:var(--muted);font-size:11px;margin-top:4px}
.manual-run-count{color:var(--text2);font:11px 'Space Mono',monospace;white-space:nowrap}
.manual-run-track{height:3px;margin-top:12px;background:var(--surface2);border-radius:3px;overflow:hidden}
.manual-run-track span{display:block;width:0;height:100%;background:var(--accent);transition:width .25s ease}
.manual-run-results{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:6px 16px;margin-top:12px;max-height:190px;overflow:auto}
.manual-run-row{display:flex;align-items:center;justify-content:space-between;gap:12px;min-width:0;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.045);font-size:11px}
.manual-run-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text2)}
.manual-run-result{white-space:nowrap;color:var(--up);font-family:'Space Mono',monospace}
.manual-run-result.is-failed{color:var(--down)}
@media(max-width:760px){.hdr>div:last-child>div:last-child{flex-wrap:wrap;justify-content:flex-end}.selection-dialog-head,.report-head{padding:21px 20px 16px}.selection-dialog-body{padding:16px 20px}.selection-groups{grid-template-columns:1fr}.manual-selection-list{max-height:230px}.selection-dialog-footer,.report-footer{align-items:flex-start;flex-direction:column;padding:14px 20px}.selection-dialog-footer>div,.report-footer>div{width:100%}.selection-dialog-footer .dialog-primary,.report-footer .dialog-primary{flex:1}.confirm-selection-summary{flex-wrap:wrap;padding:12px 20px}.confirm-selection-list{margin:0 20px 16px}.report-status{margin:16px 20px 12px}.report-metrics{grid-template-columns:repeat(2,minmax(0,1fr));margin:0 20px 16px}.report-results-section{margin:0 20px 16px}}
@media(max-width:480px){.selection-dialog,.confirm-dialog,.report-dialog{width:calc(100vw - 20px);max-height:calc(100dvh - 20px);border-radius:14px}.selection-dialog-shell,.confirm-dialog-shell,.report-dialog-shell{max-height:calc(100dvh - 20px)}.selection-dialog-head h2,.report-head h2{font-size:20px}.selection-dialog-head p,.report-head p{font-size:12px}.selection-dialog-footer>div{display:grid;grid-template-columns:1fr 1fr}.confirm-note{font-size:11px}.report-metric{padding:11px}.report-metric-value{font-size:20px}.report-footer>div{display:grid;grid-template-columns:1fr 1fr}.report-result-row{padding:9px 11px}}
@media(prefers-reduced-motion:reduce){.selection-dialog[open],.confirm-dialog[open],.report-dialog[open]{animation:none}}
.dot{width:8px;height:8px;border-radius:50%;background:var(--up);box-shadow:0 0 8px var(--up)}
.section-label{font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin:0 0 12px 2px;display:flex;align-items:center;gap:8px}
.scaling-strip{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;margin-bottom:26px}
.scale-card{background:linear-gradient(135deg,#1a1530,#14122a);border:1px solid #2e2658;border-radius:14px;padding:16px 18px;position:relative;overflow:hidden;cursor:pointer;transition:all 0.25s cubic-bezier(0.16,1,0.3,1)}
.scale-card:hover{border-color:var(--accent);box-shadow:0 8px 24px -6px rgba(124,111,255,0.35);transform:translateY(-2px)}
.scale-card:active{transform:scale(0.982)}
.scale-arrow{display:inline-block;transition:transform 0.25s ease, color 0.25s ease;font-weight:700;margin-left:4px}
.scale-card:hover .scale-arrow{transform:translate(2px,-2px);color:var(--accent)}
.scale-card-toast{position:absolute;inset:0;background:rgba(18,18,31,0.92);backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);display:flex;align-items:center;justify-content:center;gap:8px;color:#34d399;font-weight:700;font-size:14px;border-radius:14px;opacity:0;transform:scale(0.92);pointer-events:none;transition:all 0.25s cubic-bezier(0.16,1,0.3,1);z-index:10}
.scale-card-toast.show{opacity:1;transform:scale(1)}
.scale-card-rank{position:absolute;top:12px;right:14px;font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;display:flex;align-items:center}
.scale-card-name{font-size:13px;font-weight:600;color:#fff;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-right:34px}
.scale-card-val{font-size:30px;font-weight:700;color:#fff;font-family:'Space Mono',monospace;line-height:1}
.scale-card-meta{display:flex;align-items:center;gap:10px;margin-top:8px;font-size:12px}
.scale-pct{font-weight:600;font-family:'Space Mono',monospace}
.scale-spark{margin-top:10px;height:32px;position:relative}
.empty-hint{background:var(--surface);border:1px dashed var(--border);border-radius:12px;padding:22px;text-align:center;color:var(--muted);font-size:13px;margin-bottom:26px}
.lib-link{color:var(--text);text-decoration:none;border-bottom:1px solid var(--border);padding-bottom:1px;transition:color .15s,border-color .15s}
.lib-link:hover{color:var(--accent);border-color:var(--accent)}
.accordion-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.accordion-btn{width:100%;display:flex;align-items:center;gap:10px;padding:14px 18px;background:transparent;border:none;color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;font-weight:600;cursor:pointer;text-align:left;transition:background .15s}
.accordion-btn:hover{background:var(--surface2)}
.acc-meta{font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;margin-left:4px}
.acc-icon{margin-left:auto;font-size:12px;color:var(--muted);transition:transform .25s;flex-shrink:0}
.acc-icon.open{transform:rotate(180deg)}
.accordion-body{display:none;padding:0 18px 16px;overflow-x:auto;-webkit-overflow-scrolling:touch}
.accordion-body.open{display:block}
.grid-charts{display:grid;grid-template-columns:1fr;gap:14px;margin-bottom:26px}
@media(min-width:1100px){.grid-charts{grid-template-columns:380px 1fr}}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:16px 18px;min-width:0}
.panel-title{font-size:12px;font-weight:600;color:var(--text2);text-transform:uppercase;letter-spacing:.6px;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.rosca-wrap{display:flex;flex-direction:column;gap:16px;align-items:center;min-width:0}
@media(min-width:900px){.rosca-wrap{flex-direction:row}}
.rosca-canvas{width:clamp(120px,40vw,150px);height:clamp(120px,40vw,150px);position:relative;flex-shrink:0}
.legend{display:flex;flex-direction:column;gap:7px;flex:1;min-width:0;max-height:260px;overflow-y:auto;padding-right:14px;scrollbar-gutter:stable}
.legend::-webkit-scrollbar{width:6px}
.legend::-webkit-scrollbar-track{background:transparent}
.legend::-webkit-scrollbar-thumb{background:var(--border);border-radius:6px}
.legend::-webkit-scrollbar-thumb:hover{background:var(--muted)}
.leg-item{display:flex;align-items:center;gap:9px}
.leg-dot{width:10px;height:10px;border-radius:3px;flex-shrink:0}
.leg-name{font-size:12px;color:var(--text);flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.leg-val{font-size:12px;font-weight:600;color:#fff;font-family:'Space Mono',monospace}
.leg-pct{font-size:11px;color:var(--muted);font-family:'Space Mono',monospace;width:32px;text-align:right}
.chart-box{height:240px;position:relative}
.hist-box{height:300px;position:relative}
.hist-selection{display:flex;align-items:center;justify-content:space-between;gap:10px;min-height:24px;margin:-6px 0 10px;color:var(--muted);font-size:11px}
.hist-selection[hidden]{display:none}
.hist-reset{border:0;background:transparent;color:var(--accent);font:inherit;cursor:pointer;padding:4px 0;white-space:nowrap}
.hist-reset:hover{text-decoration:underline}
.hist-reset:focus-visible,.hist-pin-btn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.hist-pin-cell{min-width:78px}
.hist-pin-btn{display:inline-flex;align-items:center;gap:5px;border:1px solid var(--border);border-radius:6px;background:transparent;color:var(--muted);font:600 11px 'Space Grotesk',sans-serif;padding:5px 8px;cursor:pointer;white-space:nowrap}
.hist-pin-btn:hover{border-color:var(--accent);color:var(--text)}
.hist-pin-btn.is-pinned,.hist-pin-btn.is-focused{border-color:var(--accent);background:rgba(124,111,255,.12);color:var(--accent)}
.hist-pin-btn:disabled{cursor:not-allowed;opacity:.5}
.tbl-panel{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow-x:auto;overflow-y:hidden;-webkit-overflow-scrolling:touch}
.tbl-panel table{width:100%;min-width:1250px;table-layout:fixed;border-collapse:collapse;font-size:13px}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:var(--surface2);color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:11px 16px;text-align:left;font-weight:600}
td{padding:11px 16px;border-top:1px solid var(--border);color:var(--text2);white-space:nowrap}
th.participacao-header,td.participacao-cell{width:140px;min-width:140px;max-width:140px;overflow:hidden;box-sizing:border-box}
.participacao-wrapper{display:flex;align-items:center;gap:8px;width:100%;max-width:100%;overflow:hidden;box-sizing:border-box}
.participacao-wrapper .scalebar-bg{width:70px;min-width:70px;max-width:70px;height:5px;background:var(--border);border-radius:3px;display:inline-block;overflow:hidden;flex-shrink:0;margin:0;vertical-align:middle}
.participacao-wrapper .scalebar{height:5px;display:block;border-radius:0}
.score-cell-td{overflow:hidden}
.score-cell{display:flex;flex-direction:column;gap:3px;min-width:115px;cursor:help}
.score-main{display:flex;align-items:center;gap:6px}
.score-number{font-family:'Space Mono',monospace;font-weight:800;font-size:14px;padding:4px 8px;border-radius:7px;min-width:36px;text-align:center}
.score-icon{font-size:14px}
.score-fase{display:flex;align-items:center;gap:4px;font-size:10px;white-space:nowrap}
.fase-label{font-weight:700;text-transform:uppercase}
.fase-trend{font-family:'Space Mono',monospace}
.score-escalando .score-number{background:rgba(52,211,153,.15);color:#34d399;border:1px solid rgba(52,211,153,.35)}
.score-declinio .score-number{background:rgba(251,191,36,.15);color:#fbbf24;border:1px solid rgba(251,191,36,.35)}
.score-declinio .fase-trend{color:#fbbf24}
.score-testando .score-number{background:rgba(96,165,250,.15);color:#60a5fa;border:1px solid rgba(96,165,250,.35)}
.score-testando .fase-trend{color:#60a5fa}
.score-queda .score-number,.score-inativo .score-number{background:rgba(251,113,133,.13);color:#fb7185;border:1px solid rgba(251,113,133,.3)}
.score-queda .fase-trend,.score-inativo .fase-trend{color:#fb7185}
.score-estavel .score-number,.score-freando .score-number{background:rgba(136,136,170,.12);color:#9999b8;border:1px solid rgba(136,136,170,.28)}
.score-escalando .fase-trend{color:#34d399}
tbody tr:hover td{background:var(--surface2)}
.t-name{font-weight:600;color:#fff;white-space:normal}
.t-name-main{display:block;font-weight:600;color:#fff;margin-bottom:4px}
.t-meta-badges{display:flex;flex-wrap:wrap;gap:4px;margin-top:3px}
.t-geo-badge{font-size:10px;background:rgba(34,211,238,.1);color:#22d3ee;padding:2px 7px;border-radius:5px;font-weight:500;white-space:nowrap}
.t-nicho-badge{font-size:10px;background:rgba(167,139,250,.12);color:#a78bfa;padding:2px 7px;border-radius:5px;font-weight:500;white-space:nowrap}
.t-funil-badge{font-size:10px;background:rgba(251,191,36,.12);color:#fbbf24;padding:2px 7px;border-radius:5px;font-weight:500;white-space:nowrap}
.badge{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:7px;font-size:11px;font-weight:600;font-family:'Space Grotesk'}
.b-up{background:rgba(52,211,153,.13);color:#34d399}
.b-hot{background:rgba(167,139,250,.15);color:#a78bfa}
.b-down{background:rgba(251,113,133,.13);color:#fb7185}
.b-flat{background:rgba(136,136,170,.12);color:#9999b8}
.b-off{background:rgba(120,120,140,.1);color:#777}
.scalebar-bg{width:80px;height:5px;background:var(--border);border-radius:3px;display:inline-block;vertical-align:middle;margin-right:8px}
.scalebar{height:5px;border-radius:3px;display:block}
.spark3{font-family:'Space Mono',monospace;font-size:13px}
.win-btn{background:transparent;color:var(--muted);border:none;border-radius:5px;padding:5px 10px;font-size:11px;font-weight:600;cursor:pointer;font-family:'Space Grotesk'}
.win-btn.active{background:var(--accent);color:#fff}
.win-btn:hover{color:var(--text)}
.ig-cell{text-align:center}
.ig-link{display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;border-radius:8px;color:#e879f9;transition:background .15s}
.ig-link:hover{background:rgba(220,39,67,.15)}
.ig-none{color:var(--muted);font-size:13px}
.mono{font-family:'Space Mono',monospace}
.hist-tbl thead th{white-space:nowrap}
.hist-tbl td{font-family:'Space Mono',monospace;font-size:12px;text-align:center}
.hist-tbl td.lib-name{text-align:left;font-family:'Space Grotesk',sans-serif;font-weight:600;color:#fff;white-space:nowrap}
.hist-tbl td.date-col{color:var(--muted);text-align:left;white-space:nowrap}
.hist-slot{display:inline-block;min-width:42px;text-align:right}
.hist-slot.empty{color:var(--border)}
.tbl-scroll-x{overflow-x:auto;-webkit-overflow-scrolling:touch}
.group-title{font-size:15px;font-weight:700;color:#fff;letter-spacing:.4px;margin:0 0 16px 2px;padding-bottom:10px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
.player-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:18px 20px;margin-bottom:14px}
.player-hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.player-tipo-badge{font-size:15px;padding:2px 6px;border-radius:6px}
.player-nome{font-size:15px;font-weight:700;color:#fff;text-decoration:none}
.b-dom{background:rgba(124,111,255,.15);color:#a78bfa}
.b-key{background:rgba(251,191,36,.14);color:#fbbf24}
.b-pag{background:rgba(52,211,153,.12);color:#34d399}
.chip{display:inline-flex;align-items:center;gap:5px;background:var(--surface2);border:1px solid var(--border);border-radius:7px;padding:5px 10px;text-decoration:none;font-size:12px;font-weight:600;color:#fff}
.chip:hover{border-color:var(--accent)}
.chip-icon{font-size:13px}
.chip-label{white-space:nowrap}
.ads-count-badge{margin-left:auto;font-size:11px;font-weight:600;color:#a78bfa;background:rgba(167,139,250,.12);padding:3px 10px;border-radius:7px;white-space:nowrap}
.ads-chip-row{display:flex;flex-wrap:wrap;gap:8px}
@media(max-width:1100px){.grid-charts{grid-template-columns:1fr}.rosca-wrap{flex-direction:column}}
@media(max-width:768px){
  body{padding:12px}
  .hdr{flex-wrap:wrap;gap:8px}
  .hdr-live{margin-left:0;width:100%}
  .hdr-admin-btn{width:100%;justify-content:center;box-sizing:border-box}
  .manual-check-btn{width:100%;box-sizing:border-box}
  .hdr h1{font-size:15px}
  .hdr-sub{font-size:10px}
  .scale-card-val{font-size:24px}
  .grid-charts{grid-template-columns:1fr}
  .rosca-wrap{flex-direction:column;align-items:flex-start}
  .rosca-canvas{width:120px;height:120px}
  .hist-box{height:220px}
}
@media(max-width:480px){.scaling-strip{grid-template-columns:1fr}}
.search-wrap{position:relative;margin-bottom:14px}
.search-wrap svg{position:absolute;left:13px;top:50%;transform:translateY(-50%);color:var(--muted);pointer-events:none}
.search-wrap input{width:100%;padding:10px 14px 10px 38px;border-radius:10px;background:var(--surface);border:1px solid var(--border);color:var(--text);font-family:'Space Grotesk',sans-serif;font-size:13px;outline:none;transition:.18s;box-sizing:border-box}
.search-wrap input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(124,111,255,.15)}
.search-wrap input::placeholder{color:var(--muted)}
</style>
</head>
<body>

<div class="hdr">
  <div>
    <h1>📊 Lowticket Monitor</h1>
    <div class="hdr-sub" id="upd"></div>
  </div>
  <div style="margin-left:auto;display:flex;flex-direction:column;align-items:flex-end;gap:8px">
    <div class="hdr-live" style="margin-left:0"><span class="dot"></span><span id="livecount"></span></div>
    <div style="display:flex;gap:8px">
      <button type="button" class="manual-check-btn" id="manual-check-button" title="Conferir todos os itens monitorados">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 7v5h-5"/><path d="M20 12a8 8 0 1 1-2.34-5.66L20 7"/></svg>
        <span id="manual-check-label">Checar todas</span>
      </button>
      <button type="button" class="manual-customize-btn" id="manual-customize-button">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="4" y="4" width="6" height="6" rx="1"/><rect x="14" y="4" width="6" height="6" rx="1"/><rect x="4" y="14" width="6" height="6" rx="1"/><path d="m15 17 2 2 4-4"/></svg>
        Personalizar checagem
      </button>
      <a href="/admin" class="hdr-admin-btn">⚙️ Ir para Admin</a>
      <a href="/funis" class="hdr-admin-btn">🔀 Ver Mapa de Funis</a>
    </div>
  </div>
</div>

<section class="manual-run-panel" id="manual-run-panel" hidden>
  <div class="manual-run-head">
    <div>
      <div class="manual-run-title" id="manual-run-title">Checagem manual</div>
      <div class="manual-run-current" id="manual-run-current" role="status" aria-live="polite"></div>
    </div>
    <div style="display:flex;align-items:center;gap:12px">
      <div class="manual-run-count" id="manual-run-count"></div>
      <button type="button" class="manual-run-dismiss" id="manual-run-dismiss" aria-label="Fechar relatório e quadro de checagem" title="Fechar relatório e quadro de checagem" hidden>×</button>
    </div>
  </div>
  <div class="manual-run-track" role="progressbar" aria-label="Progresso da checagem" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"><span id="manual-run-progress"></span></div>
  <div class="manual-run-results" id="manual-run-results"></div>
  <button type="button" class="manual-report-open" id="manual-report-open" hidden>Ver relatório completo</button>
</section>

<dialog class="selection-dialog" id="manual-selection-dialog" aria-labelledby="manual-selection-title">
  <div class="selection-dialog-shell">
    <header class="selection-dialog-head">
      <div><div class="dialog-eyebrow">ESCOPO DA COLETA</div><h2 id="manual-selection-title">Personalizar checagem</h2><p>Escolha exatamente o que deseja conferir agora.</p></div>
      <button type="button" class="dialog-close" id="manual-selection-close" aria-label="Fechar seleção">×</button>
    </header>
    <div class="selection-dialog-body">
      <label class="manual-selection-search-wrap"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></svg><input class="manual-selection-search" id="manual-selection-search" type="search" placeholder="Buscar página ou URL monitorada" aria-label="Buscar página ou URL monitorada"></label>
      <div class="selection-groups">
        <section class="selection-group" aria-labelledby="manual-pages-title">
          <header><div><span class="selection-group-icon">P</span><h3 id="manual-pages-title">Páginas Monitoradas</h3></div><span class="selection-group-count" id="manual-pages-count">0</span></header>
          <div class="manual-selection-list" id="manual-pages-list" role="group" aria-label="Páginas monitoradas"><div class="selection-empty">Carregando páginas...</div></div>
        </section>
        <section class="selection-group" aria-labelledby="manual-urls-title">
          <header><div><span class="selection-group-icon selection-group-icon-url">U</span><h3 id="manual-urls-title">URLs Monitoradas</h3></div><span class="selection-group-count" id="manual-urls-count">0</span></header>
          <div class="manual-selection-list" id="manual-urls-list" role="group" aria-label="URLs monitoradas"><div class="selection-empty">Carregando URLs...</div></div>
        </section>
        <section class="selection-group" aria-labelledby="manual-keywords-title">
          <header><div><span class="selection-group-icon">K</span><h3 id="manual-keywords-title">Palavras-chave</h3></div><span class="selection-group-count" id="manual-keywords-count">0</span></header>
          <div class="manual-selection-list" id="manual-keywords-list" role="group" aria-label="Palavras-chave monitoradas"><div class="selection-empty">Carregando palavras-chave...</div></div>
        </section>
      </div>
    </div>
    <footer class="selection-dialog-footer"><span class="manual-selection-message" id="manual-selection-message">Nenhum item selecionado</span><div><button type="button" class="dialog-secondary" id="manual-selection-cancel">Cancelar</button><button type="button" class="dialog-primary" id="manual-check-selected" disabled>Realizar checagem</button></div></footer>
  </div>
</dialog>

<dialog class="confirm-dialog" id="manual-confirm-dialog" aria-labelledby="manual-confirm-title" aria-describedby="manual-confirm-description">
  <div class="confirm-dialog-shell">
    <header class="selection-dialog-head"><div><div class="dialog-eyebrow">CONFIRMAÇÃO NECESSÁRIA</div><h2 id="manual-confirm-title">Conferir estes itens?</h2><p id="manual-confirm-description"></p></div><button type="button" class="dialog-close" id="manual-confirm-close" aria-label="Voltar para seleção">×</button></header>
    <div class="confirm-selection-summary" id="manual-confirm-summary"></div>
    <div class="confirm-selection-list" id="manual-confirm-list"></div>
    <footer class="selection-dialog-footer"><span class="confirm-note">A coleta só começa após sua confirmação.</span><div><button type="button" class="dialog-secondary" id="manual-confirm-back">Voltar</button><button type="button" class="dialog-primary" id="manual-confirm-start">Confirmar e iniciar</button></div></footer>
  </div>
</dialog>

<dialog class="report-dialog" id="manual-report-dialog" aria-labelledby="manual-report-title" aria-describedby="manual-report-subtitle manual-report-status">
  <div class="report-dialog-shell">
    <header class="report-head"><div><div class="dialog-eyebrow">LOWTICKET MONITOR · RESULTADO</div><h2 id="manual-report-title">Relatório da checagem</h2><p id="manual-report-subtitle"></p></div><button type="button" class="dialog-close" id="manual-report-close" aria-label="Fechar relatório e quadro de checagem">×</button></header>
    <div class="report-status" id="manual-report-status"></div>
    <div class="report-metrics" id="manual-report-metrics"></div>
    <section class="report-results-section"><header><h3>Resultado por item monitorado</h3><span id="manual-report-results-count"></span></header><div class="report-results" id="manual-report-results"></div></section>
    <footer class="report-footer"><span id="manual-report-timestamp"></span><div><button type="button" class="dialog-secondary" id="manual-report-dismiss">Fechar definitivamente</button><button type="button" class="dialog-primary" id="manual-report-refresh">Atualizar dashboard</button></div></footer>
  </div>
</dialog>

<div class="group-title">📡 BIBLIOTECAS — rastreio por página</div>

<div class="section-label">🚀 Escalando agora — bibliotecas em ascensão</div>
<div class="scaling-strip" id="pag_scaling"></div>
<div class="empty-hint" id="pag_scaling-empty" style="display:none">Nenhum item em ascensão no período. Conforme as coletas acumulam, o que cresce aparece aqui.</div>

<div class="grid-charts">
  <div class="panel">
    <div class="panel-title">🍩 Distribuição atual</div>
    <div class="rosca-wrap">
      <div class="rosca-canvas"><canvas id="pag_cRosca"></canvas></div>
      <div class="legend" id="pag_legend"></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
    <div class="hist-selection" id="pag_hist-selection" hidden><span id="pag_hist-status" aria-live="polite"></span><button class="hist-reset" id="pag_hist-reset" type="button">Restaurar padrão</button></div>
    <div class="hist-box"><canvas id="pag_cHist"></canvas></div>
  </div>
</div>

<div class="section-label">📋 Resumo completo</div>
<div id="pag_window_selector" style="display:flex;align-items:center;gap:8px;margin-bottom:12px;flex-wrap:wrap">
  <span style="font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.6px">Janela:</span>
  <div style="display:flex;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:3px;gap:3px">
    <button type="button" data-w="3" class="win-btn active">3D</button>
    <button type="button" data-w="7" class="win-btn">7D</button>
    <button type="button" data-w="14" class="win-btn">14D</button>
    <button type="button" data-w="30" class="win-btn">30D</button>
    <button type="button" data-w="custom" class="win-btn">Custom</button>
  </div>
  <div id="pag_custom_range" style="display:none;align-items:center;gap:6px;margin-left:8px">
    <input type="date" id="pag_custom_start" style="background:#0f0f1e;border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:12px;padding:6px 8px">
    <span style="color:var(--muted)">até</span>
    <input type="date" id="pag_custom_end" style="background:#0f0f1e;border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:12px;padding:6px 8px">
    <button type="button" id="pag_custom_apply" style="background:var(--accent);color:#fff;border:none;border-radius:6px;padding:6px 12px;font-size:11px;font-weight:600;cursor:pointer">Aplicar</button>
  </div>
</div>
<div class="search-wrap">
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3-3"/></svg>
  <input type="text" id="pag_busca" placeholder="Buscar por nome, URL, geo ou nicho..." oninput="filtrarTabela('pag_')">
</div>
<div class="tbl-panel">
  <table>
    <thead><tr>
      <th>#</th><th>Bibliotecas</th><th>Gráfico</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
      <th>Δ Total</th><th>Tendência</th><th id="pag_th_score" tabindex="0" role="button" aria-sort="descending" title="Clique para ordenar pelo score" style="cursor:pointer">SCORE ↕</th><th class="participacao-header">Participação</th><th id="pag_th_janela">3D</th><th>Instagram</th>
    </tr></thead>
    <tbody id="pag_tbody"></tbody>
  </table>
  <div class="empty-hint" id="pag_busca-vazio" style="display:none;margin:0;border:none;border-top:1px solid var(--border);border-radius:0">Nenhum resultado encontrado.</div>
</div>

<div style="height:36px"></div>

<div class="group-title">🌐 DOMÍNIOS — rastreio por URL</div>

<div class="section-label">🚀 Escalando agora — domínios em ascensão</div>
<div class="scaling-strip" id="dom_scaling"></div>
<div class="empty-hint" id="dom_scaling-empty" style="display:none">Nenhum item em ascensão no período. Conforme as coletas acumulam, o que cresce aparece aqui.</div>

<div class="grid-charts">
  <div class="panel">
    <div class="panel-title">🍩 Distribuição atual</div>
    <div class="rosca-wrap">
      <div class="rosca-canvas"><canvas id="dom_cRosca"></canvas></div>
      <div class="legend" id="dom_legend"></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
    <div class="hist-selection" id="dom_hist-selection" hidden><span id="dom_hist-status" aria-live="polite"></span><button class="hist-reset" id="dom_hist-reset" type="button">Restaurar padrão</button></div>
    <div class="hist-box"><canvas id="dom_cHist"></canvas></div>
  </div>
</div>

<div class="section-label">📋 Resumo completo</div>
<div class="search-wrap">
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3-3"/></svg>
  <input type="text" id="dom_busca" placeholder="Buscar por nome, URL, geo ou nicho..." oninput="filtrarTabela('dom_')">
</div>
<div class="tbl-panel">
  <table>
    <thead><tr>
      <th>#</th><th>Domínios</th><th>Gráfico</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
      <th>Δ Total</th><th>Tendência</th><th class="participacao-header">Participação</th><th>3 dias</th>
    </tr></thead>
    <tbody id="dom_tbody"></tbody>
  </table>
  <div class="empty-hint" id="dom_busca-vazio" style="display:none;margin:0;border:none;border-top:1px solid var(--border);border-radius:0">Nenhum resultado encontrado.</div>
</div>

<div style="height:36px"></div>

  <div class="group-title">🔑 PALAVRAS-CHAVE — rastreio por keyword</div>
  <div class="section-label">🚀 Escalando agora — palavras-chave em ascensão</div>
  <div class="scaling-strip" id="key_scaling"></div>
  <div class="empty-hint" id="key_scaling-empty" style="display:none">Nenhuma palavra-chave em ascensão.</div>

  <div class="grid-charts">
    <div class="panel">
      <div class="panel-title">🍩 Distribuição atual</div>
      <div class="rosca-wrap">
        <div class="rosca-canvas"><canvas id="key_cRosca"></canvas></div>
        <div class="legend" id="key_legend"></div>
      </div>
    </div>
    <div class="panel">
      <div class="panel-title">📈 Evolução histórica — média diária <span style="color:var(--down);font-weight:400;text-transform:none;letter-spacing:0;margin-left:4px">● dia de descoberta</span></div>
      <div class="hist-selection" id="key_hist-selection" hidden><span id="key_hist-status" aria-live="polite"></span><button class="hist-reset" id="key_hist-reset" type="button">Restaurar padrão</button></div>
      <div class="hist-box"><canvas id="key_cHist"></canvas></div>
    </div>
  </div>

  <div class="section-label">📋 Resumo completo</div>
  <div class="search-wrap">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3-3"/></svg>
    <input type="text" id="key_busca" placeholder="Buscar por palavra-chave, URL, geo ou nicho..." oninput="filtrarTabela('key_')">
  </div>
  <div class="tbl-panel">
    <table>
      <thead><tr>
        <th>#</th><th>Palavras-chave</th><th>Gráfico</th><th>Descoberta</th><th>Inicial</th><th>Atual</th><th>Última Checagem</th>
        <th>Δ Total</th><th>Tendência</th><th class="participacao-header">Participação</th><th>3 dias</th>
      </tr></thead>
      <tbody id="key_tbody"></tbody>
    </table>
    <div class="empty-hint" id="key_busca-vazio" style="display:none;margin:0;border:none;border-top:1px solid var(--border);border-radius:0">Nenhum resultado encontrado.</div>
  </div>

  <div style="height:36px"></div>

  <div class="group-title">🔀 Mapeamento de Funis</div>
<div class="empty-hint" style="display:flex;align-items:center;justify-content:center;gap:14px">
  <span>O mapeamento completo de funis agora tem uma página dedicada.</span>
  <a href="/funis" style="font-size:13px;font-weight:600;color:var(--accent);text-decoration:none;border:1px solid var(--accent);padding:8px 18px;border-radius:8px;white-space:nowrap">🔀 Abrir Mapa de Funis</a>
</div>

<div style="height:36px"></div>

<div class="group-title">📢 Mapeamento ADS</div>
${adsPaginas.length === 0
  ? '<div class="empty-hint">Nenhum ADS mapeado ainda. Cadastre em Admin → 🔀 Funis → Nova Etapa (tipo ADS) — não precisa conectar a nada.</div>'
  : `<div class="player-caminhos" style="margin-bottom:26px">${adsCardsHtml}</div>`}

<div style="height:36px"></div>

<div class="group-title">📅 Histórico de Coletas</div>


<div class="accordion-wrap">
  <button class="accordion-btn" onclick="toggleAccordion('pag_hist-section','pag_acc-icon')">
    <span>📡 Histórico de Coletas — Bibliotecas · 03h · 12h · 22h</span>
    <span class="acc-meta" id="pag_acc-meta"></span>
    <span class="acc-icon" id="pag_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="pag_hist-section">Carregando histórico...</div>
</div>

<div class="accordion-wrap" style="margin-top:12px">
  <button class="accordion-btn" onclick="toggleAccordion('dom_hist-section','dom_acc-icon')">
    <span>🌐 Histórico de Coletas — Domínios · 03h · 05h · 12h · 22h</span>
    <span class="acc-meta" id="dom_acc-meta"></span>
    <span class="acc-icon" id="dom_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="dom_hist-section">Carregando histórico...</div>
</div>

<div class="accordion-wrap" style="margin-top:12px">
  <button class="accordion-btn" onclick="toggleAccordion('key_hist-section','key_acc-icon')">
    <span>🔑 Histórico de Coletas — Palavras-chave · 06h</span>
    <span class="acc-meta" id="key_acc-meta"></span>
    <span class="acc-icon" id="key_acc-icon">▼</span>
  </button>
  <div class="accordion-body" id="key_hist-section">Carregando histórico...</div>
</div>

<script>
const IG_SVG=${JSON.stringify(IG_SVG)};
document.getElementById("upd").textContent="Atualizado "+new Date().toLocaleString("pt-BR")+"  ·  páginas 03h · 12h · 22h · domínios 05h · palavras-chave 06h";
let pagWindow=3;
let pagCustom=null;
let pagWindowCustom=false;
try{
  const storedWindow=localStorage.getItem("pag_window")||"3";
  const parsedWindow=parseInt(storedWindow,10);
  pagWindow=[3,7,14,30].includes(parsedWindow)?parsedWindow:3;
  pagWindowCustom=storedWindow==="custom";
  pagCustom=JSON.parse(localStorage.getItem("pag_custom")||"null");
  if(!pagCustom||typeof pagCustom.start!=="string"||typeof pagCustom.end!=="string")pagCustom=null;
}catch(error){
  console.warn("Não foi possível carregar a janela salva das bibliotecas: "+error.message);
}

function toggleAccordion(bodyId,iconId){
  const body=document.getElementById(bodyId);
  const icon=document.getElementById(iconId);
  body.classList.toggle('open');
  icon.classList.toggle('open');
}

function render(D,HD,P){
const COR=["#7c6fff","#34d399","#fb7185","#fbbf24","#22d3ee","#a78bfa","#f97316","#4ade80","#ec4899","#38bdf8","#facc15","#2dd4bf","#fb923c","#a3e635","#e879f9","#60a5fa"];
function med(s){const v=Object.values(s).filter(x=>!isNaN(x));return v.length?Math.round(v.reduce((a,b)=>a+b,0)/v.length):null}
function fd(dk){const[y,m,d]=dk.split("-");return d+"/"+m}
function fdFull(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y}
function escapeAttr(value){return String(value).replace(/[&<>"']/g,ch=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]))}
const{pags,ultima,primeira,mon,meta}=D;
const LP=Object.keys(pags).sort();
const dSet=new Set();LP.forEach(p=>Object.keys(pags[p]).forEach(d=>dSet.add(d)));
const datas=Array.from(dSet).sort();

function serie(pag){return datas.map(dk=>pags[pag]?.[dk]?med(pags[pag][dk]):null)}
function slope(datas,valores){
  if(valores.length<2||datas.length!==valores.length)return 0;
  const rawXs=datas.map((date,index)=>{
    const timestamp=Date.parse(date+"T00:00:00Z");
    return Number.isFinite(timestamp)?timestamp/86400000:index;
  });
  const baseX=rawXs[0];
  const xs=rawXs.map(value=>value-baseX);
  const n=valores.length;
  const sumX=xs.reduce((sum,value)=>sum+value,0);
  const sumX2=xs.reduce((sum,value)=>sum+value*value,0);
  const sumY=valores.reduce((sum,value)=>sum+value,0);
  const sumXY=xs.reduce((sum,value,index)=>sum+value*valores[index],0);
  const denominator=n*sumX2-sumX*sumX;
  return denominator?(n*sumXY-sumX*sumY)/denominator:0;
}
function slopeHistorico(pag){
  const s=serie(pag).filter(v=>v!==null);
  if(s.length<2)return 0;
  const recent=s.slice(-3);
  return recent[recent.length-1]-recent[0];
}
function info(pag){
  const at=ultima[pag]?.ads??0;
  const ini=mon[pag]?.ini??at;
  const vn=at-ini;
  const pct=ini>0?Math.round(((at-ini)/ini)*100):0;
  const sl=slopeHistorico(pag);
  let cls,label,color;
  if(at===0){cls="b-off";label="Inativo";color=getComputedStyle(document.documentElement).getPropertyValue('--muted')}
  else if(pct>50){cls="b-hot";label="Escalando forte";color="#a78bfa"}
  else if(sl>0&&pct>5){cls="b-up";label="Crescendo";color="#34d399"}
  else if(sl<0&&pct<-5){cls="b-down";label="Cortando";color="#fb7185"}
  else if(vn===0){cls="b-flat";label="Estável";color="#9999b8"}
  else if(vn>0){cls="b-up";label="Subindo";color="#34d399"}
  else{cls="b-down";label="Caindo";color="#fb7185"}
  return{at,ini,vn,pct,sl,cls,label,color};
}
function computeWindowStats(pagName,windowDays,customRange){
  let dateKeys=Object.keys(pags[pagName]||{}).sort();
  if(customRange){
    dateKeys=dateKeys.filter(dk=>dk>=customRange.start&&dk<=customRange.end);
  }else{
    dateKeys=dateKeys.slice(-windowDays);
  }
  const serie=dateKeys.map(dk=>med(pags[pagName][dk])).filter(value=>value!==null);
  if(serie.length<2)return{pct:0,delta:0,slope:0,label:"Estável",cls:"b-flat",serie,primeiro:serie[0]??0,ultimo:serie[0]??0};
  const n=serie.length;
  const firstChunk=serie.slice(0,Math.max(1,Math.floor(n*0.3)));
  const lastChunk=serie.slice(Math.floor(n*0.7));
  const primeiro=firstChunk.reduce((sum,value)=>sum+value,0)/firstChunk.length;
  const ultimo=lastChunk.reduce((sum,value)=>sum+value,0)/lastChunk.length;
  const delta=ultimo-primeiro;
  const pct=primeiro>0?(delta/primeiro)*100:0;
  const sumX=(n*(n-1))/2;
  const sumX2=(n*(n-1)*(2*n-1))/6;
  const sumY=serie.reduce((sum,value)=>sum+value,0);
  const sumXY=serie.reduce((sum,value,index)=>sum+index*value,0);
  const denominator=n*sumX2-sumX*sumX;
  const slope=denominator?(n*sumXY-sumX*sumY)/denominator:0;
  const at=ultima[pagName]?.ads??0;
  let label="Estável",cls="b-flat";
  if(at===0){label="Inativo";cls="b-off"}
  else if(pct>50&&slope>0){label="Escalando forte";cls="b-hot"}
  else if(pct>15&&slope>0){label="Crescendo";cls="b-up"}
  else if(delta>0){label="Subindo";cls="b-up"}
  else if(Math.abs(pct)<=5){label="Estável";cls="b-flat"}
  else if(delta<0&&pct>-15){label="Caindo";cls="b-down"}
  else if(pct<-15&&slope<0){label="Cortando";cls="b-down"}
  return{pct,delta,slope,label,cls,serie,primeiro,ultimo};
}
function shiftDateKey(dateKey,days){
  const date=new Date(dateKey+"T00:00:00Z");
  date.setUTCDate(date.getUTCDate()+days);
  return date.toISOString().slice(0,10);
}
function computeScoreAndFase(pagName){
  const at=ultima[pagName]?.ads;
  if(at===0||at===null||at===undefined)return{score:0,fase:"💀 INATIVO",label:"Morto",velocidadeAtual:0,aceleracao:0,veredito:"Morto"};

  const serieCompleta=datas
    .map(dk=>({data:dk,valor:pags[pagName]?.[dk]?med(pags[pagName][dk]):null}))
    .filter(point=>point.valor!==null);
  const ultimoDia=datas[datas.length-1]||new Date().toISOString().slice(0,10);
  let inicioAtual,fimAtual,inicioAnterior,fimAnterior;
  const customStartMs=Date.parse((pagCustom?.start||"")+"T00:00:00Z");
  const customEndMs=Date.parse((pagCustom?.end||"")+"T00:00:00Z");
  if(pagWindowCustom&&pagCustom&&Number.isFinite(customStartMs)&&Number.isFinite(customEndMs)&&pagCustom.start<=pagCustom.end){
    inicioAtual=pagCustom.start;
    fimAtual=pagCustom.end;
    const diasJanela=Math.max(1,Math.round((Date.parse(fimAtual+"T00:00:00Z")-Date.parse(inicioAtual+"T00:00:00Z"))/86400000)+1);
    fimAnterior=shiftDateKey(inicioAtual,-1);
    inicioAnterior=shiftDateKey(inicioAtual,-diasJanela);
  }else{
    const diasJanela=Math.max(1,Number(pagWindow)||3);
    fimAtual=ultimoDia;
    inicioAtual=shiftDateKey(fimAtual,-(diasJanela-1));
    fimAnterior=shiftDateKey(inicioAtual,-1);
    inicioAnterior=shiftDateKey(inicioAtual,-diasJanela);
  }

  const atual=serieCompleta.filter(point=>point.data>=inicioAtual&&point.data<=fimAtual);
  const anterior=serieCompleta.filter(point=>point.data>=inicioAnterior&&point.data<=fimAnterior);
  const velocidadeAtual=slope(atual.map(point=>point.data),atual.map(point=>point.valor));
  const velocidadeAnterior=slope(anterior.map(point=>point.data),anterior.map(point=>point.valor));
  const aceleracao=velocidadeAtual-velocidadeAnterior;
  const ini=mon[pagName]?.ini||at;
  const vn=at-ini;
  let score=0;
  if(at>=10&&at<=30)score+=35;
  else if(at<=80)score+=25;
  else if(at<=150)score+=12;
  else if(at<=300)score+=3;
  if(velocidadeAtual>0.5)score+=30;
  else if(velocidadeAtual>0.1)score+=20;
  else if(velocidadeAtual>0)score+=8;
  if(velocidadeAtual>0.1&&aceleracao>=-0.05)score+=20;
  else if(velocidadeAtual>0.1&&aceleracao< -0.05)score+=5;
  else if(velocidadeAtual< -0.1&&aceleracao>0)score+=8;
  if(vn>=10&&vn<=150)score+=15;
  else if(vn>150&&vn<=400)score+=8;
  score=Math.max(0,Math.min(100,Math.round(score)));

  let fase;
  if(serieCompleta.length<=4&&at>0&&at<=20)fase="🧪 TESTANDO";
  else if(velocidadeAtual>0.1&&aceleracao>=-0.05)fase="🚀 ESCALANDO";
  else if(velocidadeAtual>0.1&&aceleracao< -0.05)fase="⚠️ EM DECLÍNIO";
  else if(Math.abs(velocidadeAtual)<=0.1)fase="➖ ESTÁVEL";
  else if(velocidadeAtual< -0.1&&aceleracao<=0)fase="📉 EM QUEDA";
  else if(velocidadeAtual< -0.1&&aceleracao>0)fase="🔄 FREANDO QUEDA";
  else fase="➖ ESTÁVEL";
  const veredito=score>=85&&fase.includes("🚀")?"✅ Vale modelar"
    :fase.includes("⚠️")?"⚠️ Cuidado - em declínio"
    :fase.includes("🧪")?"👀 Testando - entrar barato"
    :"❌ Ignorar";
  return{score,fase,label:fase,velocidadeAtual,aceleracao,veredito};
}
function scoreCellContent(result,pagName){
  const fase=result.fase||"➖ ESTÁVEL";
  const faseClass=fase.includes("🚀")?"score-escalando"
    :fase.includes("DECLÍNIO")?"score-declinio"
    :fase.includes("TESTANDO")?"score-testando"
    :fase.includes("QUEDA")?"score-queda"
    :fase.includes("INATIVO")?"score-inativo"
    :fase.includes("FREANDO")?"score-freando"
    :"score-estavel";
  const faseEmoji=fase.split(/\s+/)[0];
  const labelSemEmoji=fase.replace(/^\S+\s*/,"");
  const stats=computeWindowStats(pagName,pagWindow,pagWindowCustom?pagCustom:null);
  const pctJanela=Math.round(Number(stats.pct)||0);
  const trendIcon=fase.includes("DECLÍNIO")?"↘"
    :fase.includes("ESCALANDO")?"↗"
    :fase.includes("FREANDO")?"↗"
    :pctJanela>0?"↗":pctJanela<0?"↘":"→";
  const trendLabel=fase.includes("DECLÍNIO")
    ?"Desacelerando"
    :(pctJanela>0?"+":"")+pctJanela+"%";
  const title="Velocidade: "+result.velocidadeAtual.toFixed(2)+" ads/dia na janela "+windowLabel()
    +" | Aceleração: "+result.aceleracao.toFixed(2)+" | Variação: "+(pctJanela>0?"+":"")+pctJanela+"%"
    +" | "+result.veredito;
  return'<div class="score-cell '+faseClass+'" title="'+escapeAttr(title)+'">'
    +'<div class="score-main"><span class="score-number">'+result.score+'</span><span class="score-icon">'+faseEmoji+'</span></div>'
    +'<div class="score-fase"><span class="fase-label">'+labelSemEmoji+'</span><span class="fase-trend">'+trendIcon+' '+trendLabel+'</span></div>'
    +'</div>';
}
let scoreSortDirection=-1;
const scoreByName=new Map();
if(P==="pag_")LP.forEach(pag=>scoreByName.set(pag,computeScoreAndFase(pag)));
function windowLabel(){
  return pagWindowCustom?"CUSTOM":pagWindow+"D";
}
function windowTrendHtml(stats){
  return'<span class="badge '+stats.cls+'">'+stats.label+'</span>';
}
function windowValueHtml(stats){
  if(stats.delta>0)return'<span style="color:#34d399">▲ sub</span> <small style="color:var(--muted)">+'+Math.round(stats.pct)+'%</small>';
  if(stats.delta<0)return'<span style="color:#fb7185">▼ cai</span> <small style="color:var(--muted)">'+Math.round(stats.pct)+'%</small>';
  return'<span style="color:var(--muted)">= est</span>';
}
function updateResumoBibliotecas(){
  if(P!=="pag_")return;
  const header=document.getElementById("pag_th_janela");
  if(header)header.textContent=windowLabel();
  document.querySelectorAll("#pag_tbody tr").forEach(function(row){
    const pagName=row.dataset.pagName;
    if(!pagName)return;
    const stats=computeWindowStats(pagName,pagWindow,pagWindowCustom?pagCustom:null);
    const trendCell=row.querySelector("[data-role='pag-window-trend']");
    const windowCell=row.querySelector("[data-role='pag-window-value']");
    if(trendCell)trendCell.innerHTML=windowTrendHtml(stats);
    if(windowCell){
      windowCell.innerHTML=windowValueHtml(stats);
      windowCell.dataset.label=windowLabel();
    }
  });
  updateScoreCellsAndSort();
  const selector=document.getElementById("pag_window_selector");
  if(!selector)return;
  selector.querySelectorAll(".win-btn").forEach(function(button){
    button.classList.toggle("active",button.dataset.w===(pagWindowCustom?"custom":String(pagWindow)));
  });
  const range=document.getElementById("pag_custom_range");
  if(range)range.style.display=pagWindowCustom?"flex":"none";
  if(pagCustom){
    document.getElementById("pag_custom_start").value=pagCustom.start;
    document.getElementById("pag_custom_end").value=pagCustom.end;
  }
}
function setupResumoBibliotecas(){
  if(P!=="pag_")return;
  const selector=document.getElementById("pag_window_selector");
  if(!selector)return;
  selector.querySelectorAll(".win-btn").forEach(function(button){
    button.addEventListener("click",function(){
      if(button.dataset.w==="custom"){
        pagWindowCustom=true;
        try{localStorage.setItem("pag_window","custom")}catch(error){console.warn("Não foi possível salvar a janela das bibliotecas: "+error.message)}
      }else{
        pagWindow=parseInt(button.dataset.w,10);
        pagWindowCustom=false;
        try{localStorage.setItem("pag_window",String(pagWindow))}catch(error){console.warn("Não foi possível salvar a janela das bibliotecas: "+error.message)}
      }
      updateResumoBibliotecas();
    });
  });
  document.getElementById("pag_custom_apply").addEventListener("click",function(){
    const start=document.getElementById("pag_custom_start").value;
    const end=document.getElementById("pag_custom_end").value;
    if(!start||!end||start>end){
      window.alert("Selecione um intervalo válido: a data inicial deve ser anterior ou igual à data final.");
      return;
    }
    pagCustom={start,end};
    pagWindowCustom=true;
    try{
      localStorage.setItem("pag_custom",JSON.stringify(pagCustom));
      localStorage.setItem("pag_window","custom");
    }catch(error){console.warn("Não foi possível salvar o intervalo personalizado das bibliotecas: "+error.message)}
    updateResumoBibliotecas();
  });
  console.log("[DASHBOARD] seletor janela pag_ pronto, Instagram mantido em bibliotecas");
}

const porAds=[...LP].sort((a,b)=>P==="pag_"
  ?(scoreByName.get(b)?.score||0)-(scoreByName.get(a)?.score||0)||(ultima[b]?.ads||0)-(ultima[a]?.ads||0)
  :(ultima[b]?.ads||0)-(ultima[a]?.ads||0));
function updateScoreCellsAndSort(){
  if(P!=="pag_")return;
  const scoreTbody=document.getElementById("pag_tbody");
  if(!scoreTbody)return;
  const rows=[...scoreTbody.querySelectorAll("tr")];
  for(const row of rows){
    const pagName=row.dataset.pagName;
    if(!pagName)continue;
    const result=computeScoreAndFase(pagName);
    scoreByName.set(pagName,result);
    const cell=row.querySelector("[data-role='pag-score']");
    if(cell){
      cell.innerHTML=scoreCellContent(result,pagName);
      cell.dataset.scoreValue=String(result.score);
    }
  }
  rows.sort((a,b)=>{
    const scoreA=Number(a.querySelector("[data-role='pag-score']")?.dataset.scoreValue)||0;
    const scoreB=Number(b.querySelector("[data-role='pag-score']")?.dataset.scoreValue)||0;
    return (scoreA-scoreB)*scoreSortDirection;
  });
  rows.forEach((row,index)=>{
    row.cells[0].textContent=String(index+1);
    scoreTbody.appendChild(row);
  });
}
const maxAds=ultima[porAds[0]]?.ads||1;
const MAX_PINNED=4;
const storageKey="viva_dashboard_pinned_"+P.replace(/_$/g,"");
const chartSeries=porAds.slice(0,8);
const pinButtons=new Map();
let histChart=null;
let storageUnavailable=false;

function corBiblioteca(nome){
  let hash=2166136261;
  for(let i=0;i<nome.length;i++){
    hash^=nome.charCodeAt(i);
    hash=Math.imul(hash,16777619);
  }
  return COR[(hash>>>0)%COR.length];
}

function salvarPinned(){
  try{
    localStorage.setItem(storageKey,JSON.stringify(pinned));
    storageUnavailable=false;
  }catch(e){storageUnavailable=true}
}

function carregarPinned(){
  let raw;
  try{
    raw=localStorage.getItem(storageKey);
  }catch(e){storageUnavailable=true;return []}
  let stored;
  try{stored=JSON.parse(raw||"[]")}catch(e){
    try{localStorage.removeItem(storageKey)}catch(error){storageUnavailable=true}
    return [];
  }
  if(!Array.isArray(stored)){
    try{localStorage.removeItem(storageKey)}catch(e){storageUnavailable=true}
    return [];
  }
  const clean=Array.from(new Set(stored.filter(nome=>typeof nome==="string"&&LP.includes(nome)&&!chartSeries.includes(nome)))).slice(0,MAX_PINNED);
  if(JSON.stringify(clean)!==JSON.stringify(stored)){
    try{localStorage.setItem(storageKey,JSON.stringify(clean));}catch(e){storageUnavailable=true}
  }
  return clean;
}

let pinned=carregarPinned();
let focoHist=pinned[pinned.length-1]||null;

function bibliotecasHistorico(){
  return Array.from(new Set([...chartSeries,...pinned]));
}

function atualizarBotoesFixacao(){
  pinButtons.forEach((button,nome)=>{
    const isPinned=pinned.includes(nome);
    const isAutomatic=chartSeries.includes(nome);
    const isFocused=focoHist===nome;
    button.textContent="📌 "+(isPinned?"Fixada":isAutomatic?(isFocused?"Em destaque":"Destacar"):"Fixar");
    button.classList.toggle("is-pinned",isPinned);
    button.classList.toggle("is-focused",!isPinned&&isFocused);
    button.disabled=!isPinned&&!isAutomatic&&pinned.length>=MAX_PINNED;
    button.title=isPinned?"Desafixar do gráfico histórico":isAutomatic?(isFocused?"Remover destaque":"Destacar no gráfico histórico"):button.disabled?"Limite de 4 fixações. Desafixe uma biblioteca primeiro.":"Fixar e destacar no gráfico histórico";
    button.setAttribute("aria-label",isPinned?"Desafixar "+nome+" do gráfico histórico":isAutomatic?(isFocused?"Remover destaque de "+nome:"Destacar "+nome+" no gráfico histórico"):("Fixar "+nome+" no gráfico histórico"));
    button.setAttribute("aria-pressed",String(isPinned||isFocused));
  });
}

function atualizarSelecaoHistorico(){
  const selection=document.getElementById(P+"hist-selection");
  const status=document.getElementById(P+"hist-status");
  if(selection&&status){
    selection.hidden=pinned.length===0&&focoHist===null&&!storageUnavailable;
    const selectionStatus=pinned.length>0?pinned.length+(pinned.length===1?" biblioteca fixada":" bibliotecas fixadas")+" · "+bibliotecasHistorico().length+" séries no gráfico":focoHist?"Destacando: "+focoHist+" · "+bibliotecasHistorico().length+" séries no gráfico":"";
    status.textContent=selectionStatus+(storageUnavailable?(selectionStatus?" · ":"")+"Não será mantido após recarregar":"");
  }
  atualizarBotoesFixacao();
}

function criarDatasetsHistorico(){
  return bibliotecasHistorico().map(pag=>{
    const didK=primeira[pag]||null;
    const cor=corBiblioteca(pag);
    const isFocused=focoHist===pag;
    const isDimmed=focoHist!==null&&!isFocused;
    return{
      label:pag,
      data:serie(pag),
      borderColor:isDimmed?cor+"66":cor,
      backgroundColor:"transparent",
      borderWidth:isFocused?3:isDimmed?1.25:2,
      pointBackgroundColor:datas.map(dk=>dk===didK?"#fb7185":isDimmed?cor+"66":cor),
      pointRadius:datas.map(dk=>dk===didK?5:isFocused?3:2),
      pointHoverRadius:6,
      tension:.35,
      spanGaps:true
    };
  });
}

function atualizarGraficoHistorico(){
  if(!histChart)return;
  const visibilidade=new Map(histChart.data.datasets.map((dataset,index)=>[dataset.label,histChart.isDatasetVisible(index)]));
  histChart.data.datasets=criarDatasetsHistorico();
  histChart.data.datasets.forEach((dataset,index)=>{
    histChart.setDatasetVisibility(index,visibilidade.has(dataset.label)?visibilidade.get(dataset.label):true);
  });
  histChart.update("none");
}

function rolarParaGraficoHistorico(){
  const canvas=document.getElementById(P+"cHist");
  const painel=canvas?.closest(".panel");
  if(!painel)return;
  painel.scrollIntoView({
    behavior:window.matchMedia("(prefers-reduced-motion: reduce)").matches?"auto":"smooth",
    block:"center"
  });
}

function alternarFixacao(nome){
  let deveRolar=false;
  if(pinned.includes(nome)){
    pinned=pinned.filter(item=>item!==nome);
    if(focoHist===nome)focoHist=pinned[pinned.length-1]||null;
  }else if(chartSeries.includes(nome)){
    deveRolar=focoHist!==nome;
    focoHist=deveRolar?nome:null;
  }else{
    if(pinned.length>=MAX_PINNED)return;
    pinned=[...pinned,nome];
    focoHist=nome;
    deveRolar=true;
  }
  salvarPinned();
  atualizarSelecaoHistorico();
  atualizarGraficoHistorico();
  if(deveRolar)rolarParaGraficoHistorico();
}

const resetHist=document.getElementById(P+"hist-reset");
if(resetHist)resetHist.addEventListener("click",function(){
  pinned=[];
  focoHist=null;
  salvarPinned();
  atualizarSelecaoHistorico();
  atualizarGraficoHistorico();
});

const escalando=LP.map(p=>({p,...info(p)}))
  .filter(x=>x.at>0&&(x.label==="Escalando forte"||x.label==="Crescendo"||x.label==="Subindo")&&x.pct>0)
  .sort((a,b)=>b.pct-a.pct);

const strip=document.getElementById(P+"scaling");
if(escalando.length===0){
  document.getElementById(P+"scaling-empty").style.display="block";
}else{
  escalando.forEach((x,i)=>{
    const card=document.createElement("div");
    card.className="scale-card";
    const urlBib = ultima[x.p]?.url || "";
    card.title = "Clique para copiar o link e abrir a biblioteca de " + x.p;
    card.onclick = function() {
      if (urlBib) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(urlBib).catch(()=>{});
        } else {
          const ta = document.createElement("textarea");
          ta.value = urlBib;
          document.body.appendChild(ta);
          ta.select();
          try { document.execCommand("copy"); } catch(e){}
          document.body.removeChild(ta);
        }
        window.open(urlBib, "_blank", "noopener");
      }
      let oldToast = card.querySelector(".scale-card-toast");
      if (oldToast) oldToast.remove();
      const toast = document.createElement("div");
      toast.className = "scale-card-toast";
      toast.innerHTML = '<span style="font-size:16px">✓</span> Link copiado!';
      card.appendChild(toast);
      void toast.offsetWidth;
      toast.classList.add("show");
      setTimeout(()=>{
        toast.classList.remove("show");
        setTimeout(()=>{ toast.remove(); }, 300);
      }, 2000);
    };
    card.innerHTML='<div class="scale-card-rank">#'+(i+1)+' <span class="scale-arrow">↗</span></div>'
      +'<div class="scale-card-name">'+x.p+'</div>'
      +'<div class="scale-card-val">'+x.at.toLocaleString("pt-BR")+'</div>'
      +'<div class="scale-card-meta"><span class="scale-pct" style="color:'+x.color+'">'+(x.pct>=0?"+":"")+x.pct+'%</span>'
      +'<span style="color:var(--muted)">'+(x.vn>=0?"+":"")+x.vn+' ads desde início</span></div>'
      +'<div class="scale-spark"><canvas id="'+P+'spark'+i+'"></canvas></div>';
    strip.appendChild(card);
  });
}

const tbody=document.getElementById(P+"tbody");
porAds.forEach((pag,idx)=>{
  const x=info(pag);
  const pagWindowStats=P==="pag_"?computeWindowStats(pag,pagWindow,pagWindowCustom?pagCustom:null):null;
  const did=primeira[pag]?fdFull(primeira[pag]):"—";
  const s=serie(pag).filter(v=>v!==null);
  const last3=s.slice(-3);
  const spark3=last3.length>=2?(last3[last3.length-1]>last3[0]?'<span style="color:#34d399">▲ sub</span>':last3[last3.length-1]<last3[0]?'<span style="color:#fb7185">▼ cai</span>':'<span style="color:#888">= est</span>'):"—";
  const partPct=Math.round((x.at/maxAds)*100);
  const corLib=corBiblioteca(pag);

  // Monta célula do nome com badges de geo e nicho
  const m=meta?.[pag]||{};
  const geoBadge=m.geo?'<span class="t-geo-badge">🌍 '+m.geo+'</span>':'';
  const nichoBadge=m.nicho?'<span class="t-nicho-badge">🏷️ '+m.nicho+'</span>':'';
  const funilBadge=m.funil?'<span class="t-funil-badge">🎯 '+m.funil+'</span>':'';
  const metaBadgesHtml=(geoBadge||nichoBadge||funilBadge)?'<div class="t-meta-badges">'+geoBadge+nichoBadge+funilBadge+'</div>':'';
  const nomeCell='<span class="t-name-main"><a href="'+(ultima[pag]?.url||'#')+'" target="_blank" rel="noopener" class="lib-link">'+pag+'</a></span>'+metaBadgesHtml;
  const instagramCell=P==="pag_"
    ?'<td class="ig-cell" data-label="Instagram">'+(m.instagram_url
      ?'<a href="'+escapeAttr(m.instagram_url)+'" target="_blank" rel="noopener noreferrer" class="ig-link" title="Ver Instagram">'+IG_SVG+'</a>'
      :'<span class="ig-none">—</span>')+'</td>'
    :'';

  const tr=document.createElement("tr");
  if(P==="pag_")tr.dataset.pagName=pag;
  const checkAt=ultima[pag]?.ultimaColeta;
  const tentativa=ultima[pag]?.tentativa;
  const checkStatus=tentativa?.status;
  const checkLabels={falha_timeout:"timeout",falha_bloqueio:"bloqueio Meta",falha_parse:"não lido",falha_url_invalida:"URL inválida",falha_gravacao:"falha ao salvar"};
  const checkStatusHtml=tentativa?.relevante&&checkLabels[checkStatus]
    ?'<div style="color:#fb7185;font-size:10px" title="'+escapeAttr(tentativa.error||"")+'">'+checkLabels[checkStatus]+'</div>'
    :'';
  tr.dataset.search=(pag+" "+(ultima[pag]?.url||"")+" "+(m.geo||"")+" "+(m.nicho||"")).toLowerCase();
  const trendCell=P==="pag_"
    ?'<td data-label="Tendência" data-role="pag-window-trend">'+windowTrendHtml(pagWindowStats)+'</td>'
    :'<td data-label="Tendência"><span class="badge '+x.cls+'">'+x.label+'</span></td>';
  const scoreData=P==="pag_"?scoreByName.get(pag):null;
  const scoreCell=scoreData
    ?'<td class="score-cell-td" data-label="SCORE" data-role="pag-score" data-score-value="'+scoreData.score+'">'+scoreCellContent(scoreData,pag)+'</td>'
    :'';
  const windowCell=P==="pag_"
    ?'<td class="spark3" data-label="'+windowLabel()+'" data-role="pag-window-value">'+windowValueHtml(pagWindowStats)+'</td>'
    :'<td class="spark3" data-label="3 dias">'+spark3+'</td>';
  tr.innerHTML=
    '<td class="mono" data-label="#" style="color:var(--muted)">'+(idx+1)+'</td>'
    +'<td class="t-name" data-label="Nome">'+nomeCell+'</td>'
    +'<td class="hist-pin-cell" data-label="Gráfico"></td>'
    +'<td data-label="Descoberta" style="color:var(--muted)">'+did+'</td>'
    +'<td class="mono" data-label="Inicial">'+(mon[pag]?.ini==null?'—':x.ini)+'</td>'
    +'<td class="mono" data-label="Atual" style="color:#fff;font-weight:600">'+(ultima[pag]?.ads==null?'—':x.at)+'</td>'
    +'<td class="last-check-cell" data-slug="'+escapeAttr(ultima[pag]?.slug||"")+'" data-label="Últ. Checagem" style="color:var(--muted);font-family:Space Mono,monospace;font-size:11px">'
    +(checkAt?new Date(checkAt).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'—')
    +checkStatusHtml
    +'</td>'
    +'<td class="mono" data-label="Δ Total" style="color:'+(x.vn>0?"#34d399":x.vn<0?"#fb7185":"#888")+'">'+(x.vn>=0?"+":"")+x.vn+'</td>'
    +trendCell
    +scoreCell
    +'<td class="participacao-cell" data-label="Participação"><div class="participacao-wrapper"><span class="scalebar-bg"><span class="scalebar" style="width:'+partPct+'%;background:'+corLib+'"></span></span><span class="pct" style="font-size:11px;font-family:\'Space Mono\',monospace;color:var(--muted);flex-shrink:0">'+partPct+'%</span></div></td>'
    +windowCell
    +instagramCell;
  const pinButton=document.createElement("button");
  pinButton.type="button";
  pinButton.className="hist-pin-btn";
  pinButton.addEventListener("click",function(){alternarFixacao(pag)});
  pinButtons.set(pag,pinButton);
  tr.querySelector(".hist-pin-cell").appendChild(pinButton);
  tbody.appendChild(tr);
});
if(P==="pag_"){
  const scoreHeader=document.getElementById("pag_th_score");
  if(scoreHeader){
    scoreHeader.addEventListener("click",function(){
      scoreSortDirection*=-1;
      scoreHeader.setAttribute("aria-sort",scoreSortDirection===-1?"descending":"ascending");
      updateScoreCellsAndSort();
    });
    scoreHeader.addEventListener("keydown",function(event){
      if(event.key==="Enter"||event.key===" "){
        event.preventDefault();
        scoreHeader.click();
      }
    });
  }
}
atualizarBotoesFixacao();
if(P==="pag_"){
  setupResumoBibliotecas();
  updateResumoBibliotecas();
}

const ro=porAds.filter(p=>(ultima[p]?.ads||0)>0);
const totalRo=ro.reduce((s,p)=>s+(ultima[p]?.ads||0),0);
const legend=document.getElementById(P+"legend");
ro.forEach((p,i)=>{
  const at=ultima[p]?.ads||0;
  const pct=totalRo>0?Math.round((at/totalRo)*100):0;
  const it=document.createElement("div");it.className="leg-item";
  it.innerHTML='<span class="leg-dot" style="background:'+corBiblioteca(p)+'"></span>'
    +'<span class="leg-name" title="'+p+'">'+p+'</span>'
    +'<span class="leg-val">'+at.toLocaleString("pt-BR")+'</span>'
    +'<span class="leg-pct">'+pct+'%</span>';
  legend.appendChild(it);
});

new Chart(document.getElementById(P+"cRosca"),{
  type:"doughnut",
  data:{labels:ro,datasets:[{data:ro.map(p=>ultima[p]?.ads||0),backgroundColor:ro.map(corBiblioteca),borderWidth:2,borderColor:"#12121f"}]},
  options:{responsive:true,maintainAspectRatio:false,cutout:"62%",plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>" "+ctx.label+": "+ctx.parsed.toLocaleString("pt-BR")+" ads"}}}}
});

histChart=new Chart(document.getElementById(P+"cHist"),{
  type:"line",
  data:{labels:datas.map(fd),datasets:criarDatasetsHistorico()},
  options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:"bottom",labels:{color:"#b8b8d0",font:{size:11,family:"Space Grotesk"},padding:10,boxWidth:8,usePointStyle:true}},tooltip:{callbacks:{label:ctx=>" "+ctx.dataset.label+": "+(ctx.parsed.y??"—")+" ads"}}},scales:{x:{ticks:{color:"#7a7a98",font:{size:11}},grid:{color:"#1c1c30"}},y:{ticks:{color:"#7a7a98",font:{size:11}},grid:{color:"#1c1c30"},beginAtZero:false}}}
});
atualizarSelecaoHistorico();

escalando.forEach((x,i)=>{
  const el=document.getElementById(P+"spark"+i);
  if(!el)return;
  new Chart(el,{type:"line",data:{labels:datas,datasets:[{data:serie(x.p),borderColor:x.color,backgroundColor:"transparent",borderWidth:2,pointRadius:0,tension:.4,spanGaps:true}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}},elements:{line:{borderCapStyle:"round"}}}});
});

// ── Tabela histórica de coletas ──────────────────────────────────────────────
const histSection=document.getElementById(P+"hist-section");
if(!HD.dates.length||!HD.libs.length){
  histSection.innerHTML='<div class="empty-hint" style="margin:0">Nenhum dado histórico disponível ainda. Aguarde as próximas coletas automáticas.</div>';
}else{
  function fdH(dk){const[y,m,d]=dk.split("-");return d+"/"+m+"/"+y.slice(2)}
  function slotCell(nome,dk,slot){
    const v=HD.map[nome]?.[dk]?.[slot];
    if(v===undefined||v===null)return '<span class="hist-slot empty">—</span>';
    return '<span class="hist-slot">'+v+'</span>';
  }
  const historySlots=P==="dom_"?[3,5,12,22]:P==="key_"?[6]:[3,12,22];
  let thead='<thead><tr>'
    +'<th style="text-align:left;white-space:nowrap">Biblioteca</th>'
    +'<th style="text-align:left;white-space:nowrap">Data</th>'
    +historySlots.map(slot=>'<th style="text-align:center">'+String(slot).padStart(2,"0")+'h</th>').join("")
    +'</tr></thead>';
  let tbody2='<tbody>';
  let rowCount=0;
  for(const lib of HD.libs){
    let firstForLib=true;
    for(const dk of HD.dates){
      const slots=HD.map[lib]?.[dk];
      if(!slots)continue;
      const hasAny=historySlots.some(slot=>slots[slot]!==undefined);
      if(!hasAny)continue;
      tbody2+='<tr>'
        +'<td class="lib-name">'+(firstForLib?lib:'')+'</td>'
        +'<td class="date-col">'+fdH(dk)+'</td>'
        +historySlots.map(slot=>'<td>'+slotCell(lib,dk,slot)+'</td>').join("")
        +'</tr>';
      firstForLib=false;
      rowCount++;
    }
    if(!firstForLib){
      tbody2+='<tr style="height:4px;background:var(--bg)"><td colspan="'+(historySlots.length+2)+'"></td></tr>';
    }
  }
  tbody2+='</tbody>';
  histSection.innerHTML='<div class="tbl-scroll-x"><table class="hist-tbl">'+thead+tbody2+'</table></div>';
  const metaEl=document.getElementById(P+"acc-meta");
  if(metaEl)metaEl.textContent='('+rowCount+' registros)';
}
}

function filtrarTabela(P){
  const termo=document.getElementById(P+"busca").value.trim().toLowerCase();
  const linhas=document.querySelectorAll("#"+P+"tbody tr");
  let visiveis=0;
  linhas.forEach(function(tr){
    const match=!termo||(tr.dataset.search||"").includes(termo);
    tr.style.display=match?"":"none";
    if(match)visiveis++;
  });
  const msg=document.getElementById(P+"busca-vazio");
  if(msg)msg.style.display=visiveis===0?"block":"none";
}

const D_DOM=__DADOS_DOM__;
const HD_DOM=__HIST_DOM__;
const D_KEY=__DADOS_KEY__;
const HD_KEY=__HIST_KEY__;
const D_PAG=__DADOS_PLACEHOLDER__;
const HD_PAG=__HIST_PLACEHOLDER__;

const totalLibs=Object.keys(D_DOM.pags).length+Object.keys(D_PAG.pags).length+Object.keys(D_KEY.pags).length;
document.getElementById("livecount").textContent=Object.keys(D_DOM.pags).length+" domínios · "+Object.keys(D_KEY.pags).length+" palavras-chave · "+Object.keys(D_PAG.pags).length+" Páginas/FanPage";

render(D_PAG,HD_PAG,"pag_");
render(D_DOM,HD_DOM,"dom_");
render(D_KEY,HD_KEY,"key_");

const manualCheckButton=document.getElementById("manual-check-button");
const manualCheckLabel=document.getElementById("manual-check-label");
const manualCustomizeButton=document.getElementById("manual-customize-button");
const manualSelectionDialog=document.getElementById("manual-selection-dialog");
const manualPagesList=document.getElementById("manual-pages-list");
const manualUrlsList=document.getElementById("manual-urls-list");
const manualKeywordsList=document.getElementById("manual-keywords-list");
const manualSelectionSearch=document.getElementById("manual-selection-search");
const manualPagesCount=document.getElementById("manual-pages-count");
const manualUrlsCount=document.getElementById("manual-urls-count");
const manualKeywordsCount=document.getElementById("manual-keywords-count");
const manualSelectionMessage=document.getElementById("manual-selection-message");
const manualCheckSelectedButton=document.getElementById("manual-check-selected");
const manualConfirmDialog=document.getElementById("manual-confirm-dialog");
const manualConfirmTitle=document.getElementById("manual-confirm-title");
const manualConfirmDescription=document.getElementById("manual-confirm-description");
const manualConfirmSummary=document.getElementById("manual-confirm-summary");
const manualConfirmList=document.getElementById("manual-confirm-list");
const manualConfirmStart=document.getElementById("manual-confirm-start");
const manualRunPanel=document.getElementById("manual-run-panel");
const manualRunTitle=document.getElementById("manual-run-title");
const manualRunCurrent=document.getElementById("manual-run-current");
const manualRunCount=document.getElementById("manual-run-count");
const manualRunDismiss=document.getElementById("manual-run-dismiss");
const manualRunProgress=document.getElementById("manual-run-progress");
const manualRunTrack=manualRunProgress.parentElement;
const manualRunResults=document.getElementById("manual-run-results");
const manualReportOpen=document.getElementById("manual-report-open");
const manualReportDialog=document.getElementById("manual-report-dialog");
const manualReportSubtitle=document.getElementById("manual-report-subtitle");
const manualReportStatus=document.getElementById("manual-report-status");
const manualReportMetrics=document.getElementById("manual-report-metrics");
const manualReportResultsCount=document.getElementById("manual-report-results-count");
const manualReportResults=document.getElementById("manual-report-results");
const manualReportTimestamp=document.getElementById("manual-report-timestamp");
const MANUAL_REPORT_DISMISSED_KEY="lowticket-manual-check-dismissed-run";
let dismissedManualRunId=null;
const manualStatusLabels={
  ok:"Conferida",
  ok_zero:"0 anúncios",
  falha_bloqueio:"Bloqueio Meta",
  falha_timeout:"Timeout/rede",
  falha_parse:"Contador não lido",
  falha_url_invalida:"URL inválida",
  falha_gravacao:"Falha ao salvar",
  falha_execucao:"Falha na execução",
};
const validManualCheckStates=new Set(["idle","running","completed","completed_with_errors","failed"]);
const validManualResultStates=new Set(Object.keys(manualStatusLabels));
const MANUAL_START_RETRY_WINDOW_MS=60_000;
let dashboardReloadingRunId=null;
let manualSelectionLoaded=false;
let manualPages=[];
let pendingManualSlugs=[];
let pendingManualSnapshot=[];
let pendingManualRun=null;
let manualRequestTrackingId=null;
let manualDisplayedRunId=null;
let manualRetryTimer=null;
const MANUAL_SELECTION_LIMIT=500;

function selectedManualSlugs(){
  return Array.from(manualSelectionDialog.querySelectorAll("input[type=checkbox]:checked"),function(input){return input.value});
}

async function fingerprintManualSnapshot(snapshot){
  const canonical=snapshot
    .map(page=>[page.slug,page.nome,page.url])
    .sort((left,right)=>left[0]<right[0]?-1:left[0]>right[0]?1:0);
  const digest=await crypto.subtle.digest("SHA-256",new TextEncoder().encode(JSON.stringify(canonical)));
  return Array.from(new Uint8Array(digest),byte=>byte.toString(16).padStart(2,"0")).join("");
}

function updateManualSelectionCount(){
  const total=selectedManualSlugs().length;
  for(const checkbox of manualSelectionDialog.querySelectorAll("input[type=checkbox]")){
    checkbox.disabled=total>=MANUAL_SELECTION_LIMIT&&!checkbox.checked;
  }
  manualSelectionMessage.textContent=total>=MANUAL_SELECTION_LIMIT?"Limite de 500 itens por checagem personalizada atingido.":total?total+" item"+(total===1?"":"s")+" selecionado"+(total===1?"":"s"):"Nenhum item selecionado";
  manualCheckSelectedButton.textContent="Realizar checagem · "+total;
  manualCheckSelectedButton.disabled=total===0||manualCheckButton.disabled;
}

function renderManualSelection(pages){
  manualPages=[...pages].sort((a,b)=>a.nome.localeCompare(b.nome,"pt-BR"));
  const groups=[
    {items:manualPages.filter(page=>page.tipo==="pagina"),list:manualPagesList,count:manualPagesCount,empty:"Nenhuma página monitorada cadastrada."},
    {items:manualPages.filter(page=>page.tipo==="dominio"),list:manualUrlsList,count:manualUrlsCount,empty:"Nenhuma URL monitorada cadastrada."},
    {items:manualPages.filter(page=>page.tipo==="keyword"),list:manualKeywordsList,count:manualKeywordsCount,empty:"Nenhuma palavra-chave monitorada cadastrada."},
  ];
  for(const group of groups){
    group.list.replaceChildren();
    group.count.textContent=String(group.items.length);
    if(!group.items.length){
      const empty=document.createElement("div");
      empty.className="selection-empty";
      empty.textContent=group.empty;
      group.list.appendChild(empty);
      continue;
    }
    group.items.forEach(function(page){
      const label=document.createElement("label");
      label.className="manual-selection-option";
      label.dataset.search=(page.nome+" "+page.tipo+" "+page.url).toLowerCase();
      const checkbox=document.createElement("input");
      checkbox.type="checkbox";
      checkbox.value=page.slug;
      checkbox.setAttribute("aria-label","Selecionar "+page.nome);
      checkbox.addEventListener("change",updateManualSelectionCount);
      const copy=document.createElement("span");
      copy.className="manual-selection-option-copy";
      const name=document.createElement("span");
      name.className="manual-selection-option-name";
      name.textContent=page.nome;
      const url=document.createElement("span");
      url.className="manual-selection-option-url";
      url.textContent=page.url;
      copy.append(name,url);
      label.append(checkbox,copy);
      group.list.appendChild(label);
    });
  }
  updateManualSelectionCount();
}

async function loadManualSelection(force=false){
  if(manualSelectionLoaded&&!force)return;
  manualSelectionLoaded=true;
  try{
    const response=await fetch("/api/paginas",{cache:"no-store"});
    if(!response.ok)throw new Error("Não foi possível carregar as bibliotecas.");
    const pages=await response.json();
    if(!Array.isArray(pages))throw new Error("A lista de bibliotecas está inválida.");
    renderManualSelection(pages);
  }catch(e){
    manualSelectionLoaded=false;
    for(const list of [manualPagesList,manualUrlsList,manualKeywordsList]){
      const error=document.createElement("div");
      error.className="selection-empty";
      error.textContent="Falha ao carregar a lista. Feche e abra para tentar novamente.";
      list.replaceChildren(error);
    }
  }
}

manualCustomizeButton.addEventListener("click",function(){
  manualSelectionDialog.showModal();
  manualSelectionSearch.focus();
  loadManualSelection();
});
document.getElementById("manual-selection-close").addEventListener("click",function(){manualSelectionDialog.close()});
document.getElementById("manual-selection-cancel").addEventListener("click",function(){manualSelectionDialog.close()});

manualSelectionSearch.addEventListener("input",function(){
  const term=manualSelectionSearch.value.trim().toLowerCase();
  manualSelectionDialog.querySelectorAll(".manual-selection-option").forEach(function(option){
    option.hidden=term!==""&&!option.dataset.search.includes(term);
  });
});

function openManualConfirmation(slugs,scope){
  const selected=scope==="todas"?[...manualPages]:slugs.map(slug=>manualPages.find(page=>page.slug===slug)).filter(Boolean);
  if(!selected.length)return;
  pendingManualSlugs=scope==="todas"?null:selected.map(page=>page.slug);
  pendingManualSnapshot=selected.map(page=>({slug:page.slug,nome:page.nome,url:page.url}));
  const pages=selected.filter(page=>page.tipo==="pagina");
  const urls=selected.filter(page=>page.tipo==="dominio");
  const keywords=selected.filter(page=>page.tipo==="keyword");
  manualConfirmTitle.textContent=scope==="todas"?"Confirmar checagem de todas?":"Confirmar checagem personalizada?";
  manualConfirmDescription.textContent=scope==="todas"?"Serão checadas todas as "+selected.length+" bibliotecas monitoradas. Revise a lista completa antes de iniciar.":"A checagem vai consultar exatamente estes "+selected.length+" itens. Revise nomes e URLs antes de iniciar.";
  manualConfirmSummary.replaceChildren();
  for(const label of [
    selected.length+" selecionado"+(selected.length===1?"":"s"),
    pages.length+" página"+(pages.length===1?"":"s"),
    urls.length+" URL"+(urls.length===1?"":"s"),
    keywords.length+" palavra-chave"+(keywords.length===1?"":"s"),
  ]){
    const badge=document.createElement("span");
    badge.textContent=label;
    manualConfirmSummary.appendChild(badge);
  }
  manualConfirmList.replaceChildren();
  for(const page of selected){
    const row=document.createElement("div");
    row.className="confirm-selection-item";
    const name=document.createElement("strong");
    name.textContent=(page.tipo==="dominio"?"URL monitorada · ":page.tipo==="keyword"?"Palavra-chave monitorada · ":"Página monitorada · ")+page.nome;
    const url=document.createElement("code");
    url.textContent=page.url;
    row.append(name,url);
    manualConfirmList.appendChild(row);
  }
  manualConfirmDialog.showModal();
}

manualCheckSelectedButton.addEventListener("click",function(){
  const slugs=selectedManualSlugs();
  if(!slugs.length)return;
  openManualConfirmation(slugs,"selecionadas");
});

async function openAllManualConfirmation(){
  if(manualCheckButton.disabled)return;
  manualCheckButton.disabled=true;
  manualCustomizeButton.disabled=true;
  manualCheckLabel.textContent="Preparando...";
  try{
    await loadManualSelection(true);
    if(!manualSelectionLoaded)throw new Error("Não foi possível carregar os itens monitorados. Tente novamente.");
    if(!manualPages.length)throw new Error("Nenhum item monitorado está cadastrado.");
    openManualConfirmation(null,"todas");
  }catch(error){
    manualRunPanel.hidden=false;
    manualRunTitle.textContent="Não foi possível preparar a checagem";
    manualRunCurrent.textContent=error.message;
    manualRunCount.textContent="";
    manualRunResults.replaceChildren();
  }finally{
    manualCheckButton.disabled=false;
    manualCustomizeButton.disabled=false;
    manualCheckLabel.textContent="Checar todas";
    updateManualSelectionCount();
  }
}

document.getElementById("manual-confirm-close").addEventListener("click",function(){manualConfirmDialog.close()});
document.getElementById("manual-confirm-back").addEventListener("click",function(){manualConfirmDialog.close()});
manualConfirmStart.addEventListener("click",function(){
  const slugs=pendingManualSlugs===null?null:[...pendingManualSlugs];
  const snapshot=[...pendingManualSnapshot];
  if(!snapshot.length)return;
  pendingManualSlugs=[];
  pendingManualSnapshot=[];
  manualConfirmDialog.close();
  if(manualSelectionDialog.open)manualSelectionDialog.close();
  startDashboardCheck(slugs,snapshot);
});

function renderManualReport(state){
  const resultados=state.resultados||[];
  const total=Number(state.total)||0;
  const sucesso=Number(state.sucesso)||0;
  const falha=Number(state.falha)||0;
  const inicio=state.iniciadoEm?new Date(state.iniciadoEm):null;
  const fim=state.finalizadoEm?new Date(state.finalizadoEm):null;
  const duracao=inicio&&fim?Math.max(0,fim.getTime()-inicio.getTime()):null;
  const totalSegundos=duracao===null?null:Math.round(duracao/1000);
  const duracaoLabel=totalSegundos===null?"":totalSegundos<60?totalSegundos+" s":Math.floor(totalSegundos/60)+" min "+(totalSegundos%60)+" s";
  const escopo=state.escopo==="selecionadas"?"Seleção personalizada":"Todas as monitoradas";
  const dataFim=fim&&!Number.isNaN(fim.getTime())?fim.toLocaleString("pt-BR",{dateStyle:"long",timeStyle:"short"}):"Horário indisponível";
  manualReportSubtitle.textContent=escopo+" · "+dataFim;
  const hasExecutionError=state.status==="completed_with_errors"||state.status==="failed";
  manualReportStatus.classList.toggle("has-failures",falha>0||hasExecutionError);
  manualReportStatus.textContent=state.status==="failed"?"Execução interrompida":state.status==="completed_with_errors"?(falha>0?"Checagem concluída com falhas":state.erro||"Checagem concluída com falhas"):falha>0?"Checagem concluída com falhas":"Checagem concluída";
  manualReportMetrics.replaceChildren();
  const metricas=[
    ["No escopo",total],
    ["Concluídas",Number(state.concluidos)||0],
    ["Com sucesso",sucesso],
    ["Com falha",falha],
  ];
  for(const [label,value] of metricas){
    const metric=document.createElement("div");
    metric.className="report-metric";
    const metricLabel=document.createElement("div");
    metricLabel.className="report-metric-label";
    metricLabel.textContent=label;
    const metricValue=document.createElement("div");
    metricValue.className="report-metric-value";
    metricValue.textContent=Number(value).toLocaleString("pt-BR");
    metric.append(metricLabel,metricValue);
    manualReportMetrics.appendChild(metric);
  }
  manualReportResultsCount.textContent=resultados.length+" de "+total+" itens";
  manualReportResults.replaceChildren();
  if(!resultados.length){
    const empty=document.createElement("div");
    empty.className="report-empty";
    empty.textContent=state.erro||"Nenhum resultado foi registrado nesta execução.";
    manualReportResults.appendChild(empty);
  }
  for(const item of resultados){
    const row=document.createElement("div");
    row.className="report-result-row";
    const copy=document.createElement("div");
    copy.className="report-result-copy";
    const name=document.createElement("span");
    name.className="report-result-name";
    name.textContent=item.nome||item.slug||"Biblioteca sem nome";
    const detail=document.createElement("span");
    detail.className="report-result-detail";
    const ok=item.count!==null&&item.count!==undefined;
    detail.textContent=ok?"Coleta concluída com sucesso":(item.falha||manualStatusLabels[item.status]||"Não foi possível concluir a coleta.");
    const value=document.createElement("span");
    value.className="report-result-value";
    value.textContent=ok?Number(item.count).toLocaleString("pt-BR")+" anúncios":(manualStatusLabels[item.status]||"Falhou");
    if(!ok)value.classList.add("is-failed");
    copy.append(name,detail);
    row.append(copy,value);
    manualReportResults.appendChild(row);
  }
  manualReportTimestamp.textContent="Início: "+(inicio&&!Number.isNaN(inicio.getTime())?inicio.toLocaleString("pt-BR"):"indisponível")+(duracaoLabel?" · Duração: "+duracaoLabel:"");
}

manualReportOpen.addEventListener("click",function(){
  if(!manualReportDialog.open)manualReportDialog.showModal();
});
function dismissManualReport(runId){
  if(runId){
    dismissedManualRunId=runId;
    try{
      localStorage.setItem(MANUAL_REPORT_DISMISSED_KEY,runId);
    }catch(error){
      console.warn("Não foi possível salvar a dispensa do relatório neste navegador: "+error.message);
    }
  }
  manualReportDialog.close();
  manualRunPanel.hidden=true;
  manualReportOpen.hidden=true;
}
manualRunDismiss.addEventListener("click",function(){dismissManualReport(manualDisplayedRunId||manualRequestTrackingId)});
document.getElementById("manual-report-close").addEventListener("click",function(){dismissManualReport(manualDisplayedRunId||manualRequestTrackingId)});
document.getElementById("manual-report-dismiss").addEventListener("click",function(){dismissManualReport(manualDisplayedRunId||manualRequestTrackingId)});
manualReportDialog.addEventListener("cancel",function(event){
  event.preventDefault();
  dismissManualReport(manualDisplayedRunId||manualRequestTrackingId);
});
document.getElementById("manual-report-refresh").addEventListener("click",function(){
  manualReportDialog.close();
  window.location.reload();
});

function renderManualCheck(state){
  const estaRodando=state.status==="running";
  const finalizado=state.status==="completed"||state.status==="completed_with_errors"||state.status==="failed";
  const ocupadoSemRelatorio=state.ocupado&&!estaRodando&&!finalizado;
  if(state.runId)manualDisplayedRunId=state.runId;
  manualCheckButton.disabled=estaRodando||state.ocupado;
  manualCustomizeButton.disabled=estaRodando||state.ocupado;
  manualCheckLabel.textContent=estaRodando?"Conferindo...":"Checar todas";
  updateManualSelectionCount();
  if(state.status==="idle"&&!state.ocupado){
    manualRunPanel.hidden=true;
    manualReportOpen.hidden=true;
    manualRunDismiss.hidden=true;
    return;
  }
  if(finalizado&&state.runId){
    let dismissedRunId=dismissedManualRunId;
    try{
      dismissedRunId=localStorage.getItem(MANUAL_REPORT_DISMISSED_KEY)||dismissedRunId;
    }catch(error){
      console.warn("Não foi possível consultar a dispensa salva do relatório: "+error.message);
    }
    if(dismissedRunId===state.runId){
      manualRunPanel.hidden=true;
      manualReportOpen.hidden=true;
      manualRunDismiss.hidden=true;
      if(manualReportDialog.open)manualReportDialog.close();
      return;
    }
  }
  manualRunPanel.hidden=false;
  if(ocupadoSemRelatorio){
    manualRunTitle.textContent="Já existe uma coleta em andamento";
    manualRunCurrent.textContent="Aguarde a execução atual terminar para iniciar outra.";
    manualRunCount.textContent="";
    manualRunProgress.style.width="0%";
    manualRunTrack.setAttribute("aria-valuenow","0");
    manualRunResults.replaceChildren();
    return;
  }

  const progresso=state.total?Math.round((state.concluidos/state.total)*100):0;
  manualRunTitle.textContent=state.status==="running"?(state.escopo==="selecionadas"?"Conferindo selecionadas":"Conferindo itens monitorados"):state.status==="completed"?"Checagem concluída":state.status==="failed"?"Não foi possível concluir a checagem":"Checagem concluída com falhas";
  manualRunCurrent.textContent=state.status==="running"?(state.atual?"Conferindo: "+state.atual:"Preparando coleta..."):(state.erro||"Resultado atualizado na dashboard.");
  manualRunCount.textContent=state.concluidos+" / "+state.total+" · "+state.sucesso+" ok · "+state.falha+" falhas";
  manualRunProgress.style.width=progresso+"%";
  manualRunTrack.setAttribute("aria-valuenow",String(progresso));
  manualRunResults.replaceChildren();
  manualReportOpen.hidden=!finalizado;
  manualRunDismiss.hidden=!finalizado;
  for(const item of state.resultados||[]){
    const row=document.createElement("div");
    row.className="manual-run-row";
    row.title=item.falha||"";
    const name=document.createElement("span");
    name.className="manual-run-name";
    name.textContent=item.nome||item.slug;
    const result=document.createElement("span");
    result.className="manual-run-result";
    const ok=item.count!==null&&item.count!==undefined;
    result.textContent=ok?Number(item.count).toLocaleString("pt-BR")+" anúncios":(manualStatusLabels[item.status]||"Falhou");
    if(!ok)result.classList.add("is-failed");
    row.append(name,result);
    manualRunResults.appendChild(row);
  }

  if(finalizado){
    renderManualReport(state);
    refreshLastChecks();
    try{
      const reportKey="lowticket-manual-check-report-seen";
      if(state.runId&&sessionStorage.getItem(reportKey)!==state.runId){
        sessionStorage.setItem(reportKey,state.runId);
        if(!manualReportDialog.open)manualReportDialog.showModal();
      }
    }catch(e){if(!manualReportDialog.open)manualReportDialog.showModal()}
  }
}

async function refreshLastChecks(){
  try{
    const response=await fetch("/api/ultima-checagem",{cache:"no-store"});
    if(!response.ok)throw new Error("Falha ao consultar últimas checagens");
    const checks=await response.json();
    const cells=new Map([...document.querySelectorAll(".last-check-cell")].map(cell=>[cell.dataset.slug,cell]));
    const labels={falha_timeout:"timeout",falha_bloqueio:"bloqueio Meta",falha_parse:"não lido",falha_url_invalida:"URL inválida",falha_gravacao:"falha ao salvar"};
    for(const check of checks){
      const cell=cells.get(check.slug);
      if(!cell)continue;
      cell.replaceChildren();
      if(check.ultima_coleta_ok){
        cell.append(document.createTextNode(new Date(check.ultima_coleta_ok).toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"})));
      }else{
        cell.append(document.createTextNode("—"));
      }
      if(check.tentativa?.relevante&&labels[check.tentativa?.status]){
        const state=document.createElement("div");
        state.style.color="#fb7185";
        state.style.fontSize="10px";
        state.textContent=labels[check.tentativa.status];
        if(check.tentativa.error)state.title=check.tentativa.error;
        cell.appendChild(state);
      }
    }
  }catch(err){
    console.warn("[DASHBOARD] não foi possível atualizar últimas checagens: "+err.message);
  }
}

async function updateManualCheck(){
  try{
    const statusUrl=manualRequestTrackingId?"/api/coletar-tudo/status?requestId="+encodeURIComponent(manualRequestTrackingId):"/api/coletar-tudo/status";
    const response=await fetch(statusUrl,{cache:"no-store"});
    if(!response.ok)throw new Error("Falha ao consultar status");
    const state=await response.json();
    if(!state||!validManualCheckStates.has(state.status)||typeof state.ocupado!=="boolean"||!Array.isArray(state.resultados)
      ||![state.total,state.concluidos,state.sucesso,state.falha].every(Number.isFinite)
      ||state.resultados.some(item=>!item||typeof item!=="object"||typeof item.slug!=="string"||typeof item.nome!=="string"||!validManualResultStates.has(item.status)
        ||(item.count===null?item.status==="ok"||item.status==="ok_zero":!Number.isFinite(item.count)||item.status!=="ok"&&item.status!=="ok_zero")))throw new Error("Resposta de status inválida");
    if(pendingManualRun){
      if(state.requestId===pendingManualRun.requestId){
        manualRequestTrackingId=pendingManualRun.requestId;
        pendingManualRun=null;
      }else if(state.ocupado){
        renderManualCheck({status:"idle",ocupado:true,resultados:[]});
        setTimeout(updateManualCheck,1500);
        return;
      }else if(!state.ocupado){
        showManualStartPending();
        schedulePendingManualRetry();
        return;
      }
    }
    renderManualCheck(state);
    if(state.status==="running"||state.ocupado)setTimeout(updateManualCheck,1500);
  }catch(e){
    manualCheckButton.disabled=true;
    manualCustomizeButton.disabled=true;
    manualCheckSelectedButton.disabled=true;
    manualRunPanel.hidden=false;
    manualRunTitle.textContent="Não foi possível consultar a checagem";
    manualRunCurrent.textContent="Atualize a página para tentar novamente.";
    setTimeout(updateManualCheck,3000);
  }
}

function schedulePendingManualRetry(){
  if(!pendingManualRun)return;
  if(Date.now()-pendingManualRun.startedAt>=MANUAL_START_RETRY_WINDOW_MS){
    if(manualRetryTimer!==null){
      clearTimeout(manualRetryTimer);
      manualRetryTimer=null;
    }
    manualRunCurrent.textContent="Ainda não foi possível confirmar a execução. A tela continuará consultando o status; não inicie outra checagem.";
    setTimeout(updateManualCheck,3000);
    return;
  }
  if(manualRetryTimer!==null)return;
  const remaining=MANUAL_START_RETRY_WINDOW_MS-(Date.now()-pendingManualRun.startedAt);
  manualRetryTimer=setTimeout(function(){
    manualRetryTimer=null;
    if(!pendingManualRun)return;
    if(Date.now()-pendingManualRun.startedAt>=MANUAL_START_RETRY_WINDOW_MS){
      schedulePendingManualRetry();
      return;
    }
    startDashboardCheck(pendingManualRun.slugs,pendingManualRun.snapshot,pendingManualRun.requestId);
  },Math.min(1800,remaining));
}

function showManualStartPending(){
  manualRunPanel.hidden=false;
  manualRunTitle.textContent="Confirmando o início da checagem";
  manualRunCurrent.textContent="A resposta do servidor não foi conclusiva. Consultando o status antes de permitir outra tentativa.";
  manualRunCount.textContent="";
  manualRunResults.replaceChildren();
}

async function startDashboardCheck(slugs,snapshot,requestId=crypto.randomUUID()){
  if(manualRetryTimer!==null){
    clearTimeout(manualRetryTimer);
    manualRetryTimer=null;
  }
  if(!pendingManualRun||pendingManualRun.requestId!==requestId){
    manualRequestTrackingId=null;
    pendingManualRun={slugs:slugs?[...slugs]:null,snapshot:snapshot.map(item=>({...item})),requestId:requestId,startedAt:Date.now()};
  }
  manualCheckButton.disabled=true;
  manualCustomizeButton.disabled=true;
  manualCheckSelectedButton.disabled=true;
  manualCheckLabel.textContent="Iniciando...";
  let requestMayHaveStarted=false;
  try{
    if(!Array.isArray(snapshot)||!snapshot.length)throw new Error("A lista confirmada está vazia. Reabra a confirmação e tente novamente.");
    const payload={...(slugs?{slugs:slugs}:{}),requestId:requestId,snapshotHash:await fingerprintManualSnapshot(snapshot),snapshotCount:snapshot.length};
    const options={method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)};
    requestMayHaveStarted=true;
    const response=await fetch("/api/coletar-tudo",options);
    let data;
    try{
      data=await response.json();
    }catch{
      if(response.status>=400&&response.status<500&&response.status!==408){
        requestMayHaveStarted=false;
        throw new Error("O servidor recusou a solicitação ("+response.status+"). A checagem não foi iniciada.");
      }
      throw new Error("O servidor retornou uma resposta inconclusiva.");
    }
    if(response.status===409){
      if(data.status==="selection_changed"){
        pendingManualRun=null;
        manualRequestTrackingId=null;
        manualCheckButton.disabled=false;
        manualCustomizeButton.disabled=false;
        manualCheckLabel.textContent="Checar todas";
        manualSelectionLoaded=false;
        manualPages=[];
        manualRunPanel.hidden=false;
        manualRunTitle.textContent="A seleção precisa ser revisada";
        manualRunCurrent.textContent=data.message||"Os dados mudaram. Abra a personalização e confirme novamente.";
        manualRunCount.textContent="";
        manualRunResults.replaceChildren();
        return;
      }
      if(data.status==="request_conflict"){
        pendingManualRun=null;
        manualRequestTrackingId=null;
        manualCheckButton.disabled=false;
        manualCustomizeButton.disabled=false;
        manualCheckLabel.textContent="Checar todas";
        manualRunPanel.hidden=false;
        manualRunTitle.textContent="Não foi possível confirmar esta execução";
        manualRunCurrent.textContent=data.message||"Reabra a confirmação e tente novamente.";
        return;
      }
      renderManualCheck({status:"idle",ocupado:true,resultados:[]});
      setTimeout(updateManualCheck,1800);
      return;
    }
    if(!response.ok){
      requestMayHaveStarted=response.status>=500||response.status===408;
      if(response.status===500&&data.status==="error")requestMayHaveStarted=false;
      if(!requestMayHaveStarted){pendingManualRun=null;manualRequestTrackingId=null;}
      throw new Error(data.message||"Não foi possível iniciar a checagem.");
    }
    if(data.status!=="started"||typeof data.runId!=="string"){
      showManualStartPending();
      schedulePendingManualRetry();
      return;
    }
    manualRequestTrackingId=requestId;
    pendingManualRun=null;
    manualRunPanel.hidden=false;
    manualRunTitle.textContent="Iniciando checagem";
    manualRunCurrent.textContent=slugs?"Preparando os itens selecionados...":"Preparando os itens monitorados...";
    setTimeout(updateManualCheck,500);
  }catch(e){
    if(requestMayHaveStarted){
      showManualStartPending();
      schedulePendingManualRetry();
      return;
    }
    pendingManualRun=null;
    manualRequestTrackingId=null;
    manualCheckButton.disabled=false;
    manualCustomizeButton.disabled=false;
    manualCheckLabel.textContent="Checar todas";
    updateManualSelectionCount();
    manualRunPanel.hidden=false;
    manualRunTitle.textContent="Não foi possível iniciar a checagem";
    manualRunCurrent.textContent=e.message;
  }
}

manualCheckButton.addEventListener("click",openAllManualConfirmation);

updateManualCheck();
refreshLastChecks();
setInterval(refreshLastChecks,30000);

<\/script>
</body>
</html>`
      .replace("__DADOS_DOM__", () => dadosDom)
      .replace("__HIST_DOM__", () => histDadosDom)
      .replace("__DADOS_KEY__", () => dadosKey)
      .replace("__HIST_KEY__", () => histDadosKey)
      .replace("__DADOS_PLACEHOLDER__", () => dados)
      .replace("__HIST_PLACEHOLDER__", () => histDados));
  } catch (err) {
    res.status(500).send("Erro: " + err.message);
  }
});

// ─── Scheduler ───────────────────────────────────────────────────────────────

// O agendamento agora é feito externamente via rota GET /api/cron/tick (ex: UptimeRobot a cada 5 min)
console.log("[CRON] Usando arquitetura de Fila Assíncrona via /api/cron/tick");

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
initDb().then(() => {
  app.listen(PORT, () => console.log(`[SERVER] Running on port ${PORT}`));
});