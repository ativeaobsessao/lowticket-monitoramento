-- ============================================================================
-- SCHEMA — MONITORAMENTO DE META ADS & MAPEAMENTO DE FUNIS (VIVA Labs / lowticket)
-- Compatível com PostgreSQL / Supabase
-- ============================================================================
-- Snapshot do schema tal como é criado/mantido por initDb() em index.js.
-- Reflete o estado ATUAL do código: 7 tabelas. Este sistema NÃO possui a
-- tabela/coluna 'brand(s)' — diferente da instância DTC, aqui não há esse
-- recurso implementado no index.js, então não deve ser criado aqui.
--
-- Este arquivo é referência/documentação e para provisionar um banco NOVO
-- do zero. Em produção, quem efetivamente cria/migra as tabelas é o
-- initDb() do index.js, rodando automaticamente a cada boot do processo —
-- rodar este arquivo manualmente não é necessário no dia a dia.
--
-- Ordem de criação respeita as foreign keys: pages → funnel_nodes → funnel_edges
-- ============================================================================

SET search_path TO public;

-- ----------------------------------------------------------------------------
-- 1. TABELA PRINCIPAL DE PÁGINAS E DOMÍNIOS MONITORADOS
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pages (
  slug          TEXT PRIMARY KEY,
  nome          TEXT NOT NULL,
  url           TEXT NOT NULL,
  tipo          TEXT NOT NULL DEFAULT 'pagina'
                CHECK (tipo IN ('pagina', 'dominio', 'keyword')), -- Tipo de rastreamento
  keyword_key   TEXT,                           -- Identidade normalizada exclusiva das palavras-chave
  inicial_count INTEGER,                        -- Quantidade inicial de anúncios capturada no cadastro
  instagram_url TEXT,                           -- URL do perfil do Instagram
  geo           TEXT,                           -- Região/País de atuação
  nicho         TEXT,                           -- Segmento/Nicho da marca
  funil         TEXT,                           -- Rótulo do funil associado
  last_attempt_at TIMESTAMP,                    -- Timestamp da última checagem tentada
  last_status   TEXT,                           -- Estado da última tentativa, inclusive em andamento
  last_error    TEXT,                           -- Diagnóstico da última tentativa com falha
  created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE pages ADD COLUMN IF NOT EXISTS keyword_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pages_keyword_key_unique
  ON pages(keyword_key) WHERE tipo = 'keyword';

-- ----------------------------------------------------------------------------
-- 2. HISTÓRICO COMPLETO DE COLETAS (SCRAPINGS)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_history (
  id           SERIAL PRIMARY KEY,
  slug         TEXT NOT NULL,
  ads_count    INTEGER NOT NULL,
  slot         SMALLINT,                        -- Bibliotecas: 3/12/22; domínios: 5; keywords: 6; NULL sem slot automático
  business_date DATE,                           -- Data BRT da janela, slot 22 cruza a meia-noite
  collected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrape_history_slug ON scrape_history(slug);
CREATE INDEX IF NOT EXISTS idx_scrape_history_collected_at ON scrape_history(collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_scrape_history_slot_business_date
  ON scrape_history(slug, slot, business_date);

-- ----------------------------------------------------------------------------
-- 3. ÚLTIMA LEITURA DE ANÚNCIOS POR PÁGINA (CACHED LATEST)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_latest (
  slug         TEXT PRIMARY KEY,
  ads_count    INTEGER NOT NULL,
  collected_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- 4. TENTATIVAS DE COLETA (SUCESSOS, FALHAS E EXECUÇÕES INTERROMPIDAS)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_attempts (
  id            TEXT PRIMARY KEY,
  slug          TEXT NOT NULL,
  source        TEXT NOT NULL,
  slot          SMALLINT,
  business_date DATE,
  status        TEXT NOT NULL,
  ads_count     INTEGER,
  error         TEXT,
  started_at    TIMESTAMP NOT NULL DEFAULT NOW(),
  completed_at  TIMESTAMP,
  lease_owner   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scrape_attempts_slug_started
  ON scrape_attempts(slug, started_at DESC);

-- ----------------------------------------------------------------------------
-- 5. RESERVA DURÁVEL DO WORKER E COOLDOWN DO CRON
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_worker_lease (
  lease_key     TEXT PRIMARY KEY,
  owner_id      TEXT,
  expires_at    TIMESTAMP NOT NULL,
  blocked_until TIMESTAMP
);

INSERT INTO scrape_worker_lease (lease_key, expires_at)
VALUES ('scraper', NOW() - INTERVAL '1 day')
ON CONFLICT (lease_key) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 6. MAPEAMENTO DE FUNIS — NÓS (ETAPAS DO FUNIL)
-- ----------------------------------------------------------------------------
-- CHECK ampliado (blueprint ADS/Presell): inclui 'ads' e 'presell' além dos
-- tipos originais. Nós existentes não são afetados.
CREATE TABLE IF NOT EXISTS funnel_nodes (
  id         SERIAL PRIMARY KEY,
  slug       TEXT NOT NULL REFERENCES pages(slug) ON DELETE CASCADE,
  tipo       TEXT NOT NULL CHECK (tipo IN ('ads','advertorial','presell','tsl','vsl','quiz','whatsapp','checkout')),
  rotulo     TEXT NOT NULL,
  url        TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_nodes_slug ON funnel_nodes(slug);

-- ----------------------------------------------------------------------------
-- 7. MAPEAMENTO DE FUNIS — CONEXÕES (ARESTAS ENTRE OS NÓS)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS funnel_edges (
  id           SERIAL PRIMARY KEY,
  from_node_id INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
  to_node_id   INTEGER NOT NULL REFERENCES funnel_nodes(id) ON DELETE CASCADE,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_funnel_edges_from ON funnel_edges(from_node_id);
CREATE INDEX IF NOT EXISTS idx_funnel_edges_to ON funnel_edges(to_node_id);

-- ============================================================================
-- Fim. 7 tabelas: pages, scrape_history, scrape_latest, scrape_attempts,
-- scrape_worker_lease, funnel_nodes, funnel_edges. Conferido contra initDb()
-- em index.js — não há nenhuma outra tabela criada pela aplicação.
-- ============================================================================