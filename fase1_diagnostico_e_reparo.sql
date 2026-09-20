-- ============================================================================
-- FASE 1: DIAGNÓSTICO, REPARO DE SEQUENCES E LIMPEZA (SUPABASE SQL EDITOR)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- PASSO 1: DIAGNÓSTICO DO BANCO
-- ----------------------------------------------------------------------------

-- 1.1 Verificar timezone (esperado: UTC)
SHOW timezone;

-- 1.2 Verificar Chaves Primárias e Únicas
SELECT conrelid::regclass AS tabela, conname, contype
FROM pg_constraint
WHERE conrelid::regclass::text IN ('pages','scrape_history','scrape_latest','funnel_nodes','funnel_edges')
  AND contype IN ('p','u');

-- 1.3 Verificar se a coluna id possui o default nextval(...)
SELECT table_name, column_name, column_default
FROM information_schema.columns
WHERE column_name = 'id' 
  AND table_name IN ('scrape_history','funnel_nodes','funnel_edges');

-- 1.4 Verificar descompasso da sequence em scrape_history
SELECT 
  COALESCE((SELECT last_value::text FROM scrape_history_id_seq), 'sem_seq') AS seq_scrape_history,
  (SELECT MAX(id) FROM scrape_history) AS max_id_scrape_history;


-- ----------------------------------------------------------------------------
-- PASSO 2: REPARO DAS SEQUENCES E GARANTIA DE PKS
-- ----------------------------------------------------------------------------

-- 2.1 Garantir que as sequences existam e estejam vinculadas aos campos ID
CREATE SEQUENCE IF NOT EXISTS scrape_history_id_seq OWNED BY scrape_history.id;
ALTER TABLE scrape_history ALTER COLUMN id SET DEFAULT nextval('scrape_history_id_seq');

CREATE SEQUENCE IF NOT EXISTS funnel_nodes_id_seq OWNED BY funnel_nodes.id;
ALTER TABLE funnel_nodes ALTER COLUMN id SET DEFAULT nextval('funnel_nodes_id_seq');

CREATE SEQUENCE IF NOT EXISTS funnel_edges_id_seq OWNED BY funnel_edges.id;
ALTER TABLE funnel_edges ALTER COLUMN id SET DEFAULT nextval('funnel_edges_id_seq');

-- 2.2 Sincronizar o valor atual das sequences com o MAX(id) + 1 de cada tabela
SELECT setval('scrape_history_id_seq', COALESCE((SELECT MAX(id) FROM scrape_history), 0) + 1, false);
SELECT setval('funnel_nodes_id_seq',   COALESCE((SELECT MAX(id) FROM funnel_nodes), 0) + 1,   false);
SELECT setval('funnel_edges_id_seq',   COALESCE((SELECT MAX(id) FROM funnel_edges), 0) + 1,   false);

-- 2.3 Garantir PK em pages e scrape_latest
DO 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conrelid = 'pages'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE pages ADD PRIMARY KEY (slug);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conrelid = 'scrape_latest'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE scrape_latest ADD PRIMARY KEY (slug);
  END IF;
END ;


-- ----------------------------------------------------------------------------
-- PASSO 3: TESTE DE VALIDAÇÃO (DEVE PASSAR SEM ERRO)
-- ----------------------------------------------------------------------------
INSERT INTO scrape_history (slug, ads_count, slot) VALUES ('__teste_validacao__', 1, NULL) RETURNING id;
INSERT INTO scrape_latest (slug, ads_count) VALUES ('__teste_validacao__', 1)
  ON CONFLICT (slug) DO UPDATE SET ads_count = EXCLUDED.ads_count;

-- Limpar registro de teste
DELETE FROM scrape_history WHERE slug = '__teste_validacao__';
DELETE FROM scrape_latest  WHERE slug = '__teste_validacao__';


-- ----------------------------------------------------------------------------
-- PASSO 4: LIMPEZA DOS ZEROS FALSOS (CAUSADOS PELAS FALHAS RECENTES)
-- ----------------------------------------------------------------------------

-- 4.1 Identificar páginas com inicial_count = 0 falso
SELECT slug, nome, inicial_count, created_at 
FROM pages 
WHERE inicial_count = 0;

-- 4.2 Resetar inicial_count = 0 para NULL (permite nova captura limpa)
UPDATE pages 
SET inicial_count = NULL 
WHERE inicial_count = 0;

-- 4.3 Identificar registros em scrape_latest com ads_count = 0
SELECT slug, ads_count, collected_at 
FROM scrape_latest 
WHERE ads_count = 0;
