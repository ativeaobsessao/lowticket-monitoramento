# VIVA Labs — Meta Ads Library Monitor

Scraper para monitorar anúncios ativos na Biblioteca de Anúncios do Meta.

## Arquivos principais

- `index.js`: API, interface e execução do monitoramento.
- `scrape-orchestration.js`: timeout, slots BRT e política de retentativas.
- `schema.sql`: schema de referência para PostgreSQL/Supabase; o app também aplica migrações ao iniciar.
- `test/`: testes automatizados.
- `Dockerfile` e `railway.toml`: build e execução no Railway.

Para replicar o serviço em outro projeto, mantenha o runtime, o schema, os testes e os arquivos de deploy; adapte as regras de coleta e a apresentação à nova fonte monitorada.

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/api/healthz` | Health check |
| GET | `/api/cron/tick` | Executar uma rodada automática do slot atual |
| GET | `/api/ultima-checagem` | Última tentativa por biblioteca, inclusive falhas e execuções em andamento |
| POST | `/api/salvar` | Cadastrar nova página |
| GET | `/api/coletar/:slug` | Coletar e retornar contagem (usado pelo Make.com) |
| GET | `/api/historico/:slug` | Histórico completo de coletas |
| GET | `/api/resumo/:slug` | Resumo: min/max/média/tendência |
| GET | `/api/status` | Todas as páginas com última leitura |
| GET | `/api/paginas` | Lista de páginas cadastradas |

## Coletas e histórico

- O cron é acionado por `GET /api/cron/tick` e processa até 5 páginas por chamada nos slots 03h, 12h e 22h BRT. No horário 05:00–05:59 BRT, processa até 5 domínios no slot diário 05h; entre 06:00–06:59 BRT, até 5 palavras-chave no slot diário 06h.
- Cada página tem limite de 120 segundos, incluindo iniciar/conectar ao Chromium e extrair a contagem. Falhas temporárias são tentadas novamente somente depois da primeira passagem por todas as bibliotecas do escopo.
- A segunda passagem só começa quando o orçamento restante comporta as retentativas; uma janela insuficiente deixa o resultado da primeira passagem visível, sem interromper páginas ainda não processadas. O orçamento é de 45 minutos por grupo de até 5 bibliotecas.
- As tentativas são persistidas em `scrape_attempts`; a última checagem é exibida separadamente da última contagem válida.
- Coletas manuais atualizam a última checagem e a leitura atual, sem criar pontos nos slots do histórico automático.
- Coletas automáticas gravam no máximo um ponto por item, slot e data de negócio BRT. O slot 22 é associado ao dia em que a janela começou, inclusive após a meia-noite; domínios usam o slot 5 e a data BRT atual.
- Palavras-chave são cadastradas como `tipo='keyword'`; termos livres são convertidos em buscas ativas da Meta Ad Library e aparecem em uma seção própria no dashboard.
- O cadastro em lote aceita `keyword | Jejum Intermitente`, `palavra | Jejum | URL da Meta` e detecta automaticamente buscas `keyword_unordered` cujo termo não seja um domínio.
- `initDb()` cria e migra as tabelas necessárias automaticamente. `schema.sql` documenta o schema.

## Variáveis de ambiente

```
DATABASE_URL=postgres://user:pass@host:5432/dbname
PORT=3000
NODE_ENV=production
```

## Desenvolvimento

```bash
npm ci
npm test
npm start
```

Use a connection string PostgreSQL do Supabase para `DATABASE_URL`.

## Deploy no Railway

1. Suba o código no GitHub
2. Conecte o repositório no Railway
3. Adicione um banco PostgreSQL no Railway
4. Configure as variáveis de ambiente
5. Deploy automático

## Uso com Make.com

URL do módulo HTTP:
```
GET https://sua-url.railway.app/api/coletar/{slug}
```
Retorna apenas o número inteiro (ex: `510`).
