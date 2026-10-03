# Evidências TDD — marcação resiliente do histórico

## Origem e jornadas

Não foi fornecido um arquivo de plano. As garantias foram derivadas da solicitação:

- Cada biblioteca do escopo deve receber uma primeira tentativa antes de qualquer retentativa.
- Uma página travada deve expirar, fechar o Chromium e permitir que a fila prossiga.
- Se não houver tempo suficiente para concluir a segunda passagem, a primeira tentativa de todas as páginas permanece registrada.
- Os horários automáticos devem ser deduplicados por janela BRT e pela data de início do slot que cruza a meia-noite.
- A interface deve validar sintaticamente e exibir o estado da última tentativa separadamente da última contagem válida.

## RED/GREEN

- RED para o orçamento do lote: `node --test test\scrape-orchestration.test.js` executou 7 testes; 6 passaram e o teste novo falhou porque o helper ainda iniciava a segunda passagem sem consultar `shouldStartRetryPass`.
- GREEN final: `node --experimental-test-coverage --test test\*.test.js` — 11 testes passaram, 0 falharam.
- Verificações adicionais: `node --check index.js`, `node --check scrape-orchestration.js` e `git diff --check` passaram.

## Garantias cobertas

| # | Garantia | Teste | Tipo | Resultado |
|---|----------|-------|------|-----------|
| 1 | Toda página da primeira passagem é processada antes de qualquer retry | `test/scrape-orchestration.test.js` — `finishes the first pass before retrying failed pages` | Unitário | PASS |
| 2 | Sucessos e falhas não repetíveis não são tentados novamente | `test/scrape-orchestration.test.js` — testes de retry seletivo | Unitário | PASS |
| 3 | O orçamento insuficiente não pula páginas da primeira passagem nem inicia retries | `test/scrape-orchestration.test.js` — `finishes every first-pass page when the batch deadline leaves no retry window` | Unitário | PASS |
| 4 | Uma operação que excede o limite executa limpeza; falhas de limpeza preservam o erro de timeout | `test/scrape-orchestration.test.js` — testes de timeout | Unitário | PASS |
| 5 | Erros de operação são propagados e limites de passes inválidos são rejeitados | `test/scrape-orchestration.test.js` — testes de erro e limite | Unitário | PASS |
| 6 | Slot 22 após meia-noite pertence à data BRT em que a janela começou | `test/scrape-orchestration.test.js` — testes de slot | Unitário | PASS |
| 7 | O script embutido do dashboard continua sendo JavaScript válido | `test/dashboard-script.test.js` | Sintaxe | PASS |

## Cobertura e lacunas

O relatório de cobertura do helper `scrape-orchestration.js` foi: 100% de linhas, 100% de branches e 92,31% de funções. O runner não instrumenta `index.js`; portanto, esses percentuais não representam cobertura do scraper inteiro.

Não havia `DATABASE_URL` configurada neste ambiente. As migrações, transações, reserva PostgreSQL e coleta real da Meta ainda precisam de validação integrada em um banco de teste e de smoke test no ambiente implantado.
