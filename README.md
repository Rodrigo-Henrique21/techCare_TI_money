# Extracao Dados Financeiros

Projeto para extração de dados públicos financeiros (B3, Tesouro Direto, BACEN, CVM, IBGE).
Projeto para extração de dados públicos financeiros (B3 e BACEN).

## Delta Live Tables (Databricks)

O pipeline Delta Live Tables foi organizado de forma modular para facilitar a publicação no workspace do Databricks:

- **`databricks/pipelines/pipeline_financeiro.py`** – ponto de entrada do pipeline. Ele apenas garante que o pacote `databricks` esteja no `sys.path` e importa as camadas Bronze, Prata e Ouro.
- **`databricks/transformacoes/`** – diretório com módulos separados por camada (`bronze.py`, `prata.py`, `ouro.py`).
- **`databricks/utilitarios/`** – funções compartilhadas para configuração do catálogo/esquemas, captura das APIs externas e criação de estruturas auxiliares.

> 📁 No workspace do Databricks mantenha exatamente essa hierarquia (`databricks/pipelines`, `databricks/transformacoes`, `databricks/utilitarios`). Os módulos deixam de depender de fallbacks dinâmicos e passam a exigir os caminhos corretos para evitar ambiguidades.

As camadas tratam exclusivamente integrações da B3 e do BACEN, replicando o fluxo original dos scripts Python:

| Camada | Tabelas geradas | Descrição |
|--------|-----------------|-----------|
| Bronze | `platfunc.aafn_ing.cotacoes_b3`, `platfunc.aafn_ing.series_bacen` | Captura dados brutos do Yahoo Finance (B3) e das séries temporais do BACEN (SGS). |
| Prata | `platfunc.aafn_tgt.cotacoes_b3`, `platfunc.aafn_tgt.series_bacen` | Padroniza esquemas, aplica validações (`dlt.expect`) e remove inconsistências. |
| Ouro | `platfunc.aafn_ddm.metricas_b3`, `platfunc.aafn_ddm.indicadores_bacen` | Consolida KPIs das ações acompanhadas e um resumo das séries do BACEN. |

### Como configurar o pipeline

1. No Databricks, crie um **Delta Live Tables Pipeline** em modo *Triggered* ou *Continuous*.
2. Aponte a biblioteca principal para o arquivo `databricks/pipelines/pipeline_financeiro.py` (repositório, workspace ou DBFS).
3. Garanta previamente a existência do catálogo `platfunc` e dos esquemas `aafn_ing`, `aafn_tgt` e `aafn_ddm`. O utilitário valida essa estrutura antes de materializar qualquer tabela.
4. Configure os parâmetros opcionais via `spark.conf` no pipeline para ajustar fontes e janelas de dados:

| Chave | Descrição | Padrão |
|-------|-----------|--------|
| `techcare.b3.tickers` | Lista separada por vírgulas com os tickers da B3. | `PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3,ELET3,B3SA3` |
| `techcare.b3.start_date` / `techcare.b3.end_date` | Datas (YYYY-MM-DD) para histórico via Yahoo Finance. | `2015-01-01` / data atual |
| `techcare.bacen.series` | JSON com pares `{nome: código}` das séries SGS. | `{"selic":1178,"cdi":12,"ipca":433,"poupanca":195,"igpm":189,"inpc":188,"igpdi":190,"selic_meta":432}` |
| `techcare.bacen.start_date` / `techcare.bacen.end_date` | Intervalo de datas para as séries BACEN. | `2010-01-01` / data atual |
| `techcare.catalogo.destino` | Catálogo Unity Catalog onde o pipeline criará as tabelas. | `platfunc` |
| `techcare.esquema.bronze` | Esquema da camada Bronze (ingestão). | `aafn_ing` |
| `techcare.esquema.prata` | Esquema da camada Prata (transformação). | `aafn_tgt` |
| `techcare.esquema.ouro` | Esquema da camada Ouro (data mart). | `aafn_ddm` |

### Boas práticas aplicadas

- Cada tabela possui comentários (`comment`) e, quando aplicável, validações de qualidade com `dlt.expect`.
- As tabelas Bronze acrescentam `ingestion_timestamp` para facilitar auditoria.
- As transformações utilizam APIs do Spark (em vez de Pandas) garantindo escalabilidade.
- O catálogo `platfunc` e os esquemas `aafn_ing`, `aafn_tgt` e `aafn_ddm` são validados antes da execução, assegurando que cada camada utilize o domínio correto.
- As tabelas *gold* consolidam indicadores equivalentes aos produzidos pelos scripts Python originais referentes às integrações da B3 e do BACEN.
