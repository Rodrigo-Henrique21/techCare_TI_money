# Extracao Dados Financeiros

Projeto para extração de dados públicos financeiros (B3, Tesouro Direto, BACEN, CVM, IBGE).

## Como rodar

```bash
python -m venv venv
source venv/bin/activate   # ou venv\Scripts\activate no Windows
pip install -r requirements.txt
python main.py
```

## Azure Blob Storage
Defina o token SAS (com permissões de leitura e escrita) em uma variável de ambiente:

```bash
export AZURE_SAS_TOKEN="seu_token_sas"
```

Os scripts já enviam e buscam dados diretamente do container:

- `python main.py` – extrai os dados e envia para `raw/`
- `python transform.py` – baixa `raw/`, gera `silver/` e envia ao blob
- `python analysis.py` – baixa `silver/`, gera `gold/` e envia ao blob