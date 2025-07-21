import os
import traceback
import requests
import yfinance as yf
import pandas as pd
import sgs
import time
from bs4 import BeautifulSoup
from datetime import datetime


class ExtratorBase:
    def __init__(self, data_path):
        self.data_path = data_path
        os.makedirs(self.data_path, exist_ok=True)
    def extrair(self):
        raise NotImplementedError

class ExtratorB3:
    def __init__(self, data_path, tickers, data_inicio=None, data_fim=None):
        self.data_path = data_path
        self.tickers = tickers
        self.data_inicio = datetime.strptime(data_inicio or '01/01/2015', '%d/%m/%Y') if isinstance(data_inicio, str) else data_inicio
        self.data_fim = datetime.strptime(data_fim or datetime.today().strftime('%d/%m/%Y'), '%d/%m/%Y') if isinstance(data_fim, str) else data_fim
        os.makedirs(self.data_path, exist_ok=True)

    def extrair(self):
        for ticker in self.tickers:
            try:
                ticker_sa = f"{ticker}.SA"  # Adiciona o sufixo .SA para ações brasileiras
                print(f"[B3] Baixando {ticker} de {self.data_inicio.strftime('%d/%m/%Y')} a {self.data_fim.strftime('%d/%m/%Y')} (via yfinance)...")
                
                # Baixa os dados usando yfinance
                acao = yf.Ticker(ticker_sa)
                df = acao.history(start=self.data_inicio, end=self.data_fim)
                
                if not df.empty:
                    # Renomeia as colunas para manter compatibilidade
                    df = df.rename(columns={
                        'Open': 'Open',
                        'High': 'High',
                        'Low': 'Low',
                        'Close': 'Close',
                        'Volume': 'Volume',
                        'Dividends': 'Dividends',
                        'Stock Splits': 'Stock_Splits'
                    })
                    
                    df.reset_index(inplace=True)
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    
                    file_path = os.path.join(self.data_path, f'b3_{ticker}_{self.data_inicio.strftime("%d%m%Y")}_{self.data_fim.strftime("%d%m%Y")}.parquet')
                    df.to_parquet(file_path, index=False)
                    print(f"[B3] Dados salvos em Parquet: {file_path}")
                else:
                    print(f"[B3] Nenhum dado para {ticker}")
            except Exception as e:
                print(f"[B3] Erro ao baixar {ticker}: {e}")


class ExtratorSadipemAPI:
    ENDPOINTS = {
        "opc_cronograma_liberacoes": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/opc-cronograma-liberacoes",
        "opc_cronograma_pagamentos": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/opc-cronograma-pagamentos",
        "opc_taxa_cambio": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/opc-taxa-cambio",
        "opnc_pvl_tramitacao_deferido": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/opnc-pvl-tramitacao-deferido",
        "pvl": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/pvl",
        "res_cdp": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/res-cdp",
        "res_cronograma_pagamentos": "https://apidatalake.tesouro.gov.br/ords/sadipem/tt/res-cronograma-pagamentos",
    }

    # Colunas de data conhecidas nos endpoints, para filtrar por ano no Pandas
    COLUNAS_DATA = [
        'data_base', 'ano', 'data_protocolo', 'data_status', 'ano_pvl_nao_contratado'
    ]

    def __init__(self, data_path, anos, limite=1000):
        self.data_path = data_path
        self.anos = anos
        self.limite = limite
        os.makedirs(self.data_path, exist_ok=True)

    def _filtrar_por_anos(self, df):
        """Filtra o DataFrame para manter apenas linhas dos anos desejados (buscando em colunas de data conhecidas)."""
        if df.empty:
            return df
        df_filtrado = pd.DataFrame()
        for coluna in self.COLUNAS_DATA:
            if coluna in df.columns:
                if coluna.startswith("ano"):
                    anos_df = pd.to_numeric(df[coluna], errors='coerce')
                else:
                    # tenta converter datas em string para datetime e pegar ano
                    anos_df = pd.to_datetime(df[coluna], errors='coerce').dt.year
                mask = anos_df.isin(self.anos)
                filtrado = df[mask]
                df_filtrado = pd.concat([df_filtrado, filtrado])
        if df_filtrado.empty:
            return df  # retorna tudo caso não consiga filtrar por ano
        return df_filtrado.drop_duplicates()

    def baixar_endpoint(self, nome, url):
        print(f"[{nome}] Baixando dados do endpoint: {url}")
        todos_registros = []
        page = 1
        while True:
            params = {'page': page}
            try:
                resp = requests.get(url, params=params, timeout=60)
                if resp.status_code != 200:
                    print(f"[{nome}] Erro HTTP {resp.status_code}")
                    break
                dados = resp.json()
                items = dados.get('items', [])
                if not items:
                    break
                todos_registros.extend(items)
                print(f"[{nome}] Página {page} - {len(items)} registros")
                if self.limite and len(todos_registros) >= self.limite:
                    todos_registros = todos_registros[:self.limite]
                    print(f"[{nome}] Limite de {self.limite} registros atingido.")
                    break
                if not dados.get('hasMore', False):
                    break
                page += 1
                time.sleep(1.1)
            except Exception as e:
                print(f"[{nome}] Erro página {page}: {e}")
                break

        if todos_registros:
            df = pd.DataFrame(todos_registros)
            df = self._filtrar_por_anos(df)
            file_path = os.path.join(self.data_path, f"sadipem_{nome}_{self.anos[0]}_{self.anos[-1]}.parquet")
            df.to_parquet(file_path, index=False)
            print(f"[{nome}] Dados salvos em Parquet: {file_path}")
        else:
            print(f"[{nome}] Nenhum dado encontrado.")

    def extrair(self):
        for nome, url in self.ENDPOINTS.items():
            self.baixar_endpoint(nome, url)


class ExtratorRentabilidadeTesouroWebscraping:
    URL = "https://www.tesourodireto.com.br/mercado-de-titulos-publicos/rentabilidade-acumulada.htm"

    def __init__(self, data_path):
        self.data_path = data_path
        os.makedirs(self.data_path, exist_ok=True)

    def extrair(self):
        print("[Tesouro] Acessando página para buscar CSV...")
        try:
            resp = requests.get(self.URL, timeout=60)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            link = None
            for a in soup.find_all("a", href=True):
                if ".csv" in a["href"]:
                    link = a["href"]
                    break
            if not link:
                print("[Tesouro] Não encontrou link para o CSV na página.")
                return
            # Normalizar URL se necessário
            if not link.startswith("http"):
                link = "https://www.tesourodireto.com.br" + link
            print(f"[Tesouro] Link do CSV encontrado: {link}")

            # Baixar o CSV
            csv_resp = requests.get(link, timeout=60)
            csv_resp.raise_for_status()
            file_path = os.path.join(self.data_path, "tesouro_direto_rentabilidade.csv")
            with open(file_path, "wb") as f:
                f.write(csv_resp.content)
            print(f"[Tesouro] CSV salvo em: {file_path}")

        except Exception as e:
            print(f"[Tesouro] Erro ao baixar o CSV: {e}")


class ExtratorCustosTesouroAPI:
    ENDPOINTS = {
        "depreciacao": "https://apidatalake.tesouro.gov.br/ords/custos/tt/depreciacao",
        "demais": "https://apidatalake.tesouro.gov.br/ords/custos/tt/demais",
        "pensionistas": "https://apidatalake.tesouro.gov.br/ords/custos/tt/pensionistas",
        "pessoal_ativo": "https://apidatalake.tesouro.gov.br/ords/custos/tt/pessoal_ativo",
        "pessoal_inativo": "https://apidatalake.tesouro.gov.br/ords/custos/tt/pessoal_inativo",
        "transferencias": "https://apidatalake.tesouro.gov.br/ords/custos/tt/transferencias"
    }

    def __init__(self, data_path, anos):
        self.data_path = data_path
        os.makedirs(self.data_path, exist_ok=True)
        # self.anos = list(range(2025, 2025)) 
        self.anos = anos
    def baixar_endpoint(self, nome, url):
        todos_registros = []
        print(f"[{nome}] Baixando dados dos anos {self.anos[0]} a {self.anos[-1]}...")
        for ano in self.anos:
            page = 1
            while True:
                params = {'ano': ano, 'page': page}
                try:
                    resp = requests.get(url, params=params, timeout=90)
                    if resp.status_code != 200:
                        print(f"[{nome}] Erro HTTP {resp.status_code} para ano {ano}")
                        break
                    dados = resp.json()
                    items = dados.get('items', [])
                    if not items:
                        break
                    todos_registros.extend(items)
                    print(f"[{nome}] Ano {ano} - Página {page} - {len(items)} registros")
                    if not dados.get('hasMore', False):
                        break
                    page += 1
                    time.sleep(1.1)

                    if 1000 and len(todos_registros) >= 1000:
                        break
                except Exception as e:
                    print(f"[{nome}] Erro ano {ano} página {page}: {e}")
                    break

        if todos_registros:
            df = pd.DataFrame(todos_registros)
            file_path = os.path.join(self.data_path, f"tesouro_custos_{nome}_{self.anos[0]}_{self.anos[-1]}.parquet")
            df.to_parquet(file_path, index=False)
            print(f"[{nome}] Dados salvos em Parquet: {file_path}")
        else:
            print(f"[{nome}] Nenhum dado encontrado para os anos {self.anos[0]} a {self.anos[-1]}.")

    def extrair(self):
        for nome, url in self.ENDPOINTS.items():
            self.baixar_endpoint(nome, url)

class ExtratorBacen(ExtratorBase):
    SERIES = {
        'selic': 1178,
        'cdi': 12,
        'ipca': 433,
        'poupanca': 195
    }
    def __init__(self, data_path, data_inicio='2010-01-01', data_fim=None):
        super().__init__(data_path)
        self.data_inicio = data_inicio
        self.data_fim = data_fim or datetime.today().strftime('%Y-%m-%d')

    def extrair(self):
        for nome, codigo in self.SERIES.items():
            try:
                print(f"[BACEN] Baixando {nome}...")
                df = sgs.dataframe(codigo, start=self.data_inicio, end=self.data_fim)
                filepath = os.path.join(self.data_path, f'bacen_{nome}.csv')
                df.to_csv(filepath)
                print(f"[BACEN] Salvo: {filepath}")
            except Exception as e:
                print(f"[BACEN] Erro em {nome}: {e}")
                traceback.print_exc()

class ExtratorCVM(ExtratorBase):
    URL = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'
    def extrair(self):
        try:
            print("[CVM] Baixando base de fundos...")
            response = requests.get(self.URL, timeout=60)
            filepath = os.path.join(self.data_path, 'cvm_cad_fi.csv')
            if response.ok:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                print(f"[CVM] Salvo: {filepath}")
            else:
                print(f"[CVM] Falha ao baixar (status HTTP {response.status_code})")
        except Exception as e:
            print(f"[CVM] Erro: {e}")
            traceback.print_exc()

class ExtratorIBGE(ExtratorBase):
    URL = 'https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/2266/p/all/d/v2266%202'
    def extrair(self):
        try:
            print("[IBGE] Baixando IPCA...")
            response = requests.get(self.URL, timeout=60)
            filepath = os.path.join(self.data_path, 'ibge_ipca.json')
            if response.ok:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                print(f"[IBGE] Salvo: {filepath}")
            else:
                print(f"[IBGE] Falha ao baixar (status HTTP {response.status_code})")
        except Exception as e:
            print(f"[IBGE] Erro: {e}")
            traceback.print_exc()

class PipelineExtracao:
    def __init__(self, data_path, tickers_b3, data_inicio, data_fim):
        self.data_path = data_path
        self.extratores = [
            ExtratorB3(self.data_path, tickers_b3, data_inicio, data_fim),
            ExtratorRentabilidadeTesouroWebscraping(self.data_path),
            ExtratorCustosTesouroAPI(data_path, anos=list(range(2014, 2024))), # exemplo: últimos 10 anos
            ExtratorSadipemAPI(data_path, anos=[2019, 2020, 2021, 2022, 2023]), # exemplo: 5 anos
            ExtratorBacen(self.data_path, data_inicio='2010-01-01'),
            ExtratorCVM(self.data_path),
            ExtratorIBGE(self.data_path),
        ]
    def rodar(self):
        for extrator in self.extratores:
            print(f"\n[Pipeline] Rodando: {extrator.__class__.__name__}")
            try:
                extrator.extrair()
            except Exception as e:
                print(f"[Pipeline] Erro no extrator {extrator.__class__.__name__}: {e}")
                traceback.print_exc()

if __name__ == "__main__":
    DATA_PATH = os.path.join(os.path.dirname(__file__), 'datalake', 'raw')
    TICKERS = ['PETR4', 'VALE3', 'ITUB4', 'BBDC4', 'BBAS3']
    data_inicio = '01/01/2019'
    data_fim = datetime.today().strftime('%d/%m/%Y')
    pipeline = PipelineExtracao(DATA_PATH, TICKERS, data_inicio, data_fim)
    pipeline.rodar()
    print("\nPipeline concluído!")
