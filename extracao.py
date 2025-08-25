import os
import traceback
import requests
import yfinance as yf
import pandas as pd
import sgs
import time
import logging
import io
from bs4 import BeautifulSoup
from datetime import datetime
import shutil
from azure_storage import AzureStorageManager


class ExtratorBase:
	def __init__(self, storage_manager: AzureStorageManager):
		self.storage_manager = storage_manager
	def extrair(self):
		raise NotImplementedError

class ExtratorB3(ExtratorBase):
	def __init__(self, storage_manager, tickers, data_inicio=None, data_fim=None):
		super().__init__(storage_manager)
		self.tickers = tickers
		self.data_inicio = datetime.strptime(data_inicio or '01/01/2015', '%d/%m/%Y') if isinstance(data_inicio, str) else data_inicio
		self.data_fim = datetime.strptime(data_fim or datetime.today().strftime('%d/%m/%Y'), '%d/%m/%Y') if isinstance(data_fim, str) else data_fim

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
                    
					# Salva diretamente no blob storage
					nome_arquivo = f'b3_{ticker}_{self.data_inicio.strftime("%d%m%Y")}_{self.data_fim.strftime("%d%m%Y")}'
					self.storage_manager.save_dataframe(df, 'raw', nome_arquivo)
					print(f"[B3] Dados salvos no blob storage: raw/{nome_arquivo}.parquet")
				else:
					print(f"[B3] Nenhum dado para {ticker}")
			except Exception as e:
				logging.error(f"[B3] Erro ao baixar {ticker}: {e}")
				raise


class ExtratorRentabilidadeTesouroWebscraping(ExtratorBase):
	URL = os.environ.get("CAMINHO_TESOURO")

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
				logging.error("[Tesouro] Não encontrou link para o CSV na página.")
				return
            
			# Normalizar URL se necessário
			if not link.startswith("http"):
				link = "https://www.tesourodireto.com.br" + link
			print(f"[Tesouro] Link do CSV encontrado: {link}")

			# Baixar o CSV
			csv_resp = requests.get(link, timeout=60)
			csv_resp.raise_for_status()
            
			# Converte o conteúdo do CSV para DataFrame
			df = pd.read_csv(io.StringIO(csv_resp.content.decode('utf-8')))
            
			# Salva diretamente no blob storage
			self.storage_manager.save_dataframe(df, 'raw', 'tesouro_direto_rentabilidade', format='csv')
			print("[Tesouro] Dados salvos no blob storage: raw/tesouro_direto_rentabilidade.csv")

		except Exception as e:
			logging.error(f"[Tesouro] Erro ao baixar o CSV: {e}")
			raise


class ExtratorBacen(ExtratorBase):
	SERIES = {
		'selic': 1178,
		'cdi': 12,
		'ipca': 433,
		'poupanca': 195
	}
	def __init__(self, storage_manager, data_inicio='2010-01-01', data_fim=None):
		super().__init__(storage_manager)
		self.data_inicio = data_inicio
		self.data_fim = data_fim or datetime.today().strftime('%Y-%m-%d')

	def extrair(self):
		for nome, codigo in self.SERIES.items():
			try:
				print(f"[BACEN] Baixando {nome}...")
				df = sgs.dataframe(codigo, start=self.data_inicio, end=self.data_fim)
                
				# Salva diretamente no blob storage
				nome_arquivo = f'bacen_{nome}'
				self.storage_manager.save_dataframe(df, 'raw', nome_arquivo)
				print(f"[BACEN] Dados salvos no blob storage: raw/{nome_arquivo}.parquet")
			except Exception as e:
				logging.error(f"[BACEN] Erro em {nome}: {e}")
				raise

class ExtratorCVM(ExtratorBase):
	URL = os.environ.get("CAMINHO_CVM")
	def extrair(self):
		try:
			print("[CVM] Baixando base de fundos...")
			response = requests.get(self.URL, timeout=60)
			if response.ok:
				# Converte o conteúdo do CSV para DataFrame
				df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                
				# Salva diretamente no blob storage
				self.storage_manager.save_dataframe(df, 'raw', 'cvm_cad_fi', format='csv')
				print("[CVM] Dados salvos no blob storage: raw/cvm_cad_fi.csv")
			else:
				logging.error(f"[CVM] Falha ao baixar (status HTTP {response.status_code})")
				raise Exception(f"Status HTTP {response.status_code}")
		except Exception as e:
			logging.error(f"[CVM] Erro: {e}")
			raise

class ExtratorIBGE(ExtratorBase):
	URL = os.environ.get("CAMINHO_IBGE")
	def extrair(self):
		try:
			print("[IBGE] Baixando IPCA...")
			response = requests.get(self.URL, timeout=60)
			if response.ok:
				# Converte o conteúdo JSON para DataFrame
				df = pd.read_json(io.StringIO(response.text))
                
				# Salva diretamente no blob storage
				self.storage_manager.save_dataframe(df, 'raw', 'ibge_ipca', format='json')
				print("[IBGE] Dados salvos no blob storage: raw/ibge_ipca.json")
			else:
				logging.error(f"[IBGE] Falha ao baixar (status HTTP {response.status_code})")
				raise Exception(f"Status HTTP {response.status_code}")
		except Exception as e:
			logging.error(f"[IBGE] Erro: {e}")
			raise

class PipelineExtracao:
	def __init__(self, data_path, tickers_b3, data_inicio, data_fim):
		self.storage_manager = AzureStorageManager()
        
		self.extratores = [
			ExtratorB3(self.storage_manager, tickers_b3, data_inicio, data_fim),
			ExtratorRentabilidadeTesouroWebscraping(self.storage_manager),
			ExtratorBacen(self.storage_manager, data_inicio='2010-01-01'),
			ExtratorCVM(self.storage_manager),
			ExtratorIBGE(self.storage_manager),
		]
	def rodar(self):
		for extrator in self.extratores:
			print(f"\n[Pipeline] Rodando: {extrator.__class__.__name__}")
			try:
				# Extrai os dados diretamente para o blob storage
				extrator.extrair()
			except Exception as e:
				logging.error(f"Erro no extrator {extrator.__class__.__name__}: {str(e)}")
				raise
