import os
import pandas as pd
import logging
from datetime import datetime
import shutil
from azure_storage import AzureStorageManager

class TransformadorBase:
    def __init__(self, storage_manager: AzureStorageManager):
        self.storage_manager = storage_manager
    
    def transformar(self):
        raise NotImplementedError

class TransformadorB3(TransformadorBase):
    def transformar(self):
        print("[B3] Iniciando transformação dos dados...")
        # Lista todos os blobs da B3 na camada raw
        blobs = self.storage_manager.list_blobs(prefix='raw/b3_')
        
        for blob_path in blobs:
            if blob_path.endswith('.parquet'):
                nome_arquivo = os.path.basename(blob_path)
                print(f"[B3] Processando {nome_arquivo}")
                try:
                    # Lê o DataFrame diretamente do blob storage
                    df = self.storage_manager.read_dataframe('raw', nome_arquivo.replace('.parquet', ''))
                    
                    # Padroniza nomes das colunas
                    df.columns = df.columns.str.lower()
                    
                    # Converte data para datetime se necessário
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    
                    # Remove linhas duplicadas e valores nulos
                    df = df.drop_duplicates()
                    df = df.dropna()
                    
                    # Salva diretamente na camada silver
                    self.storage_manager.save_dataframe(df, 'silver', nome_arquivo.replace('.parquet', ''))
                    print(f"[B3] Dados transformados salvos em silver/{nome_arquivo}")
                    
                except Exception as e:
                    logging.error(f"[B3] Erro ao processar {nome_arquivo}: {e}")
                    raise

class TransformadorTesouro(TransformadorBase):
    def transformar(self):
        print("[Tesouro] Iniciando transformação dos dados...")
        
        # Processa dados de rentabilidade
        try:
            # Tenta ler o CSV diretamente do storage
            df = self.storage_manager.read_dataframe('raw', 'tesouro_direto_rentabilidade', format='csv')
            
            # Padroniza nomes das colunas
            df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
            
            # Remove linhas duplicadas e vazias
            df = df.drop_duplicates()
            df = df.dropna(how='all')
            
            # Salva diretamente na camada silver
            self.storage_manager.save_dataframe(df, 'silver', 'tesouro_rentabilidade')
            print("[Tesouro] Dados de rentabilidade transformados e salvos")
            
        except Exception as e:
            logging.error(f"[Tesouro] Erro ao processar rentabilidade: {e}")
            raise

        # Processa dados de custos
        custos_blobs = self.storage_manager.list_blobs(prefix='raw/tesouro_custos_')
        for blob_path in custos_blobs:
            if blob_path.endswith('.parquet'):
                nome_arquivo = os.path.basename(blob_path)
                try:
                    # Lê o DataFrame diretamente do storage
                    df = self.storage_manager.read_dataframe('raw', nome_arquivo.replace('.parquet', ''))
                    
                    # Padroniza nomes das colunas
                    df.columns = df.columns.str.lower()
                    
                    # Remove linhas duplicadas
                    df = df.drop_duplicates()
                    
                    # Salva diretamente na camada silver
                    self.storage_manager.save_dataframe(df, 'silver', nome_arquivo.replace('.parquet', ''))
                    print(f"[Tesouro] Dados de custos transformados e salvos: {nome_arquivo}")
                    
                except Exception as e:
                    logging.error(f"[Tesouro] Erro ao processar {nome_arquivo}: {e}")
                    raise

class TransformadorBacen(TransformadorBase):
    def transformar(self):
        print("[BACEN] Iniciando transformação dos dados...")
        # Lista todos os blobs do BACEN na camada raw
        blobs = self.storage_manager.list_blobs(prefix='raw/bacen_')
        
        for blob_path in blobs:
            if blob_path.endswith('.csv'):
                nome_arquivo = os.path.basename(blob_path)
                print(f"[BACEN] Processando {nome_arquivo}")
                try:
                    # Lê o DataFrame diretamente do storage
                    df = self.storage_manager.read_dataframe('raw', nome_arquivo.replace('.csv', ''), format='csv')
                    
                    # Padroniza nomes das colunas
                    df.columns = df.columns.str.lower()
                    
                    # Converte data para datetime
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    
                    # Remove linhas duplicadas e valores nulos
                    df = df.drop_duplicates()
                    df = df.dropna()
                    
                    # Salva diretamente na camada silver
                    self.storage_manager.save_dataframe(df, 'silver', nome_arquivo.replace('.csv', ''))
                    print(f"[BACEN] Dados transformados salvos em silver/{nome_arquivo.replace('.csv', '.parquet')}")
                    
                except Exception as e:
                    logging.error(f"[BACEN] Erro ao processar {nome_arquivo}: {e}")
                    raise

class TransformadorCVM(TransformadorBase):
    def transformar(self):
        print("[CVM] Iniciando transformação dos dados...")
        try:
            # Lê o DataFrame diretamente do storage
            df = self.storage_manager.read_dataframe('raw', 'cvm_cad_fi', format='csv')
            
            # Padroniza nomes das colunas
            df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_').str.replace('/', '_')
            
            # Remove linhas duplicadas e vazias
            df = df.drop_duplicates()
            df = df.dropna(how='all')
            
            # Remove linhas com valores nulos em colunas críticas
            colunas_criticas = ['cnpj_fundo', 'denom_social']
            df = df.dropna(subset=colunas_criticas)
            
            # Salva diretamente na camada silver
            self.storage_manager.save_dataframe(df, 'silver', 'cvm_fundos')
            print("[CVM] Dados transformados e salvos em silver/cvm_fundos.parquet")
            
        except Exception as e:
            logging.error(f"[CVM] Erro ao processar dados: {e}")
            raise

class TransformadorIBGE(TransformadorBase):
    def transformar(self):
        print("[IBGE] Iniciando transformação dos dados...")
        try:
            # Lê o DataFrame diretamente do storage
            df = self.storage_manager.read_dataframe('raw', 'ibge_ipca', format='json')
            
            # Padroniza nomes das colunas
            df.columns = df.columns.str.lower()
            
            # Remove linhas duplicadas
            df = df.drop_duplicates()
            
            # Salva diretamente na camada silver
            self.storage_manager.save_dataframe(df, 'silver', 'ibge_ipca')
            print("[IBGE] Dados transformados e salvos em silver/ibge_ipca.parquet")
            
        except Exception as e:
            logging.error(f"[IBGE] Erro ao processar dados: {e}")
            raise

class PipelineTransformacao:
    def __init__(self, raw_path=None, silver_path=None):
        self.storage_manager = AzureStorageManager()
        
        self.transformadores = [
            TransformadorB3(self.storage_manager),
            TransformadorTesouro(self.storage_manager),
            TransformadorBacen(self.storage_manager),
            TransformadorCVM(self.storage_manager),
            TransformadorIBGE(self.storage_manager)
        ]
    
    def rodar(self):
        for transformador in self.transformadores:
            print(f"\n[Pipeline] Iniciando: {transformador.__class__.__name__}")
            try:
                transformador.transformar()
            except Exception as e:
                logging.error(f"[Pipeline] Erro no transformador {transformador.__class__.__name__}: {e}")
                raise