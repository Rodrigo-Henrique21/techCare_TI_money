import os
import pandas as pd
from datetime import datetime

class TransformadorBase:
    def __init__(self, raw_path, silver_path):
        self.raw_path = raw_path
        self.silver_path = silver_path
        os.makedirs(self.silver_path, exist_ok=True)
    
    def transformar(self):
        raise NotImplementedError

class TransformadorB3(TransformadorBase):
    def transformar(self):
        print("[B3] Iniciando transformação dos dados...")
        for arquivo in os.listdir(self.raw_path):
            if arquivo.startswith('b3_') and arquivo.endswith('.parquet'):
                print(f"[B3] Processando {arquivo}")
                try:
                    # Lê o arquivo parquet
                    df = pd.read_parquet(os.path.join(self.raw_path, arquivo))
                    
                    # Padroniza nomes das colunas
                    df.columns = df.columns.str.lower()
                    
                    # Converte data para datetime se necessário
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    
                    # Remove linhas duplicadas
                    df = df.drop_duplicates()
                    
                    # Remove linhas com valores nulos
                    df = df.dropna()
                    
                    # Salva na silver
                    silver_file = os.path.join(self.silver_path, arquivo)
                    df.to_parquet(silver_file, index=False)
                    print(f"[B3] Dados transformados salvos em: {silver_file}")
                    
                except Exception as e:
                    print(f"[B3] Erro ao processar {arquivo}: {e}")

class TransformadorTesouro(TransformadorBase):
    def transformar(self):
        print("[Tesouro] Iniciando transformação dos dados...")
        # Processa dados de rentabilidade
        try:
            rentab_file = os.path.join(self.raw_path, "tesouro_direto_rentabilidade.csv")
            if os.path.exists(rentab_file):
                # Tenta ler o CSV com diferentes separadores e encodings
                try:
                    df = pd.read_csv(rentab_file, encoding='utf-8', sep=';')
                except:
                    try:
                        df = pd.read_csv(rentab_file, encoding='latin1', sep=';')
                    except:
                        df = pd.read_csv(rentab_file, encoding='utf-8', sep=',')
                
                # Padroniza nomes das colunas
                df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
                
                # Remove linhas duplicadas
                df = df.drop_duplicates()
                
                # Remove linhas completamente vazias
                df = df.dropna(how='all')
                
                # Salva na silver
                silver_file = os.path.join(self.silver_path, "tesouro_rentabilidade.parquet")
                df.to_parquet(silver_file, index=False)
                print(f"[Tesouro] Dados de rentabilidade transformados salvos em: {silver_file}")
        except Exception as e:
            print(f"[Tesouro] Erro ao processar rentabilidade: {e}")

        # Processa dados de custos
        for arquivo in os.listdir(self.raw_path):
            if arquivo.startswith('tesouro_custos_') and arquivo.endswith('.parquet'):
                try:
                    df = pd.read_parquet(os.path.join(self.raw_path, arquivo))
                    
                    # Padroniza nomes das colunas
                    df.columns = df.columns.str.lower()
                    
                    # Remove linhas duplicadas
                    df = df.drop_duplicates()
                    
                    # Salva na silver
                    silver_file = os.path.join(self.silver_path, arquivo)
                    df.to_parquet(silver_file, index=False)
                    print(f"[Tesouro] Dados de custos transformados salvos em: {silver_file}")
                except Exception as e:
                    print(f"[Tesouro] Erro ao processar {arquivo}: {e}")

class TransformadorBacen(TransformadorBase):
    def transformar(self):
        print("[BACEN] Iniciando transformação dos dados...")
        for arquivo in os.listdir(self.raw_path):
            if arquivo.startswith('bacen_') and arquivo.endswith('.csv'):
                try:
                    df = pd.read_csv(os.path.join(self.raw_path, arquivo))
                    
                    # Padroniza nomes das colunas
                    df.columns = df.columns.str.lower()
                    
                    # Converte data para datetime
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    
                    # Remove linhas duplicadas e valores nulos
                    df = df.drop_duplicates()
                    df = df.dropna()
                    
                    # Salva na silver
                    silver_file = os.path.join(self.silver_path, arquivo.replace('.csv', '.parquet'))
                    df.to_parquet(silver_file, index=False)
                    print(f"[BACEN] Dados transformados salvos em: {silver_file}")
                except Exception as e:
                    print(f"[BACEN] Erro ao processar {arquivo}: {e}")

class TransformadorCVM(TransformadorBase):
    def transformar(self):
        print("[CVM] Iniciando transformação dos dados...")
        try:
            arquivo = os.path.join(self.raw_path, 'cvm_cad_fi.csv')
            if os.path.exists(arquivo):
                # Tenta diferentes configurações de leitura do CSV
                try:
                    df = pd.read_csv(arquivo, encoding='utf-8', sep=';')
                except:
                    try:
                        df = pd.read_csv(arquivo, encoding='latin1', sep=';')
                    except:
                        df = pd.read_csv(arquivo, encoding='utf-8', sep=',', on_bad_lines='skip')
                
                # Padroniza nomes das colunas
                df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_').str.replace('/', '_')
                
                # Remove linhas duplicadas
                df = df.drop_duplicates()
                
                # Remove linhas completamente vazias
                df = df.dropna(how='all')
                
                # Remove linhas com valores nulos em colunas críticas
                colunas_criticas = ['cnpj_fundo', 'denom_social']
                df = df.dropna(subset=colunas_criticas)
                
                # Salva na silver
                silver_file = os.path.join(self.silver_path, "cvm_fundos.parquet")
                df.to_parquet(silver_file, index=False)
                print(f"[CVM] Dados transformados salvos em: {silver_file}")
        except Exception as e:
            print(f"[CVM] Erro ao processar dados: {e}")

class TransformadorIBGE(TransformadorBase):
    def transformar(self):
        print("[IBGE] Iniciando transformação dos dados...")
        try:
            arquivo = os.path.join(self.raw_path, 'ibge_ipca.json')
            if os.path.exists(arquivo):
                df = pd.read_json(arquivo)
                
                # Padroniza nomes das colunas
                df.columns = df.columns.str.lower()
                
                # Remove linhas duplicadas
                df = df.drop_duplicates()
                
                # Salva na silver
                silver_file = os.path.join(self.silver_path, "ibge_ipca.parquet")
                df.to_parquet(silver_file, index=False)
                print(f"[IBGE] Dados transformados salvos em: {silver_file}")
        except Exception as e:
            print(f"[IBGE] Erro ao processar dados: {e}")

class PipelineTransformacao:
    def __init__(self, raw_path, silver_path):
        self.transformadores = [
            TransformadorB3(raw_path, silver_path),
            TransformadorTesouro(raw_path, silver_path),
            TransformadorBacen(raw_path, silver_path),
            TransformadorCVM(raw_path, silver_path),
            TransformadorIBGE(raw_path, silver_path)
        ]
    
    def rodar(self):
        for transformador in self.transformadores:
            print(f"\n[Pipeline] Iniciando: {transformador.__class__.__name__}")
            try:
                transformador.transformar()
            except Exception as e:
                print(f"[Pipeline] Erro no transformador {transformador.__class__.__name__}: {e}")

if __name__ == "__main__":
    RAW_PATH = os.path.join(os.path.dirname(__file__), 'datalake', 'raw')
    SILVER_PATH = os.path.join(os.path.dirname(__file__), 'datalake', 'silver')
    
    pipeline = PipelineTransformacao(RAW_PATH, SILVER_PATH)
    pipeline.rodar()
    print("\nTransformação concluída!")
