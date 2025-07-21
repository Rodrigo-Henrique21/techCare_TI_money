import os
import pandas as pd
import numpy as np
from datetime import datetime

class IntegradorDados:
    def __init__(self, silver_path, gold_path):
        self.silver_path = silver_path
        self.gold_path = gold_path
        os.makedirs(self.gold_path, exist_ok=True)
        
    def carregar_dados(self):
        """Carrega todos os dados da pasta silver"""
        self.dados = {}
        
        # Carrega dados B3
        print("Carregando dados B3...")
        for arquivo in os.listdir(self.silver_path):
            if arquivo.startswith('b3_') and arquivo.endswith('.parquet'):
                ticker = arquivo.split('_')[1].split('.')[0]
                self.dados[f'b3_{ticker}'] = pd.read_parquet(os.path.join(self.silver_path, arquivo))
        
        # Carrega dados do Tesouro
        print("Carregando dados do Tesouro...")
        self.dados['tesouro_rentabilidade'] = pd.read_parquet(os.path.join(self.silver_path, 'tesouro_rentabilidade.parquet'))
        
        # Carrega dados de custos do Tesouro
        for arquivo in os.listdir(self.silver_path):
            if arquivo.startswith('tesouro_custos_') and arquivo.endswith('.parquet'):
                nome = arquivo.replace('.parquet', '')
                self.dados[nome] = pd.read_parquet(os.path.join(self.silver_path, arquivo))
        
        # Carrega IPCA
        print("Carregando IPCA...")
        self.dados['ipca'] = pd.read_parquet(os.path.join(self.silver_path, 'ibge_ipca.parquet'))
        
        # Carrega dados CVM
        print("Carregando dados CVM...")
        self.dados['fundos'] = pd.read_parquet(os.path.join(self.silver_path, 'cvm_fundos.parquet'))
        
        return self.dados
    
    def analise_b3_ipca(self):
        """Análise da correlação entre ações e IPCA"""
        print("\nAnálise B3 x IPCA")
        print("=" * 50)
        
        # Prepara dados do IPCA
        ipca = self.dados['ipca']
        
        # DataFrame para armazenar resultados
        resultados = []
        
        # Para cada ação
        for key in self.dados.keys():
            if key.startswith('b3_'):
                acao = self.dados[key]
                ticker = key.split('_')[1]
                
                print(f"\nAnalisando {ticker}")
                print("-" * 30)
                
                # Converte datas para datetime se necessário
                if 'date' in acao.columns:
                    acao['date'] = pd.to_datetime(acao['date'])
                
                # Calcula retorno mensal da ação
                acao['retorno'] = acao['close'].pct_change()
                acao['ano_mes'] = acao['date'].dt.to_period('M')
                retorno_mensal = acao.groupby('ano_mes')['retorno'].mean()
                
                retorno_medio = retorno_mensal.mean()
                volatilidade = retorno_mensal.std()
                
                print(f"Retorno médio: {retorno_medio:.2%}")
                print(f"Volatilidade: {volatilidade:.2%}")
                
                # Adiciona resultados ao DataFrame
                resultados.append({
                    'ticker': ticker,
                    'retorno_medio': retorno_medio,
                    'volatilidade': volatilidade,
                    'primeira_data': acao['date'].min(),
                    'ultima_data': acao['date'].max(),
                    'num_pregoes': len(acao),
                    'preco_medio': acao['close'].mean(),
                    'volume_medio': acao['volume'].mean() if 'volume' in acao.columns else None
                })
        
        # Salva resultados na gold
        if resultados:
            df_resultados = pd.DataFrame(resultados)
            gold_path = os.path.join(self.gold_path, 'analise_b3_ipca.parquet')
            df_resultados.to_parquet(gold_path, index=False)
            print(f"\nResultados salvos em: {gold_path}")
    
    def analise_fundos_tesouro(self):
        """Análise dos fundos que investem em títulos públicos"""
        print("\nAnálise Fundos x Tesouro")
        print("=" * 50)
        
        fundos = self.dados['fundos']
        tesouro = self.dados['tesouro_rentabilidade']
        
        # Análise básica dos fundos
        print("\nEstatísticas dos Fundos:")
        total_fundos = len(fundos)
        print(f"Total de fundos: {total_fundos}")
        
        # Resultados para fundos
        resultados_fundos = {
            'total_fundos': total_fundos,
            'data_analise': datetime.now().strftime('%Y-%m-%d'),
            'metricas': {}
        }
        
        if 'classe_anbima' in fundos.columns:
            print("\nDistribuição por classe Anbima:")
            dist_classe = fundos['classe_anbima'].value_counts()
            print(dist_classe.head())
            resultados_fundos['metricas']['distribuicao_classe'] = dist_classe.to_dict()
        
        # Análise da rentabilidade do Tesouro
        print("\nRentabilidade média do Tesouro:")
        resultados_tesouro = {}
        if 'rentabilidade' in tesouro.columns:
            rentab_media = tesouro.groupby('tipo_titulo')['rentabilidade'].mean()
            print(rentab_media)
            resultados_tesouro['rentabilidade_media'] = rentab_media.to_dict()
        
        # Salva resultados na gold
        df_fundos = pd.DataFrame([resultados_fundos])
        df_tesouro = pd.DataFrame([resultados_tesouro])
        
        gold_path_fundos = os.path.join(self.gold_path, 'analise_fundos.parquet')
        gold_path_tesouro = os.path.join(self.gold_path, 'analise_tesouro.parquet')
        
        df_fundos.to_parquet(gold_path_fundos, index=False)
        df_tesouro.to_parquet(gold_path_tesouro, index=False)
        
        print(f"\nResultados salvos em:")
        print(f"- {gold_path_fundos}")
        print(f"- {gold_path_tesouro}")
    
    def analise_custos_tesouro(self):
        """Análise dos custos do Tesouro"""
        print("\nAnálise de Custos do Tesouro")
        print("=" * 50)
        
        # Lista todos os tipos de custos
        custos_dfs = {k: v for k, v in self.dados.items() if k.startswith('tesouro_custos_')}
        
        # Lista para armazenar resultados
        resultados_custos = []
        
        for nome, df in custos_dfs.items():
            tipo_custo = nome.split('_')[-1]
            print(f"\nAnálise de {tipo_custo}")
            print("-" * 30)
            
            # Verifica colunas disponíveis para análise
            colunas_numericas = df.select_dtypes(include=[np.number]).columns
            if len(colunas_numericas) > 0:
                print("Estatísticas descritivas:")
                stats = df[colunas_numericas].describe()
                print(stats)
                
                # Adiciona resultados ao DataFrame
                resultado = {
                    'tipo_custo': tipo_custo,
                    'data_analise': datetime.now().strftime('%Y-%m-%d'),
                    'num_registros': len(df)
                }
                
                # Adiciona estatísticas para cada coluna numérica
                for col in colunas_numericas:
                    resultado[f'{col}_media'] = df[col].mean()
                    resultado[f'{col}_mediana'] = df[col].median()
                    resultado[f'{col}_desvio_padrao'] = df[col].std()
                    resultado[f'{col}_minimo'] = df[col].min()
                    resultado[f'{col}_maximo'] = df[col].max()
                
                resultados_custos.append(resultado)
        
        # Salva resultados na gold
        if resultados_custos:
            df_resultados = pd.DataFrame(resultados_custos)
            gold_path = os.path.join(self.gold_path, 'analise_custos_tesouro.parquet')
            df_resultados.to_parquet(gold_path, index=False)
            print(f"\nResultados salvos em: {gold_path}")
    
    def executar_analises(self):
        """Executa todas as análises"""
        self.carregar_dados()
        self.analise_b3_ipca()
        self.analise_fundos_tesouro()
        self.analise_custos_tesouro()

if __name__ == "__main__":
    SILVER_PATH = os.path.join(os.path.dirname(__file__), 'datalake', 'silver')
    GOLD_PATH = os.path.join(os.path.dirname(__file__), 'datalake', 'gold')
    
    integrador = IntegradorDados(SILVER_PATH, GOLD_PATH)
    integrador.executar_analises()
    print("\nTodas as análises foram concluídas e salvas na camada gold!")
