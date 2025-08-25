import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from azure_storage import AzureStorageManager


class IntegradorDados:
    def __init__(self):
        self.storage_manager = AzureStorageManager()
        
    def carregar_dados(self):
        """Carrega todos os dados da camada silver do blob storage"""
        self.dados = {}
        print("Carregando dados B3...")
        b3_blobs = self.storage_manager.list_blobs(prefix='silver/b3_')
        for blob in b3_blobs:
            nome = os.path.basename(blob).replace('.parquet', '')
            self.dados[nome] = self.storage_manager.read_dataframe('silver', nome)
        print("Carregando dados do Tesouro...")
        self.dados['tesouro_rentabilidade'] = self.storage_manager.read_dataframe('silver', 'tesouro_rentabilidade')
        custos_blobs = self.storage_manager.list_blobs(prefix='silver/tesouro_custos_')
        for blob in custos_blobs:
            nome = os.path.basename(blob).replace('.parquet', '')
            self.dados[nome] = self.storage_manager.read_dataframe('silver', nome)
        print("Carregando IPCA...")
        self.dados['ipca'] = self.storage_manager.read_dataframe('silver', 'ibge_ipca')
        print("Carregando dados CVM...")
        self.dados['fundos'] = self.storage_manager.read_dataframe('silver', 'cvm_fundos')
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
            self.storage_manager.save_dataframe(df_resultados, 'gold', 'analise_b3_ipca')
            print("\nResultados salvos no blob storage: gold/analise_b3_ipca.parquet")
    
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
        self.storage_manager.save_dataframe(df_fundos, 'gold', 'analise_fundos')
        self.storage_manager.save_dataframe(df_tesouro, 'gold', 'analise_tesouro')
        print("\nResultados salvos no blob storage:")
        print("- gold/analise_fundos.parquet")
        print("- gold/analise_tesouro.parquet")
    
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
            self.storage_manager.save_dataframe(df_resultados, 'gold', 'analise_custos_tesouro')
            print("\nResultados salvos no blob storage: gold/analise_custos_tesouro.parquet")
    
    def executar_analises(self):
        """Executa todas as análises"""
        self.carregar_dados()
        self.analise_b3_ipca()
        self.analise_fundos_tesouro()
        self.analise_custos_tesouro()

if __name__ == "__main__":
    integrador = IntegradorDados()
    integrador.executar_analises()
    print("\nTodas as análises foram concluídas e salvas na camada gold!")