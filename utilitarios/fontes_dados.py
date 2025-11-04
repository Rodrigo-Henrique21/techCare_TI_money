"""Conexões e funções de captura de dados externos utilizados pelo pipeline."""

from __future__ import annotations

import json
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Any, Union
from pyspark.sql import DataFrame, SparkSession

# Inicializa a sessão Spark
spark = (SparkSession.builder
         .appName("dlt-aafn-api_ing-dados")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())


try:  # pragma: no cover - dependência opcional em workspaces Databricks
    import sgs  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - tratamos o fallback manualmente
    sgs = None


def _converter_data(data: str, formato_entrada: str = "%d/%m/%Y", formato_saida: str = "%Y-%m-%d") -> str:
    """
    Converte data entre diferentes formatos.
    
    Args:
        data: String representando a data
        formato_entrada: Formato da data de entrada (default: DD/MM/YYYY)
        formato_saida: Formato da data de saída (default: YYYY-MM-DD)
    
    Returns:
        Data convertida no formato de saída
        
    Raises:
        ValueError: Se a data estiver em formato inválido
    """
    try:
        return datetime.strptime(data, formato_entrada).strftime(formato_saida)
    except ValueError as e:
        raise ValueError(f"Formato de data inválido. Use {formato_entrada}. Erro: {str(e)}")

def criar_dataframe_vazio(schema: Any) -> DataFrame:
    """Cria um DataFrame vazio com o schema especificado."""
    return spark.createDataFrame([], schema)

def buscar_historico_b3(tickers: Iterable[str], inicio: str, fim: str) -> pd.DataFrame:
    """
    Baixa o histórico de preços dos tickers configurados no Yahoo Finance.
    
    Args:
        tickers: Lista de códigos de ações (ex: PETR4.SA)
        inicio: Data inicial no formato DD/MM/YYYY
        fim: Data final no formato DD/MM/YYYY
        
    Returns:
        DataFrame com histórico de preços
        
    Raises:
        ValueError: Se as datas estiverem em formato inválido
    """
    import yfinance as yf
    import time
    import random
    from requests.exceptions import RequestException
    
    # Configuração do yfinance
    yf.set_tz_cache_location(None)  # Desabilita cache de timezone que pode causar problemas
    
    print(f"Iniciando busca de dados para {len(list(tickers))} tickers")
    inicio_fmt = _converter_data(inicio)
    fim_fmt = _converter_data(fim)
    print(f"Período: {inicio_fmt} até {fim_fmt}")
    
    # Headers para simular um navegador real
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    colunas = [
        "Date", "Open", "High", "Low", "Close", 
        "Volume", "Dividends", "Stock_Splits", "ticker"
    ]
    
    quadros: List[pd.DataFrame] = []
    erros: List[str] = []
    
    if not tickers:
        print("Nenhum ticker fornecido!")
        return pd.DataFrame(columns=colunas)
    
    for ticker in tickers:
        try:
            print(f"Buscando dados para {ticker}...")
            # Remove sufixo .SA se já estiver presente
            ticker_base = ticker.replace('.SA', '')
            ticker_sa = f"{ticker_base}.SA"
            
            # Configura uma sessão personalizada para o ticker com proxies padrão
            session = requests.Session()
            session.headers.update(headers)
            session.verify = True
            
            # Cria o ticker com a sessão personalizada
            acao = yf.Ticker(ticker_sa)
            acao.session = session
            
            # Tenta obter dados históricos
            historico = acao.history(
                start=inicio_fmt,
                end=fim_fmt,
                interval="1d",
                auto_adjust=True,
                prepost=False,
                actions=True,
                timeout=30
            )
            
            # Verifica se há dados
            if historico.empty:
                print(f"Nenhum dado encontrado para {ticker}")
                continue
                
            print(f"Dados encontrados para {ticker}: {len(historico)} registros")
            
            if not historico.empty:
                historico = historico.reset_index()
                historico["ticker"] = ticker_base.upper()
                
                # Garante que todas as colunas necessárias existem
                for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits']:
                    if col not in historico.columns:
                        historico[col] = 0.0
                
                # Converte tipos de dados
                historico['Date'] = pd.to_datetime(historico['Date'])
                for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits']:
                    historico[col] = pd.to_numeric(historico[col], errors='coerce').fillna(0.0)
                
                quadros.append(historico)
            
        except Exception as e:
            erro_msg = f"Erro ao processar {ticker}: {str(e)}"
            print(erro_msg)
            erros.append(erro_msg)
            continue
    
    if erros:
        print("\nAvisos durante a busca de dados:")
        for erro in erros:
            print(f"- {erro}")
            
    if not quadros:
        print("Nenhum dado encontrado para nenhum ticker!")
        return pd.DataFrame(columns=colunas)
    
    print("\nConcatenando resultados...")    
    resultado = pd.concat(quadros, ignore_index=True)
    
    # Padroniza nomes de colunas
    if "Stock Splits" in resultado.columns:
        resultado = resultado.rename(columns={"Stock Splits": "Stock_Splits"})
        
    # Garante tipos de dados corretos
    resultado['Date'] = pd.to_datetime(resultado['Date'], errors='coerce')
    resultado = resultado.dropna(subset=['Date'])  # Remove linhas com datas inválidas
    
    # Converte valores numéricos
    colunas_numericas = ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits']
    for col in colunas_numericas:
        if col in resultado.columns:
            resultado[col] = pd.to_numeric(resultado[col], errors='coerce')
            resultado[col] = resultado[col].fillna(0.0)
    
    print(f"Total de registros obtidos: {len(resultado)}")
    return resultado


def _normalizar_dataframe_bacen(nome, quadro):
    # Check if the index contains non-date values (e.g., error messages)
    if isinstance(quadro.index[0], str) and not quadro.index[0].replace("-", "").isdigit():
        raise ValueError(f"BACEN API returned an error for series '{nome}': {quadro.index[0]}")
    quadro.index = pd.to_datetime(quadro.index, errors="coerce")
    if quadro.index.isnull().any():
        raise ValueError(f"Failed to parse some dates for series '{nome}'.")
    return quadro


def format_bacen_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")


def _buscar_com_requests(
    codigo: int,
    inicio_fmt: str,
    fim_fmt: str
) -> pd.DataFrame:
    
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={inicio_fmt}&dataFinal={fim_fmt}"
    )
    headers = {"Accept": "application/json"}
    resposta = requests.get(
        url,
        headers=headers,
        timeout=30
    )
    resposta.raise_for_status()
    dados = resposta.json()
    # If dados is a dict, wrap in a list for DataFrame creation
    if isinstance(dados, dict):
        quadro = pd.DataFrame([dados])
    else:
        quadro = pd.DataFrame(dados)
    return quadro

def buscar_series_bacen(series: dict, inicio: str, fim: str) -> pd.DataFrame:
    """Busca séries temporais no BACEN."""
    # Converte datas para o formato esperado pelo BACEN (DD/MM/YYYY)
    try:
        inicio_fmt = datetime.strptime(inicio, "%d/%m/%Y").strftime("%d/%m/%Y")
        fim_fmt = datetime.strptime(fim, "%d/%m/%Y").strftime("%d/%m/%Y")
    except ValueError as e:
        raise ValueError(f"Data deve estar no formato DD/MM/YYYY: {str(e)}")
    
    # Schema padrão para dados vazios
    colunas = ["data", "valor", "serie"]
    quadros = []
    erros = []
    
    for nome, codigo in series.items():
        try:
            quadro = _buscar_com_requests(codigo, inicio_fmt, fim_fmt)
            if quadro.empty:
                erros.append(f"Nenhum dado retornado para {nome} (código {codigo})")
                continue
            
            if "error" in quadro.columns:
                erros.append(f"Erro na série {nome}: {quadro['error'].iloc[0]}")
                continue
                
            # Converte data e valor para tipos corretos
            quadro["data"] = pd.to_datetime(quadro["data"], format="%d/%m/%Y", errors='coerce')
            quadro["valor"] = pd.to_numeric(quadro["valor"], errors="coerce")
            
            # Remove linhas com datas ou valores inválidos
            quadro = quadro.dropna(subset=["data", "valor"])
            
            if quadro.empty:
                erros.append(f"Dados inválidos para série {nome}")
                continue
                
            # Define o tipo da série após limpeza
            quadro["serie"] = nome
                
            quadro["serie"] = nome
            quadros.append(quadro)
            
        except Exception as e:
            erros.append(f"Erro ao buscar {nome}: {str(e)}")
            continue
    
    if erros:
        print("Avisos durante a busca de séries BACEN:")
        for erro in erros:
            print(f"- {erro}")
    
    if not quadros:
        return pd.DataFrame(columns=["data", "valor", "serie"])
        
    return pd.concat(quadros, ignore_index=True)


__all__ = [
    "buscar_historico_b3",
    "buscar_series_bacen",
]