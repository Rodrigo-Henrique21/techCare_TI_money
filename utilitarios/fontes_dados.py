"""Conexões e funções de captura de dados externos utilizados pelo pipeline."""

from __future__ import annotations

import json
import pandas as pd
import requests
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
    inicio_fmt = _converter_data(inicio)
    fim_fmt = _converter_data(fim)
    
    colunas = [
        "Date", "Open", "High", "Low", "Close", 
        "Volume", "Dividends", "Stock_Splits", "ticker"
    ]
    
    quadros: List[pd.DataFrame] = []
    erros: List[str] = []
    
    if not tickers:
        return pd.DataFrame(columns=colunas)
    
    for ticker in tickers:
        try:
            ticker_sa = f"{ticker}.SA"
            historico = yf.Ticker(ticker_sa).history(start=inicio_fmt, end=fim_fmt)
            if not historico.empty:
                historico = historico.reset_index()
                historico["ticker"] = ticker.upper()
                quadros.append(historico)
        except Exception as e:
            erros.append(f"Erro ao buscar {ticker}: {str(e)}")
            continue
    
    if erros:
        print("Avisos durante a busca de dados:")
        for erro in erros:
            print(f"- {erro}")
            
    if not quadros:
        return pd.DataFrame(columns=colunas)
        
    resultado = pd.concat(quadros, ignore_index=True)
    if "Stock Splits" in resultado.columns:
        resultado = resultado.rename(columns={"Stock Splits": "Stock_Splits"})
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
            quadro["data"] = pd.to_datetime(quadro["data"], format="%d/%m/%Y")
            quadro["valor"] = pd.to_numeric(quadro["valor"], errors="coerce")
            quadro = quadro.dropna()
            
            if quadro.empty:
                erros.append(f"Dados inválidos para série {nome}")
                continue
                
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