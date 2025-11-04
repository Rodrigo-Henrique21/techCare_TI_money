"""Conexões e funções de captura de dados externos utilizados pelo pipeline."""

from __future__ import annotations

import json
import logging
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Any, Union
from pyspark.sql import DataFrame, SparkSession

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Adiciona um handler para o console com formatação detalhada
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Usa a sessão Spark existente do ambiente DLT
try:
    spark = SparkSession.active_builder.getOrCreate()
    logger.info("Sessão Spark obtida com sucesso do ambiente DLT")
except Exception as e:
    logger.error(f"Erro ao obter sessão Spark: {str(e)}")
    raise


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
    
    logger.info(f"Iniciando busca de dados para {len(list(tickers))} tickers")
    logger.info(f"Versão do yfinance: {yf.__version__}")
    logger.info(f"Versão do requests: {requests.__version__}")
    
    try:
        inicio_fmt = _converter_data(inicio)
        fim_fmt = _converter_data(fim)
        logger.info(f"Período convertido: {inicio_fmt} até {fim_fmt}")
    except ValueError as e:
        logger.error(f"Erro ao converter datas: {str(e)}")
        raise

    # Configuração da sessão global
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    })
    
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
            logger.info(f"=== Iniciando processamento para {ticker} ===")
            # Remove sufixo .SA se já estiver presente
            ticker_base = ticker.replace('.SA', '')
            ticker_sa = f"{ticker_base}.SA"
            logger.info(f"Ticker formatado: {ticker_sa}")
            
            try:
                # Cria o ticker com a sessão global
                logger.info(f"Criando objeto Ticker para {ticker_sa}")
                acao = yf.Ticker(ticker_sa, session=session)
                logger.debug(f"Objeto Ticker criado com sucesso: {acao}")
                
                # Log dos detalhes da sessão
                logger.info(f"Headers da sessão para {ticker_sa}: {session.headers}")
                
                # Tenta obter dados históricos usando o objeto Ticker diretamente
                logger.info(f"Iniciando download de dados históricos para {ticker_sa}")
                logger.info(f"Parâmetros: start={inicio_fmt}, end={fim_fmt}, interval=1d")
                
                historico = acao.history(
                    start=inicio_fmt,
                    end=fim_fmt,
                    interval="1d",
                    auto_adjust=True,
                    actions=True,
                    timeout=30
                )
                
                logger.info(f"Resposta recebida para {ticker_sa}")
                logger.debug(f"Tipo de retorno: {type(historico)}")
                logger.debug(f"Colunas disponíveis: {historico.columns.tolist() if not historico.empty else 'Nenhuma'}")
                
            except Exception as e:
                logger.error(f"Erro ao criar/usar objeto Ticker para {ticker_sa}", exc_info=True)
                logger.error(f"Detalhes adicionais do erro: {str(e)}")
                raise
            
            # Verifica se há dados
            if historico.empty:
                logger.warning(f"Nenhum dado encontrado para {ticker_sa}")
                logger.debug("Verificando se o objeto história foi inicializado corretamente")
                continue
                
            logger.info(f"Dados encontrados para {ticker_sa}: {len(historico)} registros")
            logger.debug(f"Primeira data disponível: {historico.index.min() if not historico.empty else 'N/A'}")
            logger.debug(f"Última data disponível: {historico.index.max() if not historico.empty else 'N/A'}")
            
            if not historico.empty:
                logger.info(f"Iniciando processamento dos dados para {ticker_sa}")
                try:
                    historico = historico.reset_index()
                    historico["ticker"] = ticker_base.upper()
                    
                    # Garante que todas as colunas necessárias existem
                    logger.debug(f"Verificando e preenchendo colunas necessárias para {ticker_sa}")
                    colunas_esperadas = ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits']
                    for col in colunas_esperadas:
                        if col not in historico.columns:
                            logger.warning(f"Coluna {col} não encontrada para {ticker_sa}. Preenchendo com zeros.")
                            historico[col] = 0.0
                    
                    # Converte tipos de dados
                    logger.debug(f"Convertendo tipos de dados para {ticker_sa}")
                    historico['Date'] = pd.to_datetime(historico['Date'])
                    for col in colunas_esperadas:
                        logger.debug(f"Convertendo coluna {col} para numérico")
                        historico[col] = pd.to_numeric(historico[col], errors='coerce')
                        nulos = historico[col].isnull().sum()
                        if nulos > 0:
                            logger.warning(f"{nulos} valores nulos encontrados na coluna {col} para {ticker_sa}")
                        historico[col] = historico[col].fillna(0.0)
                    
                    quadros.append(historico)
                    logger.info(f"Processamento concluído com sucesso para {ticker_sa}")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar dados de {ticker_sa}", exc_info=True)
                    raise
            
        except Exception as e:
            erro_msg = f"Erro ao processar {ticker}: {str(e)}"
            logger.error(erro_msg, exc_info=True)
            logger.error(f"Stack trace completo para {ticker}:", exc_info=True)
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