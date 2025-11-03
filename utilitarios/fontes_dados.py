"""Conexões e funções de captura de dados externos utilizados pelo pipeline."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf


try:  # pragma: no cover - dependência opcional em workspaces Databricks
    import sgs  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - tratamos o fallback manualmente
    sgs = None


def buscar_historico_b3(tickers: Iterable[str], inicio: str, fim: str) -> pd.DataFrame:
    """Baixa o histórico de preços dos tickers configurados no Yahoo Finance."""

    quadros: List[pd.DataFrame] = []
    for ticker in tickers:
        ticker_sa = f"{ticker}.SA"
        historico = yf.Ticker(ticker_sa).history(start=inicio, end=fim)
        if historico.empty:
            continue
        historico = historico.reset_index()
        historico["ticker"] = ticker.upper()
        quadros.append(historico)
    if not quadros:
        return pd.DataFrame(
            columns=[
                "Date",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Dividends",
                "Stock Splits",
                "ticker",
            ]
        )
    return pd.concat(quadros, ignore_index=True)


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
    inicio_fmt = format_bacen_date(inicio)
    fim_fmt = format_bacen_date(fim)
    quadros = []
    for nome, codigo in series.items():
        quadro = _buscar_com_requests(codigo, inicio_fmt, fim_fmt)
        # Ignora respostas de erro
        if not quadro.empty and "error" in quadro.columns:
            continue
        # Normaliza datas e remove inválidas
        quadro.index = pd.to_datetime(quadro["data"], errors="coerce")
        quadro = quadro[~quadro.index.isna()]
        quadro["serie"] = nome
        quadros.append(quadro)
    if not quadros:
        indice_vazio = pd.DatetimeIndex([], name="data")
        return pd.DataFrame(columns=["valor", "serie"], index=indice_vazio)
    return pd.concat(quadros).sort_index()


__all__ = [
    "buscar_historico_b3",
    "buscar_series_bacen",
]