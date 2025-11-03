"""Camada Bronze do pipeline Delta Live Tables."""

from __future__ import annotations

import json
from datetime import datetime

import dlt
from pyspark.sql import DataFrame, types as T

from utilitarios import (
    nome_tabela,
    ESQUEMAS_DESTINO,
    TABELAS_BRONZE,
    buscar_historico_b3,
    buscar_series_bacen,
    criar_dataframe_vazio,
    obter_configuracao,
    obter_lista_configuracoes,
    spark,
    timestamp_ingestao,
)


@dlt.table(
    name=nome_tabela("bronze", "cotacoes_b3"),
    comment="Cotações brutas coletadas do Yahoo Finance para os tickers configurados.",
)
def bronze_cotacoes_b3() -> DataFrame:
    """Coleta cotações históricas da B3 na API do Yahoo Finance."""

    tickers = obter_lista_configuracoes(
        "b3.tickers",
        "PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3,ELET3,B3SA3",
    )
        
    data_inicial = obter_configuracao("b3.start_date", "01/01/2020")
    data_final = obter_configuracao(
        "b3.end_date", datetime.utcnow().strftime("%d/%m/%Y")
    )

    pdf = buscar_historico_b3(tickers, data_inicial, data_final)
    pdf["ingestion_timestamp"] = timestamp_ingestao()
    schema = T.StructType(
        [
            T.StructField("Date", T.TimestampType()),
            T.StructField("Open", T.DoubleType()),
            T.StructField("High", T.DoubleType()),
            T.StructField("Low", T.DoubleType()),
            T.StructField("Close", T.DoubleType()),
            T.StructField("Volume", T.DoubleType()),
            T.StructField("Dividends", T.DoubleType()),
            T.StructField("Stock_Splits", T.DoubleType()),
            T.StructField("ticker", T.StringType()),
            T.StructField("ingestion_timestamp", T.TimestampType()),
        ]
    )
    if pdf.empty:
        return criar_dataframe_vazio(schema)
    pdf = pdf.rename(columns={"Stock Splits": "Stock_Splits"})
    return spark.createDataFrame(pdf, schema=schema)


@dlt.table(
    name=nome_tabela("bronze", "series_bacen"),
    comment="Indicadores macroeconômicos brutos consultados no serviço SGS do BACEN.",
)
def bronze_series_bacen() -> DataFrame:
    """Busca séries temporais no serviço SGS do BACEN."""

    series = json.loads(
        obter_configuracao(
            "bacen.series",
            json.dumps(
                {
                    "selic": 1178,
                    "cdi": 12,
                    "ipca": 433,
                    "poupanca": 195,
                    "igpm": 189,
                    "inpc": 188,
                    "igpdi": 190,
                    "selic_meta": 432,
                }
            ),
        )
    )
    # Pass dates as YYYY-MM-DD (no conversion)
    data_inicial = obter_configuracao("bacen.start_date", "01/01/2020")
    data_final = obter_configuracao("bacen.end_date", datetime.utcnow().strftime("%d/%m/%Y"))

    pdf = buscar_series_bacen(series, data_inicial, data_final).reset_index(drop=True)
    pdf["ingestion_timestamp"] = timestamp_ingestao()

    return spark.createDataFrame(pdf)


__all__ = [
    "bronze_cotacoes_b3",
    "bronze_series_bacen",
]