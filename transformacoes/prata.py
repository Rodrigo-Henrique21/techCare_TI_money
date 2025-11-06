"""Camada Prata do pipeline Delta Live Tables."""

from __future__ import annotations

import dlt
from pyspark.sql import DataFrame, functions as F

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    obter_nome_tabela,
    obter_metadados_tabela,
)


NOME_BRONZE_COTACOES = obter_nome_tabela("bronze", "cotacoes_b3")
NOME_BRONZE_BACEN = obter_nome_tabela("bronze", "series_bacen")

NOME_PRATA_COTACOES = obter_nome_tabela("prata", "tb_mkt_eqt_day")
NOME_PRATA_BACEN = obter_nome_tabela("prata", "tb_mkt_idx_eco")


def normalizar_colunas(df: DataFrame) -> DataFrame:
    """Padroniza o nome das colunas para minúsculo com underscores."""

    return df.select([F.col(coluna).alias(coluna.lower().replace(" ", "_")) for coluna in df.columns])


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_eqt_day"),
    comment=obter_metadados_tabela("prata", "tb_mkt_eqt_day")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"]
)
@dlt.expect_all({
    "valid_close": "close_price IS NOT NULL",
    "valid_date": "trade_date IS NOT NULL",
})
def prata_cotacoes_b3() -> DataFrame:
    """Normaliza e valida as cotações de equity coletadas na camada Bronze."""

    df = dlt.read(NOME_BRONZE_COTACOES)
    df_norm = normalizar_colunas(df)
    return df_norm.select(
        F.col("date").alias("trade_date"),
        F.col("open").alias("open_price"),
        F.col("high").alias("high_price"),
        F.col("low").alias("low_price"),
        F.col("close").alias("close_price"),
        "volume",
        F.col("dividends").alias("div_cash"),
        F.col("stock_splits").alias("split_ratio"),
        F.col("ticker").alias("symbol"),
        "ingestion_timestamp",
    )


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_idx_eco"),
    comment=obter_metadados_tabela("prata", "tb_mkt_idx_eco")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"]
)
def prata_series_bacen() -> DataFrame:
    """
    Trata as séries do BACEN garantindo consistência de tipos e nomenclatura.
    
    O campo 'frequency' indica a periodicidade do indicador econômico:
    - D: Diário (ex: Taxa Selic diária, CDI)
    - M: Mensal (ex: IPCA, IGP-M)
    - A: Anual (ex: PIB)
    
    As colunas são normalizadas para:
    - ref_date: Data de referência do indicador
    - idx_value: Valor do indicador como número decimal
    - idx_type: Identificador da série (ex: selic, ipca)
    - frequency: Frequência/periodicidade do indicador
    - ingestion_timestamp: Data e hora da ingestão do dado
    """

    df = dlt.read(NOME_BRONZE_BACEN)
    df_norm = normalizar_colunas(df)
    return df_norm.select(
        F.col("data").alias("ref_date"),
        F.col("valor").cast("double").alias("idx_value"),
        F.col("serie").alias("idx_type"),
        F.lit("D").alias("frequency"),  # D = Diário
        "ingestion_timestamp",
    )


__all__ = [
    "normalizar_colunas",
    "prata_cotacoes_b3",
    "prata_series_bacen",
]