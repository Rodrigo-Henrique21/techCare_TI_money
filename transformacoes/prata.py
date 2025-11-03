# """Camada Prata do pipeline Delta Live Tables."""

# from __future__ import annotations

# import dlt
# from pyspark.sql import DataFrame, functions as F

# from utilitarios import nome_tabela, ESQUEMAS_DESTINO, TABELAS_BRONZE, TABELAS_PRATA


# NOME_BRONZE_COTACOES = nome_tabela("bronze", "cotacoes_b3")
# NOME_BRONZE_BACEN = nome_tabela("bronze", "series_bacen")

# NOME_PRATA_COTACOES = nome_tabela("prata", "cotacoes_b3")
# NOME_PRATA_BACEN = nome_tabela("prata", "series_bacen")


# def normalizar_colunas(df: DataFrame) -> DataFrame:
#     """Padroniza o nome das colunas para minúsculo com underscores."""

#     return df.select([F.col(coluna).alias(coluna.lower().replace(" ", "_")) for coluna in df.columns])


# @dlt.table(
#     name=NOME_PRATA_COTACOES,
#     comment="Cotações da B3 tratadas com esquema padronizado e expectativas de qualidade."
# )
# @dlt.expect_all({
#     "valid_close": "close IS NOT NULL",
#     "valid_date": "date IS NOT NULL",
# })
# def prata_cotacoes_b3() -> DataFrame:
#     """Normaliza e valida as cotações coletadas na camada Bronze."""

#     df = dlt.read(NOME_BRONZE_COTACOES)
#     df_norm = normalizar_colunas(df)
#     return df_norm.select(
#         "date",
#         "open",
#         "high",
#         "low",
#         "close",
#         "volume",
#         "dividends",
#         F.col("stock_splits").alias("stock_splits"),
#         "ticker",
#         "ingestion_timestamp",
#     )


# @dlt.table(
#     name=NOME_PRATA_BACEN,
#     comment="Séries econômicas do BACEN normalizadas para consumo analítico.",
# )
# def prata_series_bacen() -> DataFrame:
#     """Trata as séries do BACEN garantindo consistência de tipos."""

#     df = dlt.read(NOME_BRONZE_BACEN)
#     df_norm = normalizar_colunas(df)
#     return df_norm.select(
#         F.col("data").alias("data"),
#         F.col("valor").cast("double").alias("valor"),
#         F.col("serie").alias("serie"),
#         "ingestion_timestamp",
#     )


# __all__ = [
#     "normalizar_colunas",
#     "prata_cotacoes_b3",
#     "prata_series_bacen",
# ]