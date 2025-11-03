# """Camada Ouro do pipeline Delta Live Tables."""

# from __future__ import annotations

# import dlt
# from pyspark.sql import DataFrame, functions as F
# from pyspark.sql.window import Window

# from utilitarios import nome_tabela,ESQUEMAS_DESTINO, TABELAS_OURO, TABELAS_PRATA


# NOME_PRATA_COTACOES = nome_tabela("prata", "cotacoes_b3")
# NOME_PRATA_SERIES = nome_tabela("prata", "series_bacen")


# @dlt.table(
#     name=TABELAS_OURO["metricas_b3"],
#     comment="Métricas mensais de desempenho das ações acompanhadas na B3.",
# )
# def ouro_metricas_b3() -> DataFrame:
#     cotacoes = dlt.read(NOME_PRATA_COTACOES)
#     janela = Window.partitionBy("ticker").orderBy("date")
#     retornos = cotacoes.withColumn("retorno", F.col("close") / F.lag("close").over(janela) - 1)
#     mensais = retornos.withColumn("ano_mes", F.date_format("date", "yyyy-MM"))
#     mensais = mensais.groupBy("ticker", "ano_mes").agg(
#         F.mean("retorno").alias("retorno_mensal")
#     )
#     metricas = mensais.groupBy("ticker").agg(
#         F.mean("retorno_mensal").alias("retorno_medio"),
#         F.stddev("retorno_mensal").alias("volatilidade"),
#     )
#     estatisticas = cotacoes.groupBy("ticker").agg(
#         F.min("date").alias("primeira_data"),
#         F.max("date").alias("ultima_data"),
#         F.count("*").alias("num_pregoes"),
#         F.mean("close").alias("preco_medio"),
#         F.mean("volume").alias("volume_medio"),
#     )
#     return metricas.join(estatisticas, "ticker", "inner")


# @dlt.table(
#     name=TABELAS_OURO["indicadores_bacen"],
#     comment="Resumo diário e mensal dos indicadores econômicos consultados no BACEN.",
# )
# def ouro_indicadores_bacen() -> DataFrame:
#     series = dlt.read(nome_tabela("prata", "series_bacen"))
#     janela_recente = Window.partitionBy("serie").orderBy(F.col("data").desc())
#     recentes = (
#         series.withColumn("ordem", F.row_number().over(janela_recente))
#         .filter(F.col("ordem") == 1)
#         .select(
#             F.col("serie"),
#             F.col("data").alias("data_recente"),
#             F.col("valor").alias("valor_recente"),
#         )
#     )
#     mensais = (
#         series.withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
#         .groupBy("serie", "ano_mes")
#         .agg(
#             F.mean("valor").alias("media_mensal"),
#             F.max("valor").alias("maximo_mensal"),
#             F.min("valor").alias("minimo_mensal"),
#         )
#     )
#     ultimo_mes = (
#         mensais.withColumn(
#             "ordem_mes", F.row_number().over(
#                 Window.partitionBy("serie").orderBy(F.col("ano_mes").desc())
#             )
#         )
#         .filter(F.col("ordem_mes") == 1)
#         .select(
#             "serie",
#             F.col("ano_mes").alias("ultimo_mes_disponivel"),
#             F.col("media_mensal").alias("media_ultimo_mes"),
#         )
#     )
#     agregadas = mensais.groupBy("serie").agg(
#         F.avg("media_mensal").alias("media_historica"),
#         F.max("maximo_mensal").alias("maximo_historico"),
#         F.min("minimo_mensal").alias("minimo_historico"),
#     )
#     return (
#         recentes.join(ultimo_mes, "serie", "left")
#         .join(agregadas, "serie", "left")
#         .select(
#             "serie",
#             "data_recente",
#             "valor_recente",
#             "ultimo_mes_disponivel",
#             "media_ultimo_mes",
#             "media_historica",
#             "maximo_historico",
#             "minimo_historico",
#         )
#     )

# __all__ = [
#     "ouro_indicadores_bacen",
#     "ouro_metricas_b3",
# ]