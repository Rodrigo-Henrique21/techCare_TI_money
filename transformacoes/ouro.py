"""Camada Ouro do pipeline Delta Live Tables."""

from __future__ import annotations

import dlt
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    obter_nome_tabela,
    obter_metadados_tabela,
)


NOME_PRATA_COTACOES = obter_nome_tabela("prata", "tb_mkt_eqt_day")
NOME_PRATA_SERIES = obter_nome_tabela("prata", "tb_mkt_idx_eco")


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_eqt_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_eqt_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"]
)
def ouro_metricas_b3() -> DataFrame:
    cotacoes = dlt.read(NOME_PRATA_COTACOES)
    janela = Window.partitionBy("symbol").orderBy("trade_date")
    retornos = cotacoes.withColumn("retorno", F.col("close_price") / F.lag("close_price").over(janela) - 1)
    mensais = retornos.withColumn("ano_mes", F.date_format("trade_date", "yyyy-MM"))
    mensais = mensais.groupBy("symbol", "ano_mes").agg(
        F.mean("retorno").alias("monthly_return")
    )
    metricas = mensais.groupBy("symbol").agg(
        F.mean("monthly_return").alias("avg_return"),
        F.stddev("monthly_return").alias("volatility"),
    )
    estatisticas = cotacoes.groupBy("symbol").agg(
        F.mean("close_price").alias("avg_price"),
        F.mean("volume").alias("avg_volume"),
        F.max("trade_date").alias("last_update"),
    )
    return metricas.join(estatisticas, "symbol", "inner")


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_dash"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_dash")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"]
)
def ouro_indicadores_bacen() -> DataFrame:
    """
    Processa o histórico mensal dos indicadores econômicos do BACEN.
    Agrupa os dados por indicador e mês, calculando estatísticas mensais.
    """
    series = dlt.read(NOME_PRATA_SERIES)
    
    # Agrupa por indicador e mês
    indicadores_mensais = (
        series
        .withColumn("ano", F.year("ref_date"))
        .withColumn("mes", F.month("ref_date"))
        .withColumn("ano_mes", F.date_format("ref_date", "yyyy-MM"))
        .groupBy("idx_type", "ano", "mes", "ano_mes")
        .agg(
            F.avg("idx_value").alias("media_mensal"),
            F.min("idx_value").alias("minimo_mensal"),
            F.max("idx_value").alias("maximo_mensal"),
            F.first("ref_date").alias("primeira_data_mes"),
            F.last("ref_date").alias("ultima_data_mes"),
            F.count("*").alias("num_medicoes")
        )
    )
    # Calcula variações mensais (MTD - Month to Date)
    primeiro_dia_mes = F.trunc(F.current_date(), "MM")
    mtd = (
        series
        .filter(F.col("ref_date") >= primeiro_dia_mes)
        .groupBy("idx_type")
        .agg(
            F.first("idx_value").alias("mtd_first"),
            F.last("idx_value").alias("mtd_last")
        )
        .withColumn("mtd_change", (F.col("mtd_last") / F.col("mtd_first") - 1) * 100)
    )
    
    # Adiciona variação em relação ao mês anterior
    janela_mes_anterior = Window.partitionBy("idx_type").orderBy("ano", "mes")
    
    resultado_final = (
        indicadores_mensais
        .withColumn(
            "media_mes_anterior",
            F.lag("media_mensal").over(janela_mes_anterior)
        )
        .withColumn(
            "variacao_mes_anterior",
            F.when(
                F.col("media_mes_anterior").isNotNull(),
                (F.col("media_mensal") / F.col("media_mes_anterior") - 1) * 100
            ).otherwise(None)
        )
        .orderBy("idx_type", "ano", "mes")
    )
    
    # Adiciona data da última atualização
    resultado_final = resultado_final.withColumn(
        "last_update",
        F.max("ultima_data_mes").over(Window.partitionBy("idx_type"))
    )
    
    # Analisa tendência dos últimos 6 meses
    seis_meses_atras = F.add_months(F.current_date(), -6)
    tendencia = (
        series
        .filter(F.col("ref_date") >= seis_meses_atras)
        .groupBy("idx_type")
        .agg(F.regr_slope("idx_value", F.unix_timestamp("ref_date")).alias("trend_6m"))
    )
    
    # Integra todas as análises no resultado final
    return (resultado_final
        .join(tendencia, "idx_type", "left")
        .select(
            "idx_type",
            "ano",
            "mes",
            "ano_mes",
            "media_mensal",
            "minimo_mensal",
            "maximo_mensal",
            "variacao_mes_anterior",
            "primeira_data_mes",
            "ultima_data_mes",
            "num_medicoes",
            "last_update",
            F.coalesce("trend_6m", F.lit(0)).alias("trend_6m")  # Tendência ou 0 se não houver dados suficientes
        )
        .orderBy("idx_type", "ano", "mes")
    )

__all__ = [
    "ouro_indicadores_bacen",
    "ouro_metricas_b3",
]