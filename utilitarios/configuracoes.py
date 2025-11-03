"""Configurações, nomenclaturas e utilitários básicos do pipeline DLT."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, types as T

CATALOGO_PADRAO = "platfunc"
ESQUEMAS_PADRAO = {
    "bronze": "aafn_ing",
    "prata": "aafn_tgt",
    "ouro": "aafn_ddm",
}

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def obter_configuracao(chave: str, padrao: Optional[str] = None) -> Optional[str]:
    """Obtém parâmetros configurados no pipeline, caindo em valores padrão quando ausentes."""

    try:
        valor = spark.conf.get(chave)
    except Exception:
        valor = None
    return valor if valor is not None else padrao


def obter_lista_configuracoes(chave: str, padrao: str = "") -> List[str]:
    """Converte valores de configuração separados por vírgula em uma lista sanitizada."""

    valor = obter_configuracao(chave, padrao) or ""
    return [item.strip() for item in valor.split(",") if item.strip()]


def _validar_identificador(nome: str, contexto: str) -> str:
    if not nome:
        raise ValueError(f"É obrigatório informar um nome para {contexto}.")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", nome):
        raise ValueError(f"O nome '{nome}' não é válido para {contexto}.")
    return nome


def _configurar_catalogo_e_esquemas() -> Tuple[str, Dict[str, str]]:
    """Garante que o catálogo e os esquemas de destino existam e estejam ativos."""

    catalogo_configurado = _validar_identificador(
        obter_configuracao("catalogo.destino", CATALOGO_PADRAO),
        "o catálogo de destino",
    )

    esquemas: Dict[str, str] = {}
    for camada, padrao in ESQUEMAS_PADRAO.items():
        chave_config = f"esquema.{camada}"
        esquema_configurado = _validar_identificador(
            obter_configuracao(chave_config, padrao),
            f"o esquema da camada {camada}",
        )
        esquemas[camada] = esquema_configurado

    try:
        spark.sql(f"USE CATALOG `{catalogo_configurado}`")
    except Exception as erro:
        raise RuntimeError(
            "Não foi possível selecionar o catálogo configurado. "
            "Garanta que ele exista antes de executar o pipeline DLT."
        ) from erro

    try:
        namespaces = {
            linha[0]
            for linha in spark.sql(f"SHOW NAMESPACES IN `{catalogo_configurado}`").collect()
        }
    except Exception as erro:
        raise RuntimeError(
            "Não foi possível listar os esquemas do catálogo configurado. "
            "Garanta que ele exista e esteja acessível antes de executar o pipeline DLT."
        ) from erro

    for camada, esquema in esquemas.items():
        if esquema not in namespaces:
            raise RuntimeError(
                "O esquema configurado para a camada "
                f"{camada} ('{esquema}') não foi encontrado no catálogo "
                f"'{catalogo_configurado}'. Crie-o previamente antes de executar o pipeline DLT."
            )

    try:
        spark.sql(f"USE SCHEMA `{esquemas['bronze']}`")
    except Exception as erro:
        raise RuntimeError(
            "Não foi possível selecionar o esquema da camada bronze mesmo após validar sua existência. "
            "Verifique as permissões do workspace antes de executar o pipeline DLT."
        ) from erro

    return catalogo_configurado, esquemas


CATALOGO_DESTINO, ESQUEMAS_DESTINO = _configurar_catalogo_e_esquemas()


TABELAS_BRONZE = {
    "cotacoes_b3": "cotacoes_b3",
    "series_bacen": "series_bacen",
}

TABELAS_PRATA = {
    "cotacoes_b3": "cotacoes_b3",
    "series_bacen": "series_bacen",
}

TABELAS_OURO = {
    "metricas_b3": "metricas_b3",
    "indicadores_bacen": "indicadores_bacen",
}

_MAPA_TABELAS = {
    "bronze": TABELAS_BRONZE,
    "prata": TABELAS_PRATA,
    "ouro": TABELAS_OURO,
}


def nome_tabela(camada: str, nome: str) -> str:
    """Monta o identificador totalmente qualificado de uma tabela DLT."""

    try:
        alias = _MAPA_TABELAS[camada][nome]
    except KeyError as erro:
        raise KeyError(
            "Informe uma camada válida (bronze, prata ou ouro) e um nome de tabela"
        ) from erro
    return f"{CATALOGO_DESTINO}.{ESQUEMAS_DESTINO[camada]}.{alias}"


def timestamp_ingestao() -> datetime:
    """Retorna o instante UTC da captura de dados para rastreabilidade."""

    return datetime.utcnow()


def criar_dataframe_vazio(schema: T.StructType) -> DataFrame:
    """Cria um DataFrame vazio compatível com o schema fornecido."""

    return spark.createDataFrame([], schema)


__all__ = [
    "CATALOGO_DESTINO",
    "ESQUEMAS_DESTINO",
    "TABELAS_BRONZE",
    "TABELAS_PRATA",
    "TABELAS_OURO",
    "criar_dataframe_vazio",
    "nome_tabela",
    "obter_configuracao",
    "obter_lista_configuracoes",
    "spark",
    "timestamp_ingestao",
]