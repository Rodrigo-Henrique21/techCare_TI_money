"""Configurações, nomenclaturas e utilitários básicos do pipeline DLT."""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# Definição dos esquemas para cada camada
ESQUEMAS = {
    "bronze": "aafn_ing",  # Ingestão - Dados brutos
    "prata": "aafn_tgt",   # Target - Dados normalizados
    "ouro": "aafn_ddm"     # Data Mart - Dados analíticos
}

# Qualidade e propriedades por camada
PROPRIEDADES_TABELAS = {
    "bronze": {
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "pipelines.trigger.interval": "12 hours",  # Atualização a cada 12 horas
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    },
    "prata": {
        "quality": "silver",
        "pipelines.reset.allowed": "false",
        "pipelines.trigger.interval": "12 hours",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    },
    "ouro": {
        "quality": "gold",
        "pipelines.reset.allowed": "false",
        "pipelines.trigger.interval": "12 hours",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
}

# Estrutura das tabelas por camada e domínio
DEFINICAO_TABELAS = {
    "bronze": {
        # Dados brutos - Camada de Ingestão (ing)
        "cotacoes_b3": {
            "descricao": "Cotações brutas da B3 (Yahoo Finance)",
            "alias": "Cotações B3",
            "colunas_esperadas": ["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits", "ticker"]
        },
        "series_bacen": {
            "descricao": "Séries temporais do BACEN (SGS)",
            "alias": "Indicadores BACEN",
            "colunas_esperadas": ["data", "valor", "serie"]
        }
    },
    "prata": {
        # Dados normalizados - Camada de Transformação (tgt)
        "tb_mkt_eqt_day": {
            "descricao": "Série histórica diária de equity",
            "alias": "Equity Diário",
            "fonte": "tb_fin_cot_b3",
            "colunas_esperadas": ["trade_date", "open_price", "high_price", "low_price", "close_price", "volume", "div_cash", "split_ratio", "symbol"]
        },
        "tb_mkt_idx_eco": {
            "descricao": "Indicadores econômicos padronizados",
            "alias": "Índices Econômicos",
            "fonte": "tb_fin_ind_bc",
            "colunas_esperadas": ["ref_date", "idx_value", "idx_type", "frequency"]
        }
    },
    "ouro": {
        # Dados analíticos - Camada de Data Mart (ddm)
        "tb_mkt_eqt_perf": {
            "descricao": "Análise de performance de equity",
            "alias": "Performance de Ativos",
            "fonte": "tb_mkt_eqt_day",
            "colunas_esperadas": ["symbol", "monthly_return", "volatility", "avg_price", "avg_volume", "last_update"]
        },
        "tb_mkt_idx_dash": {
            "descricao": "Dashboard de indicadores macroeconômicos",
            "alias": "Dashboard Macro",
            "fonte": "tb_mkt_idx_eco",
            "colunas_esperadas": ["idx_type", "current_value", "mtd_change", "ytd_change", "trend_6m", "last_update"]
        }
    }
}

# Cache de configurações para ambiente local
_config_local: Dict[str, str] = {}


def obter_configuracao(chave: str, padrao: Optional[str] = None) -> Optional[str]:
    """
    Obtém parâmetros de configuração na seguinte ordem de prioridade:
    1. Variável de ambiente
    2. Cache local
    3. Valor padrão
    """
    # Tenta variável de ambiente primeiro (converte chave.nested para CHAVE_NESTED)
    env_key = chave.replace(".", "_").upper()
    valor = os.environ.get(env_key)
    
    # Se não encontrou na env, tenta no cache local
    if valor is None:
        valor = _config_local.get(chave)
        
    # Retorna valor encontrado ou padrão
    return valor if valor is not None else padrao

def obter_lista_configuracoes(chave: str, padrao: str = "") -> List[str]:
    """Converte valores de configuração separados por vírgula em uma lista sanitizada."""
    valor = obter_configuracao(chave, padrao) or ""
    return [item.strip() for item in valor.split(",") if item.strip()]

def definir_configuracao_local(chave: str, valor: str) -> None:
    """Define uma configuração no cache local (útil para desenvolvimento e testes)."""
    _config_local[chave] = valor

def obter_nome_tabela(camada: str, nome_base: str, incluir_esquema: bool = True) -> str:
    """
    Retorna o nome completo da tabela para a camada especificada.
    
    Args:
        camada: Nome da camada ('bronze', 'prata' ou 'ouro')
        nome_base: Nome da tabela de referência
        incluir_esquema: Se True, retorna o nome completo com esquema (ex: aafn_ing.tabela)
    
    Returns:
        Nome da tabela, opcionalmente prefixado com o esquema
    """
    # Seleciona o mapeamento correto baseado na camada
    if camada == "bronze":
        mapeamento = TABELAS_BRONZE
    elif camada == "prata":
        mapeamento = TABELAS_PRATA
    elif camada == "ouro":
        mapeamento = TABELAS_OURO
    else:
        raise ValueError(f"Camada inválida: {camada}. Use: bronze, prata ou ouro")
    
    # Verifica se a tabela existe no mapeamento
    if nome_base not in mapeamento:
        raise ValueError(
            f"Tabela '{nome_base}' não encontrada na camada {camada}. "
            f"Tabelas disponíveis: {list(mapeamento.keys())}"
        )
    
    # Obtém o nome real da tabela do mapeamento
    nome_tabela = mapeamento[nome_base]
    
    # Retorna com ou sem esquema
    if incluir_esquema:
        return f"{ESQUEMAS[camada]}.{nome_tabela}"
    return nome_tabela

def obter_metadados_tabela(camada: str, nome_base: str) -> Dict[str, Any]:
    """Retorna os metadados de uma tabela específica."""
    if camada not in DEFINICAO_TABELAS:
        raise ValueError(f"Camada inválida: {camada}")
        
    if nome_base not in DEFINICAO_TABELAS[camada]:
        raise ValueError(f"Tabela '{nome_base}' não encontrada na camada {camada}")
        
    return DEFINICAO_TABELAS[camada][nome_base]


# Aliases para compatibilidade com código legado
TABELAS_BRONZE = {
    "cotacoes_b3": "cotacoes_b3",     # Cotações B3
    "series_bacen": "series_bacen"    # Indicadores BACEN
}

TABELAS_PRATA = {
    "cotacoes_b3": "tb_mkt_eqt_day",     # Equity Diário
    "series_bacen": "tb_mkt_idx_eco"      # Índices Econômicos
}

TABELAS_OURO = {
    "metricas_b3": "tb_mkt_eqt_perf",    # Performance de Ativos
    "indicadores_bacen": "tb_mkt_idx_dash" # Dashboard Macro
}

def timestamp_ingestao() -> datetime:
    """Retorna o instante UTC da captura de dados para rastreabilidade."""
    return datetime.utcnow()

# Exporta apenas o necessário
__all__ = [
    "DEFINICAO_TABELAS",
    "ESQUEMAS",
    "PROPRIEDADES_TABELAS",
    "TABELAS_BRONZE",
    "TABELAS_PRATA",
    "TABELAS_OURO",
    "obter_configuracao",
    "obter_lista_configuracoes",
    "definir_configuracao_local",
    "obter_nome_tabela",
    "obter_metadados_tabela",
    "timestamp_ingestao",
]