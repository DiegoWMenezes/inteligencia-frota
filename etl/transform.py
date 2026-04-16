"""
ETL - Transformação dos dados brutos.
Etapas:
1. Limpeza (tratar nulos, duplicatas, tipos)
2. Padronização (nomes de colunas, formatos)
3. Enriquecimento (JOIN dados reais: frota + combustível + IBGE por estado)
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

# Paths
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "staging")


# =============================================================================
# 1. LIMPEZA
# =============================================================================

def clean_anp_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza dados de combustível da ANP."""
    df = df.copy()

    # Renomear colunas para padrão
    rename_map = {
        "Regiao - Sigla": "regiao",
        "Estado - Sigla": "estado",
        "Municipio": "municipio",
        "Produto": "produto",
        "Data da Coleta": "data_coleta",
        "Valor de Venda": "valor_venda",
        "Valor de Compra": "valor_compra",
        "Bandeira": "bandeira",
        "Unidade de Medida": "unidade_medida",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Remover duplicatas
    df = df.drop_duplicates()

    # Tratar nulos
    df["valor_venda"] = pd.to_numeric(df["valor_venda"], errors="coerce")
    df["valor_compra"] = pd.to_numeric(df["valor_compra"], errors="coerce")
    df = df.dropna(subset=["valor_venda"])

    # Padronizar texto
    df["estado"] = df["estado"].str.upper().str.strip()
    df["produto"] = df["produto"].str.upper().str.strip()
    df["regiao"] = df["regiao"].str.upper().str.strip()

    # Converter data
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], format="%d/%m/%Y", errors="coerce")
    df["ano"] = df["data_coleta"].dt.year
    df["mes"] = df["data_coleta"].dt.month

    print(f"[ETL-LIMPEZA] ANP: {len(df)} registros limpos")
    return df


def clean_ibge_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza dados do IBGE."""
    df = df.copy()

    df = df.drop_duplicates()
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["valor"])

    # Padronizar estado
    if "estado" in df.columns:
        df["estado"] = df["estado"].str.upper().str.strip()

    # Padronizar indicador
    df["indicador"] = df["indicador"].str.upper().str.strip()

    # Garantir que periodo é string
    df["periodo"] = df["periodo"].astype(str)

    print(f"[ETL-LIMPEZA] IBGE: {len(df)} registros limpos")
    return df


def clean_denatran_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza dados de frota do DENATRAN."""
    df = df.copy()

    df = df.drop_duplicates()
    df["estado"] = df["estado"].str.upper().str.strip()
    df["tipo_veiculo"] = df["tipo_veiculo"].str.upper().str.strip()
    df["combustivel"] = df["combustivel"].str.upper().str.strip()
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")
    df = df.dropna(subset=["quantidade"])

    print(f"[ETL-LIMPEZA] DENATRAN: {len(df)} registros limpos")
    return df


def clean_fleet_data(df_veiculos: pd.DataFrame, df_operacoes: pd.DataFrame):
    """Limpa dados operacionais de frota."""
    df_veiculos = df_veiculos.copy()
    df_operacoes = df_operacoes.copy()

    # Veículos
    df_veiculos = df_veiculos.drop_duplicates(subset=["veiculo_id"])
    df_veiculos["estado"] = df_veiculos["estado"].str.upper().str.strip()
    df_veiculos["tipo_veiculo"] = df_veiculos["tipo_veiculo"].str.strip()
    df_veiculos["marca"] = df_veiculos["marca"].str.strip()
    df_veiculos["combustivel"] = df_veiculos["combustivel"].str.strip()
    df_veiculos["km_atual"] = pd.to_numeric(df_veiculos["km_atual"], errors="coerce")
    df_veiculos["consumo_medio_km_l"] = pd.to_numeric(df_veiculos["consumo_medio_km_l"], errors="coerce")

    # Operações
    df_operacoes = df_operacoes.drop_duplicates()
    for col in ["custo_combustivel", "custo_manutencao", "custo_seguro",
                "custo_ipva", "custo_depreciacao", "receita_estimada"]:
        df_operacoes[col] = pd.to_numeric(df_operacoes[col], errors="coerce")
    df_operacoes = df_operacoes.fillna(0)
    df_operacoes["estado"] = df_operacoes["estado"].str.upper().str.strip()

    print(f"[ETL-LIMPEZA] Frota: {len(df_veiculos)} veículos, {len(df_operacoes)} operações")
    return df_veiculos, df_operacoes


# =============================================================================
# 2. PADRONIZAÇÃO
# =============================================================================

def standardize_anp_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados ANP por estado/mês/produto (preço médio)."""
    df_agg = (
        df.groupby(["estado", "ano", "mes", "produto"])
        .agg(
            preco_medio=("valor_venda", "mean"),
            preco_min=("valor_venda", "min"),
            preco_max=("valor_venda", "max"),
            num_registros=("valor_venda", "count"),
        )
        .reset_index()
    )
    df_agg["preco_medio"] = df_agg["preco_medio"].round(2)

    # Pivotar para ter colunas por tipo de combustível
    df_pivot = df_agg.pivot_table(
        index=["estado", "ano", "mes"],
        columns="produto",
        values="preco_medio",
    ).reset_index()
    df_pivot.columns.name = None

    # Renomear colunas de combustível
    rename = {}
    for col in df_pivot.columns:
        if col not in ["estado", "ano", "mes"]:
            rename[col] = f"preco_{col.lower().replace(' ', '_').replace('10', '_s10')}"

    df_pivot = df_pivot.rename(columns=rename)

    print(f"[ETL-PADRONIZAÇÃO] ANP mensal: {len(df_pivot)} registros")
    return df_pivot


def standardize_ibge_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma dados IBGE para formato wide (indicadores como colunas)."""
    if "estado" not in df.columns:
        print("[ETL-PADRONIZAÇÃO] IBGE sem desagregação por estado - usando dados nacionais")
        df_pivot = df.pivot_table(
            index=["periodo"],
            columns="indicador",
            values="valor",
        ).reset_index()
        df_pivot.columns.name = None
        return df_pivot

    df_pivot = df.pivot_table(
        index=["estado", "periodo"],
        columns="indicador",
        values="valor",
    ).reset_index()
    df_pivot.columns.name = None

    df_pivot = df_pivot.rename(columns={"periodo": "ano"})

    print(f"[ETL-PADRONIZAÇÃO] IBGE wide: {len(df_pivot)} registros")
    return df_pivot


def standardize_denatran_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados DENATRAN por estado/ano."""
    df_agg = (
        df.groupby(["estado", "ano"])
        .agg(
            frota_total=("quantidade", "sum"),
            num_tipos=("tipo_veiculo", "nunique"),
        )
        .reset_index()
    )

    # Frota por tipo principal para frota operacional
    tipos_operacionais = ["AUTOMOVEL", "CAMINHAO", "CAMINHONETE", "CAMIONETA",
                          "MICROONIBUS", "ONIBUS", "UTILITARIO"]
    df_operacional = df[df["tipo_veiculo"].isin(tipos_operacionais)]
    df_op_agg = (
        df_operacional.groupby(["estado", "ano"])
        .agg(frota_operacional=("quantidade", "sum"))
        .reset_index()
    )

    df_final = df_agg.merge(df_op_agg, on=["estado", "ano"], how="left")
    df_final["frota_operacional"] = df_final["frota_operacional"].fillna(0).astype(int)

    print(f"[ETL-PADRONIZAÇÃO] DENATRAN resumo: {len(df_final)} registros")
    return df_final


# =============================================================================
# 3. ENRIQUECIMENTO
# =============================================================================

def enrich_fleet_data(
    df_operacoes: pd.DataFrame,
    df_veiculos: pd.DataFrame,
    df_combustivel: pd.DataFrame,
    df_ibge: pd.DataFrame,
    df_denatran: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enriquecimento principal: JOIN dados reais.
    df_frota.merge(df_combustivel, on="estado")
    """
    # Merge veículos com operações
    df_enriched = df_operacoes.merge(
        df_veiculos[["veiculo_id", "marca", "ano_fabricacao", "km_atual",
                      "status", "consumo_medio_km_l", "capacidade_carga_t"]],
        on="veiculo_id",
        how="left",
    )

    # Merge com preços de combustível por estado/mês
    df_enriched = df_enriched.merge(
        df_combustivel,
        on=["estado", "ano", "mes"],
        how="left",
    )

    # Merge com indicadores IBGE por estado/ano
    df_ibge_year = df_ibge.copy()
    if "ano" not in df_ibge_year.columns and "periodo" in df_ibge_year.columns:
        df_ibge_year["ano"] = df_ibge_year["periodo"].astype(str).str[:4].astype(int)
    elif "ano" in df_ibge_year.columns:
        df_ibge_year["ano"] = df_ibge_year["ano"].astype(int)

    # Garantir tipos compatíveis para merge
    df_enriched["ano"] = df_enriched["ano"].astype(int)
    df_enriched["mes"] = df_enriched["mes"].astype(int)

    if "estado" in df_ibge_year.columns:
        ibge_cols = [c for c in df_ibge_year.columns if c not in ["periodo"]]
        df_enriched = df_enriched.merge(
            df_ibge_year[ibge_cols],
            on=["estado", "ano"],
            how="left",
        )

    # Merge com dados DENATRAN por estado/ano
    df_enriched = df_enriched.merge(
        df_denatran[["estado", "ano", "frota_total", "frota_operacional"]],
        on=["estado", "ano"],
        how="left",
    )

    # Calcular KPIs derivados
    df_enriched["custo_total"] = (
        df_enriched["custo_combustivel"]
        + df_enriched["custo_manutencao"]
        + df_enriched["custo_seguro"]
        + df_enriched["custo_ipva"]
        + df_enriched["custo_depreciacao"]
    )

    df_enriched["custo_por_km"] = np.where(
        df_enriched["km_rodado"] > 0,
        df_enriched["custo_total"] / df_enriched["km_rodado"],
        0,
    )

    df_enriched["margem_operacional"] = np.where(
        df_enriched["receita_estimada"] > 0,
        (df_enriched["receita_estimada"] - df_enriched["custo_total"]) / df_enriched["receita_estimada"],
        0,
    )

    df_enriched["km_por_litro_real"] = np.where(
        df_enriched["litros_consumidos"] > 0,
        df_enriched["km_rodado"] / df_enriched["litros_consumidos"],
        0,
    )

    # Idade do veículo
    df_enriched["idade_veiculo"] = df_enriched["ano"] - df_enriched["ano_fabricacao"]

    print(f"[ETL-ENRIQUECIMENTO] Dados enriquecidos: {len(df_enriched)} registros")
    print(f"[ETL-ENRIQUECIMENTO] Colunas: {df_enriched.columns.tolist()}")
    return df_enriched


# =============================================================================
# PIPELINE COMPLETO
# =============================================================================

def run_etl_pipeline() -> dict:
    """
    Executa o pipeline ETL completo:
    ingestão -> limpeza -> padronização -> enriquecimento
    """
    os.makedirs(STAGING_DIR, exist_ok=True)

    print("=" * 60)
    print("INICIANDO PIPELINE ETL")
    print("=" * 60)

    # Carregar dados brutos
    print("\n[1/5] Carregando dados brutos...")
    from ingestion.anp_fuel import load_anp_data
    from ingestion.ibge_api import load_ibge_data
    from ingestion.denatran_loader import load_denatran_data
    from ingestion.fleet_generator import load_fleet_data

    df_anp = load_anp_data()
    df_ibge = load_ibge_data()
    df_denatran = load_denatran_data()
    df_veiculos, df_operacoes = load_fleet_data()

    # Limpeza
    print("\n[2/5] Limpando dados...")
    df_anp_clean = clean_anp_data(df_anp)
    df_ibge_clean = clean_ibge_data(df_ibge)
    df_denatran_clean = clean_denatran_data(df_denatran)
    df_veiculos_clean, df_operacoes_clean = clean_fleet_data(df_veiculos, df_operacoes)

    # Padronização
    print("\n[3/5] Padronizando dados...")
    df_anp_std = standardize_anp_monthly(df_anp_clean)
    df_ibge_std = standardize_ibge_wide(df_ibge_clean)
    df_denatran_std = standardize_denatran_summary(df_denatran_clean)

    # Enriquecimento
    print("\n[4/5] Enriquecendo dados...")
    df_enriched = enrich_fleet_data(
        df_operacoes_clean, df_veiculos_clean,
        df_anp_std, df_ibge_std, df_denatran_std,
    )

    # Salvar staging
    print("\n[5/5] Salvando dados em staging...")
    timestamp = datetime.now().strftime("%Y%m%d")

    paths = {}
    for name, df in [
        ("anp_mensal", df_anp_std),
        ("ibge_wide", df_ibge_std),
        ("denatran_resumo", df_denatran_std),
        ("veiculos_clean", df_veiculos_clean),
        ("frota_enriquecida", df_enriched),
    ]:
        path = os.path.join(STAGING_DIR, f"{name}_{timestamp}.csv")
        df.to_csv(path, index=False, encoding="utf-8")
        paths[name] = path
        print(f"  -> {name}: {len(df)} registros -> {path}")

    print("\n" + "=" * 60)
    print("PIPELINE ETL CONCLUÍDO")
    print("=" * 60)

    return paths


if __name__ == "__main__":
    # Adicionar raiz do projeto ao path
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    paths = run_etl_pipeline()