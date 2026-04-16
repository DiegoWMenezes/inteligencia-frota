"""
Analytics Prescritiva - Regras de decisão para a frota.
Resposta: O que fazer?
if custo_km > limite: ação = "substituir ou revisar operação"
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
DATA_DIR = os.path.join(BASE_DIR, "data")

# =============================================================================
# LIMITES E CONFIGURAÇÃO
# =============================================================================

LIMITES = {
    "custo_por_km": {
        "CRITICO": 2.50,
        "ATENCAO": 1.80,
    },
    "margem_operacional": {
        "CRITICO": 0.10,
        "ATENCAO": 0.20,
    },
    "disponibilidade_pct": {
        "CRITICO": 75.0,
        "ATENCAO": 85.0,
    },
    "km_por_litro_real": {
        "CRITICO": 3.0,
        "ATENCAO": 4.5,
    },
}

ACOES = {
    "custo_por_km_CRITICO": "Substituir veículo ou revisar operação imediatamente",
    "custo_por_km_ATENCAO": "Agendar revisão e monitorar custo semanalmente",
    "margem_operacional_CRITICO": "Rever rota e composição de custos - operação deficitária",
    "margem_operacional_ATENCAO": "Otimizar custos operacionais e negociar fornecedores",
    "disponibilidade_pct_CRITICO": "Frota com baixa disponibilidade - ampliar reserva ou terceirizar",
    "disponibilidade_pct_ATENCAO": "Intensificar manutenção preventiva",
    "km_por_litro_real_CRITICO": "Consumo anômalo - inspecionar motor e pneus imediatamente",
    "km_por_litro_real_ATENCAO": "Monitorar eficiência - verificar calibragem e hábitos de condução",
}

IDADE_RECOMENDACOES = {
    (0, 3):   "Manutenção preventiva padrão - veículo no auge",
    (4, 6):   "Acompanhar custo de manutenção - planejar reserva",
    (7, 9):   "Planejar substituição - custo tende a subir",
    (10, 14): "Avaliar substituição imediata - custo elevado de manutenção",
    (15, 99): "Substituir obrigatoriamente - veículo obsoleto e ineficiente",
}


def classificar_kpi(kpi: str, valor: float) -> tuple:
    """Classifica um KPI baseado nos limites definidos."""
    limites = LIMITES.get(kpi, {})

    if not limites:
        return "INDEFINIDO", None

    # Para disponibilidade e km/l: menor é pior
    if kpi in ["disponibilidade_pct", "km_por_litro_real", "margem_operacional"]:
        if valor < limites["CRITICO"]:
            return "CRITICO", limites["CRITICO"]
        elif valor < limites["ATENCAO"]:
            return "ATENCAO", limites["ATENCAO"]
        else:
            return "NORMAL", None
    else:  # Para custo_por_km: maior é pior
        if valor > limites["CRITICO"]:
            return "CRITICO", limites["CRITICO"]
        elif valor > limites["ATENCAO"]:
            return "ATENCAO", limites["ATENCAO"]
        else:
            return "NORMAL", None


def get_acao(kpi: str, classificacao: str) -> str:
    """Retorna a ação recomendada para o KPI e classificação."""
    key = f"{kpi}_{classificacao}"
    return ACOES.get(key, "Manter monitoramento padrão")


def get_recomendacao_idade(idade: int) -> str:
    """Retorna recomendação baseada na idade do veículo."""
    for (min_idade, max_idade), recomendacao in IDADE_RECOMENDACOES.items():
        if min_idade <= idade <= max_idade:
            return recomendacao
    return "Verificar estado geral do veículo"


def run_prescriptive_analysis(db_path: str = None) -> dict:
    """
    Executa análise prescritiva completa.
    Gera recomendações de ação para cada veículo/KPI.
    """
    from sqlalchemy import create_engine

    PG_CONFIG = {
        "host": "localhost", "port": 5432,
        "database": "inteligencia_frota",
        "user": "postgres", "password": "adm123",
    }

    url = (
        f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
        f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
    )
    sa_engine = create_engine(url)
    import psycopg2
    conn = psycopg2.connect(
        host=PG_CONFIG["host"], port=PG_CONFIG["port"],
        database=PG_CONFIG["database"],
        user=PG_CONFIG["user"], password=PG_CONFIG["password"],
    )

    print("=" * 50)
    print("ANALYTICS PRESCRITIVA - DECISÃO")
    print("=" * 50)

    # Carregar dados agregados por veículo
    query = """
    SELECT
        v.id_veiculo,
        v.tipo_veiculo,
        v.marca,
        v.ano_fabricacao,
        v.status,
        v.combustivel,
        e.sigla_estado,
        ROUND(AVG(f.custo_por_km), 4)       AS custo_por_km,
        ROUND(AVG(f.margem_operacional), 4)  AS margem_operacional,
        ROUND(AVG(f.disponibilidade_pct), 1) AS disponibilidade_pct,
        ROUND(AVG(f.km_por_litro_real), 2)   AS km_por_litro_real,
        f.idade_veiculo,
        SUM(f.custo_total)                    AS custo_total,
        SUM(f.receita_estimada)               AS receita_total
    FROM fato_frota f
    JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
    JOIN dim_estado e ON f.sk_estado = e.sk_estado
    GROUP BY v.id_veiculo, v.tipo_veiculo, v.marca, v.ano_fabricacao,
             v.status, v.combustivel, e.sigla_estado, f.idade_veiculo
    """

    df = pd.read_sql(query, sa_engine)
    conn.close()

    if df.empty:
        print("[PRESCRITIVA] Sem dados para análise.")
        return {}

    print(f"[PRESCRITIVA] Analisando {len(df)} veículos...")

    # 1. Classificar cada veículo por KPI
    alertas = []
    for _, row in df.iterrows():
        veiculo_id = row["id_veiculo"]

        # Avaliar cada KPI
        kpis_veiculo = {
            "custo_por_km": row.get("custo_por_km", 0),
            "margem_operacional": row.get("margem_operacional", 1),
            "disponibilidade_pct": row.get("disponibilidade_pct", 100),
            "km_por_litro_real": row.get("km_por_litro_real", 10),
        }

        for kpi, valor in kpis_veiculo.items():
            classificacao, limite = classificar_kpi(kpi, valor)
            if classificacao != "NORMAL":
                acao = get_acao(kpi, classificacao)
                alertas.append({
                    "veiculo_id": veiculo_id,
                    "tipo_veiculo": row["tipo_veiculo"],
                    "marca": row["marca"],
                    "estado": row["sigla_estado"],
                    "kpi": kpi,
                    "valor_atual": round(valor, 4),
                    "limite": limite,
                    "classificacao": classificacao,
                    "acao_recomendada": acao,
                })

    df_alertas = pd.DataFrame(alertas)

    # 2. Recomendações por idade
    df["recomendacao_idade"] = df["idade_veiculo"].apply(get_recomendacao_idade)

    # 3. Decisão consolidada por veículo
    decisoes = []
    for _, row in df.iterrows():
        veiculo_alertas = df_alertas[df_alertas["veiculo_id"] == row["id_veiculo"]]

        tem_critico = len(veiculo_alertas[veiculo_alertas["classificacao"] == "CRITICO"]) > 0
        tem_atencao = len(veiculo_alertas[veiculo_alertas["classificacao"] == "ATENCAO"]) > 0

        if tem_critico:
            decisao = "INTERVENCAO IMEDIATA"
            prioridade = 1
        elif tem_atencao:
            decisao = "MONITORAR E PLANEJAR"
            prioridade = 2
        else:
            decisao = "OPERACAO NORMAL"
            prioridade = 3

        decisoes.append({
            "veiculo_id": row["id_veiculo"],
            "tipo_veiculo": row["tipo_veiculo"],
            "marca": row["marca"],
            "estado": row["sigla_estado"],
            "custo_por_km": row.get("custo_por_km", 0),
            "margem_operacional": row.get("margem_operacional", 0),
            "idade_veiculo": row.get("idade_veiculo", 0),
            "decisao": decisao,
            "prioridade": prioridade,
            "recomendacao_idade": row["recomendacao_idade"],
            "num_alertas": len(veiculo_alertas),
        })

    df_decisoes = pd.DataFrame(decisoes).sort_values("prioridade")

    # 4. Resumo executivo
    resumo = {
        "total_veiculos": len(df),
        "intervencao_imediata": len(df_decisoes[df_decisoes["decisao"] == "INTERVENCAO IMEDIATA"]),
        "monitorar_planejar": len(df_decisoes[df_decisoes["decisao"] == "MONITORAR E PLANEJAR"]),
        "operacao_normal": len(df_decisoes[df_decisoes["decisao"] == "OPERACAO NORMAL"]),
        "total_alertas": len(df_alertas),
        "alertas_criticos": len(df_alertas[df_alertas["classificacao"] == "CRITICO"]),
        "alertas_atencao": len(df_alertas[df_alertas["classificacao"] == "ATENCAO"]),
    }

    # Salvar resultados
    output_dir = os.path.join(BASE_DIR, "analytics", "prescriptive")
    timestamp = datetime.now().strftime("%Y%m%d")

    paths = {}
    for name, df_result in [("alertas", df_alertas), ("decisoes", df_decisoes)]:
        if isinstance(df_result, pd.DataFrame) and not df_result.empty:
            path = os.path.join(output_dir, f"{name}_{timestamp}.csv")
            df_result.to_csv(path, index=False, encoding="utf-8")
            paths[name] = path

    # Salvar resumo
    resumo_path = os.path.join(output_dir, f"resumo_executivo_{timestamp}.txt")
    with open(resumo_path, "w", encoding="utf-8") as f:
        f.write("RESUMO EXECUTIVO - ANÁLISE PRESCRITIVA\n")
        f.write(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
        f.write("=" * 45 + "\n\n")
        f.write(f"Total de veículos analisados: {resumo['total_veiculos']}\n")
        f.write(f"Intervenção imediata:         {resumo['intervencao_imediata']}\n")
        f.write(f"Monitorar e planejar:         {resumo['monitorar_planejar']}\n")
        f.write(f"Operação normal:              {resumo['operacao_normal']}\n\n")
        f.write(f"Total de alertas:    {resumo['total_alertas']}\n")
        f.write(f"  Críticos:          {resumo['alertas_criticos']}\n")
        f.write(f"  Atenção:           {resumo['alertas_atencao']}\n")

    paths["resumo"] = resumo_path

    print(f"\n[PRESCRITIVA] Resumo:")
    print(f"  Veículos analisados:     {resumo['total_veiculos']}")
    print(f"  Intervenção imediata:    {resumo['intervencao_imediata']}")
    print(f"  Monitorar e planejar:    {resumo['monitorar_planejar']}")
    print(f"  Operação normal:         {resumo['operacao_normal']}")
    print(f"  Total de alertas:        {resumo['total_alertas']}")

    print("\n" + "=" * 50)
    print("ANÁLISE PRESCRITIVA CONCLUÍDA")
    print("=" * 50)

    return {"alertas": df_alertas, "decisoes": df_decisoes, "resumo": resumo}


if __name__ == "__main__":
    sys.path.insert(0, BASE_DIR)
    results = run_prescriptive_analysis()

    if results.get("decisoes") is not None:
        print("\n--- Top 10 Veículos Prioritários ---")
        print(results["decisoes"].head(10).to_string())

    if results.get("alertas") is not None and not results["alertas"].empty:
        print("\n--- Alertas Críticos ---")
        criticos = results["alertas"][results["alertas"]["classificacao"] == "CRITICO"]
        print(criticos.head(10).to_string())