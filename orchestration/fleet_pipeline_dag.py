"""
DAG do Airflow - Pipeline Inteligencia de Frota
Pipeline: ingestao -> staging -> transformacao -> enriquecimento -> load DW -> analytics

Requer Airflow 2.x instalado.
Execucao manual: python fleet_pipeline_dag.py (modo standalone)
"""

import os
import sys
from datetime import datetime, timedelta

# Adicionar raiz do projeto ao path
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, PROJECT_ROOT)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("[DAG] Airflow nao instalado. Pipeline executavel em modo standalone.")


# =============================================================================
# FUNCOES DO PIPELINE
# =============================================================================

def task_ingest_anp(**kwargs):
    """Ingestao de dados de combustivel da ANP."""
    from ingestion.anp_fuel import download_anp_csv, load_anp_data
    path = download_anp_csv()
    df = load_anp_data(path)
    print(f"[INGESTAO-ANP] {len(df)} registros processados")
    return path


def task_ingest_ibge(**kwargs):
    """Ingestao de dados do IBGE."""
    from ingestion.ibge_api import fetch_all_indicadores, load_ibge_data
    path = fetch_all_indicadores()
    df = load_ibge_data(path)
    print(f"[INGESTAO-IBGE] {len(df)} registros processados")
    return path


def task_ingest_denatran(**kwargs):
    """Ingestao de dados de frota do DENATRAN."""
    from ingestion.denatran_loader import download_denatran_data, load_denatran_data
    path = download_denatran_data()
    df = load_denatran_data(path)
    print(f"[INGESTAO-DENATRAN] {len(df)} registros processados")
    return path


def task_ingest_fleet(**kwargs):
    """Geracao de dados operacionais de frota."""
    from ingestion.fleet_generator import generate_fleet_data, load_fleet_data
    v_path, o_path = generate_fleet_data()
    df_v, df_o = load_fleet_data(v_path, o_path)
    print(f"[INGESTAO-FLEET] {len(df_v)} veiculos, {len(df_o)} operacoes")
    return v_path, o_path


def task_etl(**kwargs):
    """ETL: limpeza, padronizacao e enriquecimento."""
    from etl.transform import run_etl_pipeline
    paths = run_etl_pipeline()
    print(f"[ETL] Pipeline concluido: {len(paths)} arquivos gerados")
    return paths


def task_load(**kwargs):
    """Load: carregar dados no Data Warehouse."""
    from load import run_load_pipeline
    run_load_pipeline(engine="postgresql")
    print("[LOAD] Dados carregados no DW (PostgreSQL)")


def task_analytics_predictive(**kwargs):
    """Analytics preditiva: forecasting."""
    from analytics.predictive.forecast import FleetPredictor
    predictor = FleetPredictor()
    results = predictor.run_all_forecasts()
    print(f"[ANALYTICS-PREDITIVA] {len(results)} modelos executados")
    return results


def task_analytics_prescriptive(**kwargs):
    """Analytics prescritiva: regras de decisao."""
    from analytics.prescriptive.decision_rules import run_prescriptive_analysis
    results = run_prescriptive_analysis()
    print(f"[ANALYTICS-PRESCRITIVA] Analise concluida")
    return results


# =============================================================================
# DEFINICAO DA DAG
# =============================================================================

default_args = {
    "owner": "inteligencia_frota",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        "fleet_intelligence_pipeline",
        default_args=default_args,
        description="Pipeline completo de Inteligencia de Frota: ingestao -> ETL -> DW -> analytics",
        schedule_interval="@monthly",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["frota", "etl", "analytics"],
    )

    # Ingestao (paralelo)
    ingest_anp = PythonOperator(
        task_id="ingest_anp",
        python_callable=task_ingest_anp,
        dag=dag,
    )

    ingest_ibge = PythonOperator(
        task_id="ingest_ibge",
        python_callable=task_ingest_ibge,
        dag=dag,
    )

    ingest_denatran = PythonOperator(
        task_id="ingest_denatran",
        python_callable=task_ingest_denatran,
        dag=dag,
    )

    ingest_fleet = PythonOperator(
        task_id="ingest_fleet",
        python_callable=task_ingest_fleet,
        dag=dag,
    )

    # ETL (depende de toda ingestao)
    etl = PythonOperator(
        task_id="etl_transform",
        python_callable=task_etl,
        dag=dag,
    )

    # Load DW (depende do ETL)
    load_dw = PythonOperator(
        task_id="load_dw",
        python_callable=task_load,
        dag=dag,
    )

    # Analytics (depende do load)
    analytics_predictive = PythonOperator(
        task_id="analytics_predictive",
        python_callable=task_analytics_predictive,
        dag=dag,
    )

    analytics_prescriptive = PythonOperator(
        task_id="analytics_prescriptive",
        python_callable=task_analytics_prescriptive,
        dag=dag,
    )

    # Dependencias
    [ingest_anp, ingest_ibge, ingest_denatran, ingest_fleet] >> etl >> load_dw
    load_dw >> [analytics_predictive, analytics_prescriptive]


# =============================================================================
# EXECUCAO STANDALONE (sem Airflow)
# =============================================================================

def run_standalone():
    """Executa o pipeline completo sem Airflow."""
    print("=" * 60)
    print("PIPELINE INTELIGENCIA DE FROTA - EXECUCAO STANDALONE")
    print("=" * 60)

    print("\n[1/7] Ingestao ANP...")
    task_ingest_anp()

    print("\n[2/7] Ingestao IBGE...")
    task_ingest_ibge()

    print("\n[3/7] Ingestao DENATRAN...")
    task_ingest_denatran()

    print("\n[4/7] Ingestao Fleet...")
    task_ingest_fleet()

    print("\n[5/7] ETL Transform...")
    task_etl()

    print("\n[6/7] Load DW...")
    task_load()

    print("\n[7/7] Analytics Predictive...")
    task_analytics_predictive()

    print("\n[8/7] Analytics Prescriptive...")
    task_analytics_prescriptive()

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETO!")
    print("=" * 60)


if __name__ == "__main__":
    if AIRFLOW_AVAILABLE:
        print("[DAG] Airflow disponivel. DAG 'fleet_intelligence_pipeline' registrada.")
        print("[DAG] Use: airflow dags trigger fleet_intelligence_pipeline")
    else:
        print("[DAG] Modo standalone (sem Airflow)")
        run_standalone()