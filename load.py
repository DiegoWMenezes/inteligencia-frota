"""
Load - Carrega dados do staging para o Data Warehouse.
Cria as tabelas do modelo estrela e insere os dados enriquecidos.
Suporta PostgreSQL (producao) e SQLite (local).
"""

import os
import sys
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.join(os.path.dirname(__file__))
STAGING_DIR = os.path.join(BASE_DIR, "staging")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
DATA_DIR = os.path.join(BASE_DIR, "data")

# Configuracoes PostgreSQL
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "inteligencia_frota",
    "user": "postgres",
    "password": "adm123",
}

# Banco SQLite (fallback)
DB_PATH = os.path.join(DATA_DIR, "inteligencia_frota.db")


def get_connection(engine: str = "postgresql", db_path: str = None):
    """
    Retorna conexao com o banco de dados.
    engine: 'postgresql' ou 'sqlite'
    """
    if engine == "postgresql":
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(
            host=PG_CONFIG["host"],
            port=PG_CONFIG["port"],
            database=PG_CONFIG["database"],
            user=PG_CONFIG["user"],
            password=PG_CONFIG["password"],
        )
        conn.autocommit = True
        print(f"[LOAD] Conectado ao PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}")
        return conn
    else:
        import sqlite3
        db_path = db_path or DB_PATH
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        print(f"[LOAD] Conectado ao SQLite: {db_path}")
        return conn


def get_sqlalchemy_engine(engine: str = "postgresql"):
    """Retorna engine SQLAlchemy para uso com pandas to_sql."""
    from sqlalchemy import create_engine
    if engine == "postgresql":
        url = (
            f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
            f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
        )
        return create_engine(url)
    else:
        db_path = DB_PATH
        return create_engine(f"sqlite:///{db_path}")


def execute_sql_file(conn, sql_file: str, engine: str = "postgresql"):
    """Executa um arquivo SQL inteiro."""
    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    cursor = conn.cursor()
    for stmt in statements:
        try:
            cursor.execute(stmt)
        except Exception as e:
            error_msg = str(e).lower()
            # Ignorar erros de "already exists" no PostgreSQL
            if "already exists" in error_msg:
                print(f"[LOAD] Ja existe (pulando): {stmt[:60]}...")
                continue
            print(f"[LOAD] Aviso ({engine}): {e} - pulando statement")
    conn.commit()
    cursor.close()


def create_tables(conn, engine: str = "postgresql"):
    """Cria as tabelas do DW executando o DDL."""
    ddl_path = os.path.join(WAREHOUSE_DIR, "ddl.sql")
    if os.path.exists(ddl_path):
        print("[LOAD] Executando DDL...")
        execute_sql_file(conn, ddl_path, engine)
    else:
        print(f"[LOAD] DDL nao encontrado: {ddl_path}")


def load_dim_tempo(sa_engine, df: pd.DataFrame):
    """Carrega dimensao tempo a partir dos dados de operacoes."""
    anos = df["ano"].unique()
    meses = df["mes"].unique()

    records = []
    for ano in anos:
        for mes in meses:
            trimestre = (int(mes) - 1) // 3 + 1
            semestre = 1 if int(mes) <= 6 else 2
            records.append({
                "sk_tempo": int(f"{ano}{int(mes):02d}"),
                "ano": int(ano),
                "mes": int(mes),
                "trimestre": trimestre,
                "semestre": semestre,
                "nome_mes": ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                             "Jul", "Ago", "Set", "Out", "Nov", "Dez"][int(mes) - 1],
            })

    df_dim = pd.DataFrame(records)
    df_dim.to_sql("dim_tempo", sa_engine, if_exists="append", index=False)
    print(f"[LOAD] dim_tempo: {len(df_dim)} registros")


def load_dim_estado(sa_engine, df: pd.DataFrame):
    """Carrega dimensao estado."""
    estados = df["estado"].unique()

    regioes = {
        "NORTE": ["RO", "AC", "AM", "RR", "PA", "AP", "TO"],
        "NORDESTE": ["MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA"],
        "SUDESTE": ["MG", "ES", "RJ", "SP"],
        "SUL": ["PR", "SC", "RS"],
        "CENTRO OESTE": ["MS", "MT", "GO", "DF"],
    }

    estado_regiao = {}
    for regiao, ufs in regioes.items():
        for uf in ufs:
            estado_regiao[uf] = regiao

    records = []
    for i, estado in enumerate(sorted(estados), 1):
        records.append({
            "sk_estado": i,
            "sigla_estado": estado,
            "regiao": estado_regiao.get(estado, "DESCONHECIDO"),
        })

    df_dim = pd.DataFrame(records)
    df_dim.to_sql("dim_estado", sa_engine, if_exists="append", index=False)
    print(f"[LOAD] dim_estado: {len(df_dim)} registros")


def load_dim_veiculo(sa_engine, df_veiculos: pd.DataFrame):
    """Carrega dimensao veiculo."""
    df_dim = df_veiculos.copy()
    df_dim["sk_veiculo"] = range(1, len(df_dim) + 1)

    colunas = {
        "sk_veiculo": "sk_veiculo",
        "veiculo_id": "id_veiculo",
        "tipo_veiculo": "tipo_veiculo",
        "marca": "marca",
        "ano_fabricacao": "ano_fabricacao",
        "combustivel": "combustivel",
        "km_atual": "km_atual",
        "status": "status",
        "consumo_medio_km_l": "consumo_medio_km_l",
        "capacidade_carga_t": "capacidade_carga_t",
    }

    df_dim = df_dim[[c for c in colunas.keys() if c in df_dim.columns]].rename(columns=colunas)
    df_dim.to_sql("dim_veiculo", sa_engine, if_exists="append", index=False)
    print(f"[LOAD] dim_veiculo: {len(df_dim)} registros")


def load_fato_frota(sa_engine, conn, df_enriched: pd.DataFrame, df_veiculos: pd.DataFrame, engine: str = "postgresql"):
    """Carrega fato_frota com dados enriquecidos."""
    # Mapear SKs usando pandas (funciona para ambos SQLite e PostgreSQL)
    sk_estado_map = dict(
        pd.read_sql("SELECT sigla_estado, sk_estado FROM dim_estado", sa_engine).values.tolist()
    )
    sk_veiculo_map = dict(
        pd.read_sql("SELECT id_veiculo, sk_veiculo FROM dim_veiculo", sa_engine).values.tolist()
    )
    sk_tempo_map = dict(
        pd.read_sql("SELECT CAST(ano AS text) || CAST(mes AS text), sk_tempo FROM dim_tempo", sa_engine).values.tolist()
    )

    df = df_enriched.copy()
    df["sk_estado"] = df["estado"].map(sk_estado_map)
    df["sk_veiculo"] = df["veiculo_id"].map(sk_veiculo_map)
    df["sk_tempo"] = (df["ano"].astype(str) + df["mes"].astype(str).str.zfill(2)).map(sk_tempo_map)

    colunas_fato = [
        "sk_veiculo", "sk_estado", "sk_tempo",
        "km_rodado", "litros_consumidos", "preco_combustivel",
        "custo_combustivel", "custo_manutencao", "custo_seguro",
        "custo_ipva", "custo_depreciacao", "custo_total",
        "receita_estimada", "viagens_realizadas", "disponibilidade_pct",
        "custo_por_km", "margem_operacional", "km_por_litro_real",
        "idade_veiculo",
    ]

    colunas_disponiveis = [c for c in colunas_fato if c in df.columns]
    df_fato = df[colunas_disponiveis].copy()
    df_fato = df_fato.dropna(subset=["sk_veiculo", "sk_estado", "sk_tempo"])

    df_fato.insert(0, "sk_fato_frota", range(1, len(df_fato) + 1))

    df_fato.to_sql("fato_frota", sa_engine, if_exists="append", index=False)
    print(f"[LOAD] fato_frota: {len(df_fato)} registros")


def run_load_pipeline(engine: str = "postgresql", db_path: str = None):
    """
    Executa o pipeline de carga completo:
    staging -> DDL -> dimensoes -> fato
    engine: 'postgresql' ou 'sqlite'
    """
    print("=" * 60)
    print(f"INICIANDO PIPELINE DE CARGA (LOAD) - {engine.upper()}")
    print("=" * 60)

    conn = get_connection(engine, db_path)
    sa_engine = get_sqlalchemy_engine(engine)

    # Criar tabelas (DDL)
    print("\n[1/4] Criando tabelas...")
    create_tables(conn, engine)

    # Carregar dados de staging
    print("\n[2/4] Carregando dados de staging...")

    # Para PostgreSQL: dropar tabelas na ordem correta (fato primeiro, dim depois)
    if engine == "postgresql":
        cursor = conn.cursor()
        for table in ["fato_frota", "dim_veiculo", "dim_estado", "dim_tempo"]:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            except Exception:
                pass
        conn.commit()
        # Recriar tabelas via DDL
        create_tables(conn, engine)
        cursor.close()

    staging_files = sorted(os.listdir(STAGING_DIR))
    timestamp_files = [f for f in staging_files if f.endswith(".csv")]

    df_enriched = None
    df_veiculos = None

    for f in timestamp_files:
        path = os.path.join(STAGING_DIR, f)
        if "frota_enriquecida" in f:
            df_enriched = pd.read_csv(path)
        elif "veiculos_clean" in f:
            df_veiculos = pd.read_csv(path)

    if df_enriched is None:
        print("[LOAD] ERRO: Dados enriquecidos nao encontrados em staging!")
        print("[LOAD] Execute o ETL primeiro: python etl/transform.py")
        conn.close()
        return

    if df_veiculos is None:
        print("[LOAD] ERRO: Dados de veiculos nao encontrados em staging!")
        conn.close()
        return

    # Carregar dimensoes
    print("\n[3/4] Carregando dimensoes...")
    load_dim_tempo(sa_engine, df_enriched)
    load_dim_estado(sa_engine, df_enriched)
    load_dim_veiculo(sa_engine, df_veiculos)

    # Carregar fato
    print("\n[4/4] Carregando fato...")
    load_fato_frota(sa_engine, conn, df_enriched, df_veiculos, engine)

    # Criar views
    views_path = os.path.join(WAREHOUSE_DIR, "views.sql")
    if os.path.exists(views_path):
        print("\n[EXTRA] Criando views analiticas...")
        execute_sql_file(conn, views_path, engine)

    conn.close()

    db_label = f"PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}" if engine == "postgresql" else f"SQLite: {db_path or DB_PATH}"
    print("\n" + "=" * 60)
    print("PIPELINE DE CARGA CONCLUIDO")
    print(f"Banco: {db_label}")
    print("=" * 60)


if __name__ == "__main__":
    sys.path.insert(0, BASE_DIR)

    import argparse
    parser = argparse.ArgumentParser(description="Load - Pipeline de carga do Data Warehouse")
    parser.add_argument("--engine", choices=["postgresql", "sqlite"], default="postgresql",
                        help="Engine do banco de dados (default: postgresql)")
    parser.add_argument("--db-path", default=None, help="Caminho do banco SQLite (se engine=sqlite)")
    args = parser.parse_args()

    run_load_pipeline(engine=args.engine, db_path=args.db_path)