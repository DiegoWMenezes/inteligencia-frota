"""
Ingestão de dados do IBGE via API pública.
Indicadores econômicos: IPCA, PIB, Salário médio, População.
Fonte: https://servicodados.ibge.gov.br/api/v3/agregados
"""

import os
import requests
import pandas as pd
from datetime import datetime

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v3"

# Códigos dos indicadores IBGE
INDICADORES = {
    "IPCA": {
        "agregado": "7060",   # IPCA - Índice geral
        "classificacao": "315|7169",  # Índice geral
        "descricao": "Indice Nacional de Precos ao Consumidor Amplo",
    },
    "PIB": {
        "agregado": "6784",   # PIB a preços correntes
        "classificacao": "1|1",  # Total
        "descricao": "Produto Interno Bruto a precos correntes",
    },
    "SALARIO": {
        "agregado": "5938",   # Rendimento médio
        "classificacao": "1|1",
        "descricao": "Rendimento medio mensal",
    },
    "POPULACAO": {
        "agregado": "6579",   # Projeção populacional
        "classificacao": "1|1",
        "descricao": "Projecao da populacao",
    },
}

# Tabela de códigos UF do IBGE
UF_CODES = {
    "RO": 11, "AC": 12, "AM": 13, "RR": 14, "PA": 15, "AP": 16, "TO": 17,
    "MA": 21, "PI": 22, "CE": 23, "RN": 24, "PB": 25, "PE": 26,
    "AL": 27, "SE": 28, "BA": 29,
    "MG": 31, "ES": 32, "RJ": 33, "SP": 35,
    "PR": 41, "SC": 42, "RS": 43,
    "MS": 50, "MT": 51, "GO": 52, "DF": 53,
}


def fetch_ibge_indicador(indicador: str, periodo: str = "2013-2024") -> pd.DataFrame:
    """
    Busca um indicador específico da API do IBGE.
    período: formato "ano-ano" ou "ano" (ex: "2020-2024")
    """
    config = INDICADORES.get(indicador.upper())
    if not config:
        raise ValueError(f"Indicador '{indicador}' não encontrado. Use: {list(INDICADORES.keys())}")

    url = (
        f"{IBGE_BASE_URL}/agregados/{config['agregado']}/periodos/{periodo}"
        f"?variavel=all&classificacao={config['classificacao']}"
        f"&localidades=N1[all]"
    )

    print(f"[IBGE] Buscando {indicador}: {config['descricao']}")
    print(f"[IBGE] URL: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"[IBGE] Nenhum dado retornado para {indicador}")
            return pd.DataFrame()

        records = []
        for serie in data:
            for entry in serie.get("resultados", []):
                classificacoes = entry.get("classificacoes", [])
                series_data = entry.get("series", [])
                for s in series_data:
                    localidade = s.get("localidade", {})
                    for periodo_str, valor in s.get("serie", {}).items():
                        records.append({
                            "indicador": indicador,
                            "descricao": config["descricao"],
                            "periodo": periodo_str,
                            "valor": valor if valor != "..." else None,
                            "localidade_id": localidade.get("id", ""),
                            "localidade_nome": localidade.get("nome", ""),
                        })

        df = pd.DataFrame(records)
        if not df.empty:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        print(f"[IBGE] {indicador}: {len(df)} registros obtidos")
        return df

    except requests.RequestException as e:
        print(f"[IBGE] Erro ao buscar {indicador}: {e}")
        return pd.DataFrame()


def fetch_ibge_indicador_por_uf(indicador: str, periodo: str = "2013-2024") -> pd.DataFrame:
    """
    Busca indicador desagregado por UF (quando disponível).
    """
    config = INDICADORES.get(indicador.upper())
    if not config:
        raise ValueError(f"Indicador '{indicador}' não encontrado.")

    url = (
        f"{IBGE_BASE_URL}/agregados/{config['agregado']}/periodos/{periodo}"
        f"?variavel=all&classificacao={config['classificacao']}"
        f"&localidades=N3[all]"
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        records = []
        for serie in data:
            for entry in serie.get("resultados", []):
                series_data = entry.get("series", [])
                for s in series_data:
                    localidade = s.get("localidade", {})
                    for periodo_str, valor in s.get("serie", {}).items():
                        records.append({
                            "indicador": indicador,
                            "descricao": config["descricao"],
                            "periodo": periodo_str,
                            "valor": valor if valor != "..." else None,
                            "uf_id": localidade.get("id", ""),
                            "uf_nome": localidade.get("nome", ""),
                        })

        df = pd.DataFrame(records)
        if not df.empty:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        print(f"[IBGE] {indicador} por UF: {len(df)} registros")
        return df

    except requests.RequestException as e:
        print(f"[IBGE] Erro: {e}")
        return pd.DataFrame()


def fetch_all_indicadores(periodo: str = "2013-2024", output_dir: str = None) -> str:
    """
    Busca todos os indicadores e salva em CSV consolidado.
    """
    output_dir = output_dir or RAW_DIR
    os.makedirs(output_dir, exist_ok=True)

    all_dfs = []

    for indicador in INDICADORES:
        df = fetch_ibge_indicador(indicador, periodo)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        print("[IBGE] Nenhum dado obtido via API. Gerando dados simulados...")
        df = _generate_sample_ibge_data()
    else:
        df = pd.concat(all_dfs, ignore_index=True)

    timestamp = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(output_dir, f"ibge_indicadores_{timestamp}.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"[IBGE] Dados salvos em: {output_path}")
    return output_path


def _generate_sample_ibge_data() -> pd.DataFrame:
    """Gera dados simulados baseados na estrutura real do IBGE."""
    import random

    random.seed(42)
    records = []

    estados = list(UF_CODES.keys())
    anos = list(range(2018, 2025))

    ipca_base = {"NORTE": 4.5, "NORDESTE": 4.2, "SUDESTE": 4.8, "SUL": 4.3, "CENTRO OESTE": 4.6}
    pib_per_capita = {
        "SP": 65000, "RJ": 45000, "DF": 80000, "MG": 35000, "PR": 42000,
        "SC": 40000, "RS": 38000, "GO": 35000, "ES": 32000, "MT": 40000,
        "MS": 36000, "BA": 25000, "PE": 26000, "CE": 24000, "AM": 28000,
        "PA": 24000, "RO": 30000, "AC": 22000, "RR": 29000, "AP": 25000,
        "TO": 27000, "MA": 21000, "PI": 21000, "RN": 24000, "PB": 23000,
        "AL": 20000, "SE": 26000, "MG": 35000,
    }

    for ano in anos:
        for estado in estados:
            pib = pib_per_capita.get(estado, 30000) * (1 + random.uniform(-0.05, 0.15))
            salario = pib * 0.35 + random.uniform(-500, 1500)
            pop = random.randint(800000, 45000000)
            ipca = 4.0 + random.uniform(0, 3.5)

            records.append({
                "indicador": "IPCA",
                "descricao": "Indice Nacional de Precos ao Consumidor Amplo",
                "periodo": str(ano),
                "valor": round(ipca, 2),
                "estado": estado,
            })
            records.append({
                "indicador": "PIB_PER_CAPITA",
                "descricao": "PIB per capita",
                "periodo": str(ano),
                "valor": round(pib, 2),
                "estado": estado,
            })
            records.append({
                "indicador": "SALARIO_MEDIO",
                "descricao": "Rendimento medio mensal",
                "periodo": str(ano),
                "valor": round(salario, 2),
                "estado": estado,
            })
            records.append({
                "indicador": "POPULACAO",
                "descricao": "Populacao estimada",
                "periodo": str(ano),
                "valor": pop,
                "estado": estado,
            })

    df = pd.DataFrame(records)
    print(f"[IBGE] Dados simulados gerados: {len(df)} registros")
    return df


def load_ibge_data(filepath: str = None) -> pd.DataFrame:
    """Carrega os dados do IBGE em um DataFrame."""
    if filepath is None:
        files = [f for f in os.listdir(RAW_DIR) if f.startswith("ibge_indicadores")]
        if not files:
            filepath = fetch_all_indicadores()
        else:
            filepath = os.path.join(RAW_DIR, sorted(files)[-1])

    df = pd.read_csv(filepath, encoding="utf-8")
    print(f"[IBGE] Dados carregados: {len(df)} registros")
    return df


if __name__ == "__main__":
    path = fetch_all_indicadores()
    df = load_ibge_data(path)
    print(df.head(10))
    print(f"\nIndicadores: {df['indicador'].unique().tolist()}")