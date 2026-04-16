"""
Gerador de dados operacionais de frota.
Cria registros de operação simulados baseados nos dados reais de entrada
(ANP, IBGE, DENATRAN) para alimentar o pipeline de analytics.
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

ESTADOS = [
    "RO", "AC", "AM", "RR", "PA", "AP", "TO",
    "MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA",
    "MG", "ES", "RJ", "SP",
    "PR", "SC", "RS",
    "MS", "MT", "GO", "DF",
]

# Frota de empresa de saneamento: Caminhoes (core), Pickups (apoio), Carros Sedan (deslocamento)
TIPOS_VEICULO_FROTA = ["Caminhao", "Carro Sedan", "Pickup"]
TIPOS_PESOS = [0.60, 0.20, 0.20]  # 60% caminhoes, 20% sedan, 20% pickup

MARCAS = {
    "Caminhao": ["Mercedes-Benz", "Volvo", "Scania", "DAF", "Iveco", "MAN"],
    "Carro Sedan": ["Toyota Corolla", "Honda Civic", "Volkswagen Virtus", "Chevrolet Onix"],
    "Pickup": ["Toyota Hilux", "Fiat Toro", "Volkswagen Amarok", "Ford Ranger"],
}

COMBUSTIVEIS_POR_TIPO = {
    "Caminhao": "Diesel",
    "Carro Sedan": "Flex",
    "Pickup": "Diesel",
}

CONSUMO_MEDIO = {
    "Caminhao": 3.5,   # km/l
    "Carro Sedan": 12.0,
    "Pickup": 8.5,
}

CUSTO_MANUTENCAO_BASE = {
    "Caminhao": 2500, "Carro Sedan": 500, "Pickup": 900,
}

STATUS_VEICULO = ["Ativo", "Em Manutencao", "Disponivel", "Inativo"]


def generate_fleet_data(
    num_veiculos: int = 500,
    meses_historico: int = 24,
    output_dir: str = None,
) -> str:
    """
    Gera dados operacionais de frota com histórico de operações.

    Args:
        num_veiculos: Número de veículos na frota
        meses_historico: Meses de histórico a gerar
        output_dir: Diretório de saída

    Returns:
        Caminho do arquivo CSV gerado
    """
    output_dir = output_dir or RAW_DIR
    os.makedirs(output_dir, exist_ok=True)

    random.seed(42)

    # Gerar cadastro de veículos
    veiculos = []
    for i in range(1, num_veiculos + 1):
        tipo = random.choices(TIPOS_VEICULO_FROTA, weights=TIPOS_PESOS, k=1)[0]
        marca = random.choice(MARCAS[tipo])
        ano_fabricacao = random.randint(2015, 2024)
        estado = random.choice(ESTADOS)

        veiculos.append({
            "veiculo_id": f"V{1000 + i}",
            "tipo_veiculo": tipo,
            "marca": marca,
            "ano_fabricacao": ano_fabricacao,
            "estado": estado,
            "combustivel": COMBUSTIVEIS_POR_TIPO[tipo],
            "consumo_medio_km_l": CONSUMO_MEDIO[tipo],
            "km_atual": random.randint(10000, 350000),
            "status": random.choices(STATUS_VEICULO, weights=[0.70, 0.10, 0.12, 0.08], k=1)[0],
            "capacidade_carga_t": round(random.uniform(0.5, 30), 1) if tipo in ["Caminhao", "Pickup"] else 0,
        })

    df_veiculos = pd.DataFrame(veiculos)

    # Gerar histórico de operações
    data_inicio = datetime(2024, 1, 1)
    operacoes = []

    for veiculo in veiculos:
        vid = veiculo["veiculo_id"]
        tipo = veiculo["tipo_veiculo"]
        estado = veiculo["estado"]
        consumo = veiculo["consumo_medio_km_l"]

        for m in range(meses_historico):
            data_ref = data_inicio + timedelta(days=30 * m)
            mes = data_ref.month
            ano = data_ref.year

            # Variação sazonal: mais uso em meses de safra/verão
            fator_sazonal = 1.0 + 0.15 * (1 if mes in [3, 4, 5, 10, 11, 12] else 0)

            km_mensal = int(random.randint(1500, 8000) * fator_sazonal)
            litros = round(km_mensal / consumo, 2)

            # Preço do combustível varia por estado e mês
            preco_combustivel = round(random.uniform(5.20, 7.10), 2)
            custo_combustivel = round(litros * preco_combustivel, 2)

            # Manutenção
            custo_manutencao = round(CUSTO_MANUTENCAO_BASE[tipo] * random.uniform(0.5, 1.8), 2)
            if random.random() < 0.08:  # 8% chance de manutenção corretiva
                custo_manutencao *= random.uniform(2.0, 5.0)

            # Outros custos
            custo_seguro = round(random.uniform(300, 2500), 2)
            custo_ipva = round(random.uniform(500, 5000), 2) if mes == 1 else 0
            custo_depreciacao = round(random.uniform(800, 5000), 2)

            # Receita estimada
            receita = round(km_mensal * random.uniform(2.5, 8.0), 2)

            # Viagens realizadas
            viagens = random.randint(15, 60)

            operacoes.append({
                "veiculo_id": vid,
                "tipo_veiculo": tipo,
                "estado": estado,
                "ano": ano,
                "mes": mes,
                "km_rodado": km_mensal,
                "litros_consumidos": litros,
                "preco_combustivel": preco_combustivel,
                "custo_combustivel": custo_combustivel,
                "custo_manutencao": custo_manutencao,
                "custo_seguro": custo_manutencao * 0.4,
                "custo_ipva": custo_ipva,
                "custo_depreciacao": custo_depreciacao,
                "receita_estimada": receita,
                "viagens_realizadas": viagens,
                "disponibilidade_pct": round(random.uniform(75, 99), 1),
            })

    df_operacoes = pd.DataFrame(operacoes)

    # Salvar veículos
    timestamp = datetime.now().strftime("%Y%m%d")
    path_veiculos = os.path.join(output_dir, f"frota_veiculos_{timestamp}.csv")
    df_veiculos.to_csv(path_veiculos, index=False, encoding="utf-8")

    # Salvar operações
    path_operacoes = os.path.join(output_dir, f"frota_operacoes_{timestamp}.csv")
    df_operacoes.to_csv(path_operacoes, index=False, encoding="utf-8")

    print(f"[FLEET] Veículos gerados: {len(df_veiculos)}")
    print(f"[FLEET] Operações geradas: {len(df_operacoes)}")
    print(f"[FLEET] Arquivos salvos em: {output_dir}")

    return path_veiculos, path_operacoes


def load_fleet_data(filepath_veiculos: str = None, filepath_operacoes: str = None):
    """Carrega os dados de frota em DataFrames."""
    if filepath_veiculos is None or filepath_operacoes is None:
        veiculos_files = [f for f in os.listdir(RAW_DIR) if f.startswith("frota_veiculos")]
        operacoes_files = [f for f in os.listdir(RAW_DIR) if f.startswith("frota_operacoes")]

        if not veiculos_files or not operacoes_files:
            return generate_fleet_data()

        filepath_veiculos = filepath_veiculos or os.path.join(RAW_DIR, sorted(veiculos_files)[-1])
        filepath_operacoes = filepath_operacoes or os.path.join(RAW_DIR, sorted(operacoes_files)[-1])

    df_veiculos = pd.read_csv(filepath_veiculos, encoding="utf-8")
    df_operacoes = pd.read_csv(filepath_operacoes, encoding="utf-8")

    print(f"[FLEET] Veículos: {len(df_veiculos)}, Operações: {len(df_operacoes)}")
    return df_veiculos, df_operacoes


if __name__ == "__main__":
    v_path, o_path = generate_fleet_data()
    df_v, df_o = load_fleet_data(v_path, o_path)

    print("\n--- Veículos ---")
    print(df_v.head())
    print(f"\nTipos: {df_v['tipo_veiculo'].value_counts().to_dict()}")

    print("\n--- Operações ---")
    print(df_o.head())
    print(f"\nCusto total combustível: R$ {df_o['custo_combustivel'].sum():,.2f}")
    print(f"KM total rodado: {df_o['km_rodado'].sum():,}")