-- =============================================================================
-- DDL - Data Definition Language
-- Modelo Estrela: Inteligência de Frota
-- Compatível com PostgreSQL e SQLite
-- =============================================================================

-- Dimensão Tempo
CREATE TABLE IF NOT EXISTS dim_tempo (
    sk_tempo     INTEGER PRIMARY KEY,
    ano          INTEGER NOT NULL,
    mes          INTEGER NOT NULL,
    trimestre    INTEGER NOT NULL,
    semestre     INTEGER NOT NULL,
    nome_mes     VARCHAR(3) NOT NULL
);

-- Dimensão Estado
CREATE TABLE IF NOT EXISTS dim_estado (
    sk_estado     INTEGER PRIMARY KEY,
    sigla_estado  VARCHAR(2) NOT NULL,
    regiao        VARCHAR(20) NOT NULL
);

-- Dimensão Veículo
CREATE TABLE IF NOT EXISTS dim_veiculo (
    sk_veiculo          INTEGER PRIMARY KEY,
    id_veiculo          VARCHAR(10) NOT NULL,
    tipo_veiculo        VARCHAR(30) NOT NULL,
    marca               VARCHAR(50) NOT NULL,
    ano_fabricacao      INTEGER NOT NULL,
    combustivel         VARCHAR(20) NOT NULL,
    km_atual            INTEGER,
    status              VARCHAR(20),
    consumo_medio_km_l  DECIMAL(6,2),
    capacidade_carga_t  DECIMAL(6,1)
);

-- Fato Frota (tabela central do modelo estrela)
CREATE TABLE IF NOT EXISTS fato_frota (
    sk_fato_frota       INTEGER PRIMARY KEY,
    sk_veiculo          INTEGER NOT NULL,
    sk_estado           INTEGER NOT NULL,
    sk_tempo            INTEGER NOT NULL,
    km_rodado           INTEGER NOT NULL,
    litros_consumidos   DECIMAL(12,2),
    preco_combustivel   DECIMAL(6,2),
    custo_combustivel   DECIMAL(12,2),
    custo_manutencao    DECIMAL(12,2),
    custo_seguro        DECIMAL(10,2),
    custo_ipva          DECIMAL(10,2),
    custo_depreciacao   DECIMAL(10,2),
    custo_total         DECIMAL(12,2),
    receita_estimada    DECIMAL(12,2),
    viagens_realizadas  INTEGER,
    disponibilidade_pct DECIMAL(5,1),
    custo_por_km        DECIMAL(8,4),
    margem_operacional  DECIMAL(5,4),
    km_por_litro_real   DECIMAL(6,2),
    idade_veiculo       INTEGER,
    FOREIGN KEY (sk_veiculo) REFERENCES dim_veiculo(sk_veiculo),
    FOREIGN KEY (sk_estado)  REFERENCES dim_estado(sk_estado),
    FOREIGN KEY (sk_tempo)   REFERENCES dim_tempo(sk_tempo)
);

-- Índices para performance de consultas analíticas
CREATE INDEX IF NOT EXISTS idx_fato_veiculo  ON fato_frota(sk_veiculo);
CREATE INDEX IF NOT EXISTS idx_fato_estado   ON fato_frota(sk_estado);
CREATE INDEX IF NOT EXISTS idx_fato_tempo    ON fato_frota(sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_custo_km ON fato_frota(custo_por_km);