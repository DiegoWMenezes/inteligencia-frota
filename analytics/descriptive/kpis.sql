-- =============================================================================
-- ANALYTICS DESCRITIVA - KPIs da Frota
-- Respostas: O quê? Quanto? Quando?
-- =============================================================================

-- KPI 1: Custo por km (principal indicador de eficiência)
SELECT
    'Custo por Km' AS kpi,
    v.tipo_veiculo,
    e.sigla_estado,
    t.ano,
    t.mes,
    ROUND(AVG(f.custo_por_km), 4)   AS valor,
    ROUND(AVG(f.custo_combustivel / NULLIF(f.km_rodado, 0)), 4) AS componente_combustivel,
    ROUND(AVG(f.custo_manutencao / NULLIF(f.km_rodado, 0)), 4) AS componente_manutencao,
    ROUND(AVG((f.custo_seguro + f.custo_depreciacao) / NULLIF(f.km_rodado, 0)), 4) AS componente_fixo
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY v.tipo_veiculo, e.sigla_estado, t.ano, t.mes;

-- KPI 2: Consumo médio (km/l por tipo de veículo)
SELECT
    'Consumo Medio' AS kpi,
    v.tipo_veiculo,
    v.combustivel,
    ROUND(AVG(f.km_por_litro_real), 2)   AS km_por_litro,
    ROUND(AVG(f.km_rodado), 0)            AS km_medio_mensal,
    ROUND(AVG(f.litros_consumidos), 1)    AS litros_medio_mensal
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
GROUP BY v.tipo_veiculo, v.combustivel;

-- KPI 3: Utilização da frota (disponibilidade + viagens)
SELECT
    'Utilizacao Frota' AS kpi,
    e.sigla_estado,
    t.ano,
    t.mes,
    COUNT(DISTINCT f.sk_veiculo)                     AS veiculos_ativos,
    ROUND(AVG(f.disponibilidade_pct), 1)              AS disponibilidade_media,
    SUM(f.viagens_realizadas)                          AS total_viagens,
    ROUND(AVG(f.km_rodado), 0)                        AS km_medio_por_veiculo,
    ROUND(SUM(f.receita_estimada) / COUNT(DISTINCT f.sk_veiculo), 2) AS receita_por_veiculo
FROM fato_frota f
JOIN dim_estado e ON f.sk_estado = e.sk_estado
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY e.sigla_estado, t.ano, t.mes;

-- KPI 4: Composição de custos
SELECT
    'Composicao Custos' AS kpi,
    t.ano,
    ROUND(SUM(f.custo_combustivel), 2)    AS total_combustivel,
    ROUND(SUM(f.custo_manutencao), 2)     AS total_manutencao,
    ROUND(SUM(f.custo_seguro), 2)         AS total_seguro,
    ROUND(SUM(f.custo_ipva), 2)           AS total_ipva,
    ROUND(SUM(f.custo_depreciacao), 2)    AS total_depreciacao,
    ROUND(SUM(f.custo_total), 2)          AS total_geral,
    ROUND(SUM(f.custo_combustivel) * 100.0 / NULLIF(SUM(f.custo_total), 0), 1) AS pct_combustivel,
    ROUND(SUM(f.custo_manutencao) * 100.0 / NULLIF(SUM(f.custo_total), 0), 1) AS pct_manutencao
FROM fato_frota f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano;

-- KPI 5: Receita vs Custo (rentabilidade)
SELECT
    'Rentabilidade' AS kpi,
    v.tipo_veiculo,
    ROUND(SUM(f.receita_estimada), 2)      AS receita_total,
    ROUND(SUM(f.custo_total), 2)           AS custo_total,
    ROUND(SUM(f.receita_estimada) - SUM(f.custo_total), 2) AS lucro,
    ROUND(AVG(f.margem_operacional) * 100, 1) AS margem_pct
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
GROUP BY v.tipo_veiculo
ORDER BY margem_pct DESC;