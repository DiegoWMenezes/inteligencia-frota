-- =============================================================================
-- VIEWS - Camada Analítica do Data Warehouse
-- KPIs e métricas para alimentar o dashboard e relatórios
-- =============================================================================

-- View: Resumo mensal da frota (KPIs agregados por mês)
CREATE OR REPLACE VIEW vw_resumo_mensal AS
SELECT
    t.ano,
    t.mes,
    t.nome_mes,
    COUNT(DISTINCT f.sk_veiculo)                AS total_veiculos,
    SUM(f.km_rodado)                             AS total_km_rodado,
    SUM(f.custo_combustivel)                     AS total_custo_combustivel,
    SUM(f.custo_manutencao)                      AS total_custo_manutencao,
    SUM(f.custo_total)                           AS total_custo,
    SUM(f.receita_estimada)                      AS total_receita,
    SUM(f.viagens_realizadas)                     AS total_viagens,
    ROUND(AVG(f.custo_por_km), 4)                AS custo_medio_por_km,
    ROUND(AVG(f.margem_operacional), 4)           AS margem_media,
    ROUND(AVG(f.disponibilidade_pct), 1)          AS disponibilidade_media,
    ROUND(AVG(f.km_por_litro_real), 2)            AS consumo_medio_real,
    ROUND(SUM(f.receita_estimada) - SUM(f.custo_total), 2) AS lucro_operacional
FROM fato_frota f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano, t.mes, t.nome_mes;

-- View: KPIs por estado
CREATE OR REPLACE VIEW vw_kpi_estado AS
SELECT
    e.sigla_estado,
    e.regiao,
    COUNT(DISTINCT f.sk_veiculo)                AS total_veiculos,
    SUM(f.km_rodado)                             AS total_km_rodado,
    SUM(f.custo_total)                           AS total_custo,
    SUM(f.receita_estimada)                      AS total_receita,
    ROUND(AVG(f.custo_por_km), 4)                AS custo_medio_por_km,
    ROUND(AVG(f.margem_operacional), 4)           AS margem_media,
    ROUND(AVG(f.disponibilidade_pct), 1)          AS disponibilidade_media,
    ROUND(AVG(f.km_por_litro_real), 2)            AS consumo_medio_km_l,
    ROUND(SUM(f.receita_estimada) - SUM(f.custo_total), 2) AS lucro_operacional
FROM fato_frota f
JOIN dim_estado e ON f.sk_estado = e.sk_estado
GROUP BY e.sigla_estado, e.regiao;

-- View: KPIs por tipo de veículo
CREATE OR REPLACE VIEW vw_kpi_tipo_veiculo AS
SELECT
    v.tipo_veiculo,
    v.combustivel,
    COUNT(DISTINCT f.sk_veiculo)                AS total_veiculos,
    SUM(f.km_rodado)                             AS total_km_rodado,
    SUM(f.custo_total)                           AS total_custo,
    SUM(f.receita_estimada)                      AS total_receita,
    ROUND(AVG(f.custo_por_km), 4)                AS custo_medio_por_km,
    ROUND(AVG(f.custo_combustivel / NULLIF(f.km_rodado, 0)), 4) AS custo_combustivel_por_km,
    ROUND(AVG(f.custo_manutencao / NULLIF(f.km_rodado, 0)), 4) AS custo_manutencao_por_km,
    ROUND(AVG(f.margem_operacional), 4)           AS margem_media,
    ROUND(AVG(f.km_por_litro_real), 2)            AS consumo_medio_real,
    ROUND(AVG(f.disponibilidade_pct), 1)          AS disponibilidade_media
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
GROUP BY v.tipo_veiculo, v.combustivel;

-- View: Veículos com maior custo (top problema)
CREATE OR REPLACE VIEW vw_veiculos_alto_custo AS
SELECT
    v.id_veiculo,
    v.tipo_veiculo,
    v.marca,
    v.ano_fabricacao,
    v.status,
    e.sigla_estado,
    ROUND(AVG(f.custo_por_km), 4)                AS custo_medio_por_km,
    ROUND(AVG(f.margem_operacional), 4)           AS margem_media,
    ROUND(AVG(f.disponibilidade_pct), 1)          AS disponibilidade_media,
    SUM(f.custo_total)                            AS custo_total_periodo,
    SUM(f.km_rodado)                               AS km_total_periodo,
    f.idade_veiculo
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
GROUP BY v.id_veiculo, v.tipo_veiculo, v.marca, v.ano_fabricacao,
         v.status, e.sigla_estado, f.idade_veiculo
HAVING AVG(f.custo_por_km) > 1.50
ORDER BY AVG(f.custo_por_km) DESC;

-- View: Impacto do preço do combustível no custo operacional
CREATE OR REPLACE VIEW vw_impacto_combustivel AS
SELECT
    t.ano,
    t.mes,
    e.sigla_estado,
    v.tipo_veiculo,
    ROUND(AVG(f.preco_combustivel), 2)            AS preco_medio_combustivel,
    SUM(f.litros_consumidos)                       AS total_litros,
    SUM(f.custo_combustivel)                       AS total_custo_combustivel,
    SUM(f.custo_total)                              AS total_custo,
    ROUND(SUM(f.custo_combustivel) * 100.0 / NULLIF(SUM(f.custo_total), 0), 2) AS pct_combustivel_no_custo
FROM fato_frota f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
GROUP BY t.ano, t.mes, e.sigla_estado, v.tipo_veiculo;

-- View: Eficiência por faixa de idade do veículo
CREATE OR REPLACE VIEW vw_eficiencia_idade AS
SELECT
    CASE
        WHEN f.idade_veiculo <= 3 THEN 'Nova (0-3)'
        WHEN f.idade_veiculo <= 6 THEN 'Jovem (4-6)'
        WHEN f.idade_veiculo <= 9 THEN 'Madura (7-9)'
        WHEN f.idade_veiculo <= 14 THEN 'Antiga (10-14)'
        ELSE 'Obsoleta (15+)'
    END AS faixa_idade,
    COUNT(DISTINCT f.sk_veiculo)                AS total_veiculos,
    ROUND(AVG(f.custo_por_km), 4)                AS custo_medio_por_km,
    ROUND(AVG(f.custo_manutencao), 2)             AS custo_medio_manutencao,
    ROUND(AVG(f.disponibilidade_pct), 1)          AS disponibilidade_media,
    ROUND(AVG(f.km_por_litro_real), 2)            AS consumo_medio_real
FROM fato_frota f
GROUP BY faixa_idade
ORDER BY custo_medio_por_km DESC;

-- View: Ranking de veículos por margem operacional
CREATE OR REPLACE VIEW vw_ranking_margem AS
SELECT
    v.id_veiculo,
    v.tipo_veiculo,
    v.marca,
    e.sigla_estado,
    ROUND(AVG(f.margem_operacional), 4)           AS margem_media,
    ROUND(AVG(f.custo_por_km), 4)                 AS custo_por_km,
    SUM(f.receita_estimada)                         AS receita_total,
    SUM(f.custo_total)                              AS custo_total
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
GROUP BY v.id_veiculo, v.tipo_veiculo, v.marca, e.sigla_estado
ORDER BY margem_media ASC;