-- =============================================================================
-- ANALYTICS DIAGNÓSTICA - Análise de Causa Raiz
-- Respostas: Por quê? Qual o impacto?
-- =============================================================================

-- DIAGNÓSTICO 1: Impacto do preço do combustível no custo operacional
-- Correlação entre variação de preço e custo total
SELECT
    t.ano,
    t.mes,
    v.tipo_veiculo,
    e.sigla_estado,
    ROUND(AVG(f.preco_combustivel), 2)                 AS preco_combustivel,
    ROUND(AVG(f.custo_por_km), 4)                       AS custo_por_km,
    ROUND(AVG(f.custo_combustivel), 2)                  AS custo_combustivel,
    ROUND(AVG(f.custo_combustivel / NULLIF(f.custo_total, 0)) * 100, 1) AS impacto_pct,
    ROUND(LAG(AVG(f.preco_combustivel)) OVER (PARTITION BY v.tipo_veiculo ORDER BY t.ano, t.mes), 2) AS preco_anterior,
    ROUND(
        (AVG(f.preco_combustivel) - LAG(AVG(f.preco_combustivel)) OVER (
            PARTITION BY v.tipo_veiculo ORDER BY t.ano, t.mes
        )) / NULLIF(LAG(AVG(f.preco_combustivel)) OVER (
            PARTITION BY v.tipo_veiculo ORDER BY t.ano, t.mes
        ), 0) * 100, 1
    ) AS variacao_preco_pct
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano, t.mes, v.tipo_veiculo, e.sigla_estado;

-- DIAGNÓSTICO 2: Veículos com maior custo - ranking de ineficiência
SELECT
    v.id_veiculo,
    v.tipo_veiculo,
    v.marca,
    v.ano_fabricacao,
    v.status,
    e.sigla_estado,
    f.idade_veiculo,
    ROUND(AVG(f.custo_por_km), 4)             AS custo_por_km,
    ROUND(AVG(f.custo_combustivel), 2)         AS custo_combustivel_medio,
    ROUND(AVG(f.custo_manutencao), 2)         AS custo_manutencao_medio,
    ROUND(AVG(f.disponibilidade_pct), 1)      AS disponibilidade,
    ROUND(AVG(f.km_por_litro_real), 2)        AS consumo_real,
    v.consumo_medio_km_l                       AS consumo_esperado,
    ROUND((v.consumo_medio_km_l - AVG(f.km_por_litro_real)) / NULLIF(v.consumo_medio_km_l, 0) * 100, 1)
        AS desvio_consumo_pct,
    CASE
        WHEN AVG(f.custo_por_km) > 2.50 THEN 'CRITICO'
        WHEN AVG(f.custo_por_km) > 1.80 THEN 'ATENCAO'
        ELSE 'NORMAL'
    END AS classificacao_custo
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
GROUP BY v.id_veiculo, v.tipo_veiculo, v.marca, v.ano_fabricacao,
         v.status, e.sigla_estado, f.idade_veiculo, v.consumo_medio_km_l
HAVING AVG(f.custo_por_km) > 1.00
ORDER BY AVG(f.custo_por_km) DESC;

-- DIAGNÓSTICO 3: Causa raiz de alto custo - decomposição por componente
SELECT
    v.tipo_veiculo,
    f.idade_veiculo,
    CASE
        WHEN f.idade_veiculo <= 3  THEN '0-3 anos'
        WHEN f.idade_veiculo <= 6  THEN '4-6 anos'
        WHEN f.idade_veiculo <= 9  THEN '7-9 anos'
        WHEN f.idade_veiculo <= 14 THEN '10-14 anos'
        ELSE '15+ anos'
    END AS faixa_idade,
    COUNT(*)                                           AS num_registros,
    ROUND(AVG(f.custo_por_km), 4)                      AS custo_por_km,
    ROUND(AVG(f.custo_combustivel / NULLIF(f.km_rodado, 0)), 4) AS custo_comb_km,
    ROUND(AVG(f.custo_manutencao / NULLIF(f.km_rodado, 0)), 4) AS custo_man_km,
    ROUND(AVG(f.custo_seguro / NULLIF(f.km_rodado, 0)), 4)     AS custo_seg_km,
    ROUND(AVG(f.custo_depreciacao / NULLIF(f.km_rodado, 0)), 4) AS custo_dep_km,
    -- Identifica o maior componente de custo
    CASE
        WHEN AVG(f.custo_combustivel / NULLIF(f.km_rodado, 0)) >
             AVG(f.custo_manutencao / NULLIF(f.km_rodado, 0))
             AND AVG(f.custo_combustivel / NULLIF(f.km_rodado, 0)) >
             AVG(f.custo_depreciacao / NULLIF(f.km_rodado, 0))
        THEN 'COMBUSTIVEL'
        WHEN AVG(f.custo_manutencao / NULLIF(f.km_rodado, 0)) >
             AVG(f.custo_depreciacao / NULLIF(f.km_rodado, 0))
        THEN 'MANUTENCAO'
        ELSE 'DEPRECIACAO'
    END AS principal_causa
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
GROUP BY v.tipo_veiculo, f.idade_veade, faixa_idade
ORDER BY v.tipo_veiculo, faixa_idade;

-- DIAGNÓSTICO 4: Correlação idade vs custo de manutenção
SELECT
    f.idade_veiculo,
    COUNT(DISTINCT f.sk_veiculo)                     AS total_veiculos,
    ROUND(AVG(f.custo_manutencao), 2)                AS custo_manutencao_medio,
    ROUND(AVG(f.custo_por_km), 4)                     AS custo_por_km_medio,
    ROUND(AVG(f.disponibilidade_pct), 1)             AS disponibilidade,
    ROUND(AVG(f.km_por_litro_real), 2)               AS consumo_real
FROM fato_frota f
GROUP BY f.idade_veiculo
ORDER BY f.idade_veiculo;

-- DIAGNÓSTICO 5: Anomalias de consumo por estado
SELECT
    e.sigla_estado,
    e.regiao,
    v.tipo_veiculo,
    ROUND(AVG(f.preco_combustivel), 2)              AS preco_combustivel,
    ROUND(AVG(f.km_por_litro_real), 2)               AS consumo_real,
    v.consumo_medio_km_l                               AS consumo_esperado,
    ROUND((v.consumo_medio_km_l - AVG(f.km_por_litro_real)) / v.consumo_medio_km_l * 100, 1)
        AS desvio_consumo_pct,
    CASE
        WHEN AVG(f.km_por_litro_real) < v.consumo_medio_km_l * 0.7 THEN 'ANOMALO'
        WHEN AVG(f.km_por_litro_real) < v.consumo_medio_km_l * 0.85 THEN 'SUBOTIMO'
        ELSE 'NORMAL'
    END AS status_consumo
FROM fato_frota f
JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
JOIN dim_estado e ON f.sk_estado = e.sk_estado
GROUP BY e.sigla_estado, e.regiao, v.tipo_veiculo, v.consumo_medio_km_l
ORDER BY desvio_consumo_pct DESC;