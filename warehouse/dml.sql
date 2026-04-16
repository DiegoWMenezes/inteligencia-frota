-- =============================================================================
-- DML - Data Manipulation Language
-- Inserts de referência e dados de apoio para o DW Inteligência de Frota
-- =============================================================================

-- Regiões do Brasil (dimensão auxiliar já coberta por dim_estado)
-- Estes inserts são para dados de referência complementares

-- Tipos de combustível com custo médio de manutenção por km (dados de referência)
INSERT OR REPLACE INTO ref_combustivel (tipo_combustivel, custo_manutencao_por_km, emissoes_co2_g_km) VALUES
    ('Diesel',           0.35,  158.0),
    ('Flex',             0.22,  120.0),
    ('Gasolina',         0.25,  130.0),
    ('GNV',              0.18,   95.0),
    ('Eletrico',         0.12,    0.0),
    ('N/A',              0.00,    0.0);

-- Faixas de idade para análise de renovação de frota
INSERT OR REPLACE INTO ref_faixa_idade (faixa, idade_min, idade_max, recomendacao) VALUES
    ('Nova',       0, 3,  'Manutenção preventiva padrão'),
    ('Jovem',      4, 6,  'Acompanhar custo de manutenção'),
    ('Madura',     7, 9,  'Planejar substituição'),
    ('Antiga',    10, 14, 'Avaliar substituição imediata'),
    ('Obsoleta',  15, 99, 'Substituir obrigatoriamente');

-- Limites de KPIs para alertas prescritivos
INSERT OR REPLACE INTO ref_limites_kpi (kpi, valor_limite, tipo_alerta, acao_recomendada) VALUES
    ('custo_por_km',        2.50,  'CRITICO', 'Substituir ou revisar operação'),
    ('custo_por_km',        1.80,  'ATENCAO', 'Monitorar e planejar manutenção'),
    ('margem_operacional',  0.10,  'CRITICO', 'Rever rota e composição de custos'),
    ('margem_operacional',  0.20,  'ATENCAO', 'Otimizar custos operacionais'),
    ('disponibilidade_pct', 85.0,  'ATENCAO', 'Verificar manutenção preventiva'),
    ('disponibilidade_pct', 75.0,  'CRITICO', 'Frota com baixa disponibilidade'),
    ('km_por_litro_real',   3.0,   'CRITICO', 'Verificar consumo anômalo'),
    ('km_por_litro_real',   4.5,   'ATENCAO', 'Monitorar eficiência do veículo');