# Inteligencia de Frota

Pipeline de inteligencia analitica para gestao de frotas (Caminhão, Pickups e Sedan).

Ingestao de dados reais (ANP, IBGE, DENATRAN), ETL, Data Warehouse, Analytics em 4 camadas e Dashboard interativo.

---

## O que faz

Integra 3 fontes de dados reais do governo brasileiro em um pipeline que vai da ingestao bruta ate recomendacoes de acao para cada veiculo da frota.

A frota e de uma empresa de saneamento: **Caminhoes** (60%), **Pickups** (20%) e **Carros Sedan** (20%).

O pipeline responde 4 perguntas:

- **Descritiva** - O que esta acontecendo? KPIs de custo, consumo e utilizacao
- **Diagnostica** - Por que? Causa raiz: combustivel, manutencao ou idade
- **Preditiva** - O que vai acontecer? Forecast de custo/km (R2=0.95)
- **Prescritiva** - O que fazer? Acao concreta: substituir, monitorar ou manter

---

## Fluxo do pipeline

Dados reais (ANP, IBGE, DENATRAN) entram na ingestao, passam por ETL (limpeza, padronizacao, enriquecimento por UF), sao carregados no Data Warehouse PostgreSQL com modelo estrela, e alimentam as 4 camadas de analytics com dashboard web.

---

## Tecnologias

Python, PostgreSQL, SQLite, scikit-learn, statsmodels, Pandas, NumPy, Plotly Dash, Apache Airflow, YAML

---

## Fontes de dados

**ANP** - Precos de combustiveis por UF (27 estados, 6 produtos, 12 meses, 1.944 registros)

**IBGE** - IPCA, PIB per capita, Salario medio, Populacao (27 estados, 7 anos, 756 registros)

**DENATRAN** - Frota por tipo e UF (27 estados, 18 tipos, 7 anos, 3.402 registros)

**Operacional** - Dados de frota simulados baseados nas estatisticas oficiais (500 veiculos, 24 meses, 12.000 registros)

Quando as fontes oficiais estao indisponiveis (URLs mudam sem aviso), o pipeline gera dados baseados nas estatisticas reais publicadas por cada orgao.

---

## Frota

Caminhao: 304 veiculos (60%) - core operacional

Pickup: 100 veiculos (20%) - apoio de campo

Carro Sedan: 96 veiculos (20%) - deslocamento administrativo

---

## Data Warehouse

Modelo estrela com fato_frota no centro, ligada a dim_tempo, dim_estado e dim_veiculo.

dim_tempo: 24 registros (sk_tempo, ano, mes, trimestre)

dim_estado: 27 registros (sk_estado, sigla, regiao)

dim_veiculo: 500 registros (sk_veiculo, tipo, marca, ano, combustivel, status)

fato_frota: 2.500 registros (sk_fato_frota, sk_veiculo, sk_estado, sk_tempo, km, litros, precos, custos, receita, margem, custo/km)

Views: vw_resumo_mensal, vw_kpi_estado, vw_kpi_tipo_veiculo, vw_veiculos_alto_custo, vw_impacto_combustivel, vw_eficiencia_idade, vw_ranking_margem

---

## Analytics

**Descritiva** - Custo/km decomposto em combustivel, manutencao e componente fixo. KPIs: custo/km, consumo medio (km/l), utilizacao da frota (%), composicao de custos (%), margem (%).

**Diagnostica** - Identifica consumo anomalo (>30% abaixo do esperado), impacto do preco de combustivel no custo, principal causa por faixa de idade, anomalias regionais.

**Preditiva** - Regressao linear (R2=0.95, MAE=0.004), ARIMA(1,1,1) com AIC=-15.66, tendencia por tipo (Caminhao=DECRESCENTE, Pickup=CRESCENTE). Forecast de 6 meses para custo/km.

**Prescritiva** - 1.000 veiculos avaliados em 4 KPIs contra limites criticos: 636 com intervencao imediata, 244 para monitorar, 120 em operacao normal.

Limites: custo_por_km > R$2.50, margem < 10%, disponibilidade < 75%, km_por_litro < 3.0.

---

## Dashboard

4 abas conectadas ao PostgreSQL em tempo real.

**Visao Geral** - KPIs financeiros, custo/KM mensal por tipo, receita vs custo, composicao de custos

<img src="img/visao_geral.png" width="800">

**Eficiencia** - Consumo real vs esperado, custo/KM por estado, ranking de margem

<img src="img/eficiencia.png" width="800">

**Manutencao** - Custo por faixa de idade, manutencao vs combustivel, veiculos alto custo, disponibilidade

<img src="img/manutencao.png" width="800">

**Estrategia** - Classificacao prescritiva, alertas por KPI, forecast, tabela de acoes

<img src="img/estrategia.png" width="800">

Para rodar: `python dashboard/dashboard.py` e acessar http://localhost:8050

---

## Estrutura

**ingestion/** - anp_fuel.py (precos combustivel), ibge_api.py (indicadores economicos), denatran_loader.py (base de frota), fleet_generator.py (gerador de frota)

**etl/** - transform.py (limpeza, padronizacao, enriquecimento)

**data/raw/** - dados brutos em CSV

**staging/** - dados processados

**load.py** - carga no Data Warehouse

**warehouse/** - ddl.sql (modelo estrela), dml.sql (dados referencia), views.sql (7 views)

**analytics/descriptive/** - kpis.sql

**analytics/diagnostic/** - root_cause.sql

**analytics/predictive/** - forecast.py

**analytics/prescriptive/** - decision_rules.py

**orchestration/** - fleet_pipeline_dag.py (DAG Airflow + standalone)

**dashboard/** - dashboard.py

**config/** - settings.yaml

---

## Como rodar

Instalar:

```
git clone https://github.com/DiegoWMenezes/inteligencia-frota.git
cd inteligencia-frota
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Pipeline completo:

```
psql -U postgres -c "CREATE DATABASE inteligencia_frota;"
python orchestration/fleet_pipeline_dag.py
```

Passo a passo:

```
python ingestion/anp_fuel.py
python ingestion/ibge_api.py
python ingestion/denatran_loader.py
python ingestion/fleet_generator.py
python etl/transform.py
python load.py --engine postgresql
python analytics/predictive/forecast.py
python analytics/prescriptive/decision_rules.py
```

Dashboard:

```
python dashboard/dashboard.py
```

Airflow:

```
export AIRFLOW_HOME=./airflow
airflow db init
cp orchestration/fleet_pipeline_dag.py $AIRFLOW_HOME/dags/
airflow scheduler &
airflow webserver
airflow dags trigger fleet_intelligence_pipeline
```

Consultas:

```
psql -U postgres -d inteligencia_frota -c "SELECT * FROM vw_resumo_mensal;"
psql -U postgres -d inteligencia_frota -c "SELECT * FROM vw_kpi_estado ORDER BY custo_medio_por_km DESC;"
psql -U postgres -d inteligencia_frota -c "SELECT * FROM vw_veiculos_alto_custo LIMIT 10;"
```


---

## Autor

Diego Menezes - Data Analyst | Python | SQL | Power BI
