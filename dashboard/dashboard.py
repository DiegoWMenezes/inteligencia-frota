"""
Dashboard Inteligencia de Frota - Plotly Dash
4 abas: Visao Geral, Eficiencia, Manutencao, Estrategia
Conectado ao PostgreSQL
"""

import os
import sys
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State
import dash_bootstrap_components as dbc

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, BASE_DIR)

PG_URL = "postgresql+psycopg2://postgres:adm123@localhost:5432/inteligencia_frota"
from sqlalchemy import create_engine
sa = create_engine(PG_URL)

# =====================================================================
# CARREGAR DADOS
# =====================================================================
print("[DASH] Carregando dados...")

df = pd.read_sql("""
    SELECT f.km_rodado, f.litros_consumidos, f.preco_combustivel,
           f.custo_combustivel, f.custo_manutencao, f.custo_seguro,
           f.custo_ipva, f.custo_depreciacao, f.custo_total,
           f.receita_estimada, f.viagens_realizadas,
           f.disponibilidade_pct, f.custo_por_km,
           f.margem_operacional, f.km_por_litro_real, f.idade_veiculo,
           f.sk_veiculo, f.sk_estado, f.sk_tempo,
           v.tipo_veiculo, v.id_veiculo, v.marca,
           v.consumo_medio_km_l,
           e.sigla_estado, e.regiao,
           t.ano, t.mes, t.nome_mes
    FROM fato_frota f
    JOIN dim_veiculo v ON f.sk_veiculo = v.sk_veiculo
    JOIN dim_estado e ON f.sk_estado = e.sk_estado
    JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
""", sa)

vw_mensal = pd.read_sql("SELECT * FROM vw_resumo_mensal", sa)
vw_estado = pd.read_sql("SELECT * FROM vw_kpi_estado", sa)
vw_tipo = pd.read_sql("SELECT * FROM vw_kpi_tipo_veiculo", sa)
vw_alto = pd.read_sql("SELECT * FROM vw_veiculos_alto_custo", sa)
vw_idade = pd.read_sql("SELECT * FROM vw_eficiencia_idade", sa)
vw_ranking = pd.read_sql("SELECT * FROM vw_ranking_margem", sa)

# CSVs
def load_csv(folder, prefix):
    d = os.path.join(BASE_DIR, "analytics", folder)
    try:
        fs = sorted([f for f in os.listdir(d) if f.startswith(prefix) and f.endswith(".csv")])
        return pd.read_csv(os.path.join(d, fs[-1])) if fs else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

df_decisoes = load_csv("prescriptive", "decisoes")
df_alertas = load_csv("prescriptive", "alertas")
df_forecast = load_csv("predictive", "forecast_regressao")
df_tendencia = load_csv("predictive", "forecast_tendencia")

print(f"[DASH] {len(df)} registros | {df['tipo_veiculo'].nunique()} tipos | {df['sigla_estado'].nunique()} estados")

# =====================================================================
# KPIs E CORES
# =====================================================================
C = dict(a1="#1B4F72", a2="#2E86C1", a3="#5DADE2", am="#F39C12",
         vm="#E74C3C", vd="#27AE60", ci="#BDC3C7", bg="#F8F9FA", es="#2C3E50")

kpis = dict(
    custo_km=round(df["custo_por_km"].mean(), 2),
    margem=round(df["margem_operacional"].mean() * 100, 1),
    disp=round(df["disponibilidade_pct"].mean(), 1),
    veiculos=df["sk_veiculo"].nunique(),
    consumo=round(df["km_por_litro_real"].mean(), 2),
    km=int(df["km_rodado"].sum()),
    viagens=int(df["viagens_realizadas"].sum()),
    receita=round(df["receita_estimada"].sum(), 0),
    custo_total=round(df["custo_total"].sum(), 0),
    lucro=round(df["receita_estimada"].sum() - df["custo_total"].sum(), 0),
    manutencao=round(df["custo_manutencao"].sum(), 0),
)

n_interv = len(df_decisoes[df_decisoes["decisao"] == "INTERVENCAO IMEDIATA"]) if not df_decisoes.empty else 0
n_monit = len(df_decisoes[df_decisoes["decisao"] == "MONITORAR E PLANEJAR"]) if not df_decisoes.empty else 0
n_norm = len(df_decisoes[df_decisoes["decisao"] == "OPERACAO NORMAL"]) if not df_decisoes.empty else 0

# =====================================================================
# HELPERS
# =====================================================================
def fmt(v, pre="", suf=""):
    if isinstance(v, float):
        return f"{pre}{v:,.2f}{suf}"
    return f"{pre}{v:,}{suf}"

def card(t, v, pre="", suf="", cor=C["a1"]):
    return dbc.Card(dbc.CardBody([
        html.P(t, style={"fontSize":"11px","color":"#95a5a6","marginBottom":"2px",
                         "fontWeight":"600","textTransform":"uppercase","letterSpacing":"1px"}),
        html.H3(fmt(v,pre,suf), style={"fontSize":"24px","color":cor,"fontWeight":"700","margin":"0"}),
    ]), style={"borderRadius":"10px","border":"none","boxShadow":"0 2px 6px rgba(0,0,0,0.07)",
               "backgroundColor":"white","textAlign":"center","padding":"8px"})

HIDE = {"display": "none"}
SHOW = {"display": "block"}

# =====================================================================
# APP
# =====================================================================
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Inteligencia de Frota"

# =====================================================================
# LAYOUT - todas as abas no DOM, visibilidade alternada por CSS
# =====================================================================
app.layout = dbc.Container([
    # Header
    html.Div([
        html.H2("Inteligencia de Frota", style={"color":"white","fontWeight":"700","margin":"0"}),
        #html.P("ANP + IBGE + DENATRAN | PostgreSQL | Analytics 4 Camadas",
               style={"color":"#BDC3C7","margin":"0","fontSize":"13px"}),
    ], style={"backgroundColor":C["es"],"padding":"14px 20px","borderRadius":"10px","marginBottom":"12px"}),

    dcc.Tabs(id="tabs", value="vg", children=[
        dcc.Tab(label=" Visao Geral ", value="vg", style={"fontWeight":"600"}),
        dcc.Tab(label=" Eficiencia ", value="ef", style={"fontWeight":"600"}),
        dcc.Tab(label=" Manutencao ", value="mn", style={"fontWeight":"600"}),
        dcc.Tab(label=" Estrategia ", value="es", style={"fontWeight":"600"}),
    ], style={"marginBottom":"12px"}),

    # === VISAO GERAL ===
    html.Div(id="tab-vg", children=[
        dbc.Row([
            dbc.Col(card("Custo / KM", kpis["custo_km"], "R$ "), md=3),
            dbc.Col(card("Margem", kpis["margem"], suf="%"), md=3),
            dbc.Col(card("Disponibilidade", kpis["disp"], suf="%"), md=3),
            dbc.Col(card("Veiculos", kpis["veiculos"], cor=C["vd"]), md=3),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g1"), md=6),
            dbc.Col(dcc.Graph(id="g2"), md=6),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g3"), md=6),
            dbc.Col(dcc.Graph(id="g4"), md=6),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(card("Receita", kpis["receita"], "R$ ", cor=C["vd"]), md=4),
            dbc.Col(card("Custo Total", kpis["custo_total"], "R$ ", cor=C["vm"]), md=4),
            dbc.Col(card("Lucro", kpis["lucro"], "R$ "), md=4),
        ], className="mb-3"),
    ], style=SHOW),

    # === EFICIENCIA ===
    html.Div(id="tab-ef", children=[
        dbc.Row([
            dbc.Col(card("Consumo Medio", kpis["consumo"], suf=" km/l", cor=C["a2"]), md=4),
            dbc.Col(card("KM Rodado", kpis["km"], suf=" km"), md=4),
            dbc.Col(card("Viagens", kpis["viagens"]), md=4),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g5"), md=6),
            dbc.Col(dcc.Graph(id="g6"), md=6),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g7"), md=6),
            dbc.Col(dcc.Graph(id="g8"), md=6),
        ], className="mb-3"),
    ], style=HIDE),

    # === MANUTENCAO ===
    html.Div(id="tab-mn", children=[
        dbc.Row([
            dbc.Col(card("Manutencao", kpis["manutencao"], "R$ ", cor=C["vm"]), md=6),
            dbc.Col(card("Veiculos Criticos", len(vw_alto), cor=C["vm"]), md=6),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g9"), md=6),
            dbc.Col(dcc.Graph(id="g10"), md=6),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g11"), md=6),
            dbc.Col(dcc.Graph(id="g12"), md=6),
        ], className="mb-3"),
    ], style=HIDE),

    # === ESTRATEGIA ===
    html.Div(id="tab-es", children=[
        dbc.Row([
            dbc.Col(card("Intervencao", n_interv, cor=C["vm"]), md=4),
            dbc.Col(card("Monitorar", n_monit, cor=C["am"]), md=4),
            dbc.Col(card("Normal", n_norm, cor=C["vd"]), md=4),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g13"), md=6),
            dbc.Col(dcc.Graph(id="g14"), md=6),
        ], className="mb-3"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="g15"), md=6),
            dbc.Col(dcc.Graph(id="g16"), md=6),
        ], className="mb-3"),
        dbc.Row([dbc.Col(html.Div(id="tbl1"), md=12)], className="mb-3"),
    ], style=HIDE),
], fluid=True, style={"backgroundColor":C["bg"],"minHeight":"100vh","padding":"14px"})

# =====================================================================
# CALLBACK: TROCA DE ABA (visibilidade CSS)
# =====================================================================
@app.callback(
    [Output("tab-vg","style"), Output("tab-ef","style"),
     Output("tab-mn","style"), Output("tab-es","style")],
    Input("tabs","value"),
)
def switch_tab(t):
    vis = {"vg": SHOW, "ef": SHOW, "mn": SHOW, "es": SHOW}
    return (vis[t] if t=="vg" else HIDE,
            vis[t] if t=="ef" else HIDE,
            vis[t] if t=="mn" else HIDE,
            vis[t] if t=="es" else HIDE)

# =====================================================================
# CALLBACKS: VISAO GERAL
# =====================================================================
@app.callback(Output("g1","figure"), Input("tabs","value"), prevent_initial_call=False)
def vg1(_):
    m = df.groupby(["ano","mes","nome_mes","tipo_veiculo"], as_index=False).agg(custo_km=("custo_por_km","mean"))
    f = px.line(m, x="nome_mes", y="custo_km", color="tipo_veiculo", title="Custo / KM Mensal",
                color_discrete_sequence=px.colors.qualitative.Set2)
    f.update_layout(template="plotly_white", height=340, margin=dict(t=40,b=30,l=50,r=20), legend_font_size=10)
    return f

@app.callback(Output("g2","figure"), Input("tabs","value"), prevent_initial_call=False)
def vg2(_):
    rc = df.groupby("ano",as_index=False).agg(receita=("receita_estimada","sum"),custo=("custo_total","sum"))
    f = go.Figure()
    f.add_bar(x=rc["ano"],y=rc["receita"],name="Receita",marker_color=C["vd"])
    f.add_bar(x=rc["ano"],y=rc["custo"],name="Custo",marker_color=C["vm"])
    f.update_layout(barmode="group",title="Receita vs Custo",template="plotly_white",height=340,
                    margin=dict(t=40,b=30,l=50,r=20))
    return f

@app.callback(Output("g3","figure"), Input("tabs","value"), prevent_initial_call=False)
def vg3(_):
    rg = df.groupby("regiao",as_index=False).agg(custo_km=("custo_por_km","mean")).sort_values("custo_km")
    f = px.bar(rg, y="regiao", x="custo_km", orientation="h", title="Custo / KM por Regiao",
               color="custo_km", color_continuous_scale=[C["vd"],C["am"],C["vm"]])
    f.update_layout(template="plotly_white",height=340,margin=dict(t=40,b=30,l=100,r=20),showlegend=False)
    return f

@app.callback(Output("g4","figure"), Input("tabs","value"), prevent_initial_call=False)
def vg4(_):
    cp = df.groupby("tipo_veiculo",as_index=False).agg(
        combustivel=("custo_combustivel","sum"),manutencao=("custo_manutencao","sum"),
        seguro=("custo_seguro","sum"),depreciacao=("custo_depreciacao","sum"))
    f = go.Figure()
    for col,cor,nm in [("combustivel",C["a2"],"Combustivel"),("manutencao",C["am"],"Manutencao"),
                        ("seguro",C["ci"],"Seguro"),("depreciacao",C["vm"],"Depreciacao")]:
        f.add_bar(x=cp["tipo_veiculo"],y=cp[col],name=nm,marker_color=cor)
    f.update_layout(barmode="stack",title="Composicao de Custos",template="plotly_white",height=340,
                    margin=dict(t=40,b=30,l=50,r=20))
    return f

# =====================================================================
# CALLBACKS: EFICIENCIA
# =====================================================================
@app.callback(Output("g5","figure"), Input("tabs","value"), prevent_initial_call=False)
def ef5(_):
    ct = df.groupby("tipo_veiculo",as_index=False).agg(custo_km=("custo_por_km","mean")).sort_values("custo_km")
    f = go.Figure()
    f.add_bar(y=ct["tipo_veiculo"], x=ct["custo_km"], orientation="h",
              marker_color=ct["custo_km"], marker_colorscale=[C["vd"],C["am"],C["vm"]])
    f.update_layout(title="Custo / KM por Tipo", template="plotly_white",
                    height=340, margin=dict(t=40,b=30,l=100,r=20), showlegend=False)
    return f

@app.callback(Output("g6","figure"), Input("tabs","value"), prevent_initial_call=False)
def ef6(_):
    ce = df.groupby("tipo_veiculo",as_index=False).agg(
        real=("km_por_litro_real","mean"),esperado=("consumo_medio_km_l","mean"))
    f = go.Figure()
    f.add_bar(x=ce["tipo_veiculo"],y=ce["real"],name="Real",marker_color=C["a2"])
    f.add_bar(x=ce["tipo_veiculo"],y=ce["esperado"],name="Esperado",marker_color=C["ci"])
    f.update_layout(barmode="group",title="Consumo Real vs Esperado (km/l)",
                    template="plotly_white",height=340,margin=dict(t=40,b=30,l=50,r=20))
    return f

@app.callback(Output("g7","figure"), Input("tabs","value"), prevent_initial_call=False)
def ef7(_):
    es = vw_estado.sort_values("custo_medio_por_km",ascending=True)
    f = go.Figure()
    f.add_bar(y=es["sigla_estado"], x=es["custo_medio_por_km"], orientation="h",
              marker_color=es["custo_medio_por_km"], marker_colorscale=[C["vd"],C["am"],C["vm"]])
    f.update_layout(title="Custo / KM por Estado", template="plotly_white",
                    height=600, margin=dict(t=40,b=30,l=60,r=20), showlegend=False,
                    yaxis=dict(dtick=1,tickfont=dict(size=11)))
    return f

@app.callback(Output("g8","figure"), Input("tabs","value"), prevent_initial_call=False)
def ef8(_):
    rk = vw_ranking.head(20).copy()
    rk["margem_pct"] = (rk["margem_media"] * 100).round(1)
    rk["label"] = rk["id_veiculo"] + " - " + rk["tipo_veiculo"]
    f = go.Figure()
    f.add_bar(y=rk["label"], x=rk["margem_pct"], orientation="h",
              marker_color=rk["margem_pct"], marker_colorscale=[C["vm"],C["am"],C["vd"]])
    f.update_layout(title="Margem Operacional (%) - Top 20", template="plotly_white",
                    height=400, margin=dict(t=40,b=30,l=140,r=20), showlegend=False)
    return f

# =====================================================================
# CALLBACKS: MANUTENCAO
# =====================================================================
@app.callback(Output("g9","figure"), Input("tabs","value"), prevent_initial_call=False)
def mn9(_):
    f = go.Figure()
    f.add_bar(x=vw_idade["faixa_idade"], y=vw_idade["custo_medio_por_km"],
              marker_color=vw_idade["custo_medio_por_km"], marker_colorscale=[C["vd"],C["am"],C["vm"]])
    f.update_layout(title="Custo / KM por Faixa de Idade", template="plotly_white",
                    height=340, margin=dict(t=40,b=30,l=50,r=20), showlegend=False)
    return f

@app.callback(Output("g10","figure"), Input("tabs","value"), prevent_initial_call=False)
def mn10(_):
    mc = df.groupby("ano",as_index=False).agg(manutencao=("custo_manutencao","sum"),
                                               combustivel=("custo_combustivel","sum"))
    f = go.Figure()
    f.add_bar(x=mc["ano"],y=mc["combustivel"],name="Combustivel",marker_color=C["a2"])
    f.add_bar(x=mc["ano"],y=mc["manutencao"],name="Manutencao",marker_color=C["am"])
    f.update_layout(barmode="group",title="Manutencao vs Combustivel por Ano",
                    template="plotly_white",height=340,margin=dict(t=40,b=30,l=50,r=20))
    return f

@app.callback(Output("g11","figure"), Input("tabs","value"), prevent_initial_call=False)
def mn11(_):
    if vw_alto.empty:
        return go.Figure().update_layout(title="Sem dados de veiculos alto custo", template="plotly_white")
    ac = vw_alto.head(20).copy()
    ac["label"] = ac["id_veiculo"].astype(str) + " - " + ac["tipo_veiculo"]
    f = go.Figure()
    f.add_bar(y=ac["label"], x=ac["custo_medio_por_km"], orientation="h",
              marker_color=ac["custo_medio_por_km"], marker_colorscale=[C["am"],C["vm"]],
              name="Custo/KM")
    f.add_vline(x=2.50, line_dash="dash", line_color="red", annotation_text="Critico R$2.50")
    f.update_layout(title="Veiculos Alto Custo (Top 20)", template="plotly_white",
                    height=400, margin=dict(t=40,b=30,l=160,r=20), showlegend=False)
    return f

@app.callback(Output("g12","figure"), Input("tabs","value"), prevent_initial_call=False)
def mn12(_):
    dp = df.groupby("tipo_veiculo",as_index=False).agg(disp=("disponibilidade_pct","mean"))
    f = go.Figure()
    f.add_bar(x=dp["tipo_veiculo"], y=dp["disp"],
              marker_color=[C["vm"] if d < 75 else C["am"] if d < 85 else C["vd"] for d in dp["disp"]],
              name="Disponibilidade")
    f.add_hline(y=85, line_dash="dash", line_color="orange", annotation_text="Atencao 85%")
    f.add_hline(y=75, line_dash="dash", line_color="red", annotation_text="Critico 75%")
    f.update_layout(title="Disponibilidade Media por Tipo (%)", template="plotly_white",
                    height=340, margin=dict(t=40,b=30,l=50,r=20), showlegend=False)
    return f

# =====================================================================
# CALLBACKS: ESTRATEGIA
# =====================================================================
@app.callback(Output("g13","figure"), Input("tabs","value"), prevent_initial_call=False)
def es13(_):
    if not df_decisoes.empty:
        dt = df_decisoes.groupby(["tipo_veiculo","decisao"]).size().reset_index(name="qtd")
        f = px.bar(dt, x="tipo_veiculo", y="qtd", color="decisao", barmode="stack",
                    title="Classificacao por Tipo",
                    color_discrete_map={"INTERVENCAO IMEDIATA":C["vm"],
                                        "MONITORAR E PLANEJAR":C["am"],
                                        "OPERACAO NORMAL":C["vd"]})
    else:
        f = go.Figure()
    f.update_layout(template="plotly_white",height=340,margin=dict(t=40,b=30,l=50,r=20))
    return f

@app.callback(Output("g14","figure"), Input("tabs","value"), prevent_initial_call=False)
def es14(_):
    if not df_alertas.empty:
        ak = df_alertas.groupby("kpi").size().reset_index(name="qtd")
        f = px.pie(ak, names="kpi", values="qtd", title="Alertas por KPI", hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set2)
    else:
        f = go.Figure()
    f.update_layout(template="plotly_white",height=340,margin=dict(t=40,b=30,l=50,r=20))
    return f

@app.callback(Output("g15","figure"), Input("tabs","value"), prevent_initial_call=False)
def es15(_):
    if not df_forecast.empty and not vw_mensal.empty:
        hist_labels = vw_mensal["nome_mes"] + "/" + vw_mensal["ano"].astype(str)
        fc_labels = df_forecast["mes"].astype(str) + "/" + df_forecast["ano"].astype(str)
        f = go.Figure()
        f.add_scatter(x=hist_labels, y=vw_mensal["custo_medio_por_km"],
                       name="Historico", line=dict(color=C["a2"],width=2))
        f.add_scatter(x=fc_labels, y=df_forecast["custo_por_km_previsto"],
                       name="Previsao", line=dict(color=C["am"],width=2,dash="dash"))
        f.update_layout(title="Projecao Custo / KM",template="plotly_white",
                        height=340,margin=dict(t=40,b=30,l=50,r=20))
    else:
        f = go.Figure()
    return f

@app.callback(Output("g16","figure"), Input("tabs","value"), prevent_initial_call=False)
def es16(_):
    if not df_tendencia.empty:
        f = px.bar(df_tendencia, x="tipo_veiculo", y="custo_atual",
                    title="Tendencia de Custo por Tipo",
                    color="tendencia",
                    color_discrete_map={"CRESCENTE":C["vm"],"DECRESCENTE":C["vd"]})
        f.add_scatter(x=df_tendencia["tipo_veiculo"], y=df_tendencia["custo_projecao_6m"],
                       mode="markers", name="Projecao 6m", marker=dict(color=C["am"],size=10,symbol="diamond"))
    else:
        f = go.Figure()
    f.update_layout(template="plotly_white",height=340,margin=dict(t=40,b=30,l=50,r=20))
    return f

@app.callback(Output("tbl1","children"), Input("tabs","value"), prevent_initial_call=False)
def es_tbl(_):
    if not df_decisoes.empty:
        top = df_decisoes.sort_values("prioridade").head(30)
        rows = []
        for _, r in top.iterrows():
            d = r["decisao"]
            cbg = C["vm"] if d == "INTERVENCAO IMEDIATA" else C["am"] if d == "MONITORAR E PLANEJAR" else C["vd"]
            rows.append(html.Tr([
                html.Td(str(r.get("veiculo_id","")), style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(str(r.get("tipo_veiculo","")), style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(str(r.get("marca","")), style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(str(r.get("estado","")), style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(f'{r.get("custo_por_km",0):.2f}', style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(f'{r.get("margem_operacional",0):.1%}', style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(str(r.get("idade_veiculo","")), style={"fontSize":"12px","padding":"3px 6px"}),
                html.Td(d, style={"fontSize":"11px","padding":"3px 6px","fontWeight":"600",
                                  "backgroundColor":cbg,"color":"white","borderRadius":"3px"}),
                html.Td(str(r.get("recomendacao_idade","")), style={"fontSize":"11px","padding":"3px 6px"}),
            ]))
        return dbc.Table([
            html.Thead(html.Tr([html.Th(c, style={"fontSize":"11px","fontWeight":"700","padding":"5px 6px",
                                                    "backgroundColor":C["es"],"color":"white"})
                for c in ["Veiculo","Tipo","Marca","UF","Custo/KM","Margem","Idade","Decisao","Recomendacao"]])),
            html.Tbody(rows)
        ], bordered=False, hover=True, size="sm", style={"backgroundColor":"white","borderRadius":"8px","overflow":"hidden"})
    return html.P("Sem dados", style={"textAlign":"center","color":"#95a5a6"})

# =====================================================================
# EXECUTAR
# =====================================================================
if __name__ == "__main__":
    print("[DASH] Abrindo em http://localhost:8050")
    app.run(debug=False, host="0.0.0.0", port=8050)