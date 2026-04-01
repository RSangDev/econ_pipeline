"""
dashboard/app.py — Pipeline Econômico Brasileiro
Consome o Gold layer do DuckDB construído pelo dbt.
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DW_PATH = os.path.join(ROOT, "data", "warehouse", "econ.duckdb")

st.set_page_config(page_title="Pipeline Econômico BR", page_icon="🇧🇷", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"], .block-container {
    background-color: #0a0e1a !important; color: #e2e8f0 !important;
}
[data-testid="stSidebar"], [data-testid="stSidebar"] > div {
    background-color: #060810 !important;
}
[data-testid="stSidebar"] * { color: #94a3b8 !important; }
.hero {
    background: linear-gradient(135deg, #0f2a0f 0%, #0a1f2a 50%, #1a0a2a 100%);
    border-radius: 12px; padding: 2.2rem 3rem; margin-bottom: 2rem;
    border: 1px solid #1e3a1e44;
}
.hero h1 { font-size: 2rem; color: #86efac; margin: 0; font-weight: 700; }
.hero h1 span { color: #4ade80; }
.hero p { color: #475569; font-size: 0.88rem; margin: 0.3rem 0 0; }
.kpi { background: #0d1117; border: 1px solid #1e293b;
       border-top: 3px solid #22c55e; border-radius: 8px; padding: 1.1rem 1.3rem; }
.kpi-val { font-size: 1.8rem; font-weight: 700; color: #4ade80; margin: 0.2rem 0; }
.kpi-lbl { font-size: 0.68rem; color: #475569; text-transform: uppercase;
           letter-spacing: 0.1em; font-weight: 600; }
.kpi-sub { font-size: 0.75rem; color: #334155; margin-top: 0.2rem; }
.sec { font-size: 0.95rem; font-weight: 600; color: #86efac;
       border-bottom: 1px solid #1e293b; padding-bottom: 0.4rem; margin: 1.4rem 0 0.8rem; }
</style>
""", unsafe_allow_html=True)

PL = dict(
    paper_bgcolor="#0a0e1a", plot_bgcolor="#0d1117",
    font=dict(family="Inter", color="#94a3b8", size=11),
    xaxis=dict(gridcolor="#1e293b", linecolor="#1e293b", tickfont=dict(color="#64748b")),
    yaxis=dict(gridcolor="#1e293b", linecolor="#1e293b", tickfont=dict(color="#64748b")),
    margin=dict(t=30, b=30, l=40, r=16),
)
GREEN  = "#22c55e"; BLUE = "#3b82f6"; ORANGE = "#f97316"
YELLOW = "#eab308"; RED  = "#ef4444"; PURPLE = "#a78bfa"
COLORS = [GREEN, BLUE, ORANGE, YELLOW, RED, PURPLE]


def _detect_schema(conn) -> str:
    for s in ("main", "main_gold", "gold"):
        try:
            conn.execute(f"SELECT 1 FROM {s}.gld_painel_macro LIMIT 1")
            return s
        except Exception:
            continue
    return "main"


@st.cache_data(ttl=120)
def query(sql: str) -> pd.DataFrame:
    for ro in (True, False):
        try:
            conn  = duckdb.connect(DW_PATH, read_only=ro)
            schema = _detect_schema(conn)
            resolved = sql.replace("{G}", schema)
            df = conn.execute(resolved).df()
            conn.close()
            return df
        except Exception:
            continue
    return pd.DataFrame()


def dw_ok() -> bool:
    if not os.path.exists(DW_PATH):
        return False
    for ro in (True, False):
        try:
            conn = duckdb.connect(DW_PATH, read_only=ro)
            s = _detect_schema(conn)
            conn.execute(f"SELECT 1 FROM {s}.gld_painel_macro LIMIT 1")
            conn.close()
            return True
        except Exception:
            continue
    return False


# Sidebar
with st.sidebar:
    st.markdown("""
    <div style='padding:1rem 0 1.5rem; border-bottom:1px solid #1e293b;'>
        <div style='font-size:1.1rem; font-weight:700; color:#4ade80;'>🇧🇷 Econ Pipeline</div>
        <div style='font-size:0.7rem; color:#334155; margin-top:0.3rem; line-height:1.7;'>
            IBGE · BCB · IPEA · dbt · DuckDB
        </div>
    </div>""", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    page = st.radio("Navegação", [
        "📊 Painel Macro",
        "📈 Séries Temporais",
        "🔗 Correlações",
        "⚙️ Pipeline (Airflow)",
    ], label_visibility="collapsed")
    if st.button("🔄 Recarregar"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("""
    <div style='margin-top:2rem; font-size:0.7rem; color:#1e293b; line-height:2.2;'>
        <span style='color:#f97316;'>●</span> APIs: IBGE · BCB · IPEA<br>
        <span style='color:#64748b;'>●</span> Delta Lake Bronze → Silver<br>
        <span style='color:#22c55e;'>●</span> dbt Gold → DuckDB<br>
        <span style='color:#1e293b;'>──────────</span><br>
        Airflow 2.9 · PySpark 3.5<br>
        dbt-duckdb 1.8
    </div>""", unsafe_allow_html=True)


if not dw_ok():
    st.markdown("""
    <div style='background:#0d1829; border:1px solid #1e3a5f; border-left:4px solid #22c55e;
         border-radius:8px; padding:2rem 2.5rem; margin-top:2rem;'>
        <div style='font-size:1.2rem; font-weight:700; color:#4ade80; margin-bottom:0.5rem;'>
            🇧🇷 Warehouse não encontrado
        </div>
        <p style='color:#475569; margin:0;'>Execute o pipeline primeiro.</p>
    </div>""", unsafe_allow_html=True)
    st.code("python run.py --only-pipeline", language="bash")
    st.stop()


# ═══════════════════════════════════════════════════
# PAINEL MACRO
# ═══════════════════════════════════════════════════
if page == "📊 Painel Macro":
    st.markdown("""
    <div class="hero">
        <h1>🇧🇷 Pipeline <span>Econômico</span> Brasileiro</h1>
        <p>IBGE · Banco Central · IPEA · Delta Lake · dbt · Airflow</p>
    </div>""", unsafe_allow_html=True)

    df = query("SELECT * FROM {G}.gld_painel_macro ORDER BY ano DESC")
    ultimo = df.iloc[0] if not df.empty else {}

    kpis = [
        (GREEN,  f"{ultimo.get('selic_media', 0):.1f}%",    "SELIC Média",    str(int(ultimo.get('ano', '')))),
        (RED,    f"{ultimo.get('ipca_media', 0):.2f}%",     "IPCA Médio",     "inflação mensal"),
        (YELLOW, f"R$ {ultimo.get('cambio_medio', 0):.2f}", "Câmbio USD/BRL", "média anual"),
        (ORANGE, f"{ultimo.get('desemprego_medio', 0):.1f}%","Desemprego",    "PNAD"),
        (BLUE,   f"{ultimo.get('pib_variacao_media', 0):.2f}%","PIB Var. Média","anual"),
    ]
    cols = st.columns(5)
    for col, (accent, val, lbl, sub) in zip(cols, kpis):
        with col:
            st.markdown(f"""
            <div class="kpi" style="border-top-color:{accent};">
                <div class="kpi-lbl">{lbl}</div>
                <div class="kpi-val" style="color:{accent};">{val}</div>
                <div class="kpi-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec">SELIC × IPCA por Ano</div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Bar(name="SELIC %", x=df["ano"], y=df["selic_media"],
                             marker_color=GREEN))
        fig.add_trace(go.Scatter(name="IPCA %", x=df["ano"], y=df["ipca_media"],
                                 mode="lines+markers", line=dict(color=RED, width=2.5)))
        fig.update_layout(**PL, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Câmbio USD/BRL × Desemprego</div>', unsafe_allow_html=True)
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(name="Câmbio", x=df["ano"], y=df["cambio_medio"],
                                  mode="lines+markers", line=dict(color=YELLOW, width=2.5)))
        fig2.add_trace(go.Scatter(name="Desemprego %", x=df["ano"], y=df["desemprego_medio"],
                                  mode="lines+markers", line=dict(color=ORANGE, width=2.5),
                                  yaxis="y2"))
        fig2.update_layout(**PL, height=320,
                           yaxis2=dict(overlaying="y", side="right",
                                       gridcolor="#1e293b", tickfont=dict(color="#64748b")),
                           legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig2, width="stretch")

    st.markdown('<div class="sec">Tabela Completa — Gold Layer</div>', unsafe_allow_html=True)
    st.dataframe(df, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════
# SÉRIES TEMPORAIS
# ═══════════════════════════════════════════════════
elif page == "📈 Séries Temporais":
    st.markdown("""
    <div class="hero">
        <h1>📈 Séries <span>Temporais</span></h1>
        <p>gld_serie_mensal · Variação YoY por indicador</p>
    </div>""", unsafe_allow_html=True)

    df = query("SELECT * FROM {G}.gld_serie_mensal ORDER BY indicador, ano, mes")
    indicadores = sorted(df["indicador"].unique()) if not df.empty else []
    sel = st.multiselect("Indicadores", indicadores, default=indicadores[:3])
    df_f = df[df["indicador"].isin(sel)] if sel else df

    df_f["periodo_dt"] = pd.to_datetime(
        df_f["ano"].astype(str) + "-" + df_f["mes"].astype(str).str.zfill(2) + "-01"
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec">Valor por Período</div>', unsafe_allow_html=True)
        fig = px.line(df_f, x="periodo_dt", y="valor", color="indicador",
                      color_discrete_sequence=COLORS, markers=False)
        fig.update_traces(line_width=2)
        fig.update_layout(**PL, height=340, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Variação YoY (%)</div>', unsafe_allow_html=True)
        df_yoy = df_f.dropna(subset=["variacao_yoy"])
        fig2 = px.line(df_yoy, x="periodo_dt", y="variacao_yoy", color="indicador",
                       color_discrete_sequence=COLORS)
        fig2.add_hline(y=0, line_dash="dot", line_color="#334155")
        fig2.update_layout(**PL, height=340, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig2, width="stretch")


# ═══════════════════════════════════════════════════
# CORRELAÇÕES
# ═══════════════════════════════════════════════════
elif page == "🔗 Correlações":
    st.markdown("""
    <div class="hero">
        <h1>🔗 <span>Correlações</span> entre Indicadores</h1>
        <p>gld_correlacoes · Quando o câmbio sobe, o IPCA sobe?</p>
    </div>""", unsafe_allow_html=True)

    df = query("SELECT * FROM {G}.gld_correlacoes ORDER BY ano")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec">SELIC × IPCA × Câmbio</div>', unsafe_allow_html=True)
        fig = go.Figure()
        for col_name, color, name in [
            ("selic_media", GREEN, "SELIC %"),
            ("ipca_media", RED, "IPCA %"),
        ]:
            fig.add_trace(go.Scatter(x=df["ano"], y=df[col_name],
                                     name=name, mode="lines+markers",
                                     line=dict(color=color, width=2.5)))
        fig.update_layout(**PL, height=320, legend=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.markdown('<div class="sec">Câmbio vs IPCA Defasado</div>', unsafe_allow_html=True)
        fig2 = px.bar(df, x="ano", y="cambio_vs_ipca_defasado",
                      color="cambio_vs_ipca_defasado",
                      color_continuous_scale=[[0, RED], [0.5, "#334155"], [1, GREEN]])
        fig2.add_hline(y=0, line_dash="dot", line_color="#475569")
        fig2.update_layout(**{k: v for k, v in PL.items() if k != "margin"},
                           margin=dict(t=30, b=30, l=40, r=60),
                           height=320, coloraxis_showscale=False)
        st.plotly_chart(fig2, width="stretch")

    st.markdown('<div class="sec">Tabela de Correlações</div>', unsafe_allow_html=True)
    st.dataframe(df, width="stretch", hide_index=True)


# ═══════════════════════════════════════════════════
# PIPELINE / AIRFLOW
# ═══════════════════════════════════════════════════
elif page == "⚙️ Pipeline (Airflow)":
    st.markdown("""
    <div class="hero">
        <h1>⚙️ Orquestração com <span>Airflow</span></h1>
        <p>DAG com TaskGroups · dependências complexas · ShortCircuitOperator</p>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec">Grafo de Dependências da DAG</div>', unsafe_allow_html=True)
    st.code("""
econ_pipeline_brasil  (schedule: 0 6 * * *)

  ┌─[TaskGroup: ibge]──────────────────────────┐
  │  fetch_ibge_pib ──┐                        │
  │                   ├──► validate_ibge ──► bronze_ibge
  │  fetch_ibge_pop ──┘                        │
  └────────────────────────────────────────────┘
              │
  ┌─[TaskGroup: bcb]───────────────────────────┐     ┌── silver_unify (ACID MERGE)
  │  fetch_bcb_selic  ──┐                      │     │       │
  │  fetch_bcb_ipca   ──┼──► validate_bcb ──► bronze_bcb ───┤
  │  fetch_bcb_cambio ──┘                      │     │       │
  └────────────────────────────────────────────┘     │   [TaskGroup: gold]
              │                                      │     bridge_to_duckdb
  ┌─[TaskGroup: ipea]──────────────────────────┐     │     dbt_run
  │  fetch_ipea_desemp ──┐                     │     │     dbt_test
  │                      ├──► validate_ipea ──► bronze_ipea │
  │  fetch_ipea_gini  ───┘                     │     │       │
  └────────────────────────────────────────────┘     └───────┘
                                                          │
                                                    notify_success
""", language="text")

    steps = [
        ("fetch_*",         "PythonOperator",       "Chama a API, salva CSV em data/raw/. Roda em paralelo dentro do TaskGroup."),
        ("validate_*",      "ShortCircuitOperator", "Valida contagem mínima de registros. Se falhar, pula o bronze daquela fonte sem derrubar o pipeline inteiro."),
        ("bronze_*",        "PythonOperator",       "PySpark lê o CSV e escreve no Delta Lake Bronze com schema enforcement."),
        ("silver_unify",    "PythonOperator",       "Aguarda TODOS os bronzes. Unifica fontes e executa ACID MERGE no Delta Lake Silver."),
        ("bridge_to_duckdb","PythonOperator",       "Exporta Silver → Parquet → DuckDB view para o dbt consumir."),
        ("dbt_run",         "BashOperator",         "Materializa os 3 modelos gold no DuckDB."),
        ("dbt_test",        "BashOperator",         "Valida qualidade com trigger_rule=all_done — roda mesmo se dbt_run tiver warnings."),
        ("notify_success",  "PythonOperator",       "Imprime resumo. Em produção: Slack webhook ou email."),
    ]

    for task, op, desc in steps:
        st.markdown(f"""
        <div style='background:#0d1117; border:1px solid #1e293b; border-left:3px solid #22c55e;
             border-radius:6px; padding:0.9rem 1.2rem; margin:0.3rem 0;'>
            <span style='color:#4ade80; font-weight:600; font-size:0.88rem;'>{task}</span>
            <span style='color:#334155; font-size:0.78rem; margin-left:0.8rem;'>{op}</span>
            <div style='color:#475569; font-size:0.8rem; margin-top:0.3rem;'>{desc}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec">Como rodar o Airflow</div>', unsafe_allow_html=True)
    st.code("""# Com Docker (recomendado)
cd docker
docker compose up -d
# Acesse: http://localhost:8080  (admin / admin)

# Sem Docker (local)
pip install apache-airflow==2.9.1
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow users create --username admin --password admin \\
  --firstname A --lastname B --role Admin --email a@b.com
airflow webserver &
airflow scheduler""", language="bash")
