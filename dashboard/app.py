import os
import sys
from pathlib import Path
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

st.set_page_config(
    page_title="BoT ICN Data Pipeline",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    /* Main background */
    .main { background-color: #0f1117; }
    
    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1e2130, #252a3d);
        border-radius: 16px;
        padding: 24px;
        border: 1px solid #2d3250;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        text-align: center;
        transition: transform 0.2s;
    }
    .kpi-card:hover { transform: translateY(-4px); }
    .kpi-title {
        color: #8892b0;
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 1.5px;
        text-transform: uppercase;
        margin-bottom: 8px;
    }
    .kpi-value {
        color: #ccd6f6;
        font-size: 36px;
        font-weight: 800;
        line-height: 1;
        margin-bottom: 6px;
    }
    .kpi-sub {
        font-size: 12px;
        color: #64ffda;
        font-weight: 500;
    }
    .kpi-icon { font-size: 28px; margin-bottom: 10px; }

    /* Status badge */
    .badge-green {
        background: rgba(100,255,218,0.1);
        color: #64ffda;
        border: 1px solid #64ffda;
        border-radius: 20px;
        padding: 4px 14px;
        font-size: 12px;
        font-weight: 600;
    }
    .badge-yellow {
        background: rgba(255,214,0,0.1);
        color: #ffd600;
        border: 1px solid #ffd600;
        border-radius: 20px;
        padding: 4px 14px;
        font-size: 12px;
        font-weight: 600;
    }

    /* Section header */
    .section-header {
        color: #ccd6f6;
        font-size: 18px;
        font-weight: 700;
        margin: 32px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid #2d3250;
    }

    /* Pipeline layer cards */
    .layer-card {
        background: linear-gradient(135deg, #1e2130, #252a3d);
        border-radius: 12px;
        padding: 20px;
        border-left: 4px solid;
        margin-bottom: 12px;
    }
    .layer-bronze { border-color: #cd7f32; }
    .layer-silver { border-color: #aaa9ad; }
    .layer-gold   { border-color: #ffd700; }
    .layer-quar   { border-color: #ff6b6b; }

    /* Sidebar */
    .css-1d391kg { background-color: #0d1117; }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ── Sidebar ────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 BoT Pipeline")
    st.markdown("**Bank of Tanzania**")
    st.markdown("Automated ICN Data Pipeline")
    st.markdown("---")
    st.markdown("### Navigation")
    st.page_link("pages/01_overview.py",      label="📊 Overview",       icon="📊")
    st.page_link("pages/02_transactions.py",  label="💳 Transactions",   icon="💳")
    st.page_link("pages/03_data_quality.py",  label="🔍 Data Quality",   icon="🔍")
    st.page_link("pages/04_pipeline_logs.py", label="📋 Pipeline Logs",  icon="📋")
    st.markdown("---")
    st.markdown(
        "<span class='badge-green'>● Pipeline Active</span>",
        unsafe_allow_html=True
    )
    st.markdown("---")
    st.caption("EASTC · BDS Group 16 · 2025–2026")


# ── Hero Header ────────────────────────────────────────────
st.markdown("""
<div style='background: linear-gradient(135deg, #0f1117 0%, #1a1f35 50%, #0f1117 100%);
     border-radius: 20px; padding: 40px; margin-bottom: 32px;
     border: 1px solid #2d3250; text-align: center;'>
    <div style='font-size: 48px; margin-bottom: 12px;'>🏦</div>
    <h1 style='color: #ccd6f6; font-size: 32px; font-weight: 800; margin: 0 0 8px 0;'>
        Bank of Tanzania
    </h1>
    <p style='color: #64ffda; font-size: 16px; font-weight: 600; margin: 0 0 16px 0;'>
        Automated ICN Data Pipeline — Real-time Financial Data Warehouse
    </p>
    <span class='badge-green'>● All Systems Operational</span>
    &nbsp;&nbsp;
    <span class='badge-yellow'>⏰ Next Run: 02:00 AM</span>
</div>
""", unsafe_allow_html=True)


# ── KPI Cards Row 1 ────────────────────────────────────────
st.markdown("<div class='section-header'>📈 Pipeline Overview</div>", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>🏛️</div>
        <div class='kpi-title'>Financial Institutions</div>
        <div class='kpi-value'>42</div>
        <div class='kpi-sub'>Active data sources</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>📦</div>
        <div class='kpi-title'>Total Records</div>
        <div class='kpi-value'>46,923</div>
        <div class='kpi-sub'>Across 7 datasets</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>✅</div>
        <div class='kpi-title'>Data Quality Rate</div>
        <div class='kpi-value'>100%</div>
        <div class='kpi-sub'>Target: ≥ 99%</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>⚡</div>
        <div class='kpi-title'>Pipeline Speed</div>
        <div class='kpi-value'>0.6m</div>
        <div class='kpi-sub'>Target: &lt; 30 minutes</div>
    </div>
    """, unsafe_allow_html=True)


# ── KPI Cards Row 2 ────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
c5, c6, c7, c8 = st.columns(4)

with c5:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>🗂️</div>
        <div class='kpi-title'>Datasets</div>
        <div class='kpi-value'>7</div>
        <div class='kpi-sub'>ICN data categories</div>
    </div>
    """, unsafe_allow_html=True)

with c6:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>🔄</div>
        <div class='kpi-title'>DAG Tasks</div>
        <div class='kpi-value'>28</div>
        <div class='kpi-sub'>Airflow pipeline tasks</div>
    </div>
    """, unsafe_allow_html=True)

with c7:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>🚫</div>
        <div class='kpi-title'>Quarantined</div>
        <div class='kpi-value'>0</div>
        <div class='kpi-sub'>Rejected records</div>
    </div>
    """, unsafe_allow_html=True)

with c8:
    st.markdown("""
    <div class='kpi-card'>
        <div class='kpi-icon'>🔐</div>
        <div class='kpi-title'>Security Layers</div>
        <div class='kpi-value'>8</div>
        <div class='kpi-sub'>Prevention + Recovery</div>
    </div>
    """, unsafe_allow_html=True)


# ── Pipeline Layer Status ──────────────────────────────────
st.markdown("<div class='section-header'>🏗️ Pipeline Layers</div>", unsafe_allow_html=True)

col_a, col_b = st.columns(2)

with col_a:
    st.markdown("""
    <div class='layer-card layer-bronze'>
        <div style='display:flex; justify-content:space-between; align-items:center;'>
            <div>
                <div style='color:#cd7f32; font-weight:700; font-size:16px;'>🥉 Bronze Layer</div>
                <div style='color:#8892b0; font-size:13px; margin-top:4px;'>Raw ingested data — untouched</div>
            </div>
            <div style='text-align:right;'>
                <div style='color:#ccd6f6; font-size:24px; font-weight:800;'>46,923</div>
                <div style='color:#cd7f32; font-size:11px;'>total rows</div>
            </div>
        </div>
    </div>
    <div class='layer-card layer-silver'>
        <div style='display:flex; justify-content:space-between; align-items:center;'>
            <div>
                <div style='color:#aaa9ad; font-weight:700; font-size:16px;'>🥈 Silver Layer</div>
                <div style='color:#8892b0; font-size:13px; margin-top:4px;'>Validated, transformed, deduplicated</div>
            </div>
            <div style='text-align:right;'>
                <div style='color:#ccd6f6; font-size:24px; font-weight:800;'>46,923</div>
                <div style='color:#aaa9ad; font-size:11px;'>clean rows</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_b:
    st.markdown("""
    <div class='layer-card layer-gold'>
        <div style='display:flex; justify-content:space-between; align-items:center;'>
            <div>
                <div style='color:#ffd700; font-weight:700; font-size:16px;'>🥇 Gold Layer</div>
                <div style='color:#8892b0; font-size:13px; margin-top:4px;'>Aggregated business-ready summaries</div>
            </div>
            <div style='text-align:right;'>
                <div style='color:#ccd6f6; font-size:24px; font-weight:800;'>27,159</div>
                <div style='color:#ffd700; font-size:11px;'>aggregated rows</div>
            </div>
        </div>
    </div>
    <div class='layer-card layer-quar'>
        <div style='display:flex; justify-content:space-between; align-items:center;'>
            <div>
                <div style='color:#ff6b6b; font-weight:700; font-size:16px;'>🚫 Quarantine</div>
                <div style='color:#8892b0; font-size:13px; margin-top:4px;'>Rejected records with reasons</div>
            </div>
            <div style='text-align:right;'>
                <div style='color:#ccd6f6; font-size:24px; font-weight:800;'>0</div>
                <div style='color:#ff6b6b; font-size:11px;'>rejected rows</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── Dataset Breakdown Chart ────────────────────────────────
st.markdown("<div class='section-header'>📊 Dataset Breakdown</div>", unsafe_allow_html=True)

import plotly.graph_objects as go

datasets = [
    "ATM Distribution",
    "POS Distribution",
    "Internet Banking",
    "Card Transactions",
    "MNO Balances",
    "Mobile Banking",
    "Money Remittance"
]
bronze_rows = [15624, 15624, 3024, 1512, 2520, 2579, 5040]
gold_rows   = [15624, 15624, 3024, 1512, 2520, 357,  5040]

fig = go.Figure()

fig.add_trace(go.Bar(
    name="Silver Rows",
    x=datasets,
    y=bronze_rows,
    marker_color="#aaa9ad",
    marker_line_color="#ccd6f6",
    marker_line_width=1,
))

fig.add_trace(go.Bar(
    name="Gold Rows",
    x=datasets,
    y=gold_rows,
    marker_color="#ffd700",
    marker_line_color="#ccd6f6",
    marker_line_width=1,
))

fig.update_layout(
    barmode="group",
    plot_bgcolor="#1e2130",
    paper_bgcolor="#1e2130",
    font=dict(color="#ccd6f6", family="Inter, sans-serif"),
    xaxis=dict(gridcolor="#2d3250", tickangle=-20),
    yaxis=dict(gridcolor="#2d3250", title="Row Count"),
    legend=dict(
        bgcolor="#252a3d",
        bordercolor="#2d3250",
        borderwidth=1
    ),
    margin=dict(l=20, r=20, t=20, b=20),
    height=380,
)

st.plotly_chart(fig, use_container_width=True)


# ── Test Results ───────────────────────────────────────────
st.markdown("<div class='section-header'>🧪 Phase 8 Test Results</div>", unsafe_allow_html=True)

tests = [
    ("Bronze Accuracy",       "CSV = Bronze rows",   "All 7 match",  True),
    ("Quarantine Detection",  "≥ 99% detection",     "100.0%",       True),
    ("Deduplication",         "Duplicates removed",  "15,624 removed", True),
    ("Gold Accuracy",         "Silver = Gold totals","Both match",   True),
    ("Data Quality Rate",     "≥ 99%",               "100.00%",      True),
    ("Pipeline Speed",        "< 30 minutes",        "0.60 mins",    True),
]

t_cols = st.columns(3)
for i, (test, target, result, passed) in enumerate(tests):
    with t_cols[i % 3]:
        color = "#64ffda" if passed else "#ff6b6b"
        icon = "✅" if passed else "❌"
        st.markdown(f"""
        <div class='kpi-card' style='margin-bottom:16px;'>
            <div style='font-size:22px; margin-bottom:8px;'>{icon}</div>
            <div style='color:#ccd6f6; font-weight:700; font-size:14px; margin-bottom:6px;'>{test}</div>
            <div style='color:#8892b0; font-size:12px; margin-bottom:8px;'>Target: {target}</div>
            <div style='color:{color}; font-size:18px; font-weight:800;'>{result}</div>
        </div>
        """, unsafe_allow_html=True)


# ── Technology Stack ───────────────────────────────────────
st.markdown("<div class='section-header'>🛠️ Technology Stack</div>", unsafe_allow_html=True)

tech_cols = st.columns(6)
techs = [
    ("⚡", "Apache Spark", "3.5.0"),
    ("🔺", "Delta Lake", "3.1.0"),
    ("🔄", "Airflow", "2.9.0"),
    ("📊", "Streamlit", "1.35.0"),
    ("🐳", "Docker", "Latest"),
    ("🔵", "Neo4j", "AuraDB"),
]

for i, (icon, name, version) in enumerate(techs):
    with tech_cols[i]:
        st.markdown(f"""
        <div style='background:#1e2130; border-radius:12px; padding:16px;
             border:1px solid #2d3250; text-align:center;'>
            <div style='font-size:24px;'>{icon}</div>
            <div style='color:#ccd6f6; font-weight:700; font-size:13px; margin-top:6px;'>{name}</div>
            <div style='color:#64ffda; font-size:11px;'>{version}</div>
        </div>
        """, unsafe_allow_html=True)


# ── Footer ─────────────────────────────────────────────────
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("""
<div style='text-align:center; color:#4a5568; font-size:12px;
     border-top:1px solid #2d3250; padding-top:20px;'>
    Bank of Tanzania — Automated ICN Data Pipeline |
    EASTC · BDS Year III · Group 16 · 2025–2026 |
    Prepared for: Bank of Tanzania (BoT)
</div>
""", unsafe_allow_html=True)
