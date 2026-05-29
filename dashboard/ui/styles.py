CUSTOM_CSS = """
<style>
/* ============================================
   GLOBAL STYLES — Modern Dark Theme
   ============================================ */
:root {
    --primary: #6366f1;
    --primary-light: #818cf8;
    --secondary: #8b5cf6;
    --success: #10b981;
    --warning: #f59e0b;
    --danger: #ef4444;
    --bg-dark: #0f172a;
    --bg-card: #1e293b;
    --bg-hover: #334155;
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-muted: #64748b;
    --border: #334155;
    --shadow: 0 4px 6px -1px rgba(0,0,0,0.3), 0 2px 4px -1px rgba(0,0,0,0.2);
}

#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: var(--bg-dark); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }

/* ============================================
   METRIC CARDS
   ============================================ */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, var(--bg-card) 0%, rgba(99,102,241,0.1) 100%);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1rem 1.25rem;
    box-shadow: var(--shadow);
    transition: all 0.3s ease;
}
[data-testid="stMetric"]:hover {
    border-color: var(--primary);
    transform: translateY(-2px);
    box-shadow: 0 8px 16px -4px rgba(99,102,241,0.2);
}
[data-testid="stMetricLabel"] {
    color: var(--text-secondary) !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="stMetricValue"] {
    color: var(--text-primary) !important;
    font-size: 1.6rem !important;
    font-weight: 700 !important;
}

/* ============================================
   TABS — compact to fit many tabs on one row
   ============================================ */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
    background: var(--bg-card);
    padding: 0.3rem;
    border-radius: 12px;
    border: 1px solid var(--border);
    flex-wrap: wrap;          /* tabs wrap to a second line if needed */
    row-gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    height: 30px;
    padding: 0 8px;
    border-radius: 6px;
    color: var(--text-secondary);
    background: transparent;
    border: none;
    font-weight: 500;
    font-size: 0.72rem;
    transition: all 0.2s ease;
    white-space: nowrap;
}
.stTabs [data-baseweb="tab"]:hover {
    background: var(--bg-hover);
    color: var(--text-primary);
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%) !important;
    color: white !important;
}
.stTabs [data-baseweb="tab-highlight"] { display: none; }

/* ============================================
   BUTTONS
   ============================================ */
.stButton > button {
    background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
    color: white;
    border: none;
    border-radius: 8px;
    padding: 0.5rem 1.25rem;
    font-weight: 500;
    transition: all 0.2s ease;
    box-shadow: 0 2px 4px rgba(99,102,241,0.3);
}
.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 8px rgba(99,102,241,0.4);
}

/* ============================================
   INPUTS & SELECTS
   ============================================ */
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text-primary) !important;
}
.stSelectbox > div > div:hover,
.stMultiSelect > div > div:hover { border-color: var(--primary) !important; }

.stTextInput > div > div > input {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text-primary) !important;
}
.stTextInput > div > div > input:focus {
    border-color: var(--primary) !important;
    box-shadow: 0 0 0 2px rgba(99,102,241,0.2) !important;
}

/* ============================================
   DATAFRAMES
   ============================================ */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
}

/* ============================================
   EXPANDERS
   ============================================ */
.streamlit-expanderHeader {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text-primary) !important;
    font-weight: 500;
}
.streamlit-expanderHeader:hover { border-color: var(--primary) !important; }
.streamlit-expanderContent {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-top: none !important;
    border-radius: 0 0 8px 8px !important;
}

/* ============================================
   SIDEBAR
   ============================================ */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #131c31 100%);
    border-right: 1px solid var(--border);
}
[data-testid="stSidebar"] .stMarkdown h1,
[data-testid="stSidebar"] .stMarkdown h2,
[data-testid="stSidebar"] .stMarkdown h3 { color: var(--text-primary); }

/* ============================================
   CHART CONTAINERS
   ============================================ */
[data-testid="stVegaLiteChart"],
[data-testid="stArrowVegaLiteChart"] {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1rem;
}

/* ============================================
   DATE INPUT
   ============================================ */
.stDateInput > div > div > input {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text-primary) !important;
}

/* ============================================
   ALERTS
   ============================================ */
[data-testid="stInfo"] {
    background: rgba(99,102,241,0.1) !important;
    border-left: 4px solid var(--primary) !important;
    border-radius: 8px !important;
}
[data-testid="stSuccess"] {
    background: rgba(16,185,129,0.1) !important;
    border-left: 4px solid var(--success) !important;
}
[data-testid="stWarning"] {
    background: rgba(245,158,11,0.1) !important;
    border-left: 4px solid var(--warning) !important;
}
[data-testid="stError"] {
    background: rgba(239,68,68,0.1) !important;
    border-left: 4px solid var(--danger) !important;
}

hr { border-color: var(--border) !important; opacity: 0.5; }
</style>
"""

CHART_COLORS = [
    "#6366f1", "#8b5cf6", "#ec4899", "#14b8a6",
    "#f59e0b", "#10b981", "#ef4444", "#3b82f6",
    "#f97316", "#84cc16",
]

ALTAIR_THEME = {
    "config": {
        "background": "transparent",
        "title": {"color": "#f1f5f9", "fontSize": 13, "fontWeight": 600},
        "axis": {
            "labelColor": "#94a3b8",
            "titleColor": "#e2e8f0",
            "gridColor": "#334155",
            "domainColor": "#334155",
        },
        "legend": {"labelColor": "#e2e8f0", "titleColor": "#f1f5f9"},
        "view": {"stroke": "transparent"},
        "range": {"category": CHART_COLORS},
    }
}
