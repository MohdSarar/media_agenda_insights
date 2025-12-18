# dashboard/ui/styles.py

CUSTOM_CSS = """
<style>
/* ============================================
   GLOBAL STYLES - Modern Dark Theme
   ============================================ */

/* Root variables */
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
    --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3), 0 2px 4px -1px rgba(0, 0, 0, 0.2);
}

/* Hide Streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Scrollbar styling */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}
::-webkit-scrollbar-track {
    background: var(--bg-dark);
}
::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: 4px;
}
::-webkit-scrollbar-thumb:hover {
    background: var(--text-muted);
}

/* ============================================
   METRIC CARDS
   ============================================ */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, var(--bg-card) 0%, rgba(99, 102, 241, 0.1) 100%);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1rem 1.25rem;
    box-shadow: var(--shadow);
    transition: all 0.3s ease;
}

[data-testid="stMetric"]:hover {
    border-color: var(--primary);
    transform: translateY(-2px);
    box-shadow: 0 8px 16px -4px rgba(99, 102, 241, 0.2);
}

[data-testid="stMetricLabel"] {
    color: var(--text-secondary) !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

[data-testid="stMetricValue"] {
    color: var(--text-primary) !important;
    font-size: 1.75rem !important;
    font-weight: 700 !important;
}

[data-testid="stMetricDelta"] {
    font-size: 0.8rem !important;
}

[data-testid="stMetricDelta"] svg {
    width: 12px !important;
    height: 12px !important;
}

/* Positive delta */
[data-testid="stMetricDelta"][data-testid-delta="positive"] {
    color: var(--success) !important;
}

/* Negative delta */
[data-testid="stMetricDelta"][data-testid-delta="negative"] {
    color: var(--danger) !important;
}

/* ============================================
   TABS
   ============================================ */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background: var(--bg-card);
    padding: 0.5rem;
    border-radius: 12px;
    border: 1px solid var(--border);
}

.stTabs [data-baseweb="tab"] {
    height: 44px;
    padding: 0 20px;
    border-radius: 8px;
    color: var(--text-secondary);
    background: transparent;
    border: none;
    font-weight: 500;
    transition: all 0.2s ease;
}

.stTabs [data-baseweb="tab"]:hover {
    background: var(--bg-hover);
    color: var(--text-primary);
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%) !important;
    color: white !important;
}

.stTabs [data-baseweb="tab-highlight"] {
    display: none;
}

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
    box-shadow: 0 2px 4px rgba(99, 102, 241, 0.3);
}

.stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 8px rgba(99, 102, 241, 0.4);
}

.stButton > button:active {
    transform: translateY(0);
}

/* Secondary buttons */
.stButton > button[kind="secondary"] {
    background: var(--bg-card);
    border: 1px solid var(--border);
    color: var(--text-primary);
}

/* ============================================
   SELECT BOXES & INPUTS
   ============================================ */
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text-primary) !important;
}

.stSelectbox > div > div:hover,
.stMultiSelect > div > div:hover {
    border-color: var(--primary) !important;
}

.stTextInput > div > div > input {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    color: var(--text-primary) !important;
    padding: 0.5rem 0.75rem;
}

.stTextInput > div > div > input:focus {
    border-color: var(--primary) !important;
    box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.2) !important;
}

/* ============================================
   DATAFRAMES & TABLES
   ============================================ */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
}

[data-testid="stDataFrame"] > div {
    background: var(--bg-card);
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

.streamlit-expanderHeader:hover {
    border-color: var(--primary) !important;
    color: var(--primary-light) !important;
}

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
    background: linear-gradient(180deg, var(--bg-dark) 0%, #131c31 100%);
    border-right: 1px solid var(--border);
}

[data-testid="stSidebar"] .stMarkdown h1,
[data-testid="stSidebar"] .stMarkdown h2,
[data-testid="stSidebar"] .stMarkdown h3 {
    color: var(--text-primary);
}

/* ============================================
   CHARTS
   ============================================ */
[data-testid="stVegaLiteChart"],
[data-testid="stArrowVegaLiteChart"] {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1rem;
}

/* ============================================
   SLIDERS
   ============================================ */
.stSlider > div > div > div > div {
    background: var(--primary) !important;
}

.stSlider > div > div > div > div > div {
    background: white !important;
    border: 2px solid var(--primary) !important;
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
   DIVIDERS
   ============================================ */
hr {
    border-color: var(--border) !important;
    opacity: 0.5;
}

/* ============================================
   ALERTS & INFO BOXES
   ============================================ */
.stAlert {
    border-radius: 8px !important;
    border: none !important;
}

[data-testid="stInfo"] {
    background: rgba(99, 102, 241, 0.1) !important;
    border-left: 4px solid var(--primary) !important;
}

[data-testid="stSuccess"] {
    background: rgba(16, 185, 129, 0.1) !important;
    border-left: 4px solid var(--success) !important;
}

[data-testid="stWarning"] {
    background: rgba(245, 158, 11, 0.1) !important;
    border-left: 4px solid var(--warning) !important;
}

[data-testid="stError"] {
    background: rgba(239, 68, 68, 0.1) !important;
    border-left: 4px solid var(--danger) !important;
}

/* ============================================
   CUSTOM CARD COMPONENT
   ============================================ */
.custom-card {
    background: linear-gradient(135deg, var(--bg-card) 0%, rgba(99, 102, 241, 0.05) 100%);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.25rem;
    margin-bottom: 1rem;
    transition: all 0.3s ease;
}

.custom-card:hover {
    border-color: var(--primary);
    box-shadow: 0 8px 16px -4px rgba(99, 102, 241, 0.15);
}

.custom-card-title {
    color: var(--text-secondary);
    font-size: 0.85rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 0.5rem;
}

.custom-card-value {
    color: var(--text-primary);
    font-size: 2rem;
    font-weight: 700;
    line-height: 1.2;
}

.custom-card-delta {
    font-size: 0.85rem;
    margin-top: 0.25rem;
}

.custom-card-delta.positive {
    color: var(--success);
}

.custom-card-delta.negative {
    color: var(--danger);
}

/* ============================================
   PLOTLY CHART CONTAINER
   ============================================ */
.js-plotly-plot {
    border-radius: 12px;
    overflow: hidden;
}

/* ============================================
   SECTION HEADERS
   ============================================ */
.section-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 1rem;
    padding-bottom: 0.75rem;
    border-bottom: 1px solid var(--border);
}

.section-header h2 {
    color: var(--text-primary);
    font-size: 1.25rem;
    font-weight: 600;
    margin: 0;
}

.section-header .icon {
    font-size: 1.5rem;
}

/* ============================================
   BADGE COMPONENT
   ============================================ */
.badge {
    display: inline-block;
    padding: 0.25rem 0.75rem;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.badge-primary {
    background: rgba(99, 102, 241, 0.2);
    color: var(--primary-light);
}

.badge-success {
    background: rgba(16, 185, 129, 0.2);
    color: #34d399;
}

.badge-warning {
    background: rgba(245, 158, 11, 0.2);
    color: #fbbf24;
}

.badge-danger {
    background: rgba(239, 68, 68, 0.2);
    color: #f87171;
}

/* ============================================
   RESPONSIVE ADJUSTMENTS
   ============================================ */
@media (max-width: 768px) {
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 0 12px;
        font-size: 0.85rem;
    }
}
</style>
"""

# Color palette for charts
CHART_COLORS = [
    "#6366f1",  # Primary indigo
    "#8b5cf6",  # Purple
    "#ec4899",  # Pink
    "#14b8a6",  # Teal
    "#f59e0b",  # Amber
    "#10b981",  # Emerald
    "#ef4444",  # Red
    "#3b82f6",  # Blue
    "#f97316",  # Orange
    "#84cc16",  # Lime
]

PLOTLY_TEMPLATE = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(30, 41, 59, 0.5)",
        "font": {"color": "#e2e8f0", "family": "Inter, sans-serif"},
        "title": {"font": {"color": "#f1f5f9", "size": 16}},
        "xaxis": {
            "gridcolor": "rgba(51, 65, 85, 0.5)",
            "linecolor": "#334155",
            "tickfont": {"color": "#94a3b8"},
        },
        "yaxis": {
            "gridcolor": "rgba(51, 65, 85, 0.5)",
            "linecolor": "#334155",
            "tickfont": {"color": "#94a3b8"},
        },
        "legend": {
            "bgcolor": "rgba(0,0,0,0)",
            "font": {"color": "#e2e8f0"},
        },
        "colorway": CHART_COLORS,
    }
}

ALTAIR_THEME = {
    "config": {
        "background": "transparent",
        "title": {"color": "#f1f5f9", "fontSize": 14, "fontWeight": 600},
        "axis": {
            "labelColor": "#94a3b8",
            "titleColor": "#e2e8f0",
            "gridColor": "#334155",
            "domainColor": "#334155",
        },
        "legend": {
            "labelColor": "#e2e8f0",
            "titleColor": "#f1f5f9",
        },
        "view": {"stroke": "transparent"},
        "range": {"category": CHART_COLORS},
    }
}
