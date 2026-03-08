"""
╔══════════════════════════════════════════════════════════════════════════════╗
║      XAU-QUANTUM-DASHBOARD  —  Kinetic Fluid Telemetry  v4.0  FINAL       ║
║      Stochastic XAUUSD Pressure Monitor  |  AllTick API                   ║
║      GBM Drift/Vol  ·  OU Relaxation  ·  Action Coordinates  ·  Resilient ║
╚══════════════════════════════════════════════════════════════════════════════╝

DEPLOYMENT CHECKLIST
────────────────────
1.  pip install -r requirements.txt
2.  mkdir -p .streamlit
3.  Create .streamlit/secrets.toml  →  [api_xa_uusd]  api_key = "YOUR_TOKEN"
4.  streamlit run app.py

ARCHITECTURE (Blocs 1-2-3-4 fusionnés)
────────────────────────────────────────
Bloc 1 · CSS Quant-Dark  ·  API AllTick  ·  OHLCV pipeline
Bloc 2 · GBM (μ,σ,σ_rolling)  ·  OU (θ,μ_eq,T½)  ·  Turbulence (Δ²P)
Bloc 3 · ActionCoordinates (ENTRY/NDT/ERV/HEP)  ·  Safety Inhibitor
Bloc 4 · try/except resilience shell  ·  "RE-CONNEXION AU CAPTEUR" UX
"""

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timezone
import json
from dataclasses import dataclass, field

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG — must be the very first Streamlit call
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="XAU-QUANTUM-DASHBOARD",
    page_icon="⚛",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS  —  Deep dark terminal aesthetic
# ─────────────────────────────────────────────────────────────────────────────
QUANTUM_CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700;900&display=swap');

  :root {
    --green:        #00FF41;
    --green-dim:    #00CC33;
    --green-faint:  #003B0D;
    --amber:        #FFB300;
    --red:          #FF3C3C;
    --cyan:         #00E5FF;
    --purple:       #BF5FFF;
    --bg-void:      #000000;
    --bg-panel:     #050F05;
    --bg-card:      #080F08;
    --border:       #00FF4140;
    --border-glow:  #00FF4190;
    --text-mono:    'Share Tech Mono', monospace;
    --text-display: 'Orbitron', monospace;
  }

  html, body, [class*="css"] {
    font-family: var(--text-mono) !important;
    background-color: var(--bg-void) !important;
    color: var(--green) !important;
  }

  .stApp {
    background-color: var(--bg-void) !important;
    background-image: repeating-linear-gradient(
      0deg, transparent, transparent 2px,
      rgba(0,255,65,0.015) 2px, rgba(0,255,65,0.015) 4px
    );
  }

  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg-void); }
  ::-webkit-scrollbar-thumb { background: var(--green-dim); border-radius: 3px; }

  section[data-testid="stSidebar"] {
    background-color: var(--bg-panel) !important;
    border-right: 1px solid var(--border) !important;
  }

  h1, h2, h3, h4 {
    font-family: var(--text-display) !important;
    color: var(--green) !important;
    text-shadow: 0 0 12px rgba(0,255,65,0.7), 0 0 24px rgba(0,255,65,0.3);
    letter-spacing: 0.12em;
  }

  [data-testid="metric-container"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 4px !important;
    padding: 12px 16px !important;
    box-shadow: 0 0 18px rgba(0,255,65,0.08), inset 0 0 30px rgba(0,255,65,0.02);
    transition: box-shadow 0.3s ease;
  }
  [data-testid="metric-container"]:hover {
    box-shadow: 0 0 28px rgba(0,255,65,0.22), inset 0 0 30px rgba(0,255,65,0.05);
  }
  [data-testid="stMetricLabel"] {
    color: var(--green-dim) !important;
    font-size: 0.7rem !important;
    letter-spacing: 0.18em;
  }
  [data-testid="stMetricValue"] {
    color: var(--green) !important;
    font-size: 1.5rem !important;
    text-shadow: 0 0 8px rgba(0,255,65,0.8);
  }
  [data-testid="stMetricDelta"] svg { display: none; }

  [data-testid="stVerticalBlock"] > div:has([data-testid="stPlotlyChart"]) {
    background: var(--bg-panel);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 4px;
  }

  hr { border-color: var(--border-glow) !important; box-shadow: 0 0 6px rgba(0,255,65,0.3); }

  .stButton > button {
    background: var(--bg-card) !important;
    color: var(--green) !important;
    border: 1px solid var(--green-dim) !important;
    font-family: var(--text-mono) !important;
    letter-spacing: 0.1em;
    border-radius: 2px !important;
    transition: all 0.2s;
  }
  .stButton > button:hover {
    background: var(--green-faint) !important;
    box-shadow: 0 0 14px rgba(0,255,65,0.5);
  }

  .stSelectbox > div > div,
  .stTextInput > div > div > input {
    background: var(--bg-card) !important;
    color: var(--green) !important;
    border: 1px solid var(--border) !important;
    font-family: var(--text-mono) !important;
  }

  .stAlert {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    color: var(--green) !important;
  }

  .stTabs [data-baseweb="tab-list"] {
    background: var(--bg-void) !important;
    border-bottom: 1px solid var(--border) !important;
  }
  .stTabs [data-baseweb="tab"] {
    font-family: var(--text-mono) !important;
    color: var(--green-dim) !important;
    letter-spacing: 0.1em;
    font-size: 0.76rem;
  }
  .stTabs [aria-selected="true"] {
    color: var(--green) !important;
    border-bottom: 2px solid var(--green) !important;
    text-shadow: 0 0 8px rgba(0,255,65,0.8);
  }

  .quantum-badge {
    display: inline-block;
    background: var(--green-faint);
    border: 1px solid var(--green);
    color: var(--green);
    font-family: var(--text-mono);
    font-size: 0.68rem;
    padding: 2px 10px;
    border-radius: 2px;
    letter-spacing: 0.15em;
    text-shadow: 0 0 6px rgba(0,255,65,0.9);
    animation: pulse-badge 1.8s infinite;
  }
  @keyframes pulse-badge {
    0%, 100% { box-shadow: 0 0 4px rgba(0,255,65,0.4); }
    50%       { box-shadow: 0 0 14px rgba(0,255,65,0.9); }
  }

  .turb-badge {
    display: inline-block;
    background: rgba(255,60,60,0.12);
    border: 1px solid #FF3C3C;
    color: #FF3C3C;
    font-family: var(--text-mono);
    font-size: 0.7rem;
    padding: 3px 10px;
    border-radius: 2px;
    letter-spacing: 0.1em;
    animation: pulse-red 0.6s infinite;
  }
  @keyframes pulse-red {
    0%, 100% { box-shadow: 0 0 4px rgba(255,60,60,0.4); }
    50%       { box-shadow: 0 0 16px rgba(255,60,60,0.9); }
  }

  .regime-compress {
    display: inline-block;
    background: rgba(255,60,60,0.1);
    border: 1px solid #FF3C3C;
    color: #FF3C3C;
    font-family: var(--text-mono);
    font-size: 0.72rem;
    padding: 3px 12px;
    border-radius: 2px;
    letter-spacing: 0.1em;
  }
  .regime-relax {
    display: inline-block;
    background: rgba(0,255,65,0.08);
    border: 1px solid #00FF41;
    color: #00FF41;
    font-family: var(--text-mono);
    font-size: 0.72rem;
    padding: 3px 12px;
    border-radius: 2px;
    letter-spacing: 0.1em;
  }
  .regime-neutral {
    display: inline-block;
    background: rgba(255,179,0,0.08);
    border: 1px solid #FFB300;
    color: #FFB300;
    font-family: var(--text-mono);
    font-size: 0.72rem;
    padding: 3px 12px;
    border-radius: 2px;
    letter-spacing: 0.1em;
  }

  .q-analytics-panel {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 14px 16px;
    margin: 6px 0;
    box-shadow: 0 0 14px rgba(0,255,65,0.04);
  }
  .q-analytics-title {
    font-family: var(--text-display);
    font-size: 0.66rem;
    color: var(--green-dim);
    letter-spacing: 0.22em;
    margin-bottom: 10px;
    border-bottom: 1px solid var(--border);
    padding-bottom: 5px;
  }
  .q-eq {
    font-family: var(--text-mono);
    font-size: 0.70rem;
    color: var(--cyan);
    background: rgba(0,229,255,0.05);
    border-left: 2px solid var(--cyan);
    padding: 4px 10px;
    margin: 6px 0;
    letter-spacing: 0.04em;
  }

  .quantum-title {
    font-family: var(--text-display) !important;
    font-size: 2.0rem;
    font-weight: 900;
    color: var(--green);
    text-shadow: 0 0 20px rgba(0,255,65,0.8), 0 0 60px rgba(0,255,65,0.2);
    letter-spacing: 0.2em;
    line-height: 1.1;
  }
  .quantum-subtitle {
    font-family: var(--text-mono);
    font-size: 0.72rem;
    color: var(--green-dim);
    letter-spacing: 0.28em;
    margin-top: 4px;
  }

  .data-row {
    display: flex;
    justify-content: space-between;
    padding: 4px 0;
    border-bottom: 1px solid rgba(0,255,65,0.08);
    font-size: 0.78rem;
    letter-spacing: 0.06em;
  }
  .data-label { color: var(--green-dim); }
  .data-value { color: var(--green); text-shadow: 0 0 4px rgba(0,255,65,0.5); }

  .blink { animation: blink-anim 0.9s step-end infinite; }
  @keyframes blink-anim { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }

  .q-divider {
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--green), transparent);
    margin: 12px 0;
    box-shadow: 0 0 8px rgba(0,255,65,0.4);
  }
  .q-divider-cyan {
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--cyan), transparent);
    margin: 10px 0;
    box-shadow: 0 0 6px rgba(0,229,255,0.3);
  }

  /* ── Action Coordinates Panel ── */
  .ac-panel {
    background: linear-gradient(135deg, #000000 0%, #050A0A 100%);
    border: 1px solid rgba(0,229,255,0.35);
    border-radius: 4px;
    padding: 16px 18px;
    margin: 8px 0;
    box-shadow: 0 0 22px rgba(0,229,255,0.08), inset 0 0 40px rgba(0,229,255,0.02);
    position: relative;
    overflow: hidden;
  }
  .ac-panel::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, #00E5FF, transparent);
    box-shadow: 0 0 10px rgba(0,229,255,0.6);
  }
  .ac-title {
    font-family: var(--text-display);
    font-size: 0.65rem;
    color: #00E5FF;
    letter-spacing: 0.3em;
    margin-bottom: 12px;
    text-shadow: 0 0 8px rgba(0,229,255,0.7);
  }

  /* ── Individual coordinate card ── */
  .coord-card {
    background: rgba(0,0,0,0.6);
    border-radius: 3px;
    padding: 10px 14px;
    margin: 5px 0;
    border-left: 3px solid transparent;
    transition: border-color 0.3s, box-shadow 0.3s;
  }
  .coord-card.entry  { border-left-color: #00FF41; }
  .coord-card.ndt    { border-left-color: #00E5FF; }
  .coord-card.erv    { border-left-color: #FF3C3C; }
  .coord-card.hep    { border-left-color: #FFB300; }
  .coord-card:hover  { box-shadow: 0 0 12px rgba(0,229,255,0.18); }

  .coord-label {
    font-family: var(--text-display);
    font-size: 0.58rem;
    letter-spacing: 0.25em;
    margin-bottom: 3px;
  }
  .coord-label.entry { color: #00FF41; }
  .coord-label.ndt   { color: #00E5FF; }
  .coord-label.erv   { color: #FF3C3C; }
  .coord-label.hep   { color: #FFB300; }

  .coord-value {
    font-family: var(--text-mono);
    font-size: 1.35rem;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-shadow: 0 0 10px currentColor;
  }
  .coord-value.entry { color: #00FF41; }
  .coord-value.ndt   { color: #00E5FF; }
  .coord-value.erv   { color: #FF3C3C; }
  .coord-value.hep   { color: #FFB300; }

  .coord-sub {
    font-family: var(--text-mono);
    font-size: 0.65rem;
    margin-top: 2px;
    opacity: 0.7;
  }

  /* ── Inhibition / PAUSE state ── */
  .inhibit-overlay {
    background: rgba(255,60,60,0.06);
    border: 1px solid rgba(255,60,60,0.6);
    border-radius: 4px;
    padding: 18px 20px;
    text-align: center;
    animation: pulse-inhibit 1.2s ease-in-out infinite;
    margin: 8px 0;
  }
  @keyframes pulse-inhibit {
    0%, 100% { box-shadow: 0 0 8px rgba(255,60,60,0.3); }
    50%       { box-shadow: 0 0 28px rgba(255,60,60,0.8); }
  }
  .inhibit-title {
    font-family: var(--text-display);
    font-size: 0.85rem;
    color: #FF3C3C;
    letter-spacing: 0.2em;
    text-shadow: 0 0 14px rgba(255,60,60,0.9);
    margin-bottom: 8px;
  }
  .inhibit-sub {
    font-family: var(--text-mono);
    font-size: 0.7rem;
    color: rgba(255,60,60,0.75);
    letter-spacing: 0.1em;
  }
  .inhibit-zero {
    font-family: var(--text-mono);
    font-size: 1.1rem;
    color: rgba(255,60,60,0.4);
    letter-spacing: 0.3em;
    margin-top: 6px;
  }

  /* ── Signal badge ── */
  .signal-long {
    display: inline-block;
    background: rgba(0,255,65,0.12);
    border: 1px solid #00FF41;
    color: #00FF41;
    font-family: var(--text-mono);
    font-size: 0.75rem;
    padding: 4px 14px;
    border-radius: 2px;
    letter-spacing: 0.15em;
    text-shadow: 0 0 8px rgba(0,255,65,0.9);
    animation: pulse-badge 1.8s infinite;
  }
  .signal-short {
    display: inline-block;
    background: rgba(255,60,60,0.12);
    border: 1px solid #FF3C3C;
    color: #FF3C3C;
    font-family: var(--text-mono);
    font-size: 0.75rem;
    padding: 4px 14px;
    border-radius: 2px;
    letter-spacing: 0.15em;
    text-shadow: 0 0 8px rgba(255,60,60,0.9);
    animation: pulse-red 0.9s infinite;
  }
  .signal-wait {
    display: inline-block;
    background: rgba(255,179,0,0.08);
    border: 1px solid #FFB300;
    color: #FFB300;
    font-family: var(--text-mono);
    font-size: 0.75rem;
    padding: 4px 14px;
    border-radius: 2px;
    letter-spacing: 0.15em;
  }

  /* ── Stochastic oscillator bar ── */
  .stoch-bar-outer {
    background: rgba(0,229,255,0.06);
    border: 1px solid rgba(0,229,255,0.2);
    border-radius: 2px;
    height: 8px;
    margin: 6px 0;
    overflow: hidden;
  }
  .stoch-bar-inner {
    height: 100%;
    border-radius: 2px;
    transition: width 0.4s ease;
  }

  /* ── Resilience / Reconnection overlay ── */
  .reconnect-panel {
    background: rgba(0,229,255,0.04);
    border: 1px solid rgba(0,229,255,0.25);
    border-radius: 4px;
    padding: 18px 24px;
    text-align: center;
    margin: 12px 0;
    animation: pulse-reconnect 1.4s ease-in-out infinite;
  }
  @keyframes pulse-reconnect {
    0%, 100% { box-shadow: 0 0 6px rgba(0,229,255,0.15); }
    50%       { box-shadow: 0 0 22px rgba(0,229,255,0.5); }
  }
  .reconnect-title {
    font-family: var(--text-display);
    font-size: 0.82rem;
    color: #00E5FF;
    letter-spacing: 0.25em;
    text-shadow: 0 0 10px rgba(0,229,255,0.8);
    margin-bottom: 6px;
  }
  .reconnect-sub {
    font-family: var(--text-mono);
    font-size: 0.68rem;
    color: rgba(0,229,255,0.6);
    letter-spacing: 0.12em;
  }
  .reconnect-dots span {
    display: inline-block;
    width: 6px; height: 6px;
    border-radius: 50%;
    background: #00E5FF;
    margin: 0 3px;
    animation: dot-blink 1.4s step-start infinite;
  }
  .reconnect-dots span:nth-child(2) { animation-delay: 0.2s; }
  .reconnect-dots span:nth-child(3) { animation-delay: 0.4s; }
  @keyframes dot-blink {
    0%, 80%, 100% { opacity: 0.15; }
    40%           { opacity: 1; }
  }

  /* ── Step error inline badge ── */
  .step-error {
    display: inline-block;
    background: rgba(255,179,0,0.07);
    border: 1px solid rgba(255,179,0,0.3);
    border-radius: 3px;
    padding: 3px 10px;
    font-family: var(--text-mono);
    font-size: 0.66rem;
    color: #FFB300;
    letter-spacing: 0.1em;
    margin: 3px 0;
  }

  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding-top: 1.2rem !important; padding-bottom: 0.5rem !important; }
</style>
"""

st.markdown(QUANTUM_CSS, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
ALLTICK_BASE_URL      = "https://quote.tradeswitcher.com"
ALLTICK_KLINE_PATH    = "/quote-b-api/kline"
SYMBOL                = "XAUUSD.a"
INTERVAL_1M           = "1"
INTERVAL_5M           = "5"
CANDLE_FETCH_COUNT    = 120
REFRESH_SECONDS       = 2

# Analytical engine parameters
GBM_WINDOW            = 14      # rolling window in cycles for instantaneous variance
OU_MIN_SAMPLES        = 10      # minimum candles required to fit OU model
TURB_ACCEL_STD_THRESH = 2.5     # sigma threshold — second derivative anomaly filter

# Action coordinate engine constants
STOCH_K_PERIOD        = 14      # %K lookback period
STOCH_D_PERIOD        = 3       # %D smoothing period
STOCH_EXHAUSTION_ZONE = 20.0    # energy exhaustion threshold for entry signal
STOCH_OVERBOUGHT_ZONE = 80.0    # overbought threshold for short signal
NDT_SIGMA_MULTIPLE    = 2.5     # Take Profit = entry + 2.5 × σ_GBM × price
ERV_SIGMA_MULTIPLE    = 1.5     # Stop Loss   = entry − 1.5 × σ_GBM × price
HEP_FRICTION_OFFSET   = 0.002   # Break Even  = entry × (1 + 0.2%)  mechanical friction

# ─────────────────────────────────────────────────────────────────────────────
# RESILIENCE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _reconnect_ui(reason: str = "", retry: int = 0) -> None:
    """
    Display the 'RE-CONNEXION AU CAPTEUR' overlay instead of a crash/red error.
    Called whenever a pipeline step raises an unrecoverable exception.

    Parameters
    ──────────
    reason : str  — short human-readable cause (shown in sub-text)
    retry  : int  — current tick count (shown as retry counter)
    """
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    st.markdown(
        f'<div class="reconnect-panel">'
        f'<div class="reconnect-title">⟳ RE-CONNEXION AU CAPTEUR</div>'
        f'<div class="reconnect-dots">'
        f'<span></span><span></span><span></span>'
        f'</div>'
        f'<div class="reconnect-sub" style="margin-top:8px;">'
        f'{f"MOTIF : {reason}<br>" if reason else ""}'
        f'Dernière tentative : {ts}'
        f'{f"  ·  Cycle #{retry}" if retry else ""}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _safe_chart(fig_fn, *args, **kwargs) -> go.Figure:
    """
    Execute a chart-builder function inside a try/except.
    Returns an empty Figure on any error so the UI never breaks.
    """
    try:
        return fig_fn(*args, **kwargs)
    except Exception as e:
        _log_error(f"[CHART ERR] {fig_fn.__name__}: {e}")
        return go.Figure()

COLOR_UP          = "#00FF41"
COLOR_DOWN        = "#FF3C3C"
COLOR_CYAN        = "#00E5FF"
COLOR_AMBER       = "#FFB300"
COLOR_PURPLE      = "#BF5FFF"
COLOR_VOLUME_UP   = "rgba(0,255,65,0.35)"
COLOR_VOLUME_DOWN = "rgba(255,60,60,0.35)"
COLOR_BG          = "#000000"
COLOR_GRID        = "rgba(0,255,65,0.07)"
COLOR_AXIS        = "rgba(0,255,65,0.35)"
FONT_MONO         = "Share Tech Mono, monospace"


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class GBMResult:
    """Geometric Brownian Motion analytics output."""
    mu:             float      = 0.0
    sigma:          float      = 0.0
    mu_instant:     np.ndarray = field(default_factory=lambda: np.array([]))
    sigma_rolling:  np.ndarray = field(default_factory=lambda: np.array([]))
    log_returns:    np.ndarray = field(default_factory=lambda: np.array([]))
    variance_window:np.ndarray = field(default_factory=lambda: np.array([]))
    valid:          bool       = False


@dataclass
class OUResult:
    """Ornstein-Uhlenbeck relaxation model output."""
    theta:    float      = 0.0
    mu_eq:    float      = 0.0
    sigma_ou: float      = 0.0
    half_life:float      = 0.0
    residuals:np.ndarray = field(default_factory=lambda: np.array([]))
    ou_path:  np.ndarray = field(default_factory=lambda: np.array([]))
    regime:   str        = "NEUTRAL"
    valid:    bool       = False


@dataclass
class TurbulenceResult:
    """Second-derivative pressure-acceleration anomaly filter output."""
    acceleration: np.ndarray = field(default_factory=lambda: np.array([]))
    accel_zscore: np.ndarray = field(default_factory=lambda: np.array([]))
    anomaly_mask: np.ndarray = field(default_factory=lambda: np.array([], dtype=bool))
    anomaly_count:int        = 0
    latest_flag:  bool       = False
    valid:        bool       = False


@dataclass
class FluidKineticsReport:
    """Aggregated container for all stochastic analytics."""
    gbm:        GBMResult        = field(default_factory=GBMResult)
    ou:         OUResult         = field(default_factory=OUResult)
    turbulence: TurbulenceResult = field(default_factory=TurbulenceResult)
    calc_ms:    float            = 0.0


@dataclass
class ActionCoordinates:
    """
    Actuation coordinates for the field engineer.

    Four threshold levels derived from GBM σ, OU θ, and Stochastic %K/%D.

    ENTRY  — Point d'Injection
        Long  : %K crosses %D upward below STOCH_EXHAUSTION_ZONE (< 20)
                AND OU θ > 0.15 (compression imminente)
        Short : %K crosses %D downward above STOCH_OVERBOUGHT_ZONE (> 80)
                AND OU θ > 0.15

    NDT    — Seuil de Décompression Nominale (Take Profit)
        NDT  = entry  ±  NDT_SIGMA_MULTIPLE × σ_ann × entry
             = entry  ±  2.5 × σ_GBM × entry

    ERV    — Soupape de Sécurité (Stop Loss / Rupture irrévocable)
        ERV  = entry  ∓  ERV_SIGMA_MULTIPLE × σ_ann × entry
             = entry  ∓  1.5 × σ_GBM × entry

    HEP    — Équilibre Hydrostatique (Break Even)
        HEP  = entry  ×  (1  +  HEP_FRICTION_OFFSET)
             = entry  ×  1.002   (0.2% spread/frais compensation)

    INHIBITED — turbulence.latest_flag == True
        All price values zeroed; system locked; PAUSE DE SÉCURITÉ emitted.
    """
    entry:          float  = 0.0
    ndt:            float  = 0.0
    erv:            float  = 0.0
    hep:            float  = 0.0
    direction:      str    = "WAIT"     # LONG | SHORT | WAIT
    stoch_k:        float  = 50.0
    stoch_d:        float  = 50.0
    k_crosses_d:    bool   = False
    ou_theta:       float  = 0.0
    sigma_ann:      float  = 0.0
    inhibited:      bool   = False
    inhibit_reason: str    = ""
    signal_active:  bool   = False
    rr_ratio:       float  = 0.0       # Risk/Reward = |NDT-entry| / |ERV-entry|
    valid:          bool   = False


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE INITIALISATION
# ─────────────────────────────────────────────────────────────────────────────
if "kline_cache_1m"   not in st.session_state:
    st.session_state["kline_cache_1m"]   = pd.DataFrame()
if "kline_cache_5m"   not in st.session_state:
    st.session_state["kline_cache_5m"]   = pd.DataFrame()
if "last_price"       not in st.session_state:
    st.session_state["last_price"]       = 0.0
if "prev_price"       not in st.session_state:
    st.session_state["prev_price"]       = 0.0
if "tick_count"       not in st.session_state:
    st.session_state["tick_count"]       = 0
if "error_log"        not in st.session_state:
    st.session_state["error_log"]        = []
if "api_latency_ms"   not in st.session_state:
    st.session_state["api_latency_ms"]   = 0.0
if "kinetics_cache"   not in st.session_state:
    st.session_state["kinetics_cache"]   = FluidKineticsReport()
if "action_coords"    not in st.session_state:
    st.session_state["action_coords"]    = ActionCoordinates()


# ─────────────────────────────────────────────────────────────────────────────
# API LAYER
# ─────────────────────────────────────────────────────────────────────────────
def _get_api_key() -> str:
    try:
        return st.secrets["api_xa_uusd"]["api_key"]
    except KeyError as exc:
        raise RuntimeError(
            "Secret 'api_xa_uusd.api_key' not found. "
            "Add to .streamlit/secrets.toml:\n"
            "[api_xa_uusd]\napi_key = \"YOUR_TOKEN\""
        ) from exc


def _build_kline_params(interval: str, count: int) -> dict:
    return {
        "token": _get_api_key(),
        "query": json.dumps({
            "trace": f"xau-quantum-{int(time.time())}",
            "data": {
                "code":                SYMBOL,
                "kline_type":          int(interval),
                "kline_timestamp_end": 0,
                "query_kline_num":     count,
                "adjust_type":         0,
            }
        }),
    }


def fetch_quantum_telemetry(interval: str = INTERVAL_1M) -> pd.DataFrame:
    """
    Retrieve XAUUSD K-line candles from the AllTick real API.
    URL  : GET https://quote.tradeswitcher.com/quote-b-api/kline
    Auth : query param 'token' = st.secrets["api_xa_uusd"]["api_key"]
    Returns pd.DataFrame[timestamp, open, high, low, close, volume]
    or empty DataFrame on any failure.
    """
    t0 = time.perf_counter()
    try:
        params   = _build_kline_params(interval, CANDLE_FETCH_COUNT)
        response = requests.get(
            url     = f"{ALLTICK_BASE_URL}{ALLTICK_KLINE_PATH}",
            params  = params,
            timeout = 8,
            headers = {
                "Accept":     "application/json",
                "User-Agent": "XAU-QUANTUM-DASHBOARD/2.0",
            },
        )
        st.session_state["api_latency_ms"] = round(
            (time.perf_counter() - t0) * 1000, 2
        )
        response.raise_for_status()
        payload  = response.json()
        raw_list = payload.get("data", {}).get("kline_list", [])
        if not raw_list:
            _log_error(f"[WARN] Empty kline_list. Keys: {list(payload.keys())}")
            return pd.DataFrame()

        df = pd.DataFrame(raw_list)
        df.rename(columns={
            "open_price":  "open",
            "close_price": "close",
            "high_price":  "high",
            "low_price":   "low",
        }, inplace=True)

        required = {"timestamp", "open", "high", "low", "close"}
        if not required.issubset(df.columns):
            _log_error(f"[WARN] Missing cols. Got: {list(df.columns)}")
            return pd.DataFrame()

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = (
            pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
            if "volume" in df.columns else 0.0
        )
        df["timestamp"] = pd.to_datetime(
            pd.to_numeric(df["timestamp"], errors="coerce"),
            unit="s", utc=True,
        )
        df.dropna(subset=["timestamp", "open", "high", "low", "close"], inplace=True)
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    except requests.exceptions.Timeout:
        _log_error("[ERR] Timeout — no response within 8 s")
    except requests.exceptions.ConnectionError as e:
        _log_error(f"[ERR] ConnectionError: {e}")
    except requests.exceptions.HTTPError as e:
        _log_error(f"[ERR] HTTP {e.response.status_code}: {e.response.text[:200]}")
    except (ValueError, KeyError, TypeError) as e:
        _log_error(f"[ERR] Parse: {e}")
    except Exception as e:
        _log_error(f"[ERR] {type(e).__name__}: {e}")
    return pd.DataFrame()


def _log_error(msg: str) -> None:
    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S")
    entry = f"{ts}  {msg}"
    st.session_state["error_log"].append(entry)
    if len(st.session_state["error_log"]) > 40:
        st.session_state["error_log"] = st.session_state["error_log"][-40:]


# ─────────────────────────────────────────────────────────────────────────────
# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║            FLUID KINETICS  —  STOCHASTIC ANALYTICAL ENGINE               ║
# ║   GBM  (Geometric Brownian Motion)  +  OU  (Ornstein-Uhlenbeck)          ║
# ║   +  Turbulence Filter  (second-derivative anomaly detection)             ║
# ║   All computation in pure NumPy — target latency < 50 ms                 ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
# ─────────────────────────────────────────────────────────────────────────────

def _compute_gbm(prices: np.ndarray, window: int = GBM_WINDOW) -> GBMResult:
    """
    Geometric Brownian Motion parameter extraction.

    Model (continuous SDE):
        dS_t = μ · S_t · dt  +  σ · S_t · dW_t

    Log-space equivalent (Itô lemma):
        d(ln S_t) = (μ − σ²/2) · dt  +  σ · dW_t
        r_t ≡ ln(S_t / S_{t-1})  ∼  N((μ − σ²/2)·Δt,  σ²·Δt)

    Algorithm
    ──────────
    1.  r_t  = diff(ln(prices))                 → log-return vector  [n-1]
    2.  μ̂   = mean(r_t)  [per-period drift]
        σ̂   = std(r_t, ddof=1)  [per-period volatility]
    3.  Sliding window of `window` cycles via NumPy stride tricks:
        • builds view matrix  W[i, :] = r_t[i : i+window]   — zero-copy
        • variance_window[i] = Var(W[i, :])                  — instantaneous σ²
        • sigma_rolling[i]   = sqrt(variance_window[i])
        • mu_instant[i]      = mean(W[i, :])
    4.  Annualise:
        μ_ann  = μ̂  × 252
        σ_ann  = σ̂  × √252
        (convention: 252 trading days; for 1M candles this is a normalisation
         factor — the relative magnitudes remain comparable across timeframes)
    """
    result = GBMResult()
    n = len(prices)
    if n < window + 2:
        return result

    # Step 1 — log-returns  (shape: n-1,  dtype: float64)
    log_ret: np.ndarray = np.diff(np.log(prices))

    # Step 2 — global drift and volatility
    mu_raw:    float = float(np.mean(log_ret))
    sigma_raw: float = float(np.std(log_ret, ddof=1))
    if sigma_raw < 1e-14:
        return result

    # Step 3 — rolling window via stride tricks  (zero-copy view)
    n_ret:  int = len(log_ret)
    n_wins: int = n_ret - window + 1
    strides = (log_ret.strides[0], log_ret.strides[0])
    win_mat: np.ndarray = np.lib.stride_tricks.as_strided(
        log_ret,
        shape   = (n_wins, window),
        strides = strides,
    )
    variance_window: np.ndarray = np.var(win_mat,  axis=1, ddof=1)
    mu_rolling:      np.ndarray = np.mean(win_mat, axis=1)
    sigma_rolling:   np.ndarray = np.sqrt(variance_window)

    # Step 4 — annualise
    ann = 252.0
    result.mu              = mu_raw    * ann
    result.sigma           = sigma_raw * np.sqrt(ann)
    result.mu_instant      = mu_rolling    * ann
    result.sigma_rolling   = sigma_rolling * np.sqrt(ann)
    result.log_returns     = log_ret
    result.variance_window = variance_window
    result.valid           = True
    return result


def _compute_ou(prices: np.ndarray) -> OUResult:
    """
    Ornstein-Uhlenbeck mean-reversion parameter estimation.

    Continuous SDE:
        dX_t = θ · (μ − X_t) · dt  +  σ · dW_t

    Euler-Maruyama discretisation (Δt = 1 period):
        X_{t+1} − X_t = θ·μ·Δt  −  θ·Δt·X_t  +  ε_t

    Recast as OLS  (linear regression):
        ΔX_t = α  +  β · X_t  +  ε_t
    where:
        α = θ · μ · Δt     →   μ_eq  =  −α / β
        β = −θ · Δt        →   θ     =  −β          (Δt = 1 by construction)
        σ_ou = std(ε_t, ddof=1)

    Half-life of mean-reversion:
        T½ = ln(2) / θ  [in periods]

    Regime classification
    ─────────────────────
    θ > 0.15 AND |X_last − μ_eq| > 0.5·σ_ou  →  EXPLOSIVE_COMPRESSION
        Fluid is displaced far from equilibrium; high snap-back force.
    θ > 0.15 AND |X_last − μ_eq| ≤ 0.5·σ_ou  →  RETURN_TO_REST
        Fluid is near equilibrium; damped oscillation around μ_eq.
    θ ≤ 0.15                                  →  NEUTRAL
        Near-unit-root; very slow or no detectable mean-reversion.

    Note: if β ≥ 0 the process is non-stationary (unit root / explosive);
    the OU assumption is violated and valid=False is returned.
    """
    result = OUResult()
    n = len(prices)
    if n < OU_MIN_SAMPLES + 1:
        return result

    X:  np.ndarray = prices[:-1].astype(np.float64)   # X_t
    dX: np.ndarray = np.diff(prices).astype(np.float64)  # ΔX_t

    # OLS normal equations  β̂ = (AᵀA)⁻¹ Aᵀ dX
    ones: np.ndarray = np.ones(len(X), dtype=np.float64)
    A:    np.ndarray = np.column_stack([ones, X])       # design matrix (n-1, 2)
    coeffs, _, rank, _ = np.linalg.lstsq(A, dX, rcond=None)

    alpha: float = float(coeffs[0])
    beta:  float = float(coeffs[1])

    if beta >= 0.0:
        # Non-stationary: unit root or explosive regime
        result.regime = "NEUTRAL"
        result.valid  = False
        return result

    theta:    float = -beta
    mu_eq:    float = -alpha / beta
    eps:      np.ndarray = dX - (alpha + beta * X)
    sigma_ou: float = float(np.std(eps, ddof=1))
    half_life:float = float(np.log(2.0) / theta) if theta > 1e-9 else float("inf")

    # OU theoretical path (Euler-Maruyama deterministic skeleton, no noise)
    ou_path: np.ndarray = np.empty(n - 1, dtype=np.float64)
    ou_path[0] = prices[0]
    for i in range(1, n - 1):
        ou_path[i] = ou_path[i - 1] + theta * (mu_eq - ou_path[i - 1])

    # Regime classification
    x_last    = float(prices[-1])
    deviation = abs(x_last - mu_eq)
    if theta > 0.15 and deviation > 0.5 * sigma_ou:
        regime = "EXPLOSIVE_COMPRESSION"
    elif theta > 0.15:
        regime = "RETURN_TO_REST"
    else:
        regime = "NEUTRAL"

    result.theta     = round(theta,    6)
    result.mu_eq     = round(mu_eq,    4)
    result.sigma_ou  = round(sigma_ou, 6)
    result.half_life = round(half_life, 2)
    result.residuals = eps
    result.ou_path   = ou_path
    result.regime    = regime
    result.valid     = True
    return result


def _compute_turbulence(prices: np.ndarray) -> TurbulenceResult:
    """
    Second-derivative pressure-acceleration anomaly filter.

    Physical analogy:
        S(t)  = position (price / pressure)
        v(t)  = dS/dt   ≈ S_t − S_{t-1}         (first finite difference)
        a(t)  = d²S/dt² ≈ S_{t+1} − 2S_t + S_{t-1}   (second finite difference)

    Algorithm
    ──────────
    1.  a_t = prices[2:] − 2·prices[1:-1] + prices[:-2]     (shape: n-2)
    2.  z_t = (a_t − mean(a)) / std(a)                       standardise
    3.  anomaly_mask[t] = True  iff  |z_t| > TURB_ACCEL_STD_THRESH  (2.5σ)
    4.  latest_flag = anomaly_mask[-1]   → real-time turbulence status

    Flag label: "INSTABILITÉ TURBULENTE DÉTECTÉE"
    """
    result = TurbulenceResult()
    n = len(prices)
    if n < 5:
        return result

    # Second finite difference
    accel: np.ndarray = (
        prices[2:].astype(np.float64)
        - 2.0 * prices[1:-1].astype(np.float64)
        + prices[:-2].astype(np.float64)
    )

    mu_a:    float = float(np.mean(accel))
    sigma_a: float = float(np.std(accel, ddof=1))
    if sigma_a < 1e-12:
        return result

    z:     np.ndarray = (accel - mu_a) / sigma_a
    flags: np.ndarray = np.abs(z) > TURB_ACCEL_STD_THRESH

    result.acceleration  = accel
    result.accel_zscore  = z
    result.anomaly_mask  = flags
    result.anomaly_count = int(np.sum(flags))
    result.latest_flag   = bool(flags[-1]) if len(flags) > 0 else False
    result.valid         = True
    return result


def analyze_fluid_kinetics(data: pd.DataFrame) -> FluidKineticsReport:
    """
    Master stochastic analytical pipeline.

    Accepts an enriched OHLCV DataFrame; extracts the close-price vector;
    runs the three sub-engines (GBM → OU → Turbulence) in sequence using
    pure NumPy matrix operations to guarantee < 50 ms computation latency.

    Parameters
    ──────────
    data : pd.DataFrame  with column 'close' and ≥ OU_MIN_SAMPLES + 2 rows.

    Returns
    ───────
    FluidKineticsReport  with populated gbm, ou, turbulence fields and
    calc_ms (measured wall-clock execution time in milliseconds).
    """
    report = FluidKineticsReport()
    if data.empty or "close" not in data.columns:
        return report
    if len(data) < OU_MIN_SAMPLES + 2:
        return report

    t_start = time.perf_counter()

    # C-contiguous float64 array — required for stride tricks
    prices: np.ndarray = np.ascontiguousarray(
        data["close"].to_numpy(dtype=np.float64)
    )

    report.gbm        = _compute_gbm(prices, window=GBM_WINDOW)
    report.ou         = _compute_ou(prices)
    report.turbulence = _compute_turbulence(prices)

    report.calc_ms = round((time.perf_counter() - t_start) * 1000, 3)
    return report


# ─────────────────────────────────────────────────────────────────────────────
# CLASSIC TECHNICAL INDICATORS
# ─────────────────────────────────────────────────────────────────────────────
def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_g = gain.ewm(com=period - 1, adjust=False).mean()
    avg_l = loss.ewm(com=period - 1, adjust=False).mean()
    rs    = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_macd(
    series: pd.Series,
    fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema_f = compute_ema(series, fast)
    ema_s = compute_ema(series, slow)
    macd  = ema_f - ema_s
    sig   = compute_ema(macd, signal)
    return macd, sig, macd - sig


def compute_bollinger(
    series: pd.Series, period: int = 20, std_dev: float = 2.0
) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid = series.rolling(period).mean()
    s   = series.rolling(period).std()
    return mid + std_dev * s, mid, mid - std_dev * s


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"]  - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, adjust=False).mean()


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 5:
        return df
    c = df["close"]
    df["ema9"]  = compute_ema(c, 9)
    df["ema21"] = compute_ema(c, 21)
    df["ema50"] = compute_ema(c, 50)
    df["rsi14"] = compute_rsi(c, 14)
    df["macd"], df["macd_signal"], df["macd_hist"] = compute_macd(c)
    df["bb_up"], df["bb_mid"], df["bb_low"]         = compute_bollinger(c)
    df["atr14"]     = compute_atr(df, 14)
    df["up_candle"] = df["close"] >= df["open"]
    # ── Stochastic Oscillator %K / %D ────────────────────────────────────
    # %K = 100 × (Close − Lowest_Low[K]) / (Highest_High[K] − Lowest_Low[K])
    # %D = SMA(%K, D_period)
    k_p = STOCH_K_PERIOD
    d_p = STOCH_D_PERIOD
    lowest_low   = df["low"].rolling(k_p).min()
    highest_high = df["high"].rolling(k_p).max()
    denom        = (highest_high - lowest_low).replace(0, np.nan)
    df["stoch_k"] = 100.0 * (df["close"] - lowest_low) / denom
    df["stoch_d"] = df["stoch_k"].rolling(d_p).mean()
    return df


# ─────────────────────────────────────────────────────────────────────────────
# CHART BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
_BASE = dict(
    paper_bgcolor = COLOR_BG,
    plot_bgcolor  = COLOR_BG,
    font          = dict(family=FONT_MONO, color=COLOR_UP, size=10),
    margin        = dict(l=8, r=8, t=28, b=8),
    xaxis = dict(showgrid=True, gridcolor=COLOR_GRID, gridwidth=1,
                 color=COLOR_AXIS, tickfont=dict(family=FONT_MONO, size=9),
                 zeroline=False, showspikes=True,
                 spikecolor=COLOR_UP, spikethickness=1),
    yaxis = dict(showgrid=True, gridcolor=COLOR_GRID, gridwidth=1,
                 color=COLOR_AXIS, tickfont=dict(family=FONT_MONO, size=9),
                 zeroline=False, side="right",
                 showspikes=True, spikecolor=COLOR_UP, spikethickness=1),
    legend = dict(bgcolor="rgba(0,0,0,0.6)",
                  bordercolor="rgba(0,255,65,0.3)", borderwidth=1,
                  font=dict(family=FONT_MONO, size=9, color=COLOR_UP)),
    hovermode  = "x unified",
    hoverlabel = dict(bgcolor="#050F05",
                      font=dict(family=FONT_MONO, size=10, color=COLOR_UP),
                      bordercolor=COLOR_UP),
)


# ─────────────────────────────────────────────────────────────────────────────
# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║        ACTION COORDINATE ENGINE  —  Bloc 3                               ║
# ║  Entry · NDT · ERV · HEP  +  Safety Inhibition Logic                    ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
# ─────────────────────────────────────────────────────────────────────────────

def generate_action_coordinates(
    df: pd.DataFrame,
    report: FluidKineticsReport,
) -> ActionCoordinates:
    """
    Derive the four actuation thresholds from live telemetry + kinetics report.

    ══════════════════════════════════════════════════════════════════════
    INHIBITION LOGIC  (highest priority — checked first)
    ══════════════════════════════════════════════════════════════════════
    If  report.turbulence.latest_flag == True  →  PAUSE DE SÉCURITÉ
        • All price coordinates forced to 0.0
        • inhibited = True,  signal_active = False
        • No actuation data is returned to prevent manual override

    ══════════════════════════════════════════════════════════════════════
    ENTRY  —  Point d'Injection
    ══════════════════════════════════════════════════════════════════════
    Condition LONG:
        %K[-2] < %D[-2]  AND  %K[-1] >= %D[-1]   (upward cross)
        AND  %K[-1] < STOCH_EXHAUSTION_ZONE        (< 20, energy exhaustion)
        AND  report.ou.theta > 0.15                (OU compression detected)

    Condition SHORT:
        %K[-2] > %D[-2]  AND  %K[-1] <= %D[-1]   (downward cross)
        AND  %K[-1] > STOCH_OVERBOUGHT_ZONE        (> 80, overbought)
        AND  report.ou.theta > 0.15

    Entry price = close[-1]  (last confirmed candle close)

    ══════════════════════════════════════════════════════════════════════
    NDT  —  Seuil de Décompression Nominale  (Take Profit)
    ══════════════════════════════════════════════════════════════════════
    NDT_LONG  = entry  +  NDT_SIGMA_MULTIPLE × σ_ann × entry
              = entry  ×  (1  +  2.5 × σ_GBM)

    NDT_SHORT = entry  −  NDT_SIGMA_MULTIPLE × σ_ann × entry
              = entry  ×  (1  −  2.5 × σ_GBM)

    Physical meaning: price must travel 2.5 annualised standard deviations
    in the direction of the trade before the decompression phase completes.

    ══════════════════════════════════════════════════════════════════════
    ERV  —  Soupape de Sécurité  (Stop Loss / Rupture irrévocable)
    ══════════════════════════════════════════════════════════════════════
    ERV_LONG  = entry  −  ERV_SIGMA_MULTIPLE × σ_ann × entry
              = entry  ×  (1  −  1.5 × σ_GBM)

    ERV_SHORT = entry  +  ERV_SIGMA_MULTIPLE × σ_ann × entry
              = entry  ×  (1  +  1.5 × σ_GBM)

    Positioned 1.5σ beyond the structural inflection point.
    Irrévocable: once breached, no re-entry is permitted in same session.

    ══════════════════════════════════════════════════════════════════════
    HEP  —  Équilibre Hydrostatique  (Break Even)
    ══════════════════════════════════════════════════════════════════════
    HEP = entry  ×  (1  +  HEP_FRICTION_OFFSET)
        = entry  ×  1.002

    Neutral pressure point compensating for mechanical friction
    (bid/ask spread + commission).  Direction-agnostic.

    ══════════════════════════════════════════════════════════════════════
    RISK/REWARD RATIO
    ══════════════════════════════════════════════════════════════════════
    R/R = |NDT − entry|  /  |ERV − entry|
        = 2.5σ / 1.5σ  =  1.667  (theoretical constant when σ is stable)
    """
    ac = ActionCoordinates()

    # ── Guard: insufficient data ──────────────────────────────────────────
    if df.empty or len(df) < STOCH_K_PERIOD + STOCH_D_PERIOD + 2:
        return ac
    if not report.gbm.valid or not report.ou.valid:
        return ac

    # ── INHIBITION CHECK  (absolute priority) ─────────────────────────────
    if report.turbulence.valid and report.turbulence.latest_flag:
        ac.inhibited      = True
        ac.inhibit_reason = "INSTABILITÉ TURBULENTE HORS-LIMITE"
        ac.direction      = "INHIBITED"
        ac.signal_active  = False
        ac.stoch_k        = float(df["stoch_k"].iloc[-1]) if "stoch_k" in df.columns else 0.0
        ac.stoch_d        = float(df["stoch_d"].iloc[-1]) if "stoch_d" in df.columns else 0.0
        ac.ou_theta       = report.ou.theta
        ac.sigma_ann      = report.gbm.sigma
        # All actuation prices FORCED to zero — prevent manual override
        ac.entry          = 0.0
        ac.ndt            = 0.0
        ac.erv            = 0.0
        ac.hep            = 0.0
        ac.rr_ratio       = 0.0
        ac.valid          = True
        return ac

    # ── Extract stochastic values ─────────────────────────────────────────
    if "stoch_k" not in df.columns or "stoch_d" not in df.columns:
        return ac

    stoch_k_series = df["stoch_k"].dropna()
    stoch_d_series = df["stoch_d"].dropna()
    if len(stoch_k_series) < 2 or len(stoch_d_series) < 2:
        return ac

    k_now:  float = float(stoch_k_series.iloc[-1])
    d_now:  float = float(stoch_d_series.iloc[-1])
    k_prev: float = float(stoch_k_series.iloc[-2])
    d_prev: float = float(stoch_d_series.iloc[-2])

    # ── Detect %K / %D crossover ──────────────────────────────────────────
    cross_up:   bool = (k_prev < d_prev) and (k_now >= d_now)   # bullish cross
    cross_down: bool = (k_prev > d_prev) and (k_now <= d_now)   # bearish cross

    # ── OU compression condition ──────────────────────────────────────────
    ou_compressing: bool = report.ou.theta > 0.15

    # ── Entry signal evaluation ───────────────────────────────────────────
    entry_price: float = float(df["close"].iloc[-1])
    direction:   str   = "WAIT"
    signal:      bool  = False

    if cross_up and k_now < STOCH_EXHAUSTION_ZONE and ou_compressing:
        direction = "LONG"
        signal    = True
    elif cross_down and k_now > STOCH_OVERBOUGHT_ZONE and ou_compressing:
        direction = "SHORT"
        signal    = True

    # ── Retrieve annualised σ from GBM ────────────────────────────────────
    sigma_ann: float = report.gbm.sigma       # annualised; > 0 guaranteed by valid check

    # ── Compute coordinate thresholds ────────────────────────────────────
    sigma_offset_ndt: float = NDT_SIGMA_MULTIPLE * sigma_ann * entry_price   # 2.5σ × S
    sigma_offset_erv: float = ERV_SIGMA_MULTIPLE * sigma_ann * entry_price   # 1.5σ × S

    if direction == "LONG":
        ndt = entry_price + sigma_offset_ndt
        erv = entry_price - sigma_offset_erv
    elif direction == "SHORT":
        ndt = entry_price - sigma_offset_ndt
        erv = entry_price + sigma_offset_erv
    else:
        # No signal: coordinates computed relative to current price (for display)
        ndt = entry_price + sigma_offset_ndt
        erv = entry_price - sigma_offset_erv

    hep: float = entry_price * (1.0 + HEP_FRICTION_OFFSET)

    # ── Risk / Reward ratio ───────────────────────────────────────────────
    denom_rr = abs(erv - entry_price)
    rr_ratio: float = (
        round(abs(ndt - entry_price) / denom_rr, 3)
        if denom_rr > 1e-6 else 0.0
    )

    # ── Populate result ───────────────────────────────────────────────────
    ac.entry         = round(entry_price,       3)
    ac.ndt           = round(ndt,               3)
    ac.erv           = round(erv,               3)
    ac.hep           = round(hep,               3)
    ac.direction     = direction
    ac.stoch_k       = round(k_now,             3)
    ac.stoch_d       = round(d_now,             3)
    ac.k_crosses_d   = cross_up or cross_down
    ac.ou_theta      = report.ou.theta
    ac.sigma_ann     = sigma_ann
    ac.inhibited     = False
    ac.inhibit_reason= ""
    ac.signal_active = signal
    ac.rr_ratio      = rr_ratio
    ac.valid         = True
    return ac


# ─────────────────────────────────────────────────────────────────────────────
# ACTION COORDINATES UI RENDERER
# ─────────────────────────────────────────────────────────────────────────────
def render_action_coordinates(ac: ActionCoordinates) -> None:
    """
    Renders the full Action Coordinate panel inside the live fragment.

    Layout
    ──────
    Row 1: Section header + signal badge + stochastic bar
    Row 2: 4 × st.metric  (Entry | NDT | ERV | HEP)
    Row 3: Detail cards   (direction, R/R, θ, σ, %K, %D)

    If inhibited: replaces all content with PAUSE DE SÉCURITÉ overlay.
    """
    st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)

    # ── INHIBITION STATE ─────────────────────────────────────────────────
    if ac.inhibited:
        st.markdown(
            f'<div class="inhibit-overlay">'
            f'<div class="inhibit-title">⛔ PAUSE DE SÉCURITÉ — SYSTÈME VERROUILLÉ</div>'
            f'<div class="inhibit-sub">'
            f'MOTIF : {ac.inhibit_reason}<br>'
            f'Toutes les coordonnées d\'actionnement ont été neutralisées.<br>'
            f'Aucune intervention opérateur autorisée jusqu\'à stabilisation du flux.'
            f'</div>'
            f'<div class="inhibit-zero">'
            f'ENTRY: 0.000  │  NDT: 0.000  │  ERV: 0.000  │  HEP: 0.000'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        # Metric row with zeroed values
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("⛔ ENTRY  [LOCKED]", "0.000")
        c2.metric("⛔ NDT    [LOCKED]", "0.000")
        c3.metric("⛔ ERV    [LOCKED]", "0.000")
        c4.metric("⛔ HEP    [LOCKED]", "0.000")
        st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)
        return

    if not ac.valid:
        st.info("⏳ Action Coordinate Engine — accumulating data…")
        return

    # ── SECTION HEADER ────────────────────────────────────────────────────
    hcol1, hcol2, hcol3 = st.columns([2, 1, 1])
    with hcol1:
        st.markdown(
            '<div class="ac-title">⬡ COORDONNÉES D\'ACTIONNEMENT  —  INGÉNIEUR DE GARDE</div>',
            unsafe_allow_html=True,
        )
    with hcol2:
        sig_css = {
            "LONG":  "signal-long",
            "SHORT": "signal-short",
            "WAIT":  "signal-wait",
        }.get(ac.direction, "signal-wait")
        sig_icon = {"LONG": "▲ LONG", "SHORT": "▼ SHORT", "WAIT": "◌ ATTENTE"}.get(
            ac.direction, "◌ ATTENTE"
        )
        active_str = "● ACTIF" if ac.signal_active else "○ VEILLE"
        st.markdown(
            f'<div style="text-align:center; padding-top:4px;">'
            f'<span class="{sig_css}">{sig_icon}</span><br>'
            f'<span style="font-size:0.62rem; color:#00CC33;">{active_str}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    with hcol3:
        rr_color = COLOR_UP if ac.rr_ratio >= 1.5 else COLOR_AMBER
        st.markdown(
            f'<div style="text-align:center; padding-top:4px;">'
            f'<span style="font-family:{FONT_MONO}; font-size:0.68rem; color:#00CC33;">R/R RATIO</span><br>'
            f'<span style="font-family:{FONT_MONO}; font-size:1.3rem; color:{rr_color}; '
            f'text-shadow:0 0 8px {rr_color};">{ac.rr_ratio:.3f}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Stochastic oscillator visual bar ──────────────────────────────────
    k_pct = min(max(ac.stoch_k, 0.0), 100.0)
    if k_pct < 20:
        bar_color = COLOR_UP
    elif k_pct > 80:
        bar_color = COLOR_DOWN
    else:
        bar_color = COLOR_AMBER
    st.markdown(
        f'<div style="font-family:{FONT_MONO}; font-size:0.65rem; color:#00CC33; margin-top:4px;">'
        f'STOCHASTIC OSCILLATOR  %K={ac.stoch_k:.1f}  %D={ac.stoch_d:.1f}'
        f'{"  ⚡ CROSSOVER DÉTECTÉ" if ac.k_crosses_d else ""}'
        f'</div>'
        f'<div class="stoch-bar-outer">'
        f'<div class="stoch-bar-inner" style="width:{k_pct:.1f}%; '
        f'background:linear-gradient(90deg,{bar_color}66,{bar_color});"></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── 4-column metric row ───────────────────────────────────────────────
    m_entry, m_ndt, m_erv, m_hep = st.columns(4)

    entry_delta = (
        f"θ={ac.ou_theta:.4f} · σ={ac.sigma_ann:.4f}"
        if ac.ou_theta > 0 else None
    )

    m_entry.metric(
        label = "▸ ENTRY  [INJECTION]",
        value = f"{ac.entry:,.3f}" if ac.entry != 0 else "—",
        delta = entry_delta,
    )
    m_ndt.metric(
        label = "▸ NDT  [TAKE PROFIT]",
        value = f"{ac.ndt:,.3f}" if ac.ndt != 0 else "—",
        delta = f"+{NDT_SIGMA_MULTIPLE}σ × S" if ac.direction == "LONG"
                else f"−{NDT_SIGMA_MULTIPLE}σ × S",
    )
    m_erv.metric(
        label = "▸ ERV  [STOP LOSS]",
        value = f"{ac.erv:,.3f}" if ac.erv != 0 else "—",
        delta = f"−{ERV_SIGMA_MULTIPLE}σ × S" if ac.direction == "LONG"
                else f"+{ERV_SIGMA_MULTIPLE}σ × S",
    )
    m_hep.metric(
        label = "▸ HEP  [BREAK EVEN]",
        value = f"{ac.hep:,.3f}" if ac.hep != 0 else "—",
        delta = f"+{HEP_FRICTION_OFFSET*100:.1f}% friction offset",
    )

    # ── Detail cards row ──────────────────────────────────────────────────
    st.markdown('<div class="ac-panel">', unsafe_allow_html=True)
    st.markdown('<div class="ac-title">▸ PARAMÈTRES DE CALCUL</div>', unsafe_allow_html=True)

    dc1, dc2, dc3, dc4, dc5, dc6 = st.columns(6)

    dir_color = {
        "LONG":  COLOR_UP,
        "SHORT": COLOR_DOWN,
        "WAIT":  COLOR_AMBER,
    }.get(ac.direction, COLOR_AMBER)
    dc1.metric("DIRECTION",     ac.direction)
    dc2.metric("SIGNAL",        "ACTIF" if ac.signal_active else "VEILLE")
    dc3.metric("%K STOCH",      f"{ac.stoch_k:.2f}")
    dc4.metric("%D STOCH",      f"{ac.stoch_d:.2f}")
    dc5.metric("θ OU SPEED",    f"{ac.ou_theta:.5f}")
    dc6.metric("σ GBM (ann.)",  f"{ac.sigma_ann:.5f}")

    # ── Coordinate detail cards ───────────────────────────────────────────
    dist_ndt = abs(ac.ndt - ac.entry)
    dist_erv = abs(ac.erv - ac.entry)
    dist_hep = abs(ac.hep - ac.entry)

    st.markdown(
        f'<div style="display:flex; gap:10px; margin-top:10px; flex-wrap:wrap;">'

        f'<div class="coord-card entry" style="flex:1; min-width:140px;">'
        f'<div class="coord-label entry">ENTRY  —  INJECTION</div>'
        f'<div class="coord-value entry">{ac.entry:,.3f}</div>'
        f'<div class="coord-sub">%K={ac.stoch_k:.1f} · θ={ac.ou_theta:.4f}</div>'
        f'</div>'

        f'<div class="coord-card ndt" style="flex:1; min-width:140px;">'
        f'<div class="coord-label ndt">NDT  —  DÉCOMPRESSION</div>'
        f'<div class="coord-value ndt">{ac.ndt:,.3f}</div>'
        f'<div class="coord-sub">Δ={dist_ndt:,.3f}  ({NDT_SIGMA_MULTIPLE}σ×S)</div>'
        f'</div>'

        f'<div class="coord-card erv" style="flex:1; min-width:140px;">'
        f'<div class="coord-label erv">ERV  —  SOUPAPE</div>'
        f'<div class="coord-value erv">{ac.erv:,.3f}</div>'
        f'<div class="coord-sub">Δ={dist_erv:,.3f}  ({ERV_SIGMA_MULTIPLE}σ×S)</div>'
        f'</div>'

        f'<div class="coord-card hep" style="flex:1; min-width:140px;">'
        f'<div class="coord-label hep">HEP  —  ÉQUILIBRE</div>'
        f'<div class="coord-value hep">{ac.hep:,.3f}</div>'
        f'<div class="coord-sub">Δ={dist_hep:,.3f}  (+{HEP_FRICTION_OFFSET*100:.1f}%)</div>'
        f'</div>'

        f'</div>',
        unsafe_allow_html=True,
    )

    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)


def build_stochastic_chart(df: pd.DataFrame, ac: ActionCoordinates) -> go.Figure:
    """
    Stochastic oscillator chart with:
    • %K line (white/green)
    • %D line (amber)
    • Overbought (80) / Oversold (20) / Midline (50) bands
    • Entry crossover marker if signal_active
    """
    fig = go.Figure()
    if "stoch_k" not in df.columns or df.empty:
        return fig

    tail  = df.tail(80)
    ts    = tail["timestamp"]
    k_val = tail["stoch_k"].fillna(50)
    d_val = tail["stoch_d"].fillna(50)

    # Filled zone between %K and %D
    fig.add_trace(go.Scatter(
        x=ts, y=k_val, name="%K",
        line=dict(color=COLOR_UP, width=1.4),
        fill=None,
    ))
    fig.add_trace(go.Scatter(
        x=ts, y=d_val, name="%D",
        line=dict(color=COLOR_AMBER, width=1.2, dash="dot"),
        fill="tonexty",
        fillcolor="rgba(0,255,65,0.04)",
    ))

    # Threshold bands
    for lvl, col, lbl in [
        (STOCH_OVERBOUGHT_ZONE, "rgba(255,60,60,0.3)",  "OB 80"),
        (50.0,                  "rgba(255,255,255,0.1)", "MID 50"),
        (STOCH_EXHAUSTION_ZONE, "rgba(0,255,65,0.3)",   "OS 20"),
    ]:
        fig.add_hline(y=lvl, line_dash="dot", line_color=col,
                      annotation_text=lbl,
                      annotation_font=dict(family=FONT_MONO, size=8,
                                           color=col.replace("0.3)", "0.9)")))

    # Crossover entry marker
    if ac.signal_active and ac.valid and not ac.inhibited:
        last_ts = tail["timestamp"].iloc[-1]
        last_k  = float(k_val.iloc[-1])
        marker_color = COLOR_UP if ac.direction == "LONG" else COLOR_DOWN
        marker_sym   = "triangle-up" if ac.direction == "LONG" else "triangle-down"
        fig.add_trace(go.Scatter(
            x=[last_ts], y=[last_k],
            mode="markers", name=f"⚡ {ac.direction}",
            marker=dict(color=marker_color, size=14, symbol=marker_sym,
                        line=dict(color=marker_color, width=1.5)),
        ))

    fig.update_layout(
        **_BASE, height=160,
        title=dict(
            text=(f"STOCHASTIC  %K({STOCH_K_PERIOD}) · %D({STOCH_D_PERIOD})"
                  f"  │  %K={ac.stoch_k:.1f}  │  %D={ac.stoch_d:.1f}"
                  f"  │  {'⚡ CROSSOVER' if ac.k_crosses_d else 'NO CROSS'}"),
            font=dict(family=FONT_MONO, size=10, color=COLOR_UP),
        ),
        yaxis=dict(range=[0, 100], side="right",
                   tickfont=dict(size=8), showgrid=True, gridcolor=COLOR_GRID),
        margin=dict(l=4, r=4, t=30, b=4),
        showlegend=True,
        legend=dict(font=dict(family=FONT_MONO, size=8),
                    bgcolor="rgba(0,0,0,0.6)"),
    )
    return fig


def build_coord_chart(df: pd.DataFrame, ac: ActionCoordinates) -> go.Figure:
    """
    Price chart overlaying the four action coordinate levels as horizontal lines.
    Shows last 40 candles of the 1M feed with ENTRY / NDT / ERV / HEP annotations.
    """
    fig = go.Figure()
    if df.empty or not ac.valid or ac.inhibited:
        return fig

    tail = df.tail(40)
    ts   = tail["timestamp"]

    fig.add_trace(go.Candlestick(
        x=tail["timestamp"],
        open=tail["open"], high=tail["high"],
        low=tail["low"],   close=tail["close"],
        name="PRICE",
        increasing=dict(line=dict(color=COLOR_UP,   width=1), fillcolor=COLOR_UP),
        decreasing=dict(line=dict(color=COLOR_DOWN, width=1), fillcolor=COLOR_DOWN),
        whiskerwidth=0.4,
    ))

    # Coordinate lines
    levels = [
        (ac.ndt,   COLOR_CYAN,   "NDT",   "solid"),
        (ac.entry, COLOR_UP,     "ENTRY", "solid"),
        (ac.hep,   COLOR_AMBER,  "HEP",   "dash"),
        (ac.erv,   COLOR_DOWN,   "ERV",   "dot"),
    ]
    for price, color, label, dash in levels:
        if price > 0:
            fig.add_hline(
                y=price,
                line_color=color,
                line_dash=dash,
                line_width=1.5,
                annotation_text=f"{label}  {price:,.3f}",
                annotation_position="right",
                annotation_font=dict(family=FONT_MONO, size=9, color=color),
            )

    dir_color = {"LONG": COLOR_UP, "SHORT": COLOR_DOWN, "WAIT": COLOR_AMBER}.get(
        ac.direction, COLOR_AMBER
    )
    fig.update_layout(
        **_BASE, height=280,
        title=dict(
            text=(f"COORDINATE MAP  │  {ac.direction}"
                  f"  │  R/R={ac.rr_ratio:.3f}"
                  f"  │  {'⚡ SIGNAL ACTIF' if ac.signal_active else 'VEILLE'}"),
            font=dict(family=FONT_MONO, size=10, color=dir_color),
        ),
        xaxis_rangeslider_visible=False,
        margin=dict(l=4, r=4, t=32, b=4),
        showlegend=False,
    )
    fig.update_yaxes(side="right", tickfont=dict(size=8),
                     showgrid=True, gridcolor=COLOR_GRID)
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# CHART BUILDERS  (pre-existing — see below)
# ─────────────────────────────────────────────────────────────────────────────

def build_main_chart(df: pd.DataFrame, title: str = "XA·UUSD  KINETIC PRESSURE") -> go.Figure:
    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.55, 0.15, 0.15, 0.15],
        vertical_spacing=0.015,
    )
    ts = df["timestamp"]
    o, h, l, c = df["open"], df["high"], df["low"], df["close"]

    fig.add_trace(go.Candlestick(
        x=ts, open=o, high=h, low=l, close=c, name="KLINE",
        increasing=dict(line=dict(color=COLOR_UP,   width=1), fillcolor=COLOR_UP),
        decreasing=dict(line=dict(color=COLOR_DOWN, width=1), fillcolor=COLOR_DOWN),
        whiskerwidth=0.3,
    ), row=1, col=1)

    if "bb_up" in df.columns:
        fig.add_trace(go.Scatter(x=ts, y=df["bb_up"], name="BB+2σ",
            line=dict(color="rgba(0,229,255,0.35)", width=1, dash="dot")), row=1, col=1)
        fig.add_trace(go.Scatter(x=ts, y=df["bb_mid"], name="BB mid",
            line=dict(color="rgba(0,229,255,0.20)", width=1, dash="dash")), row=1, col=1)
        fig.add_trace(go.Scatter(x=ts, y=df["bb_low"], name="BB−2σ",
            line=dict(color="rgba(0,229,255,0.35)", width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(0,229,255,0.03)"), row=1, col=1)

    for col_n, col_c, lbl in [
        ("ema9",  "#FFB300",              "EMA9"),
        ("ema21", "rgba(255,60,60,0.7)",  "EMA21"),
        ("ema50", "rgba(0,255,65,0.5)",   "EMA50"),
    ]:
        if col_n in df.columns:
            fig.add_trace(go.Scatter(x=ts, y=df[col_n], name=lbl,
                line=dict(color=col_c, width=1)), row=1, col=1)

    vcol = [COLOR_VOLUME_UP if u else COLOR_VOLUME_DOWN for u in df["up_candle"]]
    fig.add_trace(go.Bar(x=ts, y=df["volume"], name="VOL",
        marker_color=vcol, showlegend=False), row=2, col=1)

    if "rsi14" in df.columns:
        fig.add_trace(go.Scatter(x=ts, y=df["rsi14"], name="RSI14",
            line=dict(color="#FFB300", width=1.2), showlegend=False), row=3, col=1)
        for lvl in [70, 30]:
            fig.add_hline(y=lvl, line_dash="dot",
                          line_color="rgba(255,255,255,0.15)", row=3, col=1)

    if "macd" in df.columns:
        hc = [COLOR_UP if v >= 0 else COLOR_DOWN for v in df["macd_hist"].fillna(0)]
        fig.add_trace(go.Bar(x=ts, y=df["macd_hist"], name="HIST",
            marker_color=hc, showlegend=False), row=4, col=1)
        fig.add_trace(go.Scatter(x=ts, y=df["macd"], name="MACD",
            line=dict(color="#00E5FF", width=1.2), showlegend=False), row=4, col=1)
        fig.add_trace(go.Scatter(x=ts, y=df["macd_signal"], name="SIG",
            line=dict(color="#FFB300", width=1.2), showlegend=False), row=4, col=1)

    lay = {**_BASE}
    lay["title"] = dict(text=title,
                        font=dict(family=FONT_MONO, size=12, color=COLOR_UP),
                        x=0.01, y=0.99)
    lay["height"] = 640
    lay["xaxis_rangeslider_visible"] = False
    for i in range(1, 5):
        ak = "yaxis" if i == 1 else f"yaxis{i}"
        lay[ak] = dict(showgrid=True, gridcolor=COLOR_GRID, gridwidth=1,
                       color=COLOR_AXIS, tickfont=dict(family=FONT_MONO, size=9),
                       zeroline=False, side="right")
    fig.update_layout(**lay)
    fig.update_xaxes(showgrid=True, gridcolor=COLOR_GRID, color=COLOR_AXIS)
    return fig


def build_depth_gauge(last: float, prev: float, high: float, low: float) -> go.Figure:
    rng   = high - low
    pct   = round(((last - low) / rng) * 100, 1) if rng > 0 else 0.0
    color = COLOR_UP if last >= prev else COLOR_DOWN
    fig   = go.Figure(go.Indicator(
        mode  = "gauge+number+delta",
        value = last,
        delta = dict(reference=prev, valueformat=".2f",
                     increasing=dict(color=COLOR_UP),
                     decreasing=dict(color=COLOR_DOWN)),
        number= dict(font=dict(family=FONT_MONO, size=22, color=color),
                     valueformat=".2f", suffix=" USD"),
        gauge = dict(
            axis=dict(range=[low * 0.998, high * 1.002], tickwidth=1,
                      tickcolor=COLOR_AXIS, tickfont=dict(family=FONT_MONO, size=8)),
            bar        = dict(color=color, thickness=0.25),
            bgcolor    = COLOR_BG, borderwidth=1,
            bordercolor= "rgba(0,255,65,0.3)",
            steps=[
                dict(range=[low * 0.998,      low + rng * 0.33], color="rgba(255,60,60,0.08)"),
                dict(range=[low + rng * 0.33, low + rng * 0.66], color="rgba(255,179,0,0.06)"),
                dict(range=[low + rng * 0.66, high * 1.002],     color="rgba(0,255,65,0.08)"),
            ],
            threshold=dict(line=dict(color=color, width=3), thickness=0.8, value=last),
        ),
        title=dict(
            text=f"PRESSURE INDEX<br><span style='font-size:0.7em'>%POS: {pct}%</span>",
            font=dict(family=FONT_MONO, size=11, color=COLOR_UP),
        ),
    ))
    fig.update_layout(paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG,
                      font=dict(family=FONT_MONO, color=COLOR_UP),
                      height=260, margin=dict(l=12, r=12, t=36, b=12))
    return fig


def build_rsi_spark(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if "rsi14" not in df.columns or df.empty:
        return fig
    rsi = df["rsi14"].dropna().tail(60)
    ts  = df.loc[rsi.index, "timestamp"]
    fig.add_trace(go.Scatter(x=ts, y=rsi, mode="lines",
        line=dict(color="#FFB300", width=1.5),
        fill="tozeroy", fillcolor="rgba(255,179,0,0.06)", name="RSI14"))
    for lvl in [70, 50, 30]:
        fig.add_hline(y=lvl, line_dash="dot",
                      line_color="rgba(255,255,255,0.15)")
    fig.update_layout(**_BASE, height=140,
        title=dict(text="RSI·14", font=dict(size=10, family=FONT_MONO)),
        yaxis=dict(range=[0, 100], side="right",
                   tickfont=dict(size=8), showgrid=True, gridcolor=COLOR_GRID),
        margin=dict(l=4, r=4, t=22, b=4), showlegend=False)
    return fig


def build_volatility_heatmap(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if "atr14" not in df.columns or df.empty:
        return fig
    tail = df.tail(30)
    atr  = tail["atr14"].fillna(0).values
    ts   = tail["timestamp"].values
    fig.add_trace(go.Bar(x=ts, y=atr, name="ATR14",
        marker=dict(color=atr,
                    colorscale=[[0.0, "rgba(0,255,65,0.2)"],
                                [0.5, "rgba(255,179,0,0.5)"],
                                [1.0, "rgba(255,60,60,0.8)"]],
                    showscale=False)))
    fig.update_layout(**_BASE, height=130,
        title=dict(text="VOLATILITY FLUX  (ATR·14)", font=dict(size=10, family=FONT_MONO)),
        yaxis=dict(side="right", tickfont=dict(size=8),
                   showgrid=True, gridcolor=COLOR_GRID),
        margin=dict(l=4, r=4, t=22, b=4), showlegend=False)
    return fig


def build_gbm_sigma_chart(df: pd.DataFrame, gbm: GBMResult) -> go.Figure:
    """
    Dual-axis chart:
      Primary Y   — rolling annualised volatility σ_rolling(t)  [cyan fill]
      Secondary Y — log-returns r_t  [green/red bars]
    Title annotates global μ_ann and σ_ann from GBM fit.
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if not gbm.valid or df.empty:
        return fig

    ts_all  = df["timestamp"].values
    n_wins  = len(gbm.sigma_rolling)
    n_ret   = len(gbm.log_returns)

    # sigma_rolling[i] aligns to the window ending at price index (GBM_WINDOW + i)
    win_end_idx = np.clip(np.arange(GBM_WINDOW, GBM_WINDOW + n_wins), 0, len(ts_all) - 1)
    ts_sigma    = ts_all[win_end_idx]

    # log-returns align to price indices [1 .. n_ret]
    ts_ret = ts_all[1: 1 + n_ret]

    fig.add_trace(go.Scatter(
        x=ts_sigma, y=gbm.sigma_rolling,
        name="σ rolling (ann.)",
        line=dict(color=COLOR_CYAN, width=1.5),
        fill="tozeroy", fillcolor="rgba(0,229,255,0.06)",
    ), secondary_y=False)

    ret_colors = [COLOR_UP if r >= 0 else COLOR_DOWN for r in gbm.log_returns]
    fig.add_trace(go.Bar(
        x=ts_ret, y=gbm.log_returns,
        name="r_t  log-return",
        marker_color=ret_colors, opacity=0.55,
    ), secondary_y=True)

    fig.update_layout(
        **_BASE, height=210,
        title=dict(
            text=(f"GBM KINETICS  │  μ(ann)={gbm.mu:+.6f}"
                  f"  │  σ(ann)={gbm.sigma:.6f}"
                  f"  │  window={GBM_WINDOW} cycles"),
            font=dict(family=FONT_MONO, size=10, color=COLOR_CYAN),
        ),
        margin=dict(l=4, r=4, t=32, b=4),
        showlegend=True,
        legend=dict(font=dict(family=FONT_MONO, size=8),
                    bgcolor="rgba(0,0,0,0.6)", x=0.01, y=0.99),
    )
    fig.update_yaxes(title_text="σ ann.", showgrid=True, gridcolor=COLOR_GRID,
                     color=COLOR_CYAN, tickfont=dict(family=FONT_MONO, size=8),
                     secondary_y=False)
    fig.update_yaxes(title_text="r_t", showgrid=False,
                     color=COLOR_UP, tickfont=dict(family=FONT_MONO, size=8),
                     secondary_y=True)
    return fig


def build_ou_chart(df: pd.DataFrame, ou: OUResult) -> go.Figure:
    """
    Four traces:
      1. S_t  actual close price        [green line]
      2. X̄_t  OU deterministic path     [purple dotted]
      3. μ_eq equilibrium horizontal    [amber dashed hline]
      4. ε_t  residuals                 [green/red bars, secondary Y]
    Title shows θ, μ_eq, T½, and current regime.
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if not ou.valid or df.empty:
        return fig

    n_path  = len(ou.ou_path)
    ts      = df["timestamp"].values
    ts_path = ts[:n_path]

    fig.add_trace(go.Scatter(
        x=ts_path, y=df["close"].values[:n_path],
        name="S_t (actual)", line=dict(color=COLOR_UP, width=1.2),
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=ts_path, y=ou.ou_path,
        name="X̄_t  (OU path)", line=dict(color=COLOR_PURPLE, width=1.2, dash="dot"),
    ), secondary_y=False)

    fig.add_hline(
        y=ou.mu_eq, line_dash="dash", line_color="rgba(255,179,0,0.5)",
        annotation_text=f"μ_eq = {ou.mu_eq:.3f}",
        annotation_font=dict(family=FONT_MONO, size=8, color=COLOR_AMBER),
    )

    n_res   = len(ou.residuals)
    ts_res  = ts[:n_res]
    res_col = ["rgba(0,255,65,0.5)" if r >= 0 else "rgba(255,60,60,0.5)"
               for r in ou.residuals]
    fig.add_trace(go.Bar(
        x=ts_res, y=ou.residuals,
        name="ε_t residuals", marker_color=res_col, opacity=0.5,
    ), secondary_y=True)

    regime_color = {"EXPLOSIVE_COMPRESSION": COLOR_DOWN,
                    "RETURN_TO_REST":        COLOR_UP,
                    "NEUTRAL":               COLOR_AMBER}.get(ou.regime, COLOR_AMBER)

    fig.update_layout(
        **_BASE, height=230,
        title=dict(
            text=(f"OU RELAXATION  │  θ={ou.theta:.6f}"
                  f"  │  μ_eq={ou.mu_eq:.3f}"
                  f"  │  T½={ou.half_life:.2f} p"
                  f"  │  σ_ou={ou.sigma_ou:.6f}"
                  f"  │  <span style='color:{regime_color}'>{ou.regime}</span>"),
            font=dict(family=FONT_MONO, size=10, color=COLOR_PURPLE),
        ),
        margin=dict(l=4, r=4, t=34, b=4),
        showlegend=True,
        legend=dict(font=dict(family=FONT_MONO, size=8),
                    bgcolor="rgba(0,0,0,0.6)", x=0.01, y=0.99),
    )
    fig.update_yaxes(title_text="Price", showgrid=True, gridcolor=COLOR_GRID,
                     color=COLOR_UP, tickfont=dict(family=FONT_MONO, size=8),
                     secondary_y=False)
    fig.update_yaxes(title_text="ε_t", showgrid=False,
                     color=COLOR_PURPLE, tickfont=dict(family=FONT_MONO, size=8),
                     secondary_y=True)
    return fig


def build_turbulence_chart(df: pd.DataFrame, turb: TurbulenceResult) -> go.Figure:
    """
    Z-score of second-derivative acceleration Δ²P(t).
    Threshold bands at ±2.5σ.
    Anomaly cycles flagged with red × markers.
    Title shows live turbulence status.
    """
    fig = go.Figure()
    if not turb.valid or df.empty:
        return fig

    n_a  = len(turb.accel_zscore)
    ts   = df["timestamp"].values
    ts_a = ts[2: 2 + n_a]   # Δ² starts at index 2

    fig.add_trace(go.Scatter(
        x=ts_a, y=turb.accel_zscore,
        name="Δ²P  z-score",
        line=dict(color=COLOR_AMBER, width=1.2),
        fill="tozeroy", fillcolor="rgba(255,179,0,0.04)",
    ))

    for lvl in [TURB_ACCEL_STD_THRESH, -TURB_ACCEL_STD_THRESH]:
        fig.add_hline(
            y=lvl, line_dash="dot", line_color="rgba(255,60,60,0.55)",
            annotation_text=f"±{TURB_ACCEL_STD_THRESH}σ",
            annotation_font=dict(family=FONT_MONO, size=8, color=COLOR_DOWN),
        )

    anom_idx = np.where(turb.anomaly_mask)[0]
    if len(anom_idx) > 0:
        fig.add_trace(go.Scatter(
            x=ts_a[anom_idx], y=turb.accel_zscore[anom_idx],
            mode="markers", name="⚠ TURBULENCE",
            marker=dict(color=COLOR_DOWN, size=9, symbol="x",
                        line=dict(color=COLOR_DOWN, width=1.5)),
        ))

    status_text = (
        "⚠  INSTABILITÉ TURBULENTE DÉTECTÉE"
        if turb.latest_flag else "✓  FLUX NOMINAL"
    )
    fig.update_layout(
        **_BASE, height=170,
        title=dict(
            text=(f"TURBULENCE FILTER  │  Δ²P z-score"
                  f"  │  anomalies: {turb.anomaly_count}"
                  f"  │  {status_text}"),
            font=dict(family=FONT_MONO, size=10,
                      color=COLOR_DOWN if turb.latest_flag else COLOR_UP),
        ),
        yaxis=dict(side="right", tickfont=dict(size=8),
                   showgrid=True, gridcolor=COLOR_GRID,
                   zeroline=True, zerolinecolor="rgba(255,255,255,0.1)"),
        margin=dict(l=4, r=4, t=32, b=4),
        showlegend=True,
        legend=dict(font=dict(family=FONT_MONO, size=8),
                    bgcolor="rgba(0,0,0,0.6)"),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS SUMMARY PANEL
# ─────────────────────────────────────────────────────────────────────────────
def render_analytics_panel(report: FluidKineticsReport) -> None:
    st.markdown('<div class="q-divider-cyan"></div>', unsafe_allow_html=True)

    gbm  = report.gbm
    ou   = report.ou
    turb = report.turbulence

    col_gbm, col_ou, col_turb = st.columns(3)

    # ── GBM ──────────────────────────────────────────────────────────────
    with col_gbm:
        st.markdown(
            '<div class="q-analytics-panel">'
            '<div class="q-analytics-title">▸ GBM  —  MOUVEMENT BROWNIEN GÉOMÉTRIQUE</div>'
            '<div class="q-eq">dS_t = μ·S_t·dt  +  σ·S_t·dW_t</div>',
            unsafe_allow_html=True,
        )
        if gbm.valid:
            mu_col  = COLOR_UP   if gbm.mu  >= 0 else COLOR_DOWN
            sig_col = COLOR_AMBER if gbm.sigma > 0.25 else COLOR_UP
            sig_inst = float(gbm.sigma_rolling[-1]) if len(gbm.sigma_rolling) > 0 else 0.0
            var_inst = float(gbm.variance_window[-1]) if len(gbm.variance_window) > 0 else 0.0
            st.markdown(
                f'<div class="data-row"><span class="data-label">μ DRIFT (ann.)</span>'
                f'<span style="color:{mu_col};font-family:{FONT_MONO};">{gbm.mu:+.6f}</span></div>'
                f'<div class="data-row"><span class="data-label">σ VOLATILITY (ann.)</span>'
                f'<span style="color:{sig_col};font-family:{FONT_MONO};">{gbm.sigma:.6f}</span></div>'
                f'<div class="data-row"><span class="data-label">σ INSTANT (last win)</span>'
                f'<span class="data-value">{sig_inst:.6f}</span></div>'
                f'<div class="data-row"><span class="data-label">VAR WINDOW (last)</span>'
                f'<span class="data-value">{var_inst:.8f}</span></div>'
                f'<div class="data-row"><span class="data-label">WINDOW SIZE</span>'
                f'<span class="data-value">{GBM_WINDOW} cycles</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<span style="color:#FF3C3C;font-size:0.75rem;">⚠ Insufficient data for GBM</span>',
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── OU ───────────────────────────────────────────────────────────────
    with col_ou:
        r_css = {"EXPLOSIVE_COMPRESSION": "regime-compress",
                 "RETURN_TO_REST":        "regime-relax",
                 "NEUTRAL":               "regime-neutral"}.get(ou.regime, "regime-neutral")
        st.markdown(
            '<div class="q-analytics-panel">'
            '<div class="q-analytics-title">▸ OU  —  RELAXATION ORNSTEIN-UHLENBECK</div>'
            '<div class="q-eq">dX_t = θ·(μ − X_t)·dt  +  σ·dW_t</div>',
            unsafe_allow_html=True,
        )
        if ou.valid:
            st.markdown(
                f'<div class="data-row"><span class="data-label">θ MEAN-REVERSION</span>'
                f'<span class="data-value">{ou.theta:.6f}</span></div>'
                f'<div class="data-row"><span class="data-label">μ_eq EQUILIBRIUM</span>'
                f'<span class="data-value">{ou.mu_eq:.4f}</span></div>'
                f'<div class="data-row"><span class="data-label">σ_ou DIFFUSION</span>'
                f'<span class="data-value">{ou.sigma_ou:.6f}</span></div>'
                f'<div class="data-row"><span class="data-label">T½ HALF-LIFE</span>'
                f'<span class="data-value">{ou.half_life:.2f} periods</span></div>'
                f'<div style="margin-top:10px;"><span class="{r_css}">{ou.regime}</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<span style="color:#FF3C3C;font-size:0.75rem;">'
                '⚠ Non-stationary / insufficient data for OU</span>',
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Turbulence ────────────────────────────────────────────────────────
    with col_turb:
        st.markdown(
            '<div class="q-analytics-panel">'
            '<div class="q-analytics-title">▸ FILTRE ANOMALIES  —  Δ²P ACCÉLÉRATION</div>'
            f'<div class="q-eq">z_t = (Δ²S_t − μ_a) / σ_a  │  seuil ±{TURB_ACCEL_STD_THRESH}σ</div>',
            unsafe_allow_html=True,
        )
        if turb.valid:
            latest_z = float(turb.accel_zscore[-1]) if len(turb.accel_zscore) > 0 else 0.0
            z_col    = COLOR_DOWN if abs(latest_z) > TURB_ACCEL_STD_THRESH else COLOR_UP
            st.markdown(
                f'<div class="data-row"><span class="data-label">LATEST z-score</span>'
                f'<span style="color:{z_col};font-family:{FONT_MONO};">{latest_z:+.4f}</span></div>'
                f'<div class="data-row"><span class="data-label">ANOMALY COUNT</span>'
                f'<span style="color:{"#FF3C3C" if turb.anomaly_count > 0 else COLOR_UP};">'
                f'{turb.anomaly_count}</span></div>'
                f'<div class="data-row"><span class="data-label">THRESHOLD</span>'
                f'<span class="data-value">±{TURB_ACCEL_STD_THRESH} σ</span></div>'
                f'<div class="data-row"><span class="data-label">CALC LATENCY</span>'
                f'<span class="data-value">{report.calc_ms:.3f} ms</span></div>',
                unsafe_allow_html=True,
            )
            badge = (
                '<span class="turb-badge">⚠ INSTABILITÉ TURBULENTE DÉTECTÉE</span>'
                if turb.latest_flag
                else '<span class="regime-relax">✓ FLUX NOMINAL</span>'
            )
            st.markdown(f'<div style="margin-top:10px;">{badge}</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown(
                '<span style="color:#FF3C3C;font-size:0.75rem;">'
                '⚠ Insufficient data for turbulence filter</span>',
                unsafe_allow_html=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="q-divider-cyan"></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
def render_header() -> None:
    c_title, c_badge = st.columns([3, 1])
    with c_title:
        st.markdown(
            '<div class="quantum-title">⚛ XAU·QUANTUM·DASHBOARD</div>'
            '<div class="quantum-subtitle">'
            'KINETIC FLUID TELEMETRY  //  XAUUSD  //  GBM·OU·COORDS  v4.0 FINAL'
            '</div>',
            unsafe_allow_html=True,
        )
    with c_badge:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
        st.markdown(
            f'<div style="text-align:right; padding-top:10px;">'
            f'<span class="quantum-badge">◉ LIVE</span><br>'
            f'<span style="font-size:0.62rem; color:#00CC33;">{now}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# METRICS ROW
# ─────────────────────────────────────────────────────────────────────────────
def render_metrics(df: pd.DataFrame, report: FluidKineticsReport) -> None:
    if df.empty:
        st.warning("⚠  No telemetry data — check API key / connectivity")
        return

    last    = df["close"].iloc[-1]
    prev    = df["close"].iloc[-2] if len(df) > 1 else last
    chg     = last - prev
    chg_pct = (chg / prev * 100) if prev != 0 else 0.0
    hi      = df["high"].max()
    lo      = df["low"].min()
    spread  = round((df["high"].iloc[-1] - df["low"].iloc[-1]), 3)
    rsi_val = df["rsi14"].iloc[-1] if "rsi14" in df.columns else float("nan")
    atr_val = df["atr14"].iloc[-1] if "atr14" in df.columns else float("nan")

    st.session_state["last_price"] = last
    st.session_state["prev_price"] = prev

    cols = st.columns(10)
    data = [
        ("LAST PRICE",   f"{last:,.3f}",      f"{chg:+.3f}"),
        ("24H CHANGE",   f"{chg_pct:+.3f} %", None),
        ("SESSION HIGH", f"{hi:,.3f}",         None),
        ("SESSION LOW",  f"{lo:,.3f}",         None),
        ("SPREAD",       f"{spread:.3f}",      None),
        ("RSI · 14",     f"{rsi_val:.1f}" if not np.isnan(rsi_val) else "N/A", None),
        ("ATR · 14",     f"{atr_val:.3f}" if not np.isnan(atr_val) else "N/A", None),
        ("μ GBM",        f"{report.gbm.mu:+.5f}" if report.gbm.valid else "N/A", None),
        ("θ OU",         f"{report.ou.theta:.5f}" if report.ou.valid else "N/A", None),
        ("API LATENCY",  f"{st.session_state['api_latency_ms']:.0f} ms", None),
    ]
    for col, (label, val, delta) in zip(cols, data):
        if delta is not None:
            col.metric(label, val, delta)
        else:
            col.metric(label, val)

    st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SIDE PANEL
# ─────────────────────────────────────────────────────────────────────────────
def render_side_panel(
    df: pd.DataFrame,
    report: FluidKineticsReport,
    ac: ActionCoordinates = None,
) -> None:
    last = st.session_state["last_price"]
    prev = st.session_state["prev_price"]

    if df.empty:
        st.plotly_chart(go.Figure(), use_container_width=True)
        return

    hi = df["high"].max()
    lo = df["low"].min()

    st.plotly_chart(
        build_depth_gauge(last, prev, hi, lo),
        use_container_width=True,
        config={"displayModeBar": False},
    )

    # ── Regime + turbulence badges ────────────────────────────────────────
    if report.ou.valid:
        r_css = {"EXPLOSIVE_COMPRESSION": "regime-compress",
                 "RETURN_TO_REST":        "regime-relax",
                 "NEUTRAL":               "regime-neutral"}.get(report.ou.regime, "regime-neutral")
        st.markdown(
            f'<div style="text-align:center;margin:6px 0;">'
            f'<span class="{r_css}">{report.ou.regime}</span></div>',
            unsafe_allow_html=True,
        )
    if report.turbulence.valid and report.turbulence.latest_flag:
        st.markdown(
            '<div style="text-align:center;margin:4px 0;">'
            '<span class="turb-badge">⚠ TURBULENCE</span></div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)

    # ── Last 6 candles ────────────────────────────────────────────────────
    st.markdown("**RECENT CANDLES**")
    tail = df.tail(6)[["timestamp", "open", "close"]].copy()
    tail["timestamp"] = tail["timestamp"].dt.strftime("%H:%M")
    for _, row in tail.iloc[::-1].iterrows():
        up    = row["close"] >= row["open"]
        color = COLOR_UP if up else COLOR_DOWN
        arrow = "▲" if up else "▼"
        st.markdown(
            f'<div class="data-row">'
            f'<span class="data-label">{row["timestamp"]}</span>'
            f'<span style="color:{color};">{arrow} {row["close"]:,.2f}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── GBM snapshot ──────────────────────────────────────────────────────
    if report.gbm.valid:
        st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)
        st.markdown("**GBM SNAPSHOT**")
        sig_inst = float(report.gbm.sigma_rolling[-1]) if len(report.gbm.sigma_rolling) > 0 else 0.0
        for lbl, val in [
            ("μ (ann)",   f"{report.gbm.mu:+.6f}"),
            ("σ (ann)",   f"{report.gbm.sigma:.6f}"),
            ("σ instant", f"{sig_inst:.6f}"),
        ]:
            st.markdown(
                f'<div class="data-row"><span class="data-label">{lbl}</span>'
                f'<span class="data-value">{val}</span></div>',
                unsafe_allow_html=True,
            )

    # ── OU snapshot ───────────────────────────────────────────────────────
    if report.ou.valid:
        st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)
        st.markdown("**OU SNAPSHOT**")
        for lbl, val in [
            ("θ",     f"{report.ou.theta:.6f}"),
            ("μ_eq",  f"{report.ou.mu_eq:.4f}"),
            ("T½",    f"{report.ou.half_life:.2f} p"),
            ("σ_ou",  f"{report.ou.sigma_ou:.6f}"),
        ]:
            st.markdown(
                f'<div class="data-row"><span class="data-label">{lbl}</span>'
                f'<span class="data-value">{val}</span></div>',
                unsafe_allow_html=True,
            )

    # ── AC snapshot ───────────────────────────────────────────────────────
    if ac is not None and ac.valid:
        st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)
        if ac.inhibited:
            st.markdown(
                '<div style="text-align:center; margin:4px 0;">'
                '<span class="turb-badge">⛔ SYSTÈME VERROUILLÉ</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown("**COORD. D'ACTIONNEMENT**")
            sig_css = {"LONG": "signal-long", "SHORT": "signal-short",
                       "WAIT": "signal-wait"}.get(ac.direction, "signal-wait")
            sig_lbl = {"LONG": "▲ LONG", "SHORT": "▼ SHORT",
                       "WAIT": "◌ ATTENTE"}.get(ac.direction, "◌ ATTENTE")
            st.markdown(
                f'<div style="text-align:center; margin:6px 0;">'
                f'<span class="{sig_css}">{sig_lbl}</span></div>',
                unsafe_allow_html=True,
            )
            for lbl, val, color in [
                ("ENTRY", f"{ac.entry:,.3f}", COLOR_UP),
                ("NDT",   f"{ac.ndt:,.3f}",   COLOR_CYAN),
                ("ERV",   f"{ac.erv:,.3f}",   COLOR_DOWN),
                ("HEP",   f"{ac.hep:,.3f}",   COLOR_AMBER),
                ("R/R",   f"{ac.rr_ratio:.3f}", COLOR_UP if ac.rr_ratio >= 1.5 else COLOR_AMBER),
            ]:
                st.markdown(
                    f'<div class="data-row">'
                    f'<span class="data-label">{lbl}</span>'
                    f'<span style="color:{color}; font-family:{FONT_MONO};">{val}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── Error log ─────────────────────────────────────────────────────────
    if st.session_state["error_log"]:
        st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)
        st.markdown("**SYSTEM LOG**")
        for entry in st.session_state["error_log"][-6:][::-1]:
            st.markdown(
                f'<div style="font-size:0.64rem;color:#FF3C3C;'
                f'font-family:Share Tech Mono,monospace;">{entry}</div>',
                unsafe_allow_html=True,
            )

    # ── Tick + latency ────────────────────────────────────────────────────
    st.markdown('<div class="q-divider"></div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="data-row"><span class="data-label">TICK COUNT</span>'
        f'<span class="data-value">{st.session_state["tick_count"]}'
        f'<span class="blink">_</span></span></div>'
        f'<div class="data-row"><span class="data-label">CALC LATENCY</span>'
        f'<span class="data-value">{report.calc_ms:.3f} ms</span></div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# FRAGMENT  —  live refresh every REFRESH_SECONDS
# ─────────────────────────────────────────────────────────────────────────────
@st.fragment(run_every=REFRESH_SECONDS)
def live_dashboard() -> None:
    """
    ╔══════════════════════════════════════════════════════════════════════╗
    ║  LIVE DASHBOARD FRAGMENT  —  XAU-QUANTUM  v4.0  FINAL              ║
    ║  Re-executes every REFRESH_SECONDS without full-page reload.        ║
    ║                                                                      ║
    ║  Resilience contract                                                 ║
    ║  ─────────────────                                                   ║
    ║  • Every pipeline step is wrapped in an isolated try/except.        ║
    ║  • On API failure → "RE-CONNEXION AU CAPTEUR" overlay.              ║
    ║  • On any calc failure → last valid cache is preserved and reused.  ║
    ║  • On any render failure → inline amber warning badge.              ║
    ║  • The fragment NEVER raises to the Streamlit runtime.              ║
    ║                                                                      ║
    ║  Execution pipeline per tick                                         ║
    ║  ──────────────────────────                                          ║
    ║  1.  fetch_quantum_telemetry (1M + 5M)                              ║
    ║  2.  enrich_dataframe        (EMA/RSI/MACD/BB/ATR/Stoch)            ║
    ║  3.  analyze_fluid_kinetics  (GBM + OU + Turbulence)                ║
    ║  4.  generate_action_coordinates (ENTRY/NDT/ERV/HEP + inhibitor)   ║
    ║  5.  render_metrics          (10-col metric bar)                     ║
    ║  6.  render_analytics_panel  (GBM/OU/Turbulence summary)            ║
    ║  7.  render_action_coordinates (4-threshold actuation panel)        ║
    ║  8.  7 chart tabs  +  side panel                                    ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """
    tick = st.session_state["tick_count"] + 1
    st.session_state["tick_count"] = tick

    # ══════════════════════════════════════════════════════════════════════
    # STEP 1 — FETCH  (API call — most likely failure point)
    # ══════════════════════════════════════════════════════════════════════
    df_1m = pd.DataFrame()
    df_5m = pd.DataFrame()
    _api_ok = True

    try:
        df_1m_raw = fetch_quantum_telemetry(INTERVAL_1M)
        df_5m_raw = fetch_quantum_telemetry(INTERVAL_5M)
    except Exception as exc:
        _log_error(f"[FETCH FATAL] {exc}")
        df_1m_raw = pd.DataFrame()
        df_5m_raw = pd.DataFrame()
        _api_ok   = False

    # ══════════════════════════════════════════════════════════════════════
    # STEP 2 — ENRICH  (indicators computation)
    # ══════════════════════════════════════════════════════════════════════
    try:
        if df_1m_raw.empty:
            if not st.session_state["kline_cache_1m"].empty:
                df_1m = st.session_state["kline_cache_1m"]
            else:
                _api_ok = False
        else:
            df_1m = enrich_dataframe(df_1m_raw)
            st.session_state["kline_cache_1m"] = df_1m

        if df_5m_raw.empty:
            if not st.session_state["kline_cache_5m"].empty:
                df_5m = st.session_state["kline_cache_5m"]
        else:
            df_5m = enrich_dataframe(df_5m_raw)
            st.session_state["kline_cache_5m"] = df_5m
    except Exception as exc:
        _log_error(f"[ENRICH ERR] {exc}")
        df_1m = st.session_state.get("kline_cache_1m", pd.DataFrame())
        df_5m = st.session_state.get("kline_cache_5m", pd.DataFrame())

    # ── Reconnection overlay when no data at all ──────────────────────────
    if df_1m.empty and not _api_ok:
        _reconnect_ui(
            reason="Signal AllTick non disponible — tentative en cours",
            retry=tick,
        )
        return

    # ══════════════════════════════════════════════════════════════════════
    # STEP 3 — STOCHASTIC ANALYTICAL ENGINE
    # ══════════════════════════════════════════════════════════════════════
    try:
        report: FluidKineticsReport = analyze_fluid_kinetics(df_1m)
        st.session_state["kinetics_cache"] = report
    except Exception as exc:
        _log_error(f"[KINETICS ERR] {exc}")
        report = st.session_state.get("kinetics_cache", FluidKineticsReport())

    # ══════════════════════════════════════════════════════════════════════
    # STEP 4 — ACTION COORDINATE ENGINE
    # ══════════════════════════════════════════════════════════════════════
    try:
        ac: ActionCoordinates = generate_action_coordinates(df_1m, report)
        st.session_state["action_coords"] = ac
    except Exception as exc:
        _log_error(f"[COORD ERR] {exc}")
        ac = st.session_state.get("action_coords", ActionCoordinates())

    # ══════════════════════════════════════════════════════════════════════
    # STEP 5 — METRICS ROW
    # ══════════════════════════════════════════════════════════════════════
    try:
        render_metrics(df_1m, report)
    except Exception as exc:
        _log_error(f"[METRICS ERR] {exc}")
        st.markdown(
            f'<div class="step-error">⚠ METRICS — RE-CONNEXION AU CAPTEUR  [{exc}]</div>',
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════
    # STEP 6 — ANALYTICS PANEL  (GBM / OU / Turbulence)
    # ══════════════════════════════════════════════════════════════════════
    try:
        render_analytics_panel(report)
    except Exception as exc:
        _log_error(f"[ANALYTICS ERR] {exc}")
        st.markdown(
            f'<div class="step-error">⚠ ANALYTICS — RE-CONNEXION AU CAPTEUR  [{exc}]</div>',
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════
    # STEP 7 — ACTION COORDINATES PANEL  (ENTRY / NDT / ERV / HEP)
    # ══════════════════════════════════════════════════════════════════════
    try:
        render_action_coordinates(ac)
    except Exception as exc:
        _log_error(f"[COORD RENDER ERR] {exc}")
        st.markdown(
            f'<div class="step-error">⚠ COORDONNÉES — RE-CONNEXION AU CAPTEUR  [{exc}]</div>',
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════
    # STEP 8 — CHARTS + SIDE PANEL
    # ══════════════════════════════════════════════════════════════════════
    col_charts, col_side = st.columns([3, 1])

    with col_charts:
        try:
            tab_1m, tab_5m, tab_coord, tab_stoch, tab_gbm, tab_ou, tab_turb = st.tabs([
                "● 1M FLUX",
                "● 5M FLUX",
                "⬡ COORDINATES",
                "◈ STOCHASTIC",
                "◈ GBM KINETICS",
                "◈ OU RELAXATION",
                "⚠ TURBULENCE",
            ])
        except Exception as exc:
            _log_error(f"[TABS ERR] {exc}")
            _reconnect_ui("Erreur de rendu des onglets", tick)
            with col_side:
                try:
                    render_side_panel(df_1m, report, ac)
                except Exception:
                    pass
            return

        # ── Tab 1M ─────────────────────────────────────────────────────
        with tab_1m:
            try:
                if not df_1m.empty:
                    st.plotly_chart(
                        _safe_chart(build_main_chart, df_1m, "XA·UUSD  KINETIC PRESSURE  [1M]"),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                else:
                    _reconnect_ui("Flux 1M — données indisponibles", tick)
            except Exception as exc:
                _log_error(f"[TAB 1M] {exc}")
                _reconnect_ui(str(exc), tick)

        # ── Tab 5M ─────────────────────────────────────────────────────
        with tab_5m:
            try:
                if not df_5m.empty:
                    st.plotly_chart(
                        _safe_chart(build_main_chart, df_5m, "XA·UUSD  KINETIC PRESSURE  [5M]"),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                else:
                    _reconnect_ui("Flux 5M — données indisponibles", tick)
            except Exception as exc:
                _log_error(f"[TAB 5M] {exc}")
                _reconnect_ui(str(exc), tick)

        # ── Tab Coordinates ────────────────────────────────────────────
        with tab_coord:
            try:
                if ac.inhibited:
                    st.markdown(
                        '<div class="inhibit-overlay">'
                        '<div class="inhibit-title">⛔ SYSTÈME VERROUILLÉ</div>'
                        '<div class="inhibit-sub">Turbulence hors-limite — '
                        'coordonnées neutralisées</div>'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                elif not df_1m.empty and ac.valid:
                    st.plotly_chart(
                        _safe_chart(build_coord_chart, df_1m, ac),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                    st.plotly_chart(
                        _safe_chart(build_stochastic_chart, df_1m, ac),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                else:
                    _reconnect_ui("Coordinate engine — accumulation des données", tick)
            except Exception as exc:
                _log_error(f"[TAB COORD] {exc}")
                _reconnect_ui(str(exc), tick)

        # ── Tab Stochastic ─────────────────────────────────────────────
        with tab_stoch:
            try:
                if not df_1m.empty:
                    st.plotly_chart(
                        _safe_chart(build_stochastic_chart, df_1m, ac),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        st.plotly_chart(
                            _safe_chart(build_rsi_spark, df_1m),
                            use_container_width=True, config={"displayModeBar": False},
                        )
                    with sc2:
                        st.plotly_chart(
                            _safe_chart(build_volatility_heatmap, df_1m),
                            use_container_width=True, config={"displayModeBar": False},
                        )
                else:
                    _reconnect_ui("Oscillateur stochastique — RE-CONNEXION", tick)
            except Exception as exc:
                _log_error(f"[TAB STOCH] {exc}")
                _reconnect_ui(str(exc), tick)

        # ── Tab GBM ────────────────────────────────────────────────────
        with tab_gbm:
            try:
                if not df_1m.empty and report.gbm.valid:
                    st.plotly_chart(
                        _safe_chart(build_gbm_sigma_chart, df_1m, report.gbm),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        st.plotly_chart(
                            _safe_chart(build_rsi_spark, df_1m),
                            use_container_width=True, config={"displayModeBar": False},
                        )
                    with sc2:
                        st.plotly_chart(
                            _safe_chart(build_volatility_heatmap, df_1m),
                            use_container_width=True, config={"displayModeBar": False},
                        )
                else:
                    _reconnect_ui("GBM engine — accumulation des données cinétiques", tick)
            except Exception as exc:
                _log_error(f"[TAB GBM] {exc}")
                _reconnect_ui(str(exc), tick)

        # ── Tab OU ─────────────────────────────────────────────────────
        with tab_ou:
            try:
                if not df_1m.empty and report.ou.valid:
                    st.plotly_chart(
                        _safe_chart(build_ou_chart, df_1m, report.ou),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                else:
                    _reconnect_ui(
                        "OU engine — processus non-stationnaire ou données insuffisantes",
                        tick,
                    )
            except Exception as exc:
                _log_error(f"[TAB OU] {exc}")
                _reconnect_ui(str(exc), tick)

        # ── Tab Turbulence ─────────────────────────────────────────────
        with tab_turb:
            try:
                if not df_1m.empty and report.turbulence.valid:
                    st.plotly_chart(
                        _safe_chart(build_turbulence_chart, df_1m, report.turbulence),
                        use_container_width=True, config={"displayModeBar": False},
                    )
                else:
                    _reconnect_ui("Filtre turbulence — accumulation des données", tick)
            except Exception as exc:
                _log_error(f"[TAB TURB] {exc}")
                _reconnect_ui(str(exc), tick)

    # ── Side panel ────────────────────────────────────────────────────────
    with col_side:
        try:
            render_side_panel(df_1m, report, ac)
        except Exception as exc:
            _log_error(f"[SIDE PANEL ERR] {exc}")
            st.markdown(
                f'<div class="step-error">⚠ SIDE PANEL — RE-CONNEXION  [{exc}]</div>',
                unsafe_allow_html=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    render_header()
    live_dashboard()


if __name__ == "__main__":
    main()
