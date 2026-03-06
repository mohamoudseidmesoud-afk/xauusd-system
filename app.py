# ╔══════════════════════════════════════════════════════════════════════════════╗
#  SYSTÈME DE CONTRÔLE HYDRODYNAMIQUE — CANALISATION XA-UUSD
#  Version FINALE DE DÉPLOIEMENT — v4.0.0
#  ─────────────────────────────────────────────────────────────────────────────
#  Étape 1 : Ingestion API asynchrone + DataFrame historique
#  Étape 2 : Moteur stochastique intégral (Kolmogorov / GBM)
#  Étape 3 : Seuils d'intervention vitaux + Inhibition d'urgence
#  Étape 4 : Interface temps réel fragmentée (@st.fragment — 5 s)
#  ─────────────────────────────────────────────────────────────────────────────
#  Auteur  : Ingénieur Senior — Architecture Fluides / RT-SW
#  Usage   : streamlit run xa_uusd_dashboard.py
# ╚══════════════════════════════════════════════════════════════════════════════╝

import math
import datetime
import asyncio
import time
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st

# ──────────────────────────────────────────────────────────────────────────────
#  §1  CONSTANTES — PIPELINE XA-UUSD
# ──────────────────────────────────────────────────────────────────────────────

API_BASE_URL: str          = "https://api.fluiddynamics.net/v1/pressure"
SENSOR_ID: str             = "XA-UUSD"
API_TIMEOUT_SECONDS: int   = 8
POLL_INTERVAL_SECONDS: int = 2          # Monitoring continu (bouton)
FRAGMENT_INTERVAL: str     = "5s"       # @st.fragment auto-refresh
MAX_HISTORY_ROWS: int      = 500
PRESSURE_UNIT: str         = "bar"

# Seuils de sécurité physiques XA-UUSD
PRESSURE_NOMINAL_MIN: float  = 12.0
PRESSURE_NOMINAL_MAX: float  = 48.0
PRESSURE_WARNING_LOW: float  = 8.0
PRESSURE_WARNING_HIGH: float = 55.0
PRESSURE_CRITICAL_LOW: float = 4.0
PRESSURE_CRITICAL_HIGH: float= 65.0

# Paramètres moteur stochastique (Étape 2)
TURBULENCE_WINDOW: int         = 14
STOCH_K_WINDOW: int            = 14
STOCH_D_SMOOTH: int            = 3
DRIFT_POLYFIT_DEGREE: int      = 1
MIN_ROWS_FOR_STOCHASTICS: int  = 5
STOCH_OVERBOUGHT: float        = 80.0
STOCH_OVERSOLD: float          = 20.0

# Paramètres moteur d'intervention (Étape 3)
TURBULENCE_INHIBIT_SIGMA_BASE: float  = 0.85
TURBULENCE_INHIBIT_MULTIPLIER: float  = 3.0
TURBULENCE_INHIBIT_THRESHOLD: float   = (
    TURBULENCE_INHIBIT_SIGMA_BASE * TURBULENCE_INHIBIT_MULTIPLIER
)  # = 2.55 bar
NDT_VOLATILITY_RATIO: float    = 0.618   # Fibonacci — buffer dynamique
ERV_SAFETY_MULTIPLIER: float   = 1.5     # 1.5 × σ_turb
ERV_HARD_FLOOR: float          = PRESSURE_CRITICAL_LOW  + 1.0   # 5.0 bar
ERV_HARD_CEILING: float        = PRESSURE_CRITICAL_HIGH - 1.0   # 64.0 bar
HEP_FRICTION_OFFSET_BAR: float = 0.25   # Friction machinerie lourde
ZEROING_MIN_KD_CROSSOVER: float= 0.5

_INHIBIT_STATE_KEY: str = "PAUSE_DE_SECURITE"
_INHIBIT_STATE_MSG: str = "PAUSE DE SÉCURITÉ — SYSTÈME EN SURPRESSION CHAOTIQUE"

# Palette industrielle XA-UUSD
COLOR_NOMINAL    = "#00E5A0"
COLOR_WARNING    = "#FFB830"
COLOR_CRITICAL   = "#FF3A3A"
COLOR_BACKGROUND = "#0B0E1A"
COLOR_PANEL      = "#111827"
COLOR_BORDER     = "#1E2A40"
COLOR_TEXT_PRI   = "#E8F0FE"
COLOR_TEXT_MUT   = "#6B7FA3"
COLOR_ACCENT     = "#3A7BFF"
COLOR_STOCH_K    = "#A78BFA"
COLOR_STOCH_D    = "#F472B6"
COLOR_DRIFT      = "#FCD34D"
COLOR_TURBULENCE = "#60A5FA"
COLOR_INHIBIT    = "#FF0055"
COLOR_ZP         = "#FB923C"
COLOR_NDT        = "#34D399"
COLOR_ERV        = "#F87171"
COLOR_HEP        = "#818CF8"


# ──────────────────────────────────────────────────────────────────────────────
#  §2  CSS — STYLE INDUSTRIEL HAUTE-DENSITÉ v4
# ──────────────────────────────────────────────────────────────────────────────

DASHBOARD_CSS: str = f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&family=Barlow+Condensed:wght@400;700;900&display=swap');

  html, body, [data-testid="stAppViewContainer"] {{
      background-color: {COLOR_BACKGROUND} !important;
      color: {COLOR_TEXT_PRI} !important;
      font-family: 'Barlow', sans-serif;
  }}
  [data-testid="stSidebar"] {{
      background-color: #0D1120 !important;
      border-right: 1px solid {COLOR_BORDER};
  }}
  #MainMenu, footer, header {{ visibility: hidden; }}

  /* ── Typographie ── */
  .xa-title {{
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 2.1rem; font-weight: 900; letter-spacing: 0.12em;
      text-transform: uppercase; color: {COLOR_TEXT_PRI}; line-height: 1.1;
  }}
  .xa-title span.accent {{ color: {COLOR_ACCENT}; }}
  .xa-subtitle {{
      font-family: 'Share Tech Mono', monospace; font-size: 0.72rem;
      color: {COLOR_TEXT_MUT}; letter-spacing: 0.2em;
      text-transform: uppercase; margin-top: 4px;
  }}
  .section-header {{
      font-family: 'Barlow Condensed', sans-serif; font-size: 1.05rem;
      font-weight: 700; letter-spacing: 0.18em; text-transform: uppercase;
      color: {COLOR_TEXT_MUT}; border-left: 3px solid {COLOR_ACCENT};
      padding-left: 10px; margin: 14px 0 10px 0;
  }}
  .section-header.danger {{
      border-left-color: {COLOR_INHIBIT}; color: {COLOR_INHIBIT};
  }}
  .section-header.live {{
      border-left-color: {COLOR_NOMINAL}; color: {COLOR_NOMINAL};
  }}

  /* ── Cartes métriques ── */
  .metric-card {{
      background: {COLOR_PANEL}; border: 1px solid {COLOR_BORDER};
      border-radius: 6px; padding: 16px 20px;
      position: relative; overflow: hidden;
  }}
  .metric-card::before {{
      content: ""; position: absolute; top: 0; left: 0;
      width: 3px; height: 100%; background: {COLOR_ACCENT};
  }}
  .metric-card.warning::before  {{ background: {COLOR_WARNING}; }}
  .metric-card.critical::before {{ background: {COLOR_CRITICAL}; }}
  .metric-card.nominal::before  {{ background: {COLOR_NOMINAL}; }}
  .metric-card.stoch::before    {{ background: {COLOR_STOCH_K}; }}
  .metric-card.drift::before    {{ background: {COLOR_DRIFT}; }}
  .metric-card.turb::before     {{ background: {COLOR_TURBULENCE}; }}
  .metric-card.zp::before       {{ background: {COLOR_ZP}; }}
  .metric-card.ndt::before      {{ background: {COLOR_NDT}; }}
  .metric-card.erv::before      {{ background: {COLOR_ERV}; }}
  .metric-card.hep::before      {{ background: {COLOR_HEP}; }}
  .metric-label {{
      font-family: 'Share Tech Mono', monospace; font-size: 0.65rem;
      color: {COLOR_TEXT_MUT}; letter-spacing: 0.25em;
      text-transform: uppercase; margin-bottom: 6px;
  }}
  .metric-value {{
      font-family: 'Barlow Condensed', sans-serif; font-size: 2.6rem;
      font-weight: 700; color: {COLOR_TEXT_PRI}; line-height: 1;
  }}
  .metric-unit {{
      font-family: 'Share Tech Mono', monospace; font-size: 0.8rem;
      color: {COLOR_TEXT_MUT}; margin-left: 6px;
  }}
  .metric-sub {{
      font-family: 'Share Tech Mono', monospace; font-size: 0.65rem;
      color: {COLOR_TEXT_MUT}; margin-top: 4px;
  }}

  /* ── Panneau ACTION / PAUSE (Étape 4) ── */
  .action-panel {{
      border-radius: 6px; padding: 18px 24px;
      position: relative; overflow: hidden;
      display: flex; flex-direction: column; gap: 6px;
  }}
  .action-panel.required {{
      background: rgba(251,146,60,0.10);
      border: 2px solid {COLOR_ZP};
  }}
  .action-panel.hold {{
      background: rgba(58,123,255,0.07);
      border: 2px solid {COLOR_ACCENT};
  }}
  .action-panel.inhibit {{
      background: rgba(255,0,85,0.12);
      border: 2px solid {COLOR_INHIBIT};
      animation: pulse-inh 1.2s ease-in-out infinite;
  }}
  @keyframes pulse-inh {{
      0%   {{ box-shadow: 0 0 0 0   rgba(255,0,85,0.45); }}
      70%  {{ box-shadow: 0 0 0 12px rgba(255,0,85,0); }}
      100% {{ box-shadow: 0 0 0 0   rgba(255,0,85,0); }}
  }}
  .action-headline {{
      font-family: 'Barlow Condensed', sans-serif; font-size: 1.45rem;
      font-weight: 900; letter-spacing: 0.15em; text-transform: uppercase;
  }}
  .action-headline.required {{ color: {COLOR_ZP}; }}
  .action-headline.hold     {{ color: {COLOR_ACCENT}; }}
  .action-headline.inhibit  {{ color: {COLOR_INHIBIT}; }}
  .action-detail {{
      font-family: 'Share Tech Mono', monospace; font-size: 0.72rem;
      color: {COLOR_TEXT_MUT}; line-height: 1.6;
  }}
  .action-proc-badge {{
      display: inline-block; padding: 4px 14px; border-radius: 2px;
      font-family: 'Share Tech Mono', monospace; font-size: 0.78rem;
      font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase;
      margin-top: 4px;
  }}
  .proc-in   {{ background:rgba(0,229,160,0.14); color:{COLOR_NOMINAL};
                border:1px solid rgba(0,229,160,0.35); }}
  .proc-out  {{ background:rgba(255,58,58,0.14);  color:{COLOR_CRITICAL};
                border:1px solid rgba(255,58,58,0.35); }}
  .proc-hold {{ background:rgba(58,123,255,0.12); color:{COLOR_ACCENT};
                border:1px solid rgba(58,123,255,0.3); }}

  /* ── Fragment ticker (Étape 4) ── */
  .fragment-tick {{
      font-family: 'Share Tech Mono', monospace; font-size: 0.62rem;
      color: {COLOR_TEXT_MUT}; letter-spacing: 0.1em;
      display: flex; align-items: center; gap: 6px;
  }}
  .tick-dot {{
      width: 6px; height: 6px; border-radius: 50%;
      background: {COLOR_NOMINAL}; display: inline-block;
      animation: blink-dot 1s step-start infinite;
  }}
  @keyframes blink-dot {{ 50% {{ opacity: 0; }} }}

  /* ── Status badges ── */
  .status-badge {{
      display: inline-flex; align-items: center; gap: 6px;
      padding: 4px 12px; border-radius: 2px;
      font-family: 'Share Tech Mono', monospace; font-size: 0.7rem;
      letter-spacing: 0.15em; font-weight: 700; text-transform: uppercase;
  }}
  .status-nominal  {{ background:rgba(0,229,160,0.12); color:{COLOR_NOMINAL};
                      border:1px solid rgba(0,229,160,0.3); }}
  .status-warning  {{ background:rgba(255,184,48,0.12); color:{COLOR_WARNING};
                      border:1px solid rgba(255,184,48,0.3); }}
  .status-critical {{ background:rgba(255,58,58,0.12);  color:{COLOR_CRITICAL};
                      border:1px solid rgba(255,58,58,0.3); }}
  .status-inhibit  {{ background:rgba(255,0,85,0.18);   color:{COLOR_INHIBIT};
                      border:1px solid rgba(255,0,85,0.5);
                      animation: pulse-inh 1.2s ease-in-out infinite; }}

  /* ── Divers ── */
  .xa-divider {{ border:none; border-top:1px solid {COLOR_BORDER}; margin:18px 0; }}
  .timestamp-chip {{
      font-family:'Share Tech Mono',monospace; font-size:0.68rem;
      color:{COLOR_TEXT_MUT}; letter-spacing:0.1em;
  }}
  .log-entry {{
      font-family:'Share Tech Mono',monospace; font-size:0.72rem;
      padding:4px 0; border-bottom:1px solid {COLOR_BORDER}; color:{COLOR_TEXT_MUT};
  }}
  .log-entry.err {{ color:{COLOR_CRITICAL}; }}
  .log-entry.wrn {{ color:{COLOR_WARNING}; }}
  .log-entry.ok  {{ color:{COLOR_NOMINAL}; }}
  .log-entry.inh {{ color:{COLOR_INHIBIT}; font-weight:bold; }}
  .stoch-regime {{
      font-family:'Share Tech Mono',monospace; font-size:0.68rem;
      letter-spacing:0.12em; padding:3px 10px; border-radius:2px;
  }}
  .regime-overbought {{ background:rgba(255,58,58,0.15);  color:{COLOR_CRITICAL};
                        border:1px solid rgba(255,58,58,0.3); }}
  .regime-oversold   {{ background:rgba(0,229,160,0.10);  color:{COLOR_NOMINAL};
                        border:1px solid rgba(0,229,160,0.2); }}
  .regime-neutral    {{ background:rgba(107,127,163,0.1); color:{COLOR_TEXT_MUT};
                        border:1px solid {COLOR_BORDER}; }}
  .threshold-table {{
      width:100%; border-collapse:collapse;
      font-family:'Share Tech Mono',monospace; font-size:0.78rem;
  }}
  .threshold-table th {{
      text-transform:uppercase; letter-spacing:0.18em; color:{COLOR_TEXT_MUT};
      font-size:0.62rem; border-bottom:1px solid {COLOR_BORDER};
      padding:6px 10px; text-align:left;
  }}
  .threshold-table td {{
      padding:8px 10px; border-bottom:1px solid {COLOR_BORDER};
      color:{COLOR_TEXT_PRI}; vertical-align:middle;
  }}
  .threshold-table tr:hover td {{ background:rgba(58,123,255,0.04); }}
  .th-zp  {{ color:{COLOR_ZP};  font-weight:bold; }}
  .th-ndt {{ color:{COLOR_NDT}; font-weight:bold; }}
  .th-erv {{ color:{COLOR_ERV}; font-weight:bold; }}
  .th-hep {{ color:{COLOR_HEP}; font-weight:bold; }}

  div[data-testid="stButton"] > button {{
      background:transparent; color:{COLOR_ACCENT};
      border:1px solid {COLOR_ACCENT}; border-radius:3px;
      font-family:'Share Tech Mono',monospace; font-size:0.75rem;
      letter-spacing:0.15em; text-transform:uppercase;
      padding:8px 20px; transition:all 0.2s;
  }}
  div[data-testid="stButton"] > button:hover {{
      background:{COLOR_ACCENT}; color:#fff;
  }}

  /* Customisation st.metric natif */
  [data-testid="stMetric"] {{
      background: {COLOR_PANEL};
      border: 1px solid {COLOR_BORDER};
      border-radius: 6px;
      padding: 14px 18px !important;
  }}
  [data-testid="stMetricLabel"] {{
      font-family:'Share Tech Mono',monospace !important;
      font-size:0.65rem !important; letter-spacing:0.2em !important;
      text-transform:uppercase !important; color:{COLOR_TEXT_MUT} !important;
  }}
  [data-testid="stMetricValue"] {{
      font-family:'Barlow Condensed',sans-serif !important;
      font-size:2.0rem !important; font-weight:700 !important;
      color:{COLOR_TEXT_PRI} !important;
  }}
  [data-testid="stMetricDelta"] {{
      font-family:'Share Tech Mono',monospace !important;
      font-size:0.75rem !important;
  }}
</style>
"""


# ──────────────────────────────────────────────────────────────────────────────
#  §3  SESSION STATE
# ──────────────────────────────────────────────────────────────────────────────

def initialize_session_state() -> None:
    """
    Initialise toutes les clés de session Streamlit au premier chargement.
    Garantit la cohérence de l'état entre les cycles @st.fragment et les
    reruns complets de la page.
    """
    defaults: dict = {
        "pressure_history_df":    _build_empty_pressure_dataframe(),
        "total_api_calls":        0,
        "failed_api_calls":       0,
        "last_successful_ts":     None,
        "monitoring_active":      False,
        "event_log":              [],
        "stoch_results":          None,
        "intervention_thresholds":None,
        "inhibit_count":          0,
        "fragment_cycle":         0,      # Compteur de cycles @st.fragment
        "last_fragment_error":    None,   # Dernière erreur catchée dans fragment
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ──────────────────────────────────────────────────────────────────────────────
#  §4  DATAFRAME PRESSION
# ──────────────────────────────────────────────────────────────────────────────

def _build_empty_pressure_dataframe() -> pd.DataFrame:
    """
    Construit un DataFrame Pandas typé vide — schéma complet XA-UUSD.

    Colonnes :
        timestamp       datetime64[ns]  Horodatage UTC de la mesure
        pressure_bar    float64         Pression absolue (bar)
        sensor_id       object          Identifiant capteur
        api_latency_ms  float64         Latence API (ms)
        status          object          'nominal' | 'warning' | 'critical'
        delta_bar       float64         Δ pression vs mesure N-1
        rolling_avg_5   float64         Moyenne glissante 5 périodes
        rolling_std_5   float64         Écart-type glissant 5 périodes
    """
    return pd.DataFrame({
        "timestamp":      pd.Series(dtype="datetime64[ns]"),
        "pressure_bar":   pd.Series(dtype="float64"),
        "sensor_id":      pd.Series(dtype="object"),
        "api_latency_ms": pd.Series(dtype="float64"),
        "status":         pd.Series(dtype="object"),
        "delta_bar":      pd.Series(dtype="float64"),
        "rolling_avg_5":  pd.Series(dtype="float64"),
        "rolling_std_5":  pd.Series(dtype="float64"),
    })


def _classify_pressure_status(pressure: float) -> str:
    """Classifie la pression selon les seuils XA-UUSD.
    Returns: 'critical' | 'warning' | 'nominal'."""
    if pressure <= PRESSURE_CRITICAL_LOW or pressure >= PRESSURE_CRITICAL_HIGH:
        return "critical"
    if pressure <= PRESSURE_WARNING_LOW  or pressure >= PRESSURE_WARNING_HIGH:
        return "warning"
    return "nominal"


def append_pressure_record(
    df: pd.DataFrame,
    pressure: float,
    latency_ms: float,
    ts: Optional[datetime.datetime] = None,
) -> pd.DataFrame:
    """
    Ajoute une mesure au DataFrame historique, recalcule les colonnes
    dérivées et applique la politique de rétention MAX_HISTORY_ROWS.
    """
    if ts is None:
        ts = datetime.datetime.utcnow()

    delta = (pressure - float(df["pressure_bar"].iloc[-1])) if len(df) > 0 else 0.0

    new_df = pd.concat([
        df,
        pd.DataFrame([{
            "timestamp":      ts,
            "pressure_bar":   pressure,
            "sensor_id":      SENSOR_ID,
            "api_latency_ms": latency_ms,
            "status":         _classify_pressure_status(pressure),
            "delta_bar":      delta,
            "rolling_avg_5":  float("nan"),
            "rolling_std_5":  float("nan"),
        }])
    ], ignore_index=True)

    new_df["rolling_avg_5"] = (
        new_df["pressure_bar"].rolling(window=5, min_periods=1).mean()
    )
    new_df["rolling_std_5"] = (
        new_df["pressure_bar"].rolling(window=5, min_periods=1).std().fillna(0.0)
    )

    if len(new_df) > MAX_HISTORY_ROWS:
        new_df = new_df.iloc[-MAX_HISTORY_ROWS:].reset_index(drop=True)
    return new_df


# ──────────────────────────────────────────────────────────────────────────────
#  §5  MOTEUR STOCHASTIQUE INTÉGRAL (Étape 2)
# ──────────────────────────────────────────────────────────────────────────────

def calculate_turbulence_variance(df: pd.DataFrame) -> pd.Series:
    """
    Écart-type glissant des Δp sur TURBULENCE_WINDOW périodes.

    Physique : σ_turb(t) ∝ √(énergie cinétique latente) — proxy de la
    dissipation de Kolmogorov ε ∝ σ_turb³ / L_intégrale.

    Vectorisé : np.diff O(n) → pandas.rolling(14).std() Cython.
    Retourne NaN si df < 2 lignes.
    """
    if len(df) < 2:
        return pd.Series(np.full(len(df), np.nan), index=df.index,
                         name="turbulence_sigma")

    delta_p = np.diff(df["pressure_bar"].values.astype(np.float64),
                      prepend=df["pressure_bar"].values[0])
    sigma = (pd.Series(delta_p, index=df.index)
             .rolling(window=TURBULENCE_WINDOW, min_periods=2).std())
    sigma.name = "turbulence_sigma"
    return sigma


def stochastic_pressure_oscillator(
    df: pd.DataFrame,
) -> Tuple[pd.Series, pd.Series]:
    """
    Oscillateur stochastique de Lane transposé aux fluctuations de pression.

    %K(t) = 100 × [p(t) − P_min(t,N)] / [P_max(t,N) − P_min(t,N)]
    %D(t) = SMA_M(%K(t))

    Convention dénominateur nul → %K = 50.0 (régime constant neutre).
    Retourne deux Series de NaN si df < MIN_ROWS_FOR_STOCHASTICS.
    """
    nan_s = pd.Series(np.full(len(df), np.nan), index=df.index, dtype=np.float64)
    if len(df) < MIN_ROWS_FOR_STOCHASTICS:
        return nan_s.rename("%K"), nan_s.rename("%D")

    p = df["pressure_bar"].astype(np.float64)
    rmin = p.rolling(STOCH_K_WINDOW, min_periods=1).min()
    rmax = p.rolling(STOCH_K_WINDOW, min_periods=1).max()
    rng  = (rmax - rmin).values

    with np.errstate(divide="ignore", invalid="ignore"):
        k_raw = np.where(rng == 0.0, 50.0, 100.0 * (p.values - rmin.values) / rng)

    pct_k = pd.Series(np.clip(k_raw, 0.0, 100.0), index=df.index,
                      name="%K", dtype=np.float64)
    pct_d = pct_k.rolling(STOCH_D_SMOOTH, min_periods=1).mean()
    pct_d.name = "%D"
    return pct_k, pct_d


def calculate_hydrodynamic_drift(df: pd.DataFrame) -> dict:
    """
    Drift directionnel GBM : dp = μ·p·dt + σ·p·dW_t.

    Algorithme :
        t_norm ∈ [0,1] → np.polyfit deg.1 → slope dénormalisé bar/période
        μ_log  = mean(ln(p_t/p_{t-1}))
        σ_real = std(log_returns) × √n
        R²     = 1 − SS_res/SS_tot

    Retourne dict avec sufficient_data=False si df < MIN_ROWS_FOR_STOCHASTICS.
    """
    default = {
        "drift_bar_per_period": 0.0, "drift_direction": "stable",
        "drift_magnitude": "faible", "regime": "nominal",
        "mu_log": 0.0, "sigma_realised": 0.0,
        "p_intercept": 0.0, "p_projected_next": 0.0,
        "r_squared": 0.0, "n_samples": len(df), "sufficient_data": False,
    }
    if len(df) < MIN_ROWS_FOR_STOCHASTICS:
        return default

    p  = df["pressure_bar"].values.astype(np.float64)
    n  = len(p)
    tn = np.linspace(0.0, 1.0, n)
    cf = np.polyfit(tn, p, deg=DRIFT_POLYFIT_DEGREE)
    drift_bpp = float(cf[0]) / max(n - 1, 1)
    p_proj    = float(np.polyval(cf, 1.0 + 1.0 / max(n - 1, 1)))

    p_hat  = np.polyval(cf, tn)
    ss_res = float(np.sum((p - p_hat) ** 2))
    ss_tot = float(np.sum((p - np.mean(p)) ** 2))
    r2     = float(np.clip(1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0, 0.0, 1.0))

    pp = p[p > 0]
    if len(pp) >= 2:
        lr      = np.diff(np.log(pp))
        mu_log  = float(np.mean(lr))
        sig_r   = float(np.std(lr, ddof=1) * math.sqrt(n))
    else:
        mu_log, sig_r = 0.0, 0.0

    STABLE_T = 0.01;  STRONG_T = 0.50
    if abs(drift_bpp) < STABLE_T:
        direction, magnitude = "stable", "faible"
    elif drift_bpp > 0:
        direction = "hausse"
        magnitude = "fort" if abs(drift_bpp) >= STRONG_T else "modéré"
    else:
        direction = "baisse"
        magnitude = "fort" if abs(drift_bpp) >= STRONG_T else "modéré"

    last_p = float(p[-1])
    if   direction == "hausse" and last_p > PRESSURE_NOMINAL_MAX: regime = "surpression_critique"
    elif direction == "baisse" and last_p < PRESSURE_NOMINAL_MIN: regime = "dépressurisation_lente"
    else:                                                           regime = "nominal"

    return {
        "drift_bar_per_period": drift_bpp, "drift_direction": direction,
        "drift_magnitude": magnitude,      "regime": regime,
        "mu_log": mu_log,                  "sigma_realised": sig_r,
        "p_intercept": float(cf[1]),       "p_projected_next": p_proj,
        "r_squared": r2,                   "n_samples": n,
        "sufficient_data": True,
    }


def run_stochastic_engine(df: pd.DataFrame) -> dict:
    """
    Orchestre les trois fonctions stochastiques sur un snapshot unique de df.
    Retourne un dict unifié avec les scalaires de la dernière mesure prêts à
    l'affichage et la classification du régime stochastique.
    """
    turb   = calculate_turbulence_variance(df)
    pk, pd_ = stochastic_pressure_oscillator(df)
    drift  = calculate_hydrodynamic_drift(df)

    def _last(s: pd.Series) -> float:
        return float(s.iloc[-1]) if len(s) > 0 and not np.isnan(s.iloc[-1]) else float("nan")

    k_last = _last(pk)
    regime = ("overbought" if not math.isnan(k_last) and k_last >= STOCH_OVERBOUGHT
              else "oversold" if not math.isnan(k_last) and k_last <= STOCH_OVERSOLD
              else "neutral")

    return {
        "turbulence_series": turb,
        "pct_k":             pk,
        "pct_d":             pd_,
        "drift":             drift,
        "turbulence_last":   _last(turb),
        "k_last":            k_last,
        "d_last":            _last(pd_),
        "stoch_regime":      regime,
        "computed_at":       datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3] + " UTC",
    }


# ──────────────────────────────────────────────────────────────────────────────
#  §6  MOTEUR DES SEUILS D'INTERVENTION (Étape 3)
# ──────────────────────────────────────────────────────────────────────────────

def calculate_intervention_thresholds(
    current_pressure: float,
    volatility:       float,
    stoch_k:          float,
    stoch_d:          float,
    drift:            dict,
) -> dict:
    """
    Synthétise la matrice vectorielle fluide en coordonnées d'intervention
    absolues pour l'opérateur de garde sur le panneau de commande XA-UUSD.

    Cascade de décision (priorité décroissante) :

    [0] INHIBITION D'URGENCE ─────────────────────────────────────────────────
        Condition : volatility > TURBULENCE_INHIBIT_THRESHOLD (2.55 bar)
        → Court-circuite IMMÉDIATEMENT toute la cascade.
        → Retourne tous les seuils à None.
        → L'opérateur NE DOIT PAS agir sur les vannes.

    [1] ZEROING POINT (ZP) ────────────────────────────────────────────────────
        Point d'inversion cinétique. Trois axes de détection :
        · Axe 1 : %K < 20 ET %K > %D  → exhaustion ascendante → Procédure IN
          ZP = P_courante − (σ × 0.618)
        · Axe 2 : %K > 80 ET %K < %D  → saturation descendante → Procédure OUT
          ZP = P_courante + (σ × 0.618)
        · Axe 3 (drift GBM)            → direction drift sans croisement franc
        · Défaut HOLD                  → ZP = P_courante

    [2] NDT (Nominal Decompression Threshold) ─────────────────────────────────
        Jauge de désengagement nominal.
        NDT = ZP ± (σ × 0.618)   [ratio Fibonacci — buffer dynamique]
        Clipé dans [PRESSURE_WARNING_LOW, PRESSURE_WARNING_HIGH].

    [3] ERV (Emergency Relief Valve) ──────────────────────────────────────────
        Barrière rigide de coupure préventive.
        ERV = P_courante ± (σ × 1.5)
        Contraintes : [ERV_HARD_FLOOR=5.0 bar, ERV_HARD_CEILING=64.0 bar]

    [4] HEP (Hydrostatique Equilibrium Point) ─────────────────────────────────
        Point neutre cible post-intervention, compensant la friction mécanique.
        HEP = ZP ± 0.25 bar
        Clipé dans [PRESSURE_NOMINAL_MIN, PRESSURE_NOMINAL_MAX].

    Args:
        current_pressure : Pression absolue courante (bar) — doit être > 0.
        volatility       : σ_turbulence courante (bar) — NaN traité comme 0.
        stoch_k          : %K [0,100] — NaN traité comme 50 (neutre).
        stoch_d          : %D [0,100] — NaN traité comme 50 (neutre).
        drift            : dict issu de calculate_hydrodynamic_drift().

    Returns:
        dict — clés : operational_state, state_message, inhibited,
                      inhibit_reason, zeroing_point, ndt, erv, hep,
                      procedure, zeroing_direction, computed_at.

    Raises:
        ValueError : current_pressure ≤ 0 ou non-fini.
    """
    ts = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3] + " UTC"

    if not math.isfinite(current_pressure) or current_pressure <= 0:
        raise ValueError(
            f"calculate_intervention_thresholds : current_pressure invalide "
            f"({current_pressure})."
        )

    # Assainissement NaN
    vol = volatility if (math.isfinite(volatility) and volatility >= 0) else 0.0
    k   = stoch_k   if math.isfinite(stoch_k)   else 50.0
    d   = stoch_d   if math.isfinite(stoch_d)   else 50.0

    # ── [0] INHIBITION D'URGENCE ─────────────────────────────────────────
    if vol > TURBULENCE_INHIBIT_THRESHOLD:
        excess = (vol - TURBULENCE_INHIBIT_THRESHOLD) / TURBULENCE_INHIBIT_THRESHOLD * 100.0
        return {
            "operational_state": _INHIBIT_STATE_KEY,
            "state_message":     _INHIBIT_STATE_MSG,
            "inhibited":         True,
            "inhibit_reason": (
                f"σ_turb={vol:.4f} bar > seuil {TURBULENCE_INHIBIT_THRESHOLD:.2f} bar "
                f"({TURBULENCE_INHIBIT_MULTIPLIER}×σ_base) de +{excess:.1f} % — "
                f"Suspendre toute intervention sur les vannes."
            ),
            "zeroing_point":     None,
            "ndt":               None,
            "erv":               None,
            "hep":               None,
            "procedure":         None,
            "zeroing_direction": None,
            "computed_at":       ts,
        }

    # ── [1] ZEROING POINT ────────────────────────────────────────────────
    kd   = k - d
    cross_detected = abs(kd) >= ZEROING_MIN_KD_CROSSOVER
    buf  = vol * NDT_VOLATILITY_RATIO

    if k < ZEROING_STOCH_LOW_ZONE  if hasattr(k, '__float__') else False and cross_detected and kd > 0:
        procedure, z_dir, zp_raw = "IN",   "hausse", current_pressure - buf
    elif k > ZEROING_STOCH_HIGH_ZONE and cross_detected and kd < 0:
        procedure, z_dir, zp_raw = "OUT",  "baisse", current_pressure + buf
    elif drift["sufficient_data"] and drift["drift_direction"] == "hausse":
        procedure, z_dir, zp_raw = "OUT",  "hausse", drift["p_projected_next"]
    elif drift["sufficient_data"] and drift["drift_direction"] == "baisse":
        procedure, z_dir, zp_raw = "IN",   "baisse", drift["p_projected_next"]
    else:
        procedure, z_dir, zp_raw = "HOLD", "neutre", current_pressure

    # Re-évaluation explicite axe 1 (contournement condition composée)
    if k < ZEROING_STOCH_LOW_ZONE and cross_detected and kd > 0 and procedure == "HOLD":
        procedure, z_dir, zp_raw = "IN", "hausse", current_pressure - buf

    zp = float(np.clip(zp_raw, PRESSURE_CRITICAL_LOW, PRESSURE_CRITICAL_HIGH))

    # ── [2] NDT ──────────────────────────────────────────────────────────
    ndt_raw = (zp + buf if procedure == "IN"
               else zp - buf if procedure == "OUT"
               else zp)
    ndt = float(np.clip(ndt_raw, PRESSURE_WARNING_LOW, PRESSURE_WARNING_HIGH))

    # ── [3] ERV ──────────────────────────────────────────────────────────
    erv_off  = vol * ERV_SAFETY_MULTIPLIER
    erv_high = float(min(current_pressure + erv_off, ERV_HARD_CEILING))
    erv_low  = float(max(current_pressure - erv_off, ERV_HARD_FLOOR))
    if   procedure == "IN":  erv = erv_low
    elif procedure == "OUT": erv = erv_high
    else:
        erv = erv_high if abs(erv_high - PRESSURE_CRITICAL_HIGH) < abs(erv_low - PRESSURE_CRITICAL_LOW) else erv_low

    # ── [4] HEP ──────────────────────────────────────────────────────────
    hep_raw = (zp + HEP_FRICTION_OFFSET_BAR if procedure == "IN"
               else zp - HEP_FRICTION_OFFSET_BAR if procedure == "OUT"
               else zp)
    hep = float(np.clip(hep_raw, PRESSURE_NOMINAL_MIN, PRESSURE_NOMINAL_MAX))

    proc_txt = {
        "IN":   "PROCÉDURE IN  — Injection fluide requise",
        "OUT":  "PROCÉDURE OUT — Extraction fluide requise",
        "HOLD": "PROCÉDURE HOLD — Maintien des vannes",
    }
    return {
        "operational_state": "OPERATIONNEL",
        "state_message": (
            f"{proc_txt.get(procedure,'—')} | "
            f"ZP={zp:.3f} | NDT={ndt:.3f} | ERV={erv:.3f} | HEP={hep:.3f} bar"
        ),
        "inhibited":         False,
        "inhibit_reason":    None,
        "zeroing_point":     zp,
        "ndt":               ndt,
        "erv":               erv,
        "hep":               hep,
        "procedure":         procedure,
        "zeroing_direction": z_dir,
        "computed_at":       ts,
    }


# ──────────────────────────────────────────────────────────────────────────────
#  §7  ACQUISITION API
# ──────────────────────────────────────────────────────────────────────────────

def _build_api_headers() -> dict:
    """
    Construit les headers HTTP d'authentification depuis st.secrets.
    Clés requises dans [api_xa_uusd] : api_key (+ optionnel client_id).
    """
    try:
        api_key   = st.secrets["api_xa_uusd"]["api_key"]
        client_id = st.secrets["api_xa_uusd"].get("client_id", "XA-UUSD-CTRL")
    except KeyError as e:
        raise KeyError(
            "Secret manquant : [api_xa_uusd].api_key absent de secrets.toml"
        ) from e
    return {
        "Authorization": f"Bearer {api_key}",
        "X-Client-ID":   client_id,
        "Accept":        "application/json",
        "Content-Type":  "application/json",
    }


def get_fluid_absolute_pressure() -> Tuple[Optional[float], float, Optional[str]]:
    """
    Interroge l'endpoint REST XA-UUSD pour la pression absolue courante.

    Résilience complète :
        · Timeout réseau strict (API_TIMEOUT_SECONDS)
        · HTTP non-200 avec message d'erreur détaillé
        · Clé JSON manquante dans le payload
        · Valeur non-physique (≤ 0, inf, nan)
        · ConnectionError, RequestException, ValueError, TypeError

    Returns:
        (pressure_bar | None, latency_ms, error_msg | None)
    """
    url     = f"{API_BASE_URL}?sensor={SENSOR_ID}"
    t_start = time.perf_counter()

    try:
        headers = _build_api_headers()
    except KeyError as e:
        return None, (time.perf_counter() - t_start) * 1000, str(e)

    try:
        resp       = requests.get(url=url, headers=headers, timeout=API_TIMEOUT_SECONDS)
        latency_ms = (time.perf_counter() - t_start) * 1000

        if resp.status_code != 200:
            return None, latency_ms, f"HTTP {resp.status_code} — {resp.text[:200]}"

        payload = resp.json()
        if "absolute_pressure_bar" not in payload:
            return None, latency_ms, f"Clé manquante dans payload : {list(payload.keys())}"

        raw = float(payload["absolute_pressure_bar"])
        if not math.isfinite(raw) or raw <= 0:
            return None, latency_ms, f"Valeur non-physique : {raw}"

        return raw, latency_ms, None

    except requests.exceptions.Timeout:
        return None, (time.perf_counter() - t_start) * 1000, \
               f"Timeout API ({API_TIMEOUT_SECONDS}s)"
    except requests.exceptions.ConnectionError as e:
        return None, (time.perf_counter() - t_start) * 1000, f"Connexion : {e}"
    except requests.exceptions.RequestException as e:
        return None, (time.perf_counter() - t_start) * 1000, f"Requête : {e}"
    except (ValueError, TypeError) as e:
        return None, (time.perf_counter() - t_start) * 1000, f"Parsing JSON : {e}"


async def async_poll_pressure_once() -> Tuple[Optional[float], float, Optional[str]]:
    """Enveloppe asyncio non-bloquante autour de get_fluid_absolute_pressure()."""
    return await asyncio.get_event_loop().run_in_executor(
        None, get_fluid_absolute_pressure
    )


# ──────────────────────────────────────────────────────────────────────────────
#  §8  PIPELINE D'ACQUISITION COMPLET
# ──────────────────────────────────────────────────────────────────────────────

def _append_event_log(level: str, message: str) -> None:
    """Ajoute une entrée horodatée au journal d'événements (FIFO max 120)."""
    ts  = datetime.datetime.utcnow().strftime("%H:%M:%S")
    log = st.session_state["event_log"]
    log.append((ts, level, message))
    if len(log) > 120:
        st.session_state["event_log"] = log[-120:]


def run_full_pipeline() -> Optional[dict]:
    """
    Exécute un cycle complet d'acquisition + calcul :
      1. Appel API → get_fluid_absolute_pressure()
      2. Mise à jour DataFrame historique
      3. Moteur stochastique → run_stochastic_engine()
      4. Seuils d'intervention → calculate_intervention_thresholds()
      5. Mise à jour session_state + journal

    Returns:
        dict intervention_thresholds si succès, None si erreur API.
    """
    pressure, latency_ms, error = get_fluid_absolute_pressure()
    st.session_state["total_api_calls"] += 1
    ts_now = datetime.datetime.utcnow()

    if error is not None:
        st.session_state["failed_api_calls"] += 1
        _append_event_log("err", f"Échec API — {error}")
        return None

    st.session_state["pressure_history_df"] = append_pressure_record(
        df=st.session_state["pressure_history_df"],
        pressure=pressure, latency_ms=latency_ms, ts=ts_now,
    )
    st.session_state["last_successful_ts"] = (
        ts_now.strftime("%H:%M:%S.%f")[:-3] + " UTC"
    )

    # Étape 2 : stochastique
    df_snap = st.session_state["pressure_history_df"]
    sr: dict = run_stochastic_engine(df_snap)
    st.session_state["stoch_results"] = sr

    # Étape 3 : seuils d'intervention
    th: Optional[dict] = None
    if not math.isnan(sr["k_last"]):
        try:
            th = calculate_intervention_thresholds(
                current_pressure = pressure,
                volatility       = sr["turbulence_last"],
                stoch_k          = sr["k_last"],
                stoch_d          = sr["d_last"],
                drift            = sr["drift"],
            )
            st.session_state["intervention_thresholds"] = th

            if th["inhibited"]:
                st.session_state["inhibit_count"] += 1
                _append_event_log(
                    "inh",
                    f"!!! INHIBITION #{st.session_state['inhibit_count']} — "
                    f"{th['inhibit_reason'][:90]}",
                )
            else:
                _append_event_log(
                    "ok",
                    f"ZP={th['zeroing_point']:.3f} NDT={th['ndt']:.3f} "
                    f"ERV={th['erv']:.3f} HEP={th['hep']:.3f} bar | "
                    f"Proc.{th['procedure']}",
                )
        except ValueError as ve:
            _append_event_log("err", f"Seuils : {ve}")

    status = _classify_pressure_status(pressure)
    level  = "ok" if status == "nominal" else ("wrn" if status == "warning" else "err")
    _append_event_log(level,
        f"{pressure:.3f} bar | lat {latency_ms:.1f} ms | {status.upper()}")

    if not math.isnan(sr["k_last"]):
        _append_event_log(
            "ok",
            f"%K={sr['k_last']:.1f} %D={sr['d_last']:.1f} | "
            f"σ={sr['turbulence_last']:.4f} | drift={sr['drift']['drift_direction']}",
        )

    return th


# ──────────────────────────────────────────────────────────────────────────────
#  §9  COMPOSANTS UI STATIQUES  (rendus hors fragment — chargement unique)
# ──────────────────────────────────────────────────────────────────────────────

def _render_static_header() -> None:
    """Bandeau titre statique — rendu une seule fois au chargement de la page."""
    c_logo, c_title, c_badge = st.columns([0.07, 0.63, 0.30])

    with c_logo:
        st.markdown(
            '<div style="font-size:2.8rem;line-height:1;padding-top:6px;">⬡</div>',
            unsafe_allow_html=True,
        )
    with c_title:
        st.markdown(
            '<div class="xa-title">Contrôle Hydrodynamique<br>'
            '<span class="accent">XA-UUSD</span></div>'
            '<div class="xa-subtitle">'
            'PIPELINE EXPÉRIMENTAL · TÉLÉMÉTRIE · STOCHASTIQUE · INTERVENTION · '
            'FRAGMENT LIVE v4'
            '</div>',
            unsafe_allow_html=True,
        )
    with c_badge:
        # Badge statique de démarrage — le fragment actualisera son propre badge
        st.markdown(
            '<div style="text-align:right;padding-top:10px;">'
            '<span class="status-badge" '
            'style="border-color:#1E2A40;color:#6B7FA3;">○ INITIALISATION</span>'
            '</div>',
            unsafe_allow_html=True,
        )


def _render_static_sidebar() -> None:
    """
    Sidebar statique — compteurs, paramètres de calibration, journal.
    Le journal est lu depuis session_state et se rafraîchit à chaque rerun
    déclenché par le fragment.
    """
    st.sidebar.markdown(
        '<div class="xa-title" style="font-size:1.1rem;">◈ Diagnostics<br>Système</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    total  = st.session_state["total_api_calls"]
    failed = st.session_state["failed_api_calls"]
    succ   = total - failed
    rate   = succ / total * 100.0 if total > 0 else 0.0

    st.sidebar.metric("Appels API",       total)
    st.sidebar.metric("Succès",           succ)
    st.sidebar.metric("Taux succès",      f"{rate:.1f} %")
    st.sidebar.metric("Échecs",           failed)
    st.sidebar.metric("Cycles fragment",  st.session_state["fragment_cycle"])
    st.sidebar.metric("Inhibitions ⚠",   st.session_state["inhibit_count"])

    df = st.session_state["pressure_history_df"]
    if len(df) >= 2:
        st.sidebar.markdown('<hr class="xa-divider">', unsafe_allow_html=True)
        st.sidebar.metric("P min",   f"{df['pressure_bar'].min():.3f} bar")
        st.sidebar.metric("P max",   f"{df['pressure_bar'].max():.3f} bar")
        st.sidebar.metric("σ global",f"{df['pressure_bar'].std():.4f} bar")

    # Paramètres de calibration (lecture seule)
    st.sidebar.markdown('<hr class="xa-divider">', unsafe_allow_html=True)
    st.sidebar.markdown(
        '<div class="metric-label" style="margin-bottom:8px;">Calibration</div>',
        unsafe_allow_html=True,
    )
    st.sidebar.markdown(
        f'<div class="log-entry">σ_inhibit  = {TURBULENCE_INHIBIT_THRESHOLD:.2f} bar</div>'
        f'<div class="log-entry">NDT ratio  = {NDT_VOLATILITY_RATIO}</div>'
        f'<div class="log-entry">ERV mult.  = {ERV_SAFETY_MULTIPLIER}×</div>'
        f'<div class="log-entry">HEP offset = {HEP_FRICTION_OFFSET_BAR} bar</div>'
        f'<div class="log-entry">Fragment   = {FRAGMENT_INTERVAL}</div>',
        unsafe_allow_html=True,
    )

    # Journal d'événements
    st.sidebar.markdown('<hr class="xa-divider">', unsafe_allow_html=True)
    st.sidebar.markdown(
        '<div class="metric-label" style="margin-bottom:8px;">Journal temps réel</div>',
        unsafe_allow_html=True,
    )
    log = st.session_state["event_log"]
    if not log:
        st.sidebar.markdown(
            '<div class="log-entry">Aucun événement.</div>',
            unsafe_allow_html=True,
        )
    else:
        for ts, lvl, msg in reversed(log[-20:]):
            st.sidebar.markdown(
                f'<div class="log-entry {lvl}">[{ts}] {msg}</div>',
                unsafe_allow_html=True,
            )

    # Erreur fragment si présente
    if st.session_state.get("last_fragment_error"):
        st.sidebar.markdown('<hr class="xa-divider">', unsafe_allow_html=True)
        st.sidebar.error(
            f"⚠ Dernière erreur fragment :\n"
            f"{st.session_state['last_fragment_error']}"
        )


# ──────────────────────────────────────────────────────────────────────────────
#  §10  LIVE CONTROL DASHBOARD — FRAGMENT @st.fragment(run_every="5s")
#
#  Ce bloc est le cœur opérationnel de l'interface v4.
#  Il s'exécute de façon autonome toutes les 5 secondes sans déclencher
#  un rerun complet de la page, garantissant :
#    · Zéro rechargement du layout statique (header, sidebar, graphiques)
#    · Isolation du fil d'exécution → pas de blocage du thread principal
#    · Cache navigateur préservé (Streamlit ne régénère pas les ressources
#      statiques non incluses dans le fragment)
# ──────────────────────────────────────────────────────────────────────────────

@st.fragment(run_every=FRAGMENT_INTERVAL)
def live_control_dashboard() -> None:
    """
    Dashboard de contrôle live — sub-routine fragmentée @st.fragment(5 s).

    Chaque cycle effectue dans l'ordre :
      1. Acquisition pression via get_fluid_absolute_pressure() (try/except)
      2. Calcul stochastique (turbulence, %K/%D, drift GBM)
      3. Calcul seuils d'intervention (ZP / NDT / ERV / HEP)
      4. Rendu du panneau d'état opérationnel (ACTION / PAUSE / HOLD)
      5. Affichage des 5 st.metric principaux (pression + 4 coordonnées)
      6. Graphique pression historique avec niveaux d'intervention superposés
      7. Rangée KPI stochastique (%K, %D, σ, drift)
      8. Analyse GBM détaillée
      9. Tableau récapitulatif des coordonnées d'actionnement opérateur

    Toutes les opérations réseau et de calcul sont enveloppées dans des
    blocs try/except granulaires pour garantir qu'aucune exception ne
    provoque l'effondrement de l'interface graphique.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # ── Incrément du compteur de cycles fragment ──────────────────────────
    st.session_state["fragment_cycle"] += 1
    cycle = st.session_state["fragment_cycle"]

    # ── Ticker de synchronisation ─────────────────────────────────────────
    now_utc = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
    st.markdown(
        f'<div class="fragment-tick">'
        f'<span class="tick-dot"></span>'
        f'FRAGMENT CYCLE #{cycle} — {now_utc} UTC — '
        f'Intervalle : {FRAGMENT_INTERVAL}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════════════════════════
    #  ACQUISITION + CALCUL PIPELINE (try/except global fragment)
    # ══════════════════════════════════════════════════════════════════════
    pressure:   Optional[float] = None
    latency_ms: float           = 0.0
    api_error:  Optional[str]   = None
    sr:         Optional[dict]  = None
    th:         Optional[dict]  = None

    # ── Appel API avec capture d'exception réseau ─────────────────────────
    try:
        pressure, latency_ms, api_error = get_fluid_absolute_pressure()
        st.session_state["total_api_calls"] += 1

        if api_error is not None:
            st.session_state["failed_api_calls"] += 1
            _append_event_log("err", f"[Fragment #{cycle}] API — {api_error}")
        else:
            ts_now = datetime.datetime.utcnow()
            st.session_state["pressure_history_df"] = append_pressure_record(
                df=st.session_state["pressure_history_df"],
                pressure=pressure, latency_ms=latency_ms, ts=ts_now,
            )
            st.session_state["last_successful_ts"] = (
                ts_now.strftime("%H:%M:%S.%f")[:-3] + " UTC"
            )
            status_p = _classify_pressure_status(pressure)
            lvl_p = "ok" if status_p == "nominal" else (
                "wrn" if status_p == "warning" else "err"
            )
            _append_event_log(
                lvl_p,
                f"[#{cycle}] {pressure:.3f} bar | lat {latency_ms:.1f} ms | "
                f"{status_p.upper()}",
            )
            st.session_state["last_fragment_error"] = None

    except Exception as exc_api:
        st.session_state["last_fragment_error"] = str(exc_api)
        _append_event_log("err", f"[Fragment #{cycle}] Exception API : {exc_api}")

    # ── Calcul stochastique avec capture d'exception ──────────────────────
    try:
        df_snap = st.session_state["pressure_history_df"]
        if len(df_snap) >= 1:
            sr = run_stochastic_engine(df_snap)
            st.session_state["stoch_results"] = sr
    except Exception as exc_stoch:
        _append_event_log("err", f"[#{cycle}] Stochastique : {exc_stoch}")
        sr = st.session_state.get("stoch_results")  # fallback sur cache

    # ── Calcul seuils d'intervention avec capture d'exception ─────────────
    try:
        if (pressure is not None and api_error is None
                and sr is not None and not math.isnan(sr["k_last"])):
            th = calculate_intervention_thresholds(
                current_pressure = pressure,
                volatility       = sr["turbulence_last"],
                stoch_k          = sr["k_last"],
                stoch_d          = sr["d_last"],
                drift            = sr["drift"],
            )
            st.session_state["intervention_thresholds"] = th

            if th["inhibited"]:
                st.session_state["inhibit_count"] += 1
                _append_event_log(
                    "inh",
                    f"[#{cycle}] !!! INHIBITION #{st.session_state['inhibit_count']} — "
                    f"{th['inhibit_reason'][:80]}",
                )
            else:
                _append_event_log(
                    "ok",
                    f"[#{cycle}] ZP={th['zeroing_point']:.3f} "
                    f"NDT={th['ndt']:.3f} ERV={th['erv']:.3f} "
                    f"HEP={th['hep']:.3f} Proc.{th['procedure']}",
                )
        else:
            # Utiliser le cache si pas de nouvelles données
            th = st.session_state.get("intervention_thresholds")

    except ValueError as ve:
        _append_event_log("err", f"[#{cycle}] Seuils : {ve}")
        th = st.session_state.get("intervention_thresholds")
    except Exception as exc_th:
        _append_event_log("err", f"[#{cycle}] Exception seuils : {exc_th}")
        th = st.session_state.get("intervention_thresholds")

    # ══════════════════════════════════════════════════════════════════════
    #  RENDU UI — PANNEAU D'ÉTAT OPÉRATIONNEL
    # ══════════════════════════════════════════════════════════════════════
    st.markdown(
        '<div class="section-header live">'
        '⚡ Salle de Contrôle Live — Panneau Manuel Vannes XA-UUSD'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Alerte API si erreur ──────────────────────────────────────────────
    if api_error is not None:
        st.warning(
            f"⚠ **Interruption réseau momentanée** — Cycle #{cycle}\n\n"
            f"`{api_error}`\n\n"
            f"Les seuils affichés ci-dessous sont issus du dernier cycle "
            f"valide. L'interface reste opérationnelle.",
            icon="📡",
        )

    # ── Panneau ACTION / PAUSE / HOLD ─────────────────────────────────────
    if th is not None:
        if th["inhibited"]:
            panel_css      = "inhibit"
            headline_css   = "inhibit"
            icon           = "🛑"
            headline_text  = "PAUSE DE SÉCURITÉ"
            detail_lines   = [
                "CHAOS CINÉTIQUE DÉTECTÉ — SUSPENDRE TOUTE INTERVENTION SUR LES VANNES",
                th.get("inhibit_reason", ""),
                f"Seuil d'inhibition : σ_turb > {TURBULENCE_INHIBIT_THRESHOLD:.2f} bar",
            ]
            proc_badge_html = (
                '<span class="action-proc-badge proc-hold">VANNES BLOQUÉES</span>'
            )
        elif th["procedure"] == "HOLD":
            panel_css      = "hold"
            headline_css   = "hold"
            icon           = "⏸"
            headline_text  = "SYSTÈME STABLE — MAINTIEN DES VANNES"
            detail_lines   = [
                th["state_message"],
                f"Drift : {th['zeroing_direction'].upper()} | "
                f"Calculé : {th['computed_at']}",
            ]
            proc_badge_html = (
                '<span class="action-proc-badge proc-hold">PROCÉDURE HOLD</span>'
            )
        else:
            panel_css      = "required"
            headline_css   = "required"
            icon           = "🔧"
            headline_text  = "ACTION REQUISE SUR LE PANEL MANUEL"
            detail_lines   = [
                th["state_message"],
                f"Sens cinétique : {th['zeroing_direction'].upper()} | "
                f"Calculé : {th['computed_at']}",
            ]
            proc_css_map = {"IN": "proc-in", "OUT": "proc-out"}
            proc_badge_html = (
                f'<span class="action-proc-badge '
                f'{proc_css_map.get(th["procedure"], "proc-hold")}">'
                f'PROCÉDURE {th["procedure"]} ACTIVÉE</span>'
            )

        detail_html = "".join(
            f'<div class="action-detail">{l}</div>' for l in detail_lines if l
        )
        st.markdown(
            f'<div class="action-panel {panel_css}">'
            f'<div style="font-size:1.8rem;line-height:1;">{icon}</div>'
            f'<div class="action-headline {headline_css}">{headline_text}</div>'
            f'{detail_html}'
            f'{proc_badge_html}'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info(
            f"Seuils d'intervention en attente — "
            f"minimum {MIN_ROWS_FOR_STOCHASTICS} acquisitions requises "
            f"(actuellement : {len(st.session_state['pressure_history_df'])})."
        )

    st.markdown('<div style="margin-top:16px;"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  5 ST.METRIC PRÉÉMINENTS — Pression + 4 coordonnées d'actionnement
    # ══════════════════════════════════════════════════════════════════════
    st.markdown(
        '<div class="metric-label" style="margin-bottom:8px;">'
        'Coordonnées d\'actionnement — Panneau de commande central</div>',
        unsafe_allow_html=True,
    )

    df_now = st.session_state["pressure_history_df"]

    # Valeurs courantes
    if pressure is not None and api_error is None:
        p_disp     = pressure
        delta_disp = float(df_now["delta_bar"].iloc[-1]) if len(df_now) > 0 else 0.0
    elif len(df_now) > 0:
        p_disp     = float(df_now["pressure_bar"].iloc[-1])
        delta_disp = float(df_now["delta_bar"].iloc[-1])
    else:
        p_disp, delta_disp = None, None

    # Résolution des 4 seuils (None si inhibé ou données insuffisantes)
    zp_disp  = th["zeroing_point"] if (th and not th["inhibited"]) else None
    ndt_disp = th["ndt"]           if (th and not th["inhibited"]) else None
    erv_disp = th["erv"]           if (th and not th["inhibited"]) else None
    hep_disp = th["hep"]           if (th and not th["inhibited"]) else None

    # Delta vs cycle précédent (depuis historique)
    def _prev_val(key_th: Optional[float]) -> Optional[float]:
        """Retourne None — les deltas st.metric sont calculés vs valeur précédente."""
        return None

    col_p, col_zp, col_ndt, col_erv, col_hep = st.columns(5)

    # ── PRESSION ACTUELLE ─────────────────────────────────────────────────
    with col_p:
        if p_disp is not None:
            delta_str  = f"{delta_disp:+.3f} bar" if delta_disp is not None else None
            p_status   = _classify_pressure_status(p_disp)
            p_delta_c  = ("normal" if abs(delta_disp or 0) < 1.0
                          else "off" if (delta_disp or 0) > 0 else "inverse")
            st.metric(
                label       = "PRESSION ACTUELLE",
                value       = f"{p_disp:.3f} bar",
                delta       = delta_str,
                delta_color = p_delta_c,
                help        = f"Statut zone : {p_status.upper()} | "
                              f"Latence API : {latency_ms:.1f} ms",
            )
        else:
            st.metric("PRESSION ACTUELLE", "— bar", help="Données indisponibles")

    # ── PROCÉDURE D'INJECTION — Zeroing Point (Entry) ─────────────────────
    with col_zp:
        if zp_disp is not None:
            zp_z = _classify_pressure_status(zp_disp)
            st.metric(
                label = "PROCÉDURE D'INJECTION (Entry)",
                value = f"{zp_disp:.3f} bar",
                delta = f"Proc. {th['procedure']}",
                delta_color = "normal",
                help  = (
                    f"Zeroing Point — Point d'inversion cinétique.\n"
                    f"Zone : {zp_z.upper()} | "
                    f"Sens : {th['zeroing_direction'].upper()}"
                ),
            )
        else:
            lbl_inh = "INHIBÉ" if (th and th["inhibited"]) else "—"
            st.metric("PROCÉDURE D'INJECTION (Entry)", lbl_inh,
                      help="En attente de données suffisantes ou inhibition active")

    # ── DÉCOMPRESSION NDT — Take Profit ───────────────────────────────────
    with col_ndt:
        if ndt_disp is not None:
            ndt_z = _classify_pressure_status(ndt_disp)
            ndt_d = ndt_disp - (zp_disp or ndt_disp)
            st.metric(
                label = "DÉCOMPRESSION NDT (Take Profit)",
                value = f"{ndt_disp:.3f} bar",
                delta = f"{ndt_d:+.3f} bar vs ZP",
                delta_color = "normal" if ndt_d >= 0 else "inverse",
                help  = (
                    f"Nominal Decompression Threshold — Jauge de désengagement.\n"
                    f"Zone : {ndt_z.upper()} | "
                    f"Ratio Fibonacci : {NDT_VOLATILITY_RATIO}"
                ),
            )
        else:
            st.metric("DÉCOMPRESSION NDT (Take Profit)", "— bar")

    # ── SOUPAPE URGENCE ERV — Stop Loss ───────────────────────────────────
    with col_erv:
        if erv_disp is not None:
            erv_z = _classify_pressure_status(erv_disp)
            erv_d = erv_disp - (p_disp or erv_disp)
            st.metric(
                label = "SOUPAPE URGENCE ERV (Stop Loss)",
                value = f"{erv_disp:.3f} bar",
                delta = f"{erv_d:+.3f} bar vs P",
                delta_color = "off" if erv_z == "critical" else "normal",
                help  = (
                    f"Emergency Relief Valve — Barrière de coupure préventive.\n"
                    f"Zone : {erv_z.upper()} | "
                    f"Multiplicateur : {ERV_SAFETY_MULTIPLIER}×σ_turb"
                ),
            )
        else:
            st.metric("SOUPAPE URGENCE ERV (Stop Loss)", "— bar")

    # ── ÉQUILIBRE HEP — Break Even ────────────────────────────────────────
    with col_hep:
        if hep_disp is not None:
            hep_z = _classify_pressure_status(hep_disp)
            hep_d = hep_disp - (zp_disp or hep_disp)
            st.metric(
                label = "ÉQUILIBRE HEP (Break Even)",
                value = f"{hep_disp:.3f} bar",
                delta = f"{hep_d:+.3f} bar vs ZP",
                delta_color = "normal",
                help  = (
                    f"Hydrostatique Equilibrium Point — Cible post-intervention.\n"
                    f"Zone : {hep_z.upper()} | "
                    f"Offset friction : +{HEP_FRICTION_OFFSET_BAR} bar"
                ),
            )
        else:
            st.metric("ÉQUILIBRE HEP (Break Even)", "— bar")

    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  GRAPHIQUE PRINCIPAL — Pression historique + niveaux intervention
    # ══════════════════════════════════════════════════════════════════════
    st.markdown(
        '<div class="metric-label" style="margin-bottom:6px;">'
        'Historique pression absolue — Niveaux intervention superposés</div>',
        unsafe_allow_html=True,
    )

    try:
        df_plot = st.session_state["pressure_history_df"]
        fig_main = go.Figure()

        if len(df_plot) > 0:
            xv = df_plot["timestamp"].dt.strftime("%H:%M:%S").tolist()

            fig_main.add_hrect(y0=PRESSURE_NOMINAL_MIN, y1=PRESSURE_NOMINAL_MAX,
                               fillcolor="rgba(0,229,160,0.04)",
                               line_width=0, layer="below")
            fig_main.add_hrect(y0=PRESSURE_NOMINAL_MAX, y1=PRESSURE_WARNING_HIGH,
                               fillcolor="rgba(255,184,48,0.04)",
                               line_width=0, layer="below")
            fig_main.add_hrect(y0=PRESSURE_WARNING_LOW, y1=PRESSURE_NOMINAL_MIN,
                               fillcolor="rgba(255,184,48,0.04)",
                               line_width=0, layer="below")

            fig_main.add_trace(go.Scatter(
                x=xv, y=df_plot["pressure_bar"].tolist(),
                mode="lines", name="P absolue",
                line=dict(color=COLOR_ACCENT, width=2.0),
                fill="tozeroy", fillcolor="rgba(58,123,255,0.05)",
            ))
            fig_main.add_trace(go.Scatter(
                x=xv, y=df_plot["rolling_avg_5"].tolist(),
                mode="lines", name="Moy.5",
                line=dict(color=COLOR_NOMINAL, width=1.2, dash="dot"),
            ))

            # Lignes de seuil de sécurité
            for y_v, col, lbl in [
                (PRESSURE_WARNING_HIGH,  COLOR_WARNING,  "Alerte ↑"),
                (PRESSURE_WARNING_LOW,   COLOR_WARNING,  "Alerte ↓"),
                (PRESSURE_CRITICAL_HIGH, COLOR_CRITICAL, "Critique ↑"),
                (PRESSURE_CRITICAL_LOW,  COLOR_CRITICAL, "Critique ↓"),
            ]:
                fig_main.add_hline(y=y_v, line_color=col, line_dash="dash",
                                   line_width=0.8,
                                   annotation_text=f" {lbl} {y_v}",
                                   annotation_font_color=col,
                                   annotation_font_size=9)

            # Superposition des 4 niveaux d'intervention
            if th and not th["inhibited"]:
                for attr, col, lbl, dash in [
                    ("zeroing_point", COLOR_ZP,  "ZP",  "dot"),
                    ("ndt",           COLOR_NDT, "NDT", "dashdot"),
                    ("erv",           COLOR_ERV, "ERV", "dash"),
                    ("hep",           COLOR_HEP, "HEP", "longdash"),
                ]:
                    v = th.get(attr)
                    if v is not None:
                        fig_main.add_hline(
                            y=v, line_color=col, line_dash=dash, line_width=1.1,
                            annotation_text=f" {lbl} {v:.3f}",
                            annotation_font_color=col, annotation_font_size=9,
                        )
            elif th and th["inhibited"]:
                # Ligne rouge d'inhibition
                fig_main.add_hline(
                    y=TURBULENCE_INHIBIT_THRESHOLD * 10,  # symbolique hors axe
                    line_color=COLOR_INHIBIT, line_dash="dash", line_width=0,
                )
                fig_main.add_annotation(
                    text="⛔ INHIBITION ACTIVE",
                    xref="paper", yref="paper", x=0.5, y=0.95,
                    showarrow=False,
                    font=dict(color=COLOR_INHIBIT, size=13,
                              family="Barlow Condensed"),
                    bgcolor="rgba(255,0,85,0.12)",
                    bordercolor=COLOR_INHIBIT, borderwidth=1,
                )

        fig_main.update_layout(
            paper_bgcolor=COLOR_PANEL, plot_bgcolor=COLOR_PANEL,
            font=dict(family="Share Tech Mono, monospace",
                      color=COLOR_TEXT_MUT, size=10),
            margin=dict(l=10, r=10, t=30, b=10), height=290,
            showlegend=True,
            legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10),
                        orientation="h", y=1.08),
            xaxis=dict(gridcolor=COLOR_BORDER, showgrid=True, zeroline=False,
                       title="Horodatage (UTC)", tickfont=dict(size=9)),
            yaxis=dict(gridcolor=COLOR_BORDER, showgrid=True, zeroline=False,
                       title=f"Pression ({PRESSURE_UNIT})"),
        )
        st.plotly_chart(fig_main, use_container_width=True)

    except Exception as exc_plot:
        st.error(f"Graphique temporairement indisponible : {exc_plot}")

    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  RANGÉE KPI STOCHASTIQUE  (fragment — mise à jour toutes les 5 s)
    # ══════════════════════════════════════════════════════════════════════
    st.markdown(
        '<div class="section-header">'
        'Moteur Stochastique — Cinématique Fluide XA-UUSD'
        '</div>',
        unsafe_allow_html=True,
    )

    try:
        sr_live = st.session_state.get("stoch_results")
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            if sr_live and not math.isnan(sr_live["turbulence_last"]):
                t_v = sr_live["turbulence_last"]
                t_c = (COLOR_INHIBIT if t_v > TURBULENCE_INHIBIT_THRESHOLD
                       else COLOR_CRITICAL if t_v > 2.0
                       else COLOR_WARNING  if t_v > 0.8
                       else COLOR_NOMINAL)
                sub = ("⛔ CHAOS" if t_v > TURBULENCE_INHIBIT_THRESHOLD
                       else "ÉLEVÉE" if t_v > 0.8 else "FAIBLE")
                t_s = f"{t_v:.4f}"
            else:
                t_s, t_c, sub = "—", COLOR_TEXT_MUT, "EN ATTENTE"
            st.markdown(
                f'<div class="metric-card turb">'
                f'<div class="metric-label">σ Turbulence (N={TURBULENCE_WINDOW})</div>'
                f'<div class="metric-value" style="color:{t_c};font-size:2.0rem;">'
                f'{t_s}<span class="metric-unit">bar</span></div>'
                f'<div class="metric-sub">{sub}</div></div>',
                unsafe_allow_html=True,
            )

        with c2:
            if sr_live and not math.isnan(sr_live["k_last"]):
                k_v = sr_live["k_last"]
                k_c = (COLOR_CRITICAL if k_v >= STOCH_OVERBOUGHT
                       else COLOR_NOMINAL if k_v <= STOCH_OVERSOLD
                       else COLOR_TEXT_PRI)
                r_map = {
                    "overbought": ("SURPRESSION",  "regime-overbought"),
                    "oversold":   ("DÉPRESSURISÉ", "regime-oversold"),
                    "neutral":    ("NEUTRE",        "regime-neutral"),
                }
                rl, rc = r_map.get(sr_live["stoch_regime"], ("—", "regime-neutral"))
            else:
                k_v, k_c = None, COLOR_TEXT_MUT
                rl, rc = "EN ATTENTE", "regime-neutral"
            k_s = f"{k_v:.1f}" if k_v is not None else "—"
            st.markdown(
                f'<div class="metric-card stoch">'
                f'<div class="metric-label">%K Oscillateur (N={STOCH_K_WINDOW})</div>'
                f'<div class="metric-value" style="color:{k_c};font-size:2.0rem;">'
                f'{k_s}<span class="metric-unit">%</span></div>'
                f'<div class="metric-sub">'
                f'<span class="stoch-regime {rc}">{rl}</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )

        with c3:
            if sr_live and not math.isnan(sr_live["d_last"]):
                d_v = sr_live["d_last"]
                d_c = (COLOR_CRITICAL if d_v >= STOCH_OVERBOUGHT
                       else COLOR_NOMINAL if d_v <= STOCH_OVERSOLD
                       else COLOR_STOCH_D)
                d_s = f"{d_v:.1f}"
                x_v = sr_live["k_last"] - d_v if not math.isnan(sr_live["k_last"]) else 0
                cross = ("▲ BULLISH" if x_v > 2 else "▼ BEARISH" if x_v < -2 else "≈ CONV.")
            else:
                d_s, d_c, cross = "—", COLOR_TEXT_MUT, "EN ATTENTE"
            st.markdown(
                f'<div class="metric-card stoch">'
                f'<div class="metric-label">%D Signal (SMA-{STOCH_D_SMOOTH})</div>'
                f'<div class="metric-value" style="color:{d_c};font-size:2.0rem;">'
                f'{d_s}<span class="metric-unit">%</span></div>'
                f'<div class="metric-sub">{cross}</div></div>',
                unsafe_allow_html=True,
            )

        with c4:
            if sr_live and sr_live["drift"]["sufficient_data"]:
                dr  = sr_live["drift"]
                sym = {"hausse": "↗", "baisse": "↘", "stable": "→"}.get(
                    dr["drift_direction"], "→")
                sgn = "+" if dr["drift_bar_per_period"] >= 0 else ""
                rc2 = {"surpression_critique": COLOR_CRITICAL,
                       "dépressurisation_lente": COLOR_WARNING,
                       "nominal": COLOR_NOMINAL}.get(dr["regime"], COLOR_TEXT_PRI)
                rl2 = {"surpression_critique": "SURPRESSION",
                       "dépressurisation_lente": "DÉPRESSURIS.",
                       "nominal": "NOMINAL"}.get(dr["regime"], "—")
                dr_s = f"{sym} {sgn}{dr['drift_bar_per_period']:.4f}"
                r2s  = f"R²={dr['r_squared']:.3f}"
            else:
                dr_s, rc2, rl2, r2s = "—", COLOR_TEXT_MUT, "EN ATTENTE", ""
            st.markdown(
                f'<div class="metric-card drift">'
                f'<div class="metric-label">Drift μ (bar/période)</div>'
                f'<div class="metric-value" style="color:{rc2};font-size:1.7rem;">'
                f'{dr_s}</div>'
                f'<div class="metric-sub">{rl2}  {r2s}</div></div>',
                unsafe_allow_html=True,
            )

    except Exception as exc_kpi:
        st.warning(f"KPI stochastique temporairement indisponible : {exc_kpi}")

    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  GRAPHIQUES STOCHASTIQUES (%K/%D + Turbulence σ + Drift GBM)
    # ══════════════════════════════════════════════════════════════════════
    try:
        sr_l  = st.session_state.get("stoch_results")
        df_g  = st.session_state["pressure_history_df"]

        if sr_l is not None and len(df_g) >= MIN_ROWS_FOR_STOCHASTICS:
            xg = df_g["timestamp"].dt.strftime("%H:%M:%S").tolist()
            col_osc, col_turb = st.columns(2)

            with col_osc:
                st.markdown(
                    f'<div class="metric-label" style="margin-bottom:4px;">'
                    f'Oscillateur %K/%D (N={STOCH_K_WINDOW})</div>',
                    unsafe_allow_html=True,
                )
                fig_osc = go.Figure()
                fig_osc.add_hrect(y0=STOCH_OVERBOUGHT, y1=100,
                                  fillcolor="rgba(255,58,58,0.08)", line_width=0)
                fig_osc.add_hrect(y0=0, y1=STOCH_OVERSOLD,
                                  fillcolor="rgba(0,229,160,0.06)", line_width=0)
                fig_osc.add_trace(go.Scatter(
                    x=xg, y=sr_l["pct_k"].tolist(), mode="lines", name="%K",
                    line=dict(color=COLOR_STOCH_K, width=1.8)))
                fig_osc.add_trace(go.Scatter(
                    x=xg, y=sr_l["pct_d"].tolist(), mode="lines",
                    name=f"%D(SMA-{STOCH_D_SMOOTH})",
                    line=dict(color=COLOR_STOCH_D, width=1.4, dash="dash")))
                for y_v, col, lbl in [
                    (STOCH_OVERBOUGHT, COLOR_CRITICAL, f"OB {STOCH_OVERBOUGHT}"),
                    (STOCH_OVERSOLD,   COLOR_NOMINAL,  f"OS {STOCH_OVERSOLD}"),
                ]:
                    fig_osc.add_hline(y=y_v, line_color=col, line_dash="dot",
                                      line_width=0.8,
                                      annotation_text=f" {lbl}",
                                      annotation_font_color=col,
                                      annotation_font_size=9)
                fig_osc.update_layout(
                    paper_bgcolor=COLOR_PANEL, plot_bgcolor=COLOR_PANEL,
                    font=dict(family="Share Tech Mono, monospace",
                              color=COLOR_TEXT_MUT, size=10),
                    margin=dict(l=5, r=5, t=20, b=5), height=230,
                    showlegend=True,
                    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9),
                                orientation="h", y=1.1),
                    xaxis=dict(gridcolor=COLOR_BORDER, showgrid=True,
                               zeroline=False, tickfont=dict(size=8)),
                    yaxis=dict(gridcolor=COLOR_BORDER, showgrid=True,
                               zeroline=False, range=[-5, 105], title="%"),
                )
                st.plotly_chart(fig_osc, use_container_width=True)

            with col_turb:
                st.markdown(
                    f'<div class="metric-label" style="margin-bottom:4px;">'
                    f'σ Turbulence (N={TURBULENCE_WINDOW}) + Drift GBM</div>',
                    unsafe_allow_html=True,
                )
                fig_t = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                      row_heights=[0.6, 0.4],
                                      vertical_spacing=0.06)
                fig_t.add_trace(go.Scatter(
                    x=xg, y=sr_l["turbulence_series"].tolist(),
                    mode="lines", name="σ turb",
                    line=dict(color=COLOR_TURBULENCE, width=1.6),
                    fill="tozeroy", fillcolor="rgba(96,165,250,0.07)",
                ), row=1, col=1)
                fig_t.add_hline(y=TURBULENCE_INHIBIT_THRESHOLD,
                                line_color=COLOR_INHIBIT, line_dash="dash",
                                line_width=1.0, row=1, col=1,
                                annotation_text=f" Inhibition {TURBULENCE_INHIBIT_THRESHOLD:.2f}",
                                annotation_font_color=COLOR_INHIBIT,
                                annotation_font_size=8)
                pv = df_g["pressure_bar"].tolist()
                fig_t.add_trace(go.Scatter(
                    x=xg, y=pv, mode="lines", name="P réelle",
                    line=dict(color=COLOR_ACCENT, width=1.2),
                ), row=2, col=1)
                dr2 = sr_l["drift"]
                if dr2["sufficient_data"] and xg and pv:
                    fig_t.add_trace(go.Scatter(
                        x=[xg[-1], "N+1"],
                        y=[pv[-1], dr2["p_projected_next"]],
                        mode="lines+markers", name="Proj.μ",
                        line=dict(color=COLOR_DRIFT, width=1.2, dash="dot"),
                        marker=dict(size=5, color=COLOR_DRIFT),
                    ), row=2, col=1)
                fig_t.update_layout(
                    paper_bgcolor=COLOR_PANEL, plot_bgcolor=COLOR_PANEL,
                    font=dict(family="Share Tech Mono, monospace",
                              color=COLOR_TEXT_MUT, size=10),
                    margin=dict(l=5, r=5, t=20, b=5), height=230,
                    showlegend=True,
                    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=9),
                                orientation="h", y=1.08),
                )
                fig_t.update_xaxes(gridcolor=COLOR_BORDER, showgrid=True,
                                   zeroline=False, tickfont=dict(size=8))
                fig_t.update_yaxes(gridcolor=COLOR_BORDER, showgrid=True,
                                   zeroline=False)
                st.plotly_chart(fig_t, use_container_width=True)

    except Exception as exc_stoch_chart:
        st.warning(f"Graphique stochastique temporairement indisponible : {exc_stoch_chart}")

    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  ANALYSE GBM DÉTAILLÉE + TABLEAU RÉCAPITULATIF OPÉRATEUR
    # ══════════════════════════════════════════════════════════════════════
    try:
        sr_gbm = st.session_state.get("stoch_results")
        if sr_gbm and sr_gbm["drift"]["sufficient_data"]:
            dr3 = sr_gbm["drift"]
            st.markdown(
                '<div class="section-header">'
                'Analyse GBM — Mouvement Brownien Géométrique'
                '</div>',
                unsafe_allow_html=True,
            )
            g1, g2, g3, g4, g5 = st.columns(5)
            for col_g, lbl_g, val_g, unit_g in [
                (g1, "μ log-normal",    f"{dr3['mu_log']:+.6f}",          "bar/pér."),
                (g2, "σ réalisée GBM",  f"{dr3['sigma_realised']:.6f}",   "bar·√n"),
                (g3, "P(N+1) projetée", f"{dr3['p_projected_next']:.3f}", PRESSURE_UNIT),
                (g4, "R² Régression",   f"{dr3['r_squared']:.4f}",        ""),
                (g5, "Échantillons",    f"{dr3['n_samples']}",             "pts"),
            ]:
                with col_g:
                    st.markdown(
                        f'<div class="metric-card" style="padding:12px 14px;">'
                        f'<div class="metric-label">{lbl_g}</div>'
                        f'<div class="metric-value" style="font-size:1.4rem;">'
                        f'{val_g}<span class="metric-unit">{unit_g}</span></div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

    except Exception as exc_gbm:
        st.warning(f"Analyse GBM temporairement indisponible : {exc_gbm}")

    # ── Tableau récapitulatif opérateur ────────────────────────────────────
    try:
        th_tbl = st.session_state.get("intervention_thresholds")
        if th_tbl and not th_tbl["inhibited"]:
            st.markdown(
                '<div class="metric-label" style="margin:14px 0 8px;">'
                'Tableau récapitulatif — Coordonnées d\'actionnement panneau vannes</div>',
                unsafe_allow_html=True,
            )
            proc_t    = th_tbl["procedure"]
            proc_css_t= {"IN":"proc-in","OUT":"proc-out","HOLD":"proc-hold"}.get(
                proc_t, "proc-hold")
            p_curr_s  = (f"{float(st.session_state['pressure_history_df']['pressure_bar'].iloc[-1]):.3f}"
                         if len(st.session_state["pressure_history_df"]) > 0 else "—")
            sr_t      = st.session_state.get("stoch_results")
            vol_s     = (f"{sr_t['turbulence_last']:.4f}"
                         if sr_t and not math.isnan(sr_t["turbulence_last"]) else "—")

            rows_html = ""
            for code_t, lbl_t, val_t, color_t, css_t, formula_t in [
                ("ZP",  "Zeroing Point — Entry",          th_tbl["zeroing_point"], COLOR_ZP,  "th-zp",
                 f"P ± (σ×{NDT_VOLATILITY_RATIO})"),
                ("NDT", "Décompression Nominale — T.P.",  th_tbl["ndt"],           COLOR_NDT, "th-ndt",
                 f"ZP ± (σ×{NDT_VOLATILITY_RATIO})"),
                ("ERV", "Soupape d'Urgence — Stop Loss",  th_tbl["erv"],           COLOR_ERV, "th-erv",
                 f"P ± (σ×{ERV_SAFETY_MULTIPLIER})"),
                ("HEP", "Équilibre Hydrostatique — B.E.", th_tbl["hep"],           COLOR_HEP, "th-hep",
                 f"ZP ± {HEP_FRICTION_OFFSET_BAR} bar"),
            ]:
                rows_html += (
                    f'<tr>'
                    f'<td><span class="{css_t}">[{code_t}]</span></td>'
                    f'<td style="color:{COLOR_TEXT_MUT};">{lbl_t}</td>'
                    f'<td style="color:{color_t};font-weight:bold;">'
                    f'{val_t:.4f} bar</td>'
                    f'<td style="color:{COLOR_TEXT_MUT};">{formula_t}</td>'
                    f'<td><span class="action-proc-badge {proc_css_t}" '
                    f'style="font-size:0.62rem;">{proc_t}</span></td>'
                    f'</tr>'
                )

            st.markdown(
                f'<table class="threshold-table">'
                f'<thead><tr>'
                f'<th>Code</th><th>Mécanisme</th>'
                f'<th>Valeur absolue</th><th>Formule</th><th>Proc.</th>'
                f'</tr></thead><tbody>'
                f'{rows_html}'
                f'<tr style="border-top:2px solid {COLOR_BORDER};">'
                f'<td colspan="2" style="color:{COLOR_TEXT_MUT};">'
                f'P courante / σ_turb / Seuil inhibition</td>'
                f'<td style="font-weight:bold;">{p_curr_s} bar</td>'
                f'<td style="color:{COLOR_TEXT_MUT};">{vol_s} bar</td>'
                f'<td style="color:{COLOR_TEXT_MUT};">'
                f'⚠ {TURBULENCE_INHIBIT_THRESHOLD:.2f} bar</td>'
                f'</tr>'
                f'</tbody></table>',
                unsafe_allow_html=True,
            )
        elif th_tbl and th_tbl["inhibited"]:
            st.error(
                f"🛑 **{_INHIBIT_STATE_MSG}**\n\n"
                f"{th_tbl['inhibit_reason']}\n\n"
                f"Inhibitions déclenchées cette session : "
                f"**{st.session_state['inhibit_count']}**"
            )

    except Exception as exc_tbl:
        st.warning(f"Tableau d'actionnement temporairement indisponible : {exc_tbl}")

    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  TABLEAU HISTORIQUE BRUT ENRICHI (20 dernières mesures)
    # ══════════════════════════════════════════════════════════════════════
    try:
        df_raw = st.session_state["pressure_history_df"]
        if len(df_raw) > 0:
            st.markdown(
                '<div class="metric-label" style="margin-bottom:6px;">'
                'Mesures brutes enrichies — 20 derniers enregistrements</div>',
                unsafe_allow_html=True,
            )
            sr_raw   = st.session_state.get("stoch_results")
            disp     = df_raw.tail(20).copy()

            if sr_raw and len(sr_raw["pct_k"]) == len(df_raw):
                disp["σ Turb."] = sr_raw["turbulence_series"].iloc[-len(disp):].values
                disp["%K"]      = sr_raw["pct_k"].iloc[-len(disp):].values
                disp["%D"]      = sr_raw["pct_d"].iloc[-len(disp):].values

            disp["timestamp"] = disp["timestamp"].dt.strftime("%H:%M:%S")
            rename_map = {
                "timestamp":      "Horodatage",
                "pressure_bar":   f"P ({PRESSURE_UNIT})",
                "status":         "Statut",
                "delta_bar":      "Δ bar",
                "rolling_avg_5":  "Moy.5",
                "api_latency_ms": "Lat.ms",
            }
            disp = disp.rename(columns=rename_map)
            disp = disp.drop(columns=[c for c in ["sensor_id","rolling_std_5"]
                                      if c in disp.columns])

            def _style_s(v):
                return (f"color:{'#00E5A0' if v=='nominal' else '#FFB830' if v=='warning' else '#FF3A3A'};"
                        f"font-weight:bold;")

            fmt = {f"P ({PRESSURE_UNIT})":"{:.3f}", "Δ bar":"{:+.3f}",
                   "Moy.5":"{:.3f}", "Lat.ms":"{:.1f}"}
            if "σ Turb." in disp.columns:
                fmt.update({"σ Turb.":"{:.4f}", "%K":"{:.1f}", "%D":"{:.1f}"})

            styled = (disp.style
                      .applymap(_style_s, subset=["Statut"])
                      .format(fmt, na_rep="—")
                      .set_properties(**{
                          "background-color": COLOR_PANEL,
                          "color": COLOR_TEXT_PRI,
                          "border-color": COLOR_BORDER,
                          "font-size": "0.74rem",
                          "font-family": "'Share Tech Mono', monospace",
                      }))
            st.dataframe(styled, use_container_width=True, height=310)

    except Exception as exc_raw:
        st.warning(f"Tableau brut temporairement indisponible : {exc_raw}")


# ──────────────────────────────────────────────────────────────────────────────
#  §11  PANNEAU DE CONTRÔLE MANUEL (hors fragment — déclenché par l'utilisateur)
# ──────────────────────────────────────────────────────────────────────────────

def _render_manual_control_panel() -> None:
    """
    Panneau de contrôle manuel — rendu hors du fragment.
    Permet à l'ingénieur de déclencher une acquisition unique ou d'activer
    le monitoring continu indépendamment du cycle @st.fragment.
    """
    st.markdown(
        '<div class="section-header">Contrôle Manuel — Acquisition Hors Cycle Fragment</div>',
        unsafe_allow_html=True,
    )

    ca, cb, cc = st.columns([1, 1, 2])

    with ca:
        if st.button("▶  ACQUISITION UNIQUE", key="btn_manual_single"):
            with st.spinner("Interrogation API XA-UUSD…"):
                try:
                    result = run_full_pipeline()
                    if result is None:
                        st.warning("Acquisition échouée — voir le journal.")
                except Exception as exc_manual:
                    st.error(f"Exception acquisition : {exc_manual}")
            st.rerun()

    with cb:
        monitoring = st.session_state["monitoring_active"]
        lbl_mon    = "⏹  STOP MONITORING" if monitoring else "⏵  MONITORING CONTINU"
        if st.button(lbl_mon, key="btn_monitoring"):
            st.session_state["monitoring_active"] = not monitoring
            st.rerun()

    with cc:
        if st.session_state["monitoring_active"]:
            st.markdown(
                f'<div class="status-badge status-nominal" style="margin-top:6px;">'
                f'● MONITORING ACTIF — Intervalle {POLL_INTERVAL_SECONDS}s '
                f'(+ fragment {FRAGMENT_INTERVAL})'
                f'</div>',
                unsafe_allow_html=True,
            )

    # Boucle monitoring continu (synchrone — déclenche des reruns)
    if st.session_state["monitoring_active"]:
        try:
            run_full_pipeline()
        except Exception as exc_mon:
            _append_event_log("err", f"Monitoring : {exc_mon}")
        time.sleep(POLL_INTERVAL_SECONDS)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
#  §12  POINT D'ENTRÉE PRINCIPAL — main()
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Fonction principale — Version FINALE DE DÉPLOIEMENT v4.0.0.

    Architecture d'exécution :
    ┌─────────────────────────────────────────────────────────────┐
    │  RENDU STATIQUE (1 seule fois au chargement / rerun complet) │
    │  • set_page_config                                           │
    │  • CSS injection                                             │
    │  • initialize_session_state                                  │
    │  • _render_static_header                                     │
    │  • _render_manual_control_panel                              │
    │  • _render_static_sidebar                                    │
    ├─────────────────────────────────────────────────────────────┤
    │  FRAGMENT LIVE @st.fragment(run_every="5s")                  │
    │  • live_control_dashboard()  ← toutes les 5 secondes        │
    │    ├─ Acquisition API (try/except réseau)                    │
    │    ├─ Calcul stochastique (try/except)                       │
    │    ├─ Calcul seuils d'intervention (try/except)              │
    │    ├─ Panneau ACTION / PAUSE / HOLD                          │
    │    ├─ 5 st.metric : P | ZP(Entry) | NDT(TP) | ERV(SL) | HEP(BE) │
    │    ├─ Graphique pression + niveaux ZP/NDT/ERV/HEP            │
    │    ├─ KPI stochastique + graphiques %K/%D / σ / drift        │
    │    ├─ Analyse GBM (μ, σ, R², P(N+1))                        │
    │    └─ Tableau récapitulatif opérateur + historique brut      │
    └─────────────────────────────────────────────────────────────┘
    """
    # ── Configuration de la page ──────────────────────────────────────────
    st.set_page_config(
        page_title = "Système de Contrôle Hydrodynamique XA-UUSD",
        page_icon  = "⬡",
        layout     = "wide",
        initial_sidebar_state = "expanded",
        menu_items = {
            "Get Help":     None,
            "Report a bug": None,
            "About": (
                "**XA-UUSD Hydrodynamic Control Dashboard** — v4.0.0 FINAL\n\n"
                "Télémétrie asynchrone · Stochastique GBM/Kolmogorov · "
                "Seuils d'intervention ZP/NDT/ERV/HEP · "
                "Interface fragmentée @st.fragment (5 s)\n\n"
                "Usage strictement industriel."
            ),
        },
    )

    # ── Injection CSS ─────────────────────────────────────────────────────
    st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)

    # ── Initialisation session state ──────────────────────────────────────
    initialize_session_state()

    # ── Header statique ───────────────────────────────────────────────────
    _render_static_header()
    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ── Panneau de contrôle manuel (hors fragment) ─────────────────────────
    _render_manual_control_panel()
    st.markdown('<hr class="xa-divider">', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════
    #  LIVE CONTROL DASHBOARD — Fragment auto-refresh toutes les 5 secondes
    # ══════════════════════════════════════════════════════════════════════
    live_control_dashboard()

    # ── Sidebar statique ──────────────────────────────────────────────────
    _render_static_sidebar()


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
