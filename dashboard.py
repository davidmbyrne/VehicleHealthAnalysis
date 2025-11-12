
#!/usr/bin/env python3
"""
Vehicle Health & Risk Monitoring Dashboard

Streamlit interface for monitoring vehicle health metrics, risk scores,
and stress indicators derived from aggregated ULog telemetry.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional
from textwrap import dedent as _dedent

import pandas as pd
import plotly.express as px
import streamlit as st

# Ensure project root is on sys.path when running as a script
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from reports.risk_analysis import calculate_risk_score

st.set_page_config(
    page_title="Vehicle Health Command Center",
    layout="wide",
    initial_sidebar_state="expanded",
)

THEME = {
    "background": "#050b18",
    "surface": "#0f172a",
    "surface_alt": "#111f3b",
    "border": "#1f2a48",
    "accent": "#2de1c2",
    "accent_soft": "rgba(45, 225, 194, 0.18)",
    "danger": "#ff6b6b",
    "danger_soft": "rgba(255, 107, 107, 0.22)",
    "grid": "#1f2a48",
    "text_primary": "#f8fafc",
    "text_secondary": "#94a3b8",
}

PLOTLY_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": [
        "pan2d",
        "lasso2d",
        "autoScale2d",
        "zoomIn2d",
        "zoomOut2d",
    ],
}


def inject_theme() -> None:
    """Inject custom CSS for a sharper, tech-focused presentation."""
    style = _dedent(f"""
        <style>
            :root {{
                --background: {THEME['background']};
                --surface: {THEME['surface']};
                --surface-alt: {THEME['surface_alt']};
                --border: {THEME['border']};
                --accent: {THEME['accent']};
                --accent-soft: {THEME['accent_soft']};
                --danger: {THEME['danger']};
                --danger-soft: {THEME['danger_soft']};
                --grid: {THEME['grid']};
                --text-primary: {THEME['text_primary']};
                --text-secondary: {THEME['text_secondary']};
            }}

            .stApp {{
                background:
                    radial-gradient(circle at 15% 20%, rgba(37, 99, 235, 0.18), transparent 45%),
                    radial-gradient(circle at 85% 5%, rgba(45, 225, 194, 0.22), transparent 38%),
                    var(--background);
                color: var(--text-primary);
            }}

            .block-container {{
                padding: 2.5rem 3rem 3rem 3rem;
            }}

            .dashboard-title {{
                font-family: 'Inter', system-ui, sans-serif;
                font-size: 2.35rem;
                font-weight: 600;
                letter-spacing: 0.015em;
                margin-bottom: 0.4rem;
                color: var(--text-primary);
            }}

            .dashboard-subtitle {{
                color: var(--text-secondary);
                font-size: 0.98rem;
                margin-bottom: 2.2rem;
            }}

            .metric-card {{
                background: linear-gradient(135deg, rgba(15, 23, 42, 0.9), rgba(17, 31, 59, 0.92));
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 1.2rem 1.4rem;
                box-shadow: 0 26px 40px rgba(5, 11, 24, 0.45);
                position: relative;
                overflow: hidden;
                min-height: 132px;
            }}

            .metric-card::after {{
                content: "";
                position: absolute;
                inset: 0;
                background: linear-gradient(120deg, rgba(45, 225, 194, 0.24), transparent 55%);
                opacity: 0.55;
            }}

            .metric-card > * {{
                position: relative;
                z-index: 1;
            }}

            .metric-label {{
                font-size: 0.78rem;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                color: var(--text-secondary);
                margin-bottom: 0.55rem;
            }}

            .metric-value {{
                font-size: 1.95rem;
                font-weight: 600;
                color: var(--text-primary);
            }}

            .metric-caption {{
                margin-top: 0.35rem;
                font-size: 0.85rem;
                color: var(--text-secondary);
            }}

            .section-header {{
                display: flex;
                align-items: center;
                gap: 0.75rem;
                margin-top: 2.4rem;
                margin-bottom: 1.1rem;
            }}

            .section-icon {{
                width: 38px;
                height: 38px;
                border-radius: 12px;
                background: var(--accent-soft);
                display: grid;
                place-items: center;
                font-size: 1.15rem;
                color: var(--accent);
            }}

            .section-header h2 {{
                margin: 0;
                font-size: 1.38rem;
                font-weight: 600;
            }}

            .section-subtitle {{
                margin: 0.15rem 0 0 0;
                color: var(--text-secondary);
                font-size: 0.86rem;
            }}

            .tech-table {{
                border-collapse: collapse;
                width: 100%;
                background: rgba(15, 23, 42, 0.92);
                border-radius: 18px;
                overflow: hidden;
                border: 1px solid var(--border);
            }}

            .tech-table thead th {{
                background: rgba(17, 31, 59, 0.98);
                color: var(--text-primary);
                padding: 0.75rem 0.95rem;
                font-size: 0.82rem;
                letter-spacing: 0.06em;
                text-transform: uppercase;
            }}

            .tech-table tbody td {{
                padding: 0.72rem 0.95rem;
                color: var(--text-secondary);
                border-bottom: 1px solid rgba(31, 42, 72, 0.65);
                font-size: 0.92rem;
            }}

            .tech-table tbody tr:nth-child(even) td {{
                background: rgba(17, 31, 59, 0.8);
            }}

            .tech-table tbody tr:hover td {{
                background: rgba(45, 225, 194, 0.12);
                color: var(--text-primary);
            }}

            .status-pill {{
                display: inline-flex;
                align-items: center;
                gap: 0.35rem;
                font-size: 0.78rem;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                border-radius: 999px;
                padding: 0.35rem 0.85rem;
                border: 1px solid var(--border);
                background: rgba(15, 23, 42, 0.75);
            }}

            .status-pill--dead {{
                background: var(--danger-soft);
                color: var(--danger);
                border-color: rgba(255, 107, 107, 0.55);
            }}

            .status-pill--ok {{
                color: var(--accent);
                border-color: rgba(45, 225, 194, 0.4);
            }}

            .detail-card {{
                background: linear-gradient(135deg, rgba(15, 23, 42, 0.95), rgba(15, 23, 42, 0.75));
                border: 1px solid var(--border);
                border-radius: 18px;
                padding: 1.4rem 1.6rem;
                margin-bottom: 1.4rem;
            }}

            .detail-header {{
                display: flex;
                align-items: flex-start;
                justify-content: space-between;
                gap: 1rem;
            }}

            .detail-title {{
                font-size: 1.35rem;
                font-weight: 600;
            }}

            .detail-subtitle {{
                color: var(--text-secondary);
                font-size: 0.88rem;
                margin-top: 0.35rem;
            }}

            .stSidebar [data-testid="stSidebarContent"] {{
                background: rgba(15, 23, 42, 0.92);
            }}

            .stSidebar {{
                border-right: 1px solid var(--border);
            }}

            .stSidebar h2, .stSidebar h3 {{
                color: var(--text-primary);
            }}
        </style>
    """)
    st.markdown(style, unsafe_allow_html=True)


def apply_plotly_theme(fig: px.Figure) -> px.Figure:
    """Apply a consistent dark technical theme to Plotly figures."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=THEME["surface"],
        plot_bgcolor=THEME["surface_alt"],
        font=dict(color=THEME["text_primary"], family="Inter, sans-serif"),
        margin=dict(t=60, r=40, b=60, l=60),
        hoverlabel=dict(
            bgcolor=THEME["surface_alt"],
            bordercolor=THEME["accent"],
            font=dict(color=THEME["text_primary"], size=12),
        ),
    )
    fig.update_xaxes(gridcolor=THEME["grid"], zerolinecolor=THEME["grid"], showgrid=True)
    fig.update_yaxes(gridcolor=THEME["grid"], zerolinecolor=THEME["grid"], showgrid=True)
    return fig


def render_section_header(icon: Optional[str], title: str, subtitle: str = "") -> None:
    """Render a consistent section header, optionally showing an icon."""
    parts = ['<div class="section-header">']
    if icon:
        parts.append(f'    <div class="section-icon">{icon}</div>')
    parts.append("    <div>")
    parts.append(f"        <h2>{title}</h2>")
    if subtitle:
        parts.append(f'        <p class="section-subtitle">{subtitle}</p>')
    parts.append("    </div>")
    parts.append("</div>")
    st.markdown("\n".join(parts), unsafe_allow_html=True)


def render_metric_card(label: str, value: str, caption: str = "") -> None:
    caption_block = f'<div class="metric-caption">{caption}</div>' if caption else ""
    html = _dedent(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            {caption_block}
        </div>
        """
    ).strip()
    st.markdown(html, unsafe_allow_html=True)


def render_summary_metrics(filtered_df: pd.DataFrame) -> None:
    """Render high-level fleet summary metrics."""
    total_vehicles = len(filtered_df)
    dead_count = int(filtered_df["is_dead"].sum())
    avg_risk = filtered_df["risk_score"].mean()
    total_minutes = filtered_df["total_flight_time_min"].sum()
    exposure_hours = total_minutes / 60.0 if total_minutes else 0.0
    peaks_per_hour = (
        filtered_df["peak_events"].sum() / exposure_hours if exposure_hours > 0 else 0.0
    )
    clipping_per_hour = (
        filtered_df["clipping_events"].sum() / exposure_hours if exposure_hours > 0 else 0.0
    )
    ninety_fifth = filtered_df["risk_score"].quantile(0.95)
    top_vehicle = filtered_df.iloc[0]

    primary_metrics = [
        ("Fleet Observed", f"{total_vehicles}", "Vehicles matching current filters"),
        ("Dead Flagged", f"{dead_count}", "Status from isDead.csv"),
        ("Average Risk", f"{avg_risk:.2f}", "Mean composite score"),
        ("Flight Exposure", f"{total_minutes:,.0f} min", "Total analyzed flight time"),
    ]

    columns = st.columns(len(primary_metrics))
    for col, metric in zip(columns, primary_metrics):
        with col:
            render_metric_card(*metric)

    secondary_metrics = [
        ("Critical Vehicle", str(top_vehicle["vehicle_id"]), f"Score {top_vehicle['risk_score']:.2f}"),
        ("95th Percentile", f"{ninety_fifth:.2f}", "Risk tail threshold"),
        ("Peak Samples/hr", f"{peaks_per_hour:.0f}", "High acceleration density"),
        ("Clipping/hr", f"{clipping_per_hour:.0f}", "Sensor saturation rate"),
    ]

    columns = st.columns(len(secondary_metrics))
    for col, metric in zip(columns, secondary_metrics):
        with col:
            render_metric_card(*metric)


def load_csv_from_s3_or_local(csv_path: str, s3_bucket: Optional[str] = None, s3_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Load CSV file from local path or S3 if local file doesn't exist.
    
    Args:
        csv_path: Local file path
        s3_bucket: Optional S3 bucket name (from env var or parameter)
        s3_key: Optional S3 key/path (defaults to csv_path if bucket provided)
    
    Returns:
        DataFrame if file exists (locally or in S3), None otherwise
    """
    local_path = Path(csv_path)
    
    # Try local file first
    if local_path.exists():
        return pd.read_csv(local_path)
    
    # Try S3 if bucket is configured
    s3_bucket = s3_bucket or os.getenv("S3_DATA_BUCKET")
    if s3_bucket:
        try:
            import boto3
            s3_key = s3_key or csv_path
            s3 = boto3.client("s3")
            
            # Download to temp location
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False) as tmp:
                tmp_path = Path(tmp.name)
            
            s3.download_file(s3_bucket, s3_key, str(tmp_path))
            df = pd.read_csv(tmp_path)
            tmp_path.unlink()  # Clean up temp file
            return df
        except Exception as e:
            st.warning(f"Failed to load from S3 (s3://{s3_bucket}/{s3_key}): {e}")
    
    return None


def load_dead_vehicles() -> set[str]:
    """Load dead vehicle IDs from config/isDead.csv if present."""
    dead_csv = Path("config/isDead.csv")
    if not dead_csv.exists():
        return set()

    try:
        df = pd.read_csv(dead_csv)
        return set(df[df.get("dead", 0) == 1]["vehicle_id"].str.upper())
    except Exception:
        return set()


def load_data(aggregated_csv: Path | pd.DataFrame) -> pd.DataFrame:
    """Load aggregated telemetry and compute risk breakdown per vehicle.
    
    Args:
        aggregated_csv: Path to CSV file or DataFrame
    """
    if isinstance(aggregated_csv, pd.DataFrame):
        raw_df = aggregated_csv
    elif isinstance(aggregated_csv, Path):
        if not aggregated_csv.exists():
            return pd.DataFrame()
        raw_df = pd.read_csv(aggregated_csv)
    else:
        return pd.DataFrame()

    risk_rows: list[dict[str, float]] = []
    for _, row in raw_df.iterrows():
        score, breakdown = calculate_risk_score(row)
        risk_rows.append(
            {
                "vehicle_id": row.get("vehicle_id", "unknown"),
                "risk_score": score,
                "vibration_score": breakdown["vibration_score"],
                "motor_score": breakdown["motor_score"],
                "fatigue_score": breakdown.get("fatigue_score", 0.0),
                "vibration_high_pct": breakdown["vibration_high_pct"],
                "motor_saturation_pct": breakdown["motor_saturation_pct"],
                "peak_events": float(row.get("peak_accel_events", 0.0) or 0.0),
                "clipping_events": float(row.get("accel_clipping_events", 0.0) or 0.0),
                "total_flight_time_min": float(row.get("accel_total_time_s", 0.0) or 0.0) / 60.0,
                "num_logs": int(row.get("num_logs", 0) or 0),
            }
        )

    df = pd.DataFrame(risk_rows)
    if df.empty:
        return df

    return df.sort_values("risk_score", ascending=False).reset_index(drop=True)


def render_risk_table(filtered_df: pd.DataFrame) -> None:
    """Render a stylised risk table with dead vehicle highlighting and sortable columns."""
    display_df = filtered_df[
        [
            "rank",
            "vehicle_id",
            "risk_score",
            "vibration_score",
            "motor_score",
            "fatigue_score",
            "vibration_high_pct",
            "motor_saturation_pct",
            "peak_events",
            "clipping_events",
            "total_flight_time_min",
            "num_logs",
            "is_dead",
        ]
    ].copy()

    display_df.rename(
        columns={
            "rank": "Rank",
            "vehicle_id": "Vehicle",
            "risk_score": "Risk Score",
            "vibration_score": "Vib Score",
            "motor_score": "Motor Score",
            "fatigue_score": "Fatigue Score",
            "vibration_high_pct": "High Vib %",
            "motor_saturation_pct": "Sat %",
            "peak_events": "Peak Samples",
            "clipping_events": "Clipping Samples",
            "total_flight_time_min": "Flight Time (min)",
            "num_logs": "Logs",
        },
        inplace=True,
    )

    display_df["Status"] = display_df.pop("is_dead").map({True: "DEAD", False: "ACTIVE"})

    # Format numeric columns
    display_df["Risk Score"] = display_df["Risk Score"].round(2)
    display_df["Vib Score"] = display_df["Vib Score"].round(2)
    display_df["Motor Score"] = display_df["Motor Score"].round(2)
    display_df["Fatigue Score"] = display_df["Fatigue Score"].round(2)
    display_df["High Vib %"] = display_df["High Vib %"].round(1)
    display_df["Sat %"] = display_df["Sat %"].round(1)
    display_df["Flight Time (min)"] = display_df["Flight Time (min)"].round(1)

    # Configure column formatting and styling
    column_config = {
        "Rank": st.column_config.NumberColumn("Rank", format="%d"),
        "Vehicle": st.column_config.TextColumn("Vehicle"),
        "Risk Score": st.column_config.NumberColumn("Risk Score", format="%.2f"),
        "Vib Score": st.column_config.NumberColumn("Vib Score", format="%.2f"),
        "Motor Score": st.column_config.NumberColumn("Motor Score", format="%.2f"),
        "Fatigue Score": st.column_config.NumberColumn("Fatigue Score", format="%.2f"),
        "High Vib %": st.column_config.NumberColumn("High Vib %", format="%.1f%%"),
        "Sat %": st.column_config.NumberColumn("Sat %", format="%.1f%%"),
        "Peak Samples": st.column_config.NumberColumn("Peak Samples", format="%d"),
        "Clipping Samples": st.column_config.NumberColumn("Clipping Samples", format="%d"),
        "Flight Time (min)": st.column_config.NumberColumn("Flight Time (min)", format="%.1f"),
        "Logs": st.column_config.NumberColumn("Logs", format="%d"),
        "Status": st.column_config.TextColumn("Status"),
    }

    # Apply row styling for DEAD vehicles
    def highlight_dead(row: pd.Series) -> list[str]:
        if row.get("Status") == "DEAD":
            return [
                f"background-color: {THEME['danger_soft']}; color: {THEME['text_primary']};"
            ] * len(row)
        return [""] * len(row)

    styled_df = display_df.style.apply(highlight_dead, axis=1)

    st.dataframe(
        styled_df,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
    )


def render_vehicle_details(vehicle_data: pd.Series) -> None:
    """Render a focused profile for the selected vehicle."""
    status_label = "DEAD" if vehicle_data["is_dead"] else "ACTIVE"
    status_class = "status-pill--dead" if vehicle_data["is_dead"] else "status-pill--ok"

    header_html = _dedent(
        f"""
        <div class="detail-card">
            <div class="detail-header">
                <div>
                    <div class="detail-title">{vehicle_data['vehicle_id']}</div>
                    <div class="detail-subtitle">Composite risk snapshot across vibration, motor, and fatigue vectors.</div>
                </div>
                <span class="status-pill {status_class}">{status_label}</span>
            </div>
        </div>
        """
    ).strip()
    st.markdown(header_html, unsafe_allow_html=True)

    primary = [
        ("Risk Score", f"{vehicle_data['risk_score']:.2f}", "Composite index"),
        ("Vibration Score", f"{vehicle_data['vibration_score']:.2f}", "High-band exposure"),
        ("Motor Score", f"{vehicle_data['motor_score']:.2f}", "Output saturation load"),
        ("Fatigue Score", f"{vehicle_data['fatigue_score']:.2f}", "Clipping & peak stress"),
    ]
    columns = st.columns(len(primary))
    for col, metric in zip(columns, primary):
        with col:
            render_metric_card(*metric)

    secondary = [
        ("High Vib %", f"{vehicle_data['vibration_high_pct']:.1f}%", "Time >70 m/s²"),
        ("Saturation %", f"{vehicle_data['motor_saturation_pct']:.1f}%", "Motors at 1.0"),
        ("Peak Samples", f"{int(vehicle_data['peak_events'])}", "Samples >100 m/s²"),
        ("Clipping Samples", f"{int(vehicle_data['clipping_events'])}", "Sensor saturation"),
    ]
    columns = st.columns(len(secondary))
    for col, metric in zip(columns, secondary):
        with col:
            render_metric_card(*metric)

    tertiary = [
        ("Flight Time", f"{vehicle_data['total_flight_time_min']:.1f} min", "Analyzed duration"),
        ("Logs Processed", f"{vehicle_data['num_logs']}", "ULogs contributing"),
        ("Rank Position", f"#{int(vehicle_data['rank'])}", "Within current filter"),
    ]
    columns = st.columns(len(tertiary))
    for col, metric in zip(columns, tertiary):
        with col:
            render_metric_card(*metric)


def main() -> None:
    inject_theme()

    st.markdown("<div class='dashboard-title'>Vehicle Health Command Center</div>", unsafe_allow_html=True)
    st.markdown(
        "<p class='dashboard-subtitle'>Live telemetry-derived risk scoring across the fleet. Adjust filters on the left to reshape the operational view.</p>",
        unsafe_allow_html=True,
    )

    st.sidebar.header("Data Source")
    csv_path = st.sidebar.text_input(
        "Aggregated CSV Path",
        value="output/aggregated_by_vehicle.csv",
        help="Path to aggregated_by_vehicle.csv generated by the pipeline",
    )

    # Try to load CSV (local or S3)
    raw_df = load_csv_from_s3_or_local(csv_path)
    if raw_df is None:
        st.error(f"File not found: {csv_path}")
        s3_bucket = os.getenv("S3_DATA_BUCKET")
        if s3_bucket:
            st.info(f"Also checked S3 bucket: {s3_bucket}")
        st.info(
            "Run the pipeline to generate aggregated metrics:\n"
            "```bash\n"
            "python3 parallel_streaming_pipeline.py --bucket rm-prophet --prefix ulogs/\n"
            "```\n\n"
            "Or set S3_DATA_BUCKET environment variable to load from S3."
        )
        return

    # Process the loaded data (calculates risk scores)
    df = load_data(raw_df)
    dead_vehicles = load_dead_vehicles()

    if df.empty:
        st.warning("No vehicle data available. Re-run the pipeline to refresh aggregates.")
        return

    df["is_dead"] = df["vehicle_id"].str.upper().isin(dead_vehicles)

    st.sidebar.header("Filters")
    vehicle_options = sorted(df["vehicle_id"].unique())

    if "selected_vehicles" not in st.session_state:
        st.session_state.selected_vehicles = list(vehicle_options)

    select_mode = st.sidebar.radio(
        "Vehicle selection",
        options=("All vehicles", "Custom selection"),
        index=0 if len(st.session_state.selected_vehicles) == len(vehicle_options) else 1,
    )

    if select_mode == "All vehicles":
        st.session_state.selected_vehicles = list(vehicle_options)
        st.sidebar.multiselect(
            "Vehicles",
            options=vehicle_options,
            default=vehicle_options,
            key="selected_vehicles",
            disabled=True,
        )
    else:
        st.sidebar.multiselect(
            "Vehicles",
            options=vehicle_options,
            default=st.session_state.selected_vehicles,
            key="selected_vehicles",
        )

    selected_vehicles = list(st.session_state.selected_vehicles)
    show_dead_only = st.sidebar.checkbox("Show DEAD only", value=False)

    filtered_df = df[df["vehicle_id"].isin(selected_vehicles)].copy()
    if show_dead_only:
        filtered_df = filtered_df[filtered_df["is_dead"]]

    if filtered_df.empty:
        st.warning("No vehicles matched the selected filters.")
        return

    filtered_df = filtered_df.sort_values("risk_score", ascending=False).reset_index(drop=True)
    filtered_df["rank"] = filtered_df.index + 1

    render_summary_metrics(filtered_df)

    render_section_header(None, "Risk Score Rankings", "Top vehicles by composite telemetry risk score.")

    left_col, right_col = st.columns([2.3, 1.2])

    with left_col:
        fig = px.bar(
            filtered_df.head(20),
            x="risk_score",
            y="vehicle_id",
            orientation="h",
            color="is_dead",
            color_discrete_map={True: THEME["danger"], False: THEME["accent"]},
            labels={"risk_score": "Risk Score", "vehicle_id": "Vehicle", "is_dead": "Status"},
            title="Top 20 Risk Rankings",
        )
        fig.update_traces(
            marker=dict(line=dict(color=THEME["border"], width=1.2), opacity=0.9),
            hovertemplate="Vehicle %{y}<br>Score %{x:.2f}<extra></extra>",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=620, showlegend=False)
        st.plotly_chart(apply_plotly_theme(fig), use_container_width=True, config=PLOTLY_CONFIG)

    with right_col:
        risk_components = ["vibration_score", "motor_score", "fatigue_score"]
        avg_scores = filtered_df[risk_components].mean()
        fig = px.pie(
            values=avg_scores.values,
            names=["Vibration", "Motor", "Fatigue"],
            title="Average Risk Composition",
            hole=0.52,
            color=avg_scores.values,
            color_discrete_sequence=[THEME["accent"], "#3ba7f8", "#9f75ff"],
        )
        fig.update_traces(
            textposition="inside",
            textinfo="label+percent",
            pull=[0.04 if name == "Fatigue" else 0 for name in ["Vibration", "Motor", "Fatigue"]],
        )
        st.plotly_chart(apply_plotly_theme(fig), use_container_width=True, config=PLOTLY_CONFIG)

    render_section_header(None, "Risk Score Details", "Tabular snapshot with telemetry-derived stress indicators.")
    render_risk_table(filtered_df)

    render_section_header(
        None,
        "Metric Correlations",
        "Explore how vibration, motor output, and fatigue metrics interact across the fleet.",
    )

    scatter_col1, scatter_col2 = st.columns(2)
    with scatter_col1:
        fig = px.scatter(
            filtered_df,
            x="vibration_score",
            y="motor_score",
            size="fatigue_score",
            size_max=32,
            color="is_dead",
            color_discrete_map={True: THEME["danger"], False: THEME["accent"]},
            hover_data=["vehicle_id", "risk_score"],
            labels={
                "vibration_score": "Vibration Score",
                "motor_score": "Motor Score",
                "fatigue_score": "Fatigue Score",
                "is_dead": "Status",
            },
            title="Vibration vs Motor Stress",
        )
        fig.update_traces(marker=dict(line=dict(color=THEME["border"], width=0.8), opacity=0.85))
        st.plotly_chart(apply_plotly_theme(fig), use_container_width=True, config=PLOTLY_CONFIG)

    with scatter_col2:
        fig = px.scatter(
            filtered_df,
            x="peak_events",
            y="clipping_events",
            size="risk_score",
            size_max=32,
            color="is_dead",
            color_discrete_map={True: THEME["danger"], False: THEME["accent"]},
            hover_data=["vehicle_id", "risk_score"],
            labels={
                "peak_events": "Peak Acceleration Samples",
                "clipping_events": "Clipping Samples",
                "risk_score": "Risk Score",
            },
            title="Peak vs Clipping Density",
        )
        fig.update_traces(marker=dict(line=dict(color=THEME["border"], width=0.8), opacity=0.85))
        st.plotly_chart(apply_plotly_theme(fig), use_container_width=True, config=PLOTLY_CONFIG)

    render_section_header(None, "Vehicle Deep-Dive", "Inspect an individual airframe's composite metrics.")

    selected_vehicle = st.selectbox(
        "Vehicle",
        options=filtered_df["vehicle_id"],
        index=0,
    )

    vehicle_data = filtered_df[filtered_df["vehicle_id"] == selected_vehicle].iloc[0]
    render_vehicle_details(vehicle_data)


if __name__ == "__main__":
    main()
