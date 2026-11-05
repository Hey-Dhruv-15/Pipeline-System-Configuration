from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from src.pipeline.config import CONFIG
from src.pipeline.quality import EXPECTED_COLUMNS
from src.pipeline.repository import load_pipeline_snapshot

UI_CONFIG_PATH = Path(__file__).with_name("ui_config.json")


def load_ui_config() -> dict[str, Any]:
    with UI_CONFIG_PATH.open(encoding="utf-8") as config_file:
        return json.load(config_file)


UI_CONFIG = load_ui_config()
PAGE_CONFIG = UI_CONFIG["page"]
THEME = UI_CONFIG["theme"]
REFRESH = UI_CONFIG["refresh"]
DATASET_CONFIG = UI_CONFIG["datasets"]

st.set_page_config(
    page_title=PAGE_CONFIG["title"],
    page_icon=PAGE_CONFIG["icon"],
    layout=PAGE_CONFIG["layout"],
    initial_sidebar_state=PAGE_CONFIG["sidebar_state"],
)


def inject_styles(theme: dict[str, str]) -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap');

        :root {{
            --bg: {theme["background"]};
            --bg-end: {theme["background_end"]};
            --ink: {theme["ink"]};
            --muted: {theme["muted"]};
            --accent: {theme["accent"]};
            --success: {theme["success"]};
            --warning: {theme["warning"]};
            --info: {theme["info"]};
            --panel: {theme["panel"]};
            --font: {theme["font_family"]};
            --mono: {theme["mono_family"]};
        }}

        .stApp {{
            background:
                radial-gradient(circle at 10% 18%, rgba(242, 95, 76, 0.18), transparent 24%),
                radial-gradient(circle at 85% 8%, rgba(31, 138, 112, 0.18), transparent 22%),
                linear-gradient(180deg, var(--bg) 0%, var(--bg-end) 100%);
            color: var(--ink);
            font-family: var(--font);
        }}

        .block-container {{
            max-width: 1340px;
            padding-top: 1.6rem;
            padding-bottom: 2rem;
        }}

        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {theme["sidebar_start"]} 0%, {theme["sidebar_end"]} 100%);
        }}

        [data-testid="stSidebar"] * {{
            color: #f8f4ea;
        }}

        .hero-card {{
            background: linear-gradient(135deg, {theme["hero_start"]}, {theme["hero_end"]});
            border-radius: 30px;
            padding: 2rem;
            color: #f8f4ea;
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 24px 64px rgba(16, 35, 38, 0.18);
            position: relative;
            overflow: hidden;
            min-height: 240px;
        }}

        .hero-card::after {{
            content: "";
            position: absolute;
            width: 300px;
            height: 300px;
            right: -90px;
            bottom: -130px;
            background: radial-gradient(circle, rgba(242, 95, 76, 0.28), transparent 70%);
        }}

        .eyebrow {{
            display: inline-block;
            font-size: 0.76rem;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.14);
            padding: 0.3rem 0.65rem;
            color: rgba(248, 244, 234, 0.84);
            margin-bottom: 1rem;
        }}

        .hero-title {{
            font-size: clamp(2.2rem, 4vw, 3.6rem);
            line-height: 0.95;
            letter-spacing: -0.05em;
            margin: 0;
            max-width: 12ch;
        }}

        .hero-copy {{
            margin-top: 1rem;
            max-width: 70ch;
            color: rgba(248, 244, 234, 0.74);
            font-size: 0.98rem;
        }}

        .chip {{
            display: inline-block;
            margin: 0.75rem 0.45rem 0 0;
            padding: 0.34rem 0.72rem;
            border-radius: 999px;
            font-size: 0.78rem;
            background: rgba(248, 244, 234, 0.12);
            border: 1px solid rgba(255,255,255,0.12);
        }}

        .card {{
            background: var(--panel);
            border-radius: 24px;
            border: 1px solid rgba(16, 35, 38, 0.08);
            box-shadow: 0 14px 34px rgba(16, 35, 38, 0.08);
            padding: 1.1rem 1.15rem;
            min-height: 148px;
        }}

        .card-kicker {{
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.72rem;
            margin-bottom: 0.45rem;
        }}

        .card-value {{
            font-size: 2.1rem;
            line-height: 1;
            font-weight: 700;
            color: var(--ink);
            margin-bottom: 0.35rem;
        }}

        .card-copy {{
            color: var(--muted);
            font-size: 0.92rem;
        }}

        .panel {{
            background: rgba(248, 244, 234, 0.72);
            border-radius: 24px;
            border: 1px solid rgba(16, 35, 38, 0.08);
            padding: 1.15rem;
            box-shadow: 0 14px 34px rgba(16, 35, 38, 0.08);
        }}

        .panel-title {{
            font-size: 1.35rem;
            margin-bottom: 0.2rem;
            color: var(--ink);
        }}

        .panel-subtitle {{
            color: var(--muted);
            font-size: 0.92rem;
            margin-bottom: 0.9rem;
        }}

        .dataset-meta {{
            display: flex;
            gap: 0.75rem;
            flex-wrap: wrap;
            margin-bottom: 0.9rem;
            font-size: 0.82rem;
            color: var(--muted);
        }}

        .section-note {{
            border-radius: 20px;
            padding: 0.95rem 1rem;
            background: linear-gradient(90deg, rgba(31, 138, 112, 0.16), rgba(242, 95, 76, 0.12));
            border: 1px solid rgba(16, 35, 38, 0.08);
            margin-bottom: 0.9rem;
            color: var(--ink);
        }}

        .alert-card {{
            border-radius: 16px;
            padding: 0.95rem;
            border: 1px solid rgba(16,35,38,0.08);
            margin-bottom: 0.7rem;
            background: rgba(255,255,255,0.4);
        }}

        .table-caption {{
            color: var(--muted);
            font-size: 0.85rem;
            margin-bottom: 0.6rem;
        }}

        .process-step {{
            background: rgba(248, 244, 234, 0.8);
            border: 1px solid rgba(16, 35, 38, 0.08);
            border-radius: 22px;
            padding: 1rem;
            min-height: 180px;
            box-shadow: 0 14px 34px rgba(16, 35, 38, 0.08);
        }}

        .process-step-title {{
            font-size: 1.05rem;
            color: var(--ink);
            margin-bottom: 0.45rem;
            font-weight: 700;
        }}

        .process-step-copy {{
            color: var(--muted);
            font-size: 0.92rem;
            line-height: 1.45;
            margin-bottom: 0.75rem;
        }}

        .process-step-status {{
            display: inline-block;
            border-radius: 999px;
            padding: 0.28rem 0.62rem;
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            background: rgba(16, 35, 38, 0.08);
            color: var(--ink);
            margin-bottom: 0.75rem;
        }}

        .terminal {{
            background: #0e1a1c;
            color: #dcf7ef;
            border-radius: 22px;
            padding: 1rem 1.1rem;
            font-family: var(--mono);
            min-height: 220px;
            box-shadow: 0 18px 38px rgba(16, 35, 38, 0.18);
        }}

        .terminal-line {{
            margin-bottom: 0.42rem;
            white-space: pre-wrap;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def install_autorefresh(seconds: int) -> None:
    components.html(
        f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {seconds * 1000});
        </script>
        """,
        height=0,
    )


def titleize(value: str) -> str:
    return value.replace("_", " ").replace("-", " ").title()


def coerce_timestamps(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    updated = frame.copy()
    for column in columns:
        if column in updated.columns:
            updated[column] = pd.to_datetime(updated[column], errors="coerce")
    return updated


def infer_time_column(frame: pd.DataFrame, preferred: list[str] | None = None) -> str | None:
    for column in preferred or []:
        if column in frame.columns:
            return column
    for column in frame.columns:
        if pd.api.types.is_datetime64_any_dtype(frame[column]):
            return column
    for column in frame.columns:
        lowered = column.lower()
        if any(token in lowered for token in ["time", "date", "recorded", "processed", "created", "updated"]):
            return column
    return None


def infer_numeric_columns(frame: pd.DataFrame, preferred: list[str] | None = None) -> list[str]:
    ordered: list[str] = []
    for column in preferred or []:
        if column in frame.columns and pd.api.types.is_numeric_dtype(frame[column]):
            ordered.append(column)
    for column in frame.select_dtypes(include="number").columns.tolist():
        if column not in ordered:
            ordered.append(column)
    return ordered


def infer_dimension_columns(frame: pd.DataFrame, preferred: list[str] | None = None) -> list[str]:
    ordered: list[str] = []
    for column in preferred or []:
        if column in frame.columns:
            ordered.append(column)
    for column in frame.columns:
        if column in ordered:
            continue
        if not pd.api.types.is_numeric_dtype(frame[column]) and not pd.api.types.is_datetime64_any_dtype(frame[column]):
            ordered.append(column)
    return ordered


def format_value(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.1f}"
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def latest_value(frame: pd.DataFrame, column: str, default: str = "N/A") -> str:
    if frame.empty or column not in frame.columns:
        return default
    return format_value(frame.iloc[-1][column])


def normalize_frame(frame: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    dataset_cfg = DATASET_CONFIG.get(dataset_name, {})
    preferred = dataset_cfg.get("preferred_time_columns", [])
    dynamic = [
        column
        for column in frame.columns
        if any(
            token in column.lower()
            for token in ["_time", "_date", "recorded_at", "processed_at", "created_at", "updated_at", "timestamp"]
        )
    ]
    return coerce_timestamps(frame, list(dict.fromkeys(preferred + dynamic)))


def compute_quality_score(metrics_df: pd.DataFrame) -> tuple[float, dict[str, float]]:
    if metrics_df.empty:
        return 100.0, {"reliability": 100.0, "acceptance": 100.0, "throughput": 100.0}

    total_runs = max(len(metrics_df), 1)
    success_rate = (metrics_df["run_status"].eq("SUCCESS").sum() / total_runs) * 100 if "run_status" in metrics_df else 100.0
    processed = metrics_df["records_processed"].sum() if "records_processed" in metrics_df else 0
    rejected = metrics_df["rejected_records"].sum() if "rejected_records" in metrics_df else 0
    acceptance = (processed / max(processed + rejected, 1)) * 100
    avg_runtime = metrics_df["runtime_seconds"].mean() if "runtime_seconds" in metrics_df else 0.0
    throughput = min(100.0, max(35.0, 100.0 - (avg_runtime * 2.1)))
    quality = round((success_rate * 0.45) + (acceptance * 0.4) + (throughput * 0.15), 1)
    return quality, {
        "reliability": round(success_rate, 1),
        "acceptance": round(acceptance, 1),
        "throughput": round(throughput, 1),
    }


def status_tone(status: str) -> str:
    normalized = str(status).upper()
    if normalized == "SUCCESS":
        return THEME["success"]
    if normalized in {"FAILED", "ERROR"}:
        return THEME["accent"]
    if normalized in {"NO_OP", "WARNING"}:
        return THEME["warning"]
    return THEME["info"]


def empty_figure(message: str, height: int = 320) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{"text": message, "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        margin={"l": 0, "r": 0, "t": 8, "b": 0},
        font={"family": THEME["font_family"], "color": THEME["ink"]},
    )
    return fig


def quality_gauges(score: float, components: dict[str, float]) -> go.Figure:
    labels = ["Quality", "Reliability", "Acceptance", "Throughput"]
    values = [score, components["reliability"], components["acceptance"], components["throughput"]]
    colors = [THEME["accent"], THEME["success"], THEME["info"], THEME["warning"]]
    fig = go.Figure()
    for idx, (label, value) in enumerate(zip(labels, values)):
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=value,
                title={"text": label},
                domain={"x": [idx * 0.25, idx * 0.25 + 0.23], "y": [0, 1]},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": colors[idx]},
                    "bgcolor": "rgba(16,35,38,0.06)",
                    "borderwidth": 0,
                    "steps": [
                        {"range": [0, 60], "color": "rgba(244,180,0,0.18)"},
                        {"range": [60, 100], "color": "rgba(31,138,112,0.18)"},
                    ],
                },
            )
        )
    fig.update_layout(
        height=280,
        margin={"l": 0, "r": 0, "t": 8, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"family": THEME["mono_family"], "color": THEME["ink"]},
    )
    return fig


def build_time_series_chart(frame: pd.DataFrame, dataset_name: str) -> go.Figure:
    dataset_cfg = DATASET_CONFIG.get(dataset_name, {})
    time_column = infer_time_column(frame, dataset_cfg.get("preferred_time_columns"))
    numeric_columns = infer_numeric_columns(frame, dataset_cfg.get("preferred_numeric_columns"))[:3]
    if not time_column or not numeric_columns:
        return empty_figure(f"{titleize(dataset_name)} trend unavailable")
    chart_df = frame.sort_values(time_column).copy()
    melted = chart_df[[time_column] + numeric_columns].melt(id_vars=time_column, value_vars=numeric_columns, var_name="metric", value_name="value")
    fig = px.line(
        melted,
        x=time_column,
        y="value",
        color="metric",
        markers=True,
        color_discrete_sequence=[THEME["success"], THEME["accent"], THEME["warning"]],
    )
    fig.update_layout(
        height=320,
        margin={"l": 0, "r": 0, "t": 8, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title_text="",
        xaxis_title=titleize(time_column),
        yaxis_title="Value",
        font={"family": THEME["font_family"], "color": THEME["ink"]},
    )
    return fig


def build_category_chart(frame: pd.DataFrame, dataset_name: str) -> go.Figure:
    dataset_cfg = DATASET_CONFIG.get(dataset_name, {})
    category_columns = infer_dimension_columns(frame, dataset_cfg.get("preferred_dimension_columns"))
    value_columns = infer_numeric_columns(frame, dataset_cfg.get("preferred_numeric_columns"))
    if not category_columns or not value_columns:
        return empty_figure(f"{titleize(dataset_name)} comparison unavailable")
    category_column = category_columns[0]
    value_column = value_columns[0]
    chart_df = frame[[category_column, value_column]].dropna().head(10)
    if chart_df.empty:
        return empty_figure(f"{titleize(dataset_name)} comparison unavailable")
    fig = px.bar(
        chart_df,
        x=category_column,
        y=value_column,
        color=category_column,
        text_auto=True,
        color_discrete_sequence=[THEME["success"], THEME["info"], THEME["accent"], THEME["warning"]],
    )
    fig.update_layout(
        height=320,
        margin={"l": 0, "r": 0, "t": 8, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title_text="",
        xaxis_title=titleize(category_column),
        yaxis_title=titleize(value_column),
        font={"family": THEME["font_family"], "color": THEME["ink"]},
    )
    return fig


def build_lineage_figure(lineage_df: pd.DataFrame) -> go.Figure:
    if lineage_df.empty or "source_stage" not in lineage_df.columns or "target_stage" not in lineage_df.columns:
        return empty_figure("Lineage flow unavailable")
    flow_df = lineage_df.groupby(["source_stage", "target_stage"]).size().reset_index(name="transitions")
    labels = pd.Index(flow_df["source_stage"].tolist() + flow_df["target_stage"].tolist()).drop_duplicates().tolist()
    index_map = {label: idx for idx, label in enumerate(labels)}
    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    label=[titleize(label) for label in labels],
                    pad=20,
                    thickness=18,
                    color=[THEME["info"]] * len(labels),
                ),
                link=dict(
                    source=flow_df["source_stage"].map(index_map),
                    target=flow_df["target_stage"].map(index_map),
                    value=flow_df["transitions"],
                    color="rgba(31, 138, 112, 0.28)",
                ),
            )
        ]
    )
    fig.update_layout(
        height=320,
        margin={"l": 0, "r": 0, "t": 8, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"family": THEME["font_family"], "color": THEME["ink"]},
    )
    return fig


def preview_frame(frame: pd.DataFrame, limit: int = 12) -> pd.DataFrame:
    preview = frame.head(limit).copy()
    for column in preview.columns:
        if pd.api.types.is_datetime64_any_dtype(preview[column]):
            preview[column] = preview[column].dt.strftime("%Y-%m-%d %H:%M:%S")
    return preview


def summarize_frame(frame: pd.DataFrame, dataset_name: str) -> list[tuple[str, str]]:
    dataset_cfg = DATASET_CONFIG.get(dataset_name, {})
    details = [("Rows", format_value(len(frame))), ("Columns", format_value(len(frame.columns)))]
    time_column = infer_time_column(frame, dataset_cfg.get("preferred_time_columns"))
    if time_column:
        latest_timestamp = frame[time_column].dropna().max() if time_column in frame.columns else None
        if latest_timestamp is not None and not pd.isna(latest_timestamp):
            details.append(("Latest", format_value(latest_timestamp)))
    numeric_columns = infer_numeric_columns(frame, dataset_cfg.get("preferred_numeric_columns"))
    if numeric_columns:
        details.append((titleize(numeric_columns[0]), format_value(frame[numeric_columns[0]].dropna().sum())))
    return details


def build_file_key(path: Path) -> str:
    stats = path.stat()
    return f"{path.name}:{stats.st_size}:{int(stats.st_mtime)}"


def list_backend_files() -> list[Path]:
    patterns = ["*.csv", "*.json", "*.log"]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(CONFIG.raw_dir.glob(pattern))
    return sorted({path.resolve(): path for path in files}.values(), key=lambda item: item.name.lower())


def layer_record_count(frame: pd.DataFrame) -> str:
    return format_value(len(frame)) if not frame.empty else "0"


def render_terminal(events: list[str]) -> None:
    lines = "".join(f'<div class="terminal-line">$ {event}</div>' for event in events)
    st.markdown(f'<div class="terminal">{lines}</div>', unsafe_allow_html=True)


def render_process_step(title: str, status: str, body: str, facts: list[tuple[str, str]]) -> None:
    facts_markup = "".join(f'<span class="chip">{label}: {value}</span>' for label, value in facts)
    st.markdown(
        f"""
        <div class="process-step">
            <div class="process-step-status">{status}</div>
            <div class="process-step-title">{title}</div>
            <div class="process-step-copy">{body}</div>
            <div>{facts_markup}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_backend_events(snapshot: dict[str, pd.DataFrame]) -> list[str]:
    backend_files = list_backend_files()
    processed_df = snapshot["processed_files"]
    processed_keys = set(processed_df["file_key"].astype(str).tolist()) if "file_key" in processed_df.columns and not processed_df.empty else set()
    events: list[str] = []
    for path in backend_files[:6]:
        is_new = build_file_key(path) not in processed_keys
        prefix = "New data file detected" if is_new else "Previously ingested file observed"
        events.append(f"{prefix}: {path.name}")
    if backend_files:
        events.append(f"Monitoring input directory: {CONFIG.raw_dir}")
    if any(build_file_key(path) not in processed_keys for path in backend_files):
        events.append("Triggering pipeline execution through Airflow sensor and Spark job...")
    else:
        events.append("Awaiting new files in raw landing zone...")
    return events


def load_dag_metadata() -> dict[str, str]:
    dag_path = Path(CONFIG.project_root) / "dags" / "lakehouse_pipeline_dag.py"
    metadata = {
        "scheduled_dag_id": "N/A",
        "scheduled_schedule": "N/A",
        "event_dag_id": "N/A",
        "sensor_task": "N/A",
        "job_task": "N/A",
    }
    if not dag_path.exists():
        return metadata
    dag_text = dag_path.read_text(encoding="utf-8")
    patterns = {
        "scheduled_dag_id": r'dag_id="([^"]+)"[\s\S]*?description="Scheduled daily',
        "scheduled_schedule": r'description="Scheduled daily[\s\S]*?schedule="([^"]+)"',
        "event_dag_id": r'dag_id="([^"]+)"[\s\S]*?description="Event-driven',
        "sensor_task": r'PythonSensor\(\s*task_id="([^"]+)"',
        "job_task": r'BashOperator\(\s*task_id="([^"]+)"',
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, dag_text, re.DOTALL)
        if match:
            metadata[key] = match.group(1)
    return metadata


def load_platform_metadata() -> dict[str, str]:
    compose_path = Path(CONFIG.project_root) / "docker-compose.yml"
    mcp_path = Path(CONFIG.project_root) / "src" / "mcp_server" / "server.py"
    metadata = {"dashboard_service": "dashboard", "ai_interface": "mcp", "dashboard_port": "8501"}
    if compose_path.exists():
        compose_text = compose_path.read_text(encoding="utf-8")
        if re.search(r"^\s*dashboard:\s*$", compose_text, re.MULTILINE):
            metadata["dashboard_service"] = "dashboard"
        port_match = re.search(r'dashboard:[\s\S]*?published:\s*"?(8501)"?', compose_text)
        if not port_match:
            port_match = re.search(r'dashboard:[\s\S]*?-\s*"(8501):8501"', compose_text)
        if port_match:
            metadata["dashboard_port"] = port_match.group(1)
    if mcp_path.exists():
        mcp_text = mcp_path.read_text(encoding="utf-8")
        name_match = re.search(r'FastMCP\("([^"]+)"\)', mcp_text)
        if name_match:
            metadata["ai_interface"] = name_match.group(1)
    return metadata


def render_workflow_screen(snapshot: dict[str, pd.DataFrame]) -> None:
    dag_metadata = load_dag_metadata()
    platform_metadata = load_platform_metadata()
    backend_files = list_backend_files()
    processed_df = snapshot["processed_files"]
    metrics_df = snapshot["metrics"]
    lineage_df = snapshot["lineage"]
    rejects_df = snapshot["rejects"]
    alerts_df = snapshot["alerts"]
    parser_runs_df = snapshot["parser_runs"]
    quality_events_df = snapshot["quality_events"]
    processed_keys = set(processed_df["file_key"].astype(str).tolist()) if "file_key" in processed_df.columns and not processed_df.empty else set()
    new_backend_files = [path for path in backend_files if build_file_key(path) not in processed_keys]

    render_section_header("Backend Workflow", "Live process view generated from pipeline inputs, medallion outputs, Airflow wiring, and quality rules.")
    terminal_col, summary_col = st.columns([1.05, 0.95])
    with terminal_col:
        render_terminal(build_backend_events(snapshot))
    with summary_col:
        render_process_step(
            "Backend Ingestion",
            "Active",
            "Backend services drop unstructured operational patient records into the monitored raw directory, where the pipeline detects fresh arrivals from the current environment.",
            [
                ("Raw Dir", str(CONFIG.raw_dir)),
                ("Observed Files", format_value(len(backend_files))),
                ("New Files", format_value(len(new_backend_files))),
                ("Patterns", ", ".join(CONFIG.source_patterns)),
            ],
        )

    step_columns = st.columns(3)
    with step_columns[0]:
        render_process_step(
            "Airflow Trigger",
            "Scheduled",
            "Airflow exposes one scheduled DAG for the required 5:00 AM run and one event-driven DAG that waits for new raw files before launching Spark.",
            [
                ("Scheduled DAG", dag_metadata["scheduled_dag_id"]),
                ("Daily Cron", dag_metadata["scheduled_schedule"]),
                ("Event DAG", dag_metadata["event_dag_id"]),
                ("Sensor", dag_metadata["sensor_task"]),
                ("Job", dag_metadata["job_task"]),
            ],
        )
    with step_columns[1]:
        render_process_step(
            "Bronze Preservation",
            "Tracing",
            "Incoming records are written to the Bronze layer with source file, ingestion time, and run metadata so the original payload remains auditable.",
            [
                ("Bronze Dir", str(CONFIG.bronze_dir)),
                ("Lineage Events", format_value(len(lineage_df))),
                ("Processed Files", format_value(len(processed_df))),
            ],
        )
    with step_columns[2]:
        render_process_step(
            "Self-Healing Validation",
            "Repairing",
            "Schema standardization, null filling, duplicate removal, format validation, and reject isolation are applied using the current quality engine.",
            [
                ("Expected Schema", ", ".join(EXPECTED_COLUMNS)),
                ("Reject Rows", layer_record_count(rejects_df)),
                ("Alert Threshold", CONFIG.alert_reject_rate_threshold),
                ("Quality Events", format_value(len(quality_events_df))),
            ],
        )

    layer_columns = st.columns(3)
    with layer_columns[0]:
        render_process_step(
            "Silver Layer",
            "Structured",
            "Validated records are written into Silver after cleanup, producing a structured dataset ready for business-safe downstream use.",
            [
                ("Silver Dir", str(CONFIG.silver_dir)),
                ("Metrics Rows", layer_record_count(metrics_df)),
                ("Latest Run", latest_value(metrics_df.sort_values(infer_time_column(metrics_df, ['recorded_at'])), "run_id", "N/A") if not metrics_df.empty and infer_time_column(metrics_df, ["recorded_at"]) else "N/A"),
            ],
        )
    with layer_columns[1]:
        render_process_step(
            "Gold Analytics",
            "Publishing",
            "Aggregated analytics outputs are generated in Gold for summaries, segmentation, and business-facing consumption.",
            [
                ("Gold Dir", str(CONFIG.gold_dir)),
                ("Gold Tables", format_value(sum(1 for name in ["gold_age", "gold_segments", "gold_summary"] if not snapshot[name].empty))),
                ("Alerts", layer_record_count(alerts_df)),
            ],
        )
    with layer_columns[2]:
        render_process_step(
            "Monitoring Interfaces",
            "Exposed",
            "Status, metrics, lineage, and alerts are surfaced through the dashboard and AI interface for operational monitoring and analysis.",
            [
                ("Dashboard Service", platform_metadata["dashboard_service"]),
                ("Dashboard Port", platform_metadata["dashboard_port"]),
                ("AI Interface", platform_metadata["ai_interface"]),
                ("Metadata Tables", format_value(sum(1 for name in ["metrics", "run_status", "alerts", "lineage", "processed_files", "parser_runs", "quality_events"] if not snapshot[name].empty))),
                ("Parser Runs", format_value(len(parser_runs_df))),
            ],
        )


def render_hero(summary: dict[str, Any], mode_label: str) -> None:
    chip_markup = "".join(f'<span class="chip">{label}: {value}</span>' for label, value in summary["chips"])
    st.markdown(
        f"""
        <div class="hero-card">
            <div class="eyebrow">{PAGE_CONFIG["title"]}</div>
            <div class="hero-title">{summary["headline"]}</div>
            <div class="hero-copy">{summary["description"]}</div>
            <div>{chip_markup}<span class="chip">Mode: {mode_label}</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_header(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="panel"><div class="panel-title">{title}</div><div class="panel-subtitle">{subtitle}</div></div>',
        unsafe_allow_html=True,
    )


def render_kpi_card(label: str, value: str, footnote: str, tone: str | None = None) -> None:
    st.markdown(
        f'<div class="card"><div class="card-kicker">{label}</div><div class="card-value">{value}</div><div class="card-copy">{footnote}</div></div>',
        unsafe_allow_html=True,
    )
    if tone:
        st.markdown(
            f"<div style='height:6px;border-radius:999px;background:{tone};margin-top:-10px;'></div>",
            unsafe_allow_html=True,
        )


def build_summary(snapshot: dict[str, pd.DataFrame], mode_label: str) -> dict[str, Any]:
    active_datasets = [name for name, frame in snapshot.items() if not frame.empty]
    primary_dataset = DATASET_CONFIG.get(active_datasets[0], {}).get("title", titleize(active_datasets[0])) if active_datasets else "No Data"
    return {
        "headline": PAGE_CONFIG["title"],
        "description": f"{mode_label} view across {len(active_datasets)} active datasets from {CONFIG.app_name}.",
        "chips": [
            ("Data Root", str(Path(os.getenv("PIPELINE_DATA_ROOT", CONFIG.data_root)))),
            ("Primary Dataset", primary_dataset),
            ("Active Tables", format_value(len(active_datasets))),
        ],
    }


def build_overview_cards(snapshot: dict[str, pd.DataFrame], quality_score: float) -> list[dict[str, str]]:
    metrics_df = snapshot["metrics"]
    processed_df = snapshot["processed_files"]
    run_status_df = snapshot["run_status"]
    alerts_df = snapshot["alerts"]
    status_column = "status" if "status" in run_status_df.columns else ""
    time_column = infer_time_column(run_status_df, ["recorded_at"])
    latest_status = latest_value(run_status_df.sort_values(time_column), status_column, "N/A") if time_column and status_column and not run_status_df.empty else "N/A"
    actionable_alerts = len(alerts_df[alerts_df["severity"].isin(["critical", "warning"])]) if "severity" in alerts_df.columns and not alerts_df.empty else 0
    cards = [
        {"label": "Quality Score", "value": f"{quality_score:.1f}", "foot": "Derived from current run metrics.", "tone": ""},
        {"label": "Runs Tracked", "value": format_value(len(metrics_df)), "foot": "Metric records detected in snapshot.", "tone": ""},
        {
            "label": "Files Onboarded",
            "value": format_value(processed_df["file_name"].nunique() if "file_name" in processed_df.columns and not processed_df.empty else 0),
            "foot": "Unique processed files discovered.",
            "tone": "",
        },
        {"label": "Current Status", "value": latest_status, "foot": f"Actionable alerts: {format_value(actionable_alerts)}", "tone": status_tone(latest_status)},
    ]
    if not metrics_df.empty and "records_processed" in metrics_df.columns:
        cards.append(
            {
                "label": "Rows Promoted",
                "value": format_value(metrics_df["records_processed"].sum()),
                "foot": "Total processed rows across tracked runs.",
                "tone": "",
            }
        )
    return cards


def render_alert_stack(alerts_df: pd.DataFrame) -> None:
    if alerts_df.empty:
        st.success("No alert events recorded.")
        return
    time_column = infer_time_column(alerts_df, ["recorded_at"])
    display_df = alerts_df.sort_values(time_column, ascending=False) if time_column else alerts_df
    for _, row in display_df.head(5).iterrows():
        severity = str(row.get("severity", "info"))
        background = {
            "critical": "rgba(242, 95, 76, 0.14)",
            "warning": "rgba(244, 180, 0, 0.15)",
        }.get(severity, "rgba(31, 138, 112, 0.12)")
        st.markdown(
            f"""
            <div class="alert-card" style="background:{background}">
                <div style="font-size:0.76rem; text-transform:uppercase; letter-spacing:0.12em; color:{THEME["muted"]};">
                    {severity} | {row.get('alert_type', 'alert')} | {row.get('delivery_state', 'logged')}
                </div>
                <div style="font-size:1rem; color:{THEME["ink"]}; margin-top:0.35rem;">
                    {row.get('message', '')}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def latest_failures(run_status_df: pd.DataFrame) -> pd.DataFrame:
    if run_status_df.empty or "status" not in run_status_df.columns:
        return pd.DataFrame(columns=["run_id", "status", "message"])
    failures = run_status_df[run_status_df["status"].isin(["FAILED", "NO_OP"])].copy()
    time_column = infer_time_column(failures, ["recorded_at"])
    if time_column:
        failures = failures.sort_values(time_column, ascending=False)
    return failures.head(8)


def render_dataset_panels(snapshot: dict[str, pd.DataFrame]) -> None:
    for dataset_name, frame in snapshot.items():
        if frame.empty:
            continue
        title = DATASET_CONFIG.get(dataset_name, {}).get("title", titleize(dataset_name))
        meta_markup = "".join(f"<span>{label}: {value}</span>" for label, value in summarize_frame(frame, dataset_name))
        left_col, right_col = st.columns([1.05, 0.95])
        with left_col:
            st.markdown(
                f"""
                <div class="panel">
                    <div class="card-kicker">{title}</div>
                    <div class="panel-title">{title}</div>
                    <div class="dataset-meta">{meta_markup}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            has_time = infer_time_column(frame, DATASET_CONFIG.get(dataset_name, {}).get("preferred_time_columns")) is not None
            has_numeric = bool(infer_numeric_columns(frame, DATASET_CONFIG.get(dataset_name, {}).get("preferred_numeric_columns")))
            chart = build_time_series_chart(frame, dataset_name) if has_time and has_numeric else build_category_chart(frame, dataset_name)
            st.plotly_chart(
                chart,
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"dataset-chart-{dataset_name}",
            )
        with right_col:
            st.markdown(
                f"""
                <div class="panel">
                    <div class="card-kicker">Preview</div>
                    <div class="panel-title">{title} Snapshot</div>
                    <div class="table-caption">Top rows from the currently loaded dataset.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.dataframe(preview_frame(frame), use_container_width=True, hide_index=True)


ESSENTIAL_DATASET_ORDER = [
    "metrics",
    "processed_files",
    "run_status",
    "alerts",
    "parser_runs",
    "quality_events",
    "rejects",
    "gold_summary",
    "gold_age",
    "gold_segments",
]


def latest_run_status(run_status_df: pd.DataFrame) -> tuple[str, str]:
    if run_status_df.empty or "status" not in run_status_df.columns:
        return "N/A", "N/A"
    time_column = infer_time_column(run_status_df, ["recorded_at"])
    ordered = run_status_df.sort_values(time_column) if time_column else run_status_df
    latest = ordered.iloc[-1]
    return format_value(latest.get("run_id", "N/A")), format_value(latest.get("status", "N/A"))


def build_live_event_feed(snapshot: dict[str, pd.DataFrame]) -> pd.DataFrame:
    events: list[dict[str, str]] = []

    for path in list_backend_files():
        events.append(
            {
                "time": "",
                "source": "raw",
                "event": f"observed file {path.name}",
            }
        )

    dataset_event_specs = [
        ("processed_files", ["processed_at"], lambda row: f"processed {row.get('file_name', 'file')}"),
        ("run_status", ["recorded_at"], lambda row: f"{row.get('status', 'status')} {row.get('run_id', '')}".strip()),
        ("alerts", ["recorded_at"], lambda row: f"{row.get('severity', 'alert')} {row.get('alert_type', '')}".strip()),
        ("quality_events", ["recorded_at"], lambda row: f"{row.get('event_type', 'quality_event')} {row.get('record_count', '')}".strip()),
        ("parser_runs", ["recorded_at"], lambda row: f"parser {row.get('source_type', '')} {row.get('records_seen', '')}".strip()),
    ]

    for dataset_name, preferred_times, builder in dataset_event_specs:
        frame = snapshot[dataset_name]
        if frame.empty:
            continue
        time_column = infer_time_column(frame, preferred_times)
        ordered = frame.sort_values(time_column, ascending=False) if time_column else frame
        for _, row in ordered.head(12).iterrows():
            events.append(
                {
                    "time": format_value(row.get(time_column)) if time_column else "",
                    "source": DATASET_CONFIG.get(dataset_name, {}).get("title", titleize(dataset_name)),
                    "event": builder(row),
                }
            )

    if not events:
        return pd.DataFrame(columns=["time", "source", "event"])

    event_frame = pd.DataFrame(events)
    sortable_time = pd.to_datetime(event_frame["time"], errors="coerce")
    return event_frame.assign(_sort_time=sortable_time).sort_values("_sort_time", ascending=False).drop(columns="_sort_time").head(25)


def render_live_event_feed(snapshot: dict[str, pd.DataFrame]) -> None:
    event_frame = build_live_event_feed(snapshot)
    if event_frame.empty:
        st.info("No live backend or pipeline events available.")
        return
    st.dataframe(event_frame, use_container_width=True, hide_index=True)


def essential_snapshot(snapshot: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {
        dataset_name: snapshot[dataset_name]
        for dataset_name in ESSENTIAL_DATASET_ORDER
        if dataset_name in snapshot and not snapshot[dataset_name].empty
    }


def main() -> None:
    inject_styles(THEME)
    st.sidebar.markdown(f"## {PAGE_CONFIG['title']}")
    st.sidebar.code(str(Path(os.getenv("PIPELINE_DATA_ROOT", CONFIG.data_root))), language=None)
    refresh = st.sidebar.slider(
        "Auto-refresh seconds",
        REFRESH["min_seconds"],
        REFRESH["max_seconds"],
        REFRESH["default_seconds"],
        REFRESH["step_seconds"],
    )
    install_autorefresh(refresh)
    live_mode = CONFIG.metrics_dir.exists() and bool(list(CONFIG.metrics_dir.rglob("*.parquet")))
    use_demo_seed = st.sidebar.toggle("Use seeded demo fallback", value=not live_mode)
    show_raw_tables = st.sidebar.toggle("Show raw tables", value=False)
    st.sidebar.caption("The dashboard reads live parquet outputs when available and can fall back to seeded demo data.")

    snapshot = load_pipeline_snapshot(use_demo_seed=use_demo_seed)
    snapshot = {name: normalize_frame(frame, name) for name, frame in snapshot.items()}
    essential = essential_snapshot(snapshot)

    metrics_df = snapshot["metrics"]
    processed_df = snapshot["processed_files"]
    run_status_df = snapshot["run_status"]
    alerts_df = snapshot["alerts"]
    reject_df = snapshot["rejects"]

    mode_label = "Live Lakehouse" if live_mode else "Seeded Demo"
    quality_score, quality_components = compute_quality_score(metrics_df)
    summary = build_summary(essential or snapshot, mode_label)
    render_hero(summary, mode_label)

    st.write("")
    latest_run_id, latest_status = latest_run_status(run_status_df)
    overview_cards = [
        {"label": "run_id", "value": latest_run_id, "foot": "Latest observed pipeline run.", "tone": ""},
        {"label": "status", "value": latest_status, "foot": "Latest recorded run state.", "tone": status_tone(latest_status)},
        {
            "label": "processed_files",
            "value": format_value(processed_df["file_name"].nunique() if "file_name" in snapshot["processed_files"].columns and not snapshot["processed_files"].empty else 0),
            "foot": "Distinct backend files tracked.",
            "tone": "",
        },
        {
            "label": "alerts",
            "value": format_value(len(alerts_df)),
            "foot": "Current alert records in metadata.",
            "tone": "",
        },
        {"label": "quality_score", "value": f"{quality_score:.1f}", "foot": "Computed from live metrics only.", "tone": ""},
    ]
    for column, card in zip(st.columns(len(overview_cards)), overview_cards):
        with column:
            render_kpi_card(titleize(card["label"]), card["value"], card["foot"], card["tone"] or None)

    st.write("")
    left, right = st.columns([1.1, 0.9])
    with left:
        render_section_header("Live Events", "Current backend observations and pipeline events from raw files and metadata tables.")
        render_live_event_feed(snapshot)
    with right:
        render_section_header("Quality", "Reliability, acceptance, and throughput from the current metrics table.")
        st.plotly_chart(quality_gauges(quality_score, quality_components), use_container_width=True, config={"displayModeBar": False})

    st.write("")
    metrics_col, alerts_col = st.columns([1.05, 0.95])
    with metrics_col:
        metrics_title = DATASET_CONFIG["metrics"]["title"]
        render_section_header(metrics_title, "Trend chart selected from detected time and numeric columns.")
        st.plotly_chart(build_time_series_chart(metrics_df, "metrics"), use_container_width=True, config={"displayModeBar": False})
    with alerts_col:
        alerts_title = DATASET_CONFIG["alerts"]["title"]
        render_section_header(alerts_title, "Recent alert events in the current snapshot.")
        render_alert_stack(alerts_df)

    st.write("")
    render_section_header("Live Outputs", "Only active datasets from the current pipeline state are shown below.")
    render_dataset_panels(essential)

    if show_raw_tables:
        st.write("")
        st.markdown("### Raw Inspection")
        raw_left, raw_right = st.columns(2)
        with raw_left:
            st.write("Metrics")
            time_column = infer_time_column(metrics_df, ["recorded_at"])
            display_metrics = metrics_df.sort_values(time_column, ascending=False) if time_column and not metrics_df.empty else metrics_df
            st.dataframe(preview_frame(display_metrics, limit=50) if not display_metrics.empty else display_metrics, use_container_width=True, hide_index=True)
        with raw_right:
            st.write("Rejects")
            st.dataframe(preview_frame(reject_df, limit=50), use_container_width=True, hide_index=True)

    st.caption(f"Auto-refresh target: every {refresh} seconds. Run with `streamlit run app/dashboard.py`.")


if __name__ == "__main__":
    main()
