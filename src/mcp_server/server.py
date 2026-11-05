from typing import Any

from mcp.server.fastmcp import FastMCP

from src.pipeline.repository import load_pipeline_snapshot


mcp = FastMCP("lakehouse-control-room")


def _serialise_records(records: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return records[:limit]


@mcp.tool()
def get_pipeline_overview(use_demo_seed: bool = True) -> dict[str, Any]:
    snapshot = load_pipeline_snapshot(use_demo_seed=use_demo_seed)
    metrics_df = snapshot["metrics"]
    run_status_df = snapshot["run_status"]
    alerts_df = snapshot["alerts"]
    parser_runs_df = snapshot["parser_runs"]
    quality_events_df = snapshot["quality_events"]

    total_processed = int(metrics_df["records_processed"].sum()) if "records_processed" in metrics_df.columns and not metrics_df.empty else 0
    total_rejected = int(metrics_df["rejected_records"].sum()) if "rejected_records" in metrics_df.columns and not metrics_df.empty else 0
    total_runs = len(metrics_df)
    latest_status = (
        str(run_status_df.sort_values("recorded_at").iloc[-1]["status"])
        if not run_status_df.empty and "status" in run_status_df.columns
        else "NO_RUNS"
    )
    active_alerts = (
        int(alerts_df["severity"].isin(["warning", "critical"]).sum())
        if not alerts_df.empty and "severity" in alerts_df.columns
        else 0
    )

    return {
        "total_runs": total_runs,
        "latest_status": latest_status,
        "records_processed": total_processed,
        "rejected_records": total_rejected,
        "active_alerts": active_alerts,
        "parser_runs": int(len(parser_runs_df)),
        "quality_events": int(len(quality_events_df)),
    }


@mcp.tool()
def get_executive_summary(use_demo_seed: bool = True) -> dict[str, Any]:
    summary_df = load_pipeline_snapshot(use_demo_seed=use_demo_seed)["gold_summary"]
    if summary_df.empty:
        return {"headline": "No business summary available", "business_message": "", "recommended_action": ""}

    latest = summary_df.sort_values("processed_time", ascending=False).iloc[0].to_dict()
    return {
        "run_id": latest.get("run_id"),
        "headline": latest.get("headline"),
        "business_message": latest.get("business_message"),
        "acceptance_rate": latest.get("acceptance_rate"),
        "recommended_action": latest.get("recommended_action"),
    }


@mcp.tool()
def get_recent_alerts(limit: int = 5, use_demo_seed: bool = True) -> list[dict[str, Any]]:
    alerts_df = load_pipeline_snapshot(use_demo_seed=use_demo_seed)["alerts"]
    if alerts_df.empty:
        return []
    alerts = alerts_df.sort_values("recorded_at", ascending=False).to_dict(orient="records")
    return _serialise_records(alerts, limit)


@mcp.tool()
def get_customer_segments(use_demo_seed: bool = True) -> list[dict[str, Any]]:
    segments_df = load_pipeline_snapshot(use_demo_seed=use_demo_seed)["gold_segments"]
    if segments_df.empty:
        return []
    return segments_df.sort_values("distinct_customers", ascending=False).to_dict(orient="records")


@mcp.tool()
def get_parser_health(limit: int = 10, use_demo_seed: bool = True) -> list[dict[str, Any]]:
    parser_df = load_pipeline_snapshot(use_demo_seed=use_demo_seed)["parser_runs"]
    if parser_df.empty:
        return []
    parser_runs = parser_df.sort_values("recorded_at", ascending=False).to_dict(orient="records")
    return _serialise_records(parser_runs, limit)


@mcp.tool()
def get_quality_events(limit: int = 10, use_demo_seed: bool = True) -> list[dict[str, Any]]:
    events_df = load_pipeline_snapshot(use_demo_seed=use_demo_seed)["quality_events"]
    if events_df.empty:
        return []
    events = events_df.sort_values("recorded_at", ascending=False).to_dict(orient="records")
    return _serialise_records(events, limit)


if __name__ == "__main__":
    mcp.run()
