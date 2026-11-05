# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-11-05

### Added
- **Self-Healing Quality Engine**:
  - Dynamic schema conformance across raw files (CSV, JSON, LOG, TXT).
  - Null-like token sanitization (`null`, `none`, `n/a`, `na`, `undefined`, `nan`).
  - Integer extraction with regex cleansing from mixed strings.
  - Outlier handling and median age dynamic imputation.
  - Automated deduplication across business primary keys `(id, name, age)`.
  - Quarantine / Dead-Letter Queue (DLQ) routing with granular reject reasoning.
- **Medallion Lakehouse Storage Layer**:
  - Delta Lake & Parquet multi-tier outputs (Bronze, Silver, Gold Segments, Gold Summary).
  - Checkpointed incremental batch processing with idempotency hashing.
- **Observability & Metadata Tracking**:
  - Source file lineage tracking with MD5/fingerprint state store.
  - Granular quality event counters and parser metrics per run.
  - Webhook alerting mechanism with configurable reject-rate thresholds.
- **Agentic Model Context Protocol (FastMCP) Server**:
  - `lakehouse-control-room` service for AI assistants (Claude, Gemini, ChatGPT).
  - Exposed tools: `get_pipeline_overview`, `get_executive_summary`, `get_active_alerts`, `get_customer_segments`.
- **Operations Center & UI**:
  - Streamlit real-time interactive operations dashboard.
  - Modern React 18 / Vite frontend client for executive metrics.
  - Fallback support to seeded `demo_data/` for immediate zero-touch exploration.
- **Orchestration**:
  - Apache Airflow scheduled daily batch DAG (`0 5 * * *`).
  - Airflow event-driven DAG with raw landing directory file watcher.
- **Developer & Production Suite**:
  - Multi-service `docker-compose.yml` with healthchecks.
  - GitHub Actions CI pipeline, CodeQL security scanning, Dependabot config.
  - Issue templates, PR template, Contributor Covenant Code of Conduct, Apache 2.0 license.
  - Unit test suite for configurations, source contracts, quality logic, and MCP tools.
