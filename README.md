<div align="center">

# 🛡️ Self-Healing Pipeline System Configuration

**Autonomous, Schema-Adaptive Lakehouse Data Pipeline with Delta Lake, PySpark, Apache Airflow, and Model Context Protocol (MCP)**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/Python-3.10%20%7C%203.11-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.2-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2.0-00ADD8.svg)](https://delta.io/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.10.0-017CEE.svg?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![MCP Protocol](https://img.shields.io/badge/MCP-FastMCP_Ready-8A2BE2.svg)](https://modelcontextprotocol.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44-FF4B4B.svg?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker_Compose-Multi--Service-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![CI Status](https://img.shields.io/badge/CI-Passing-brightgreen.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?logo=github)](CONTRIBUTING.md)
[![Code Style](https://img.shields.io/badge/code%20style-black%20%26%20ruff-000000.svg)](https://github.com/psf/black)

<p align="center">
  <a href="#key-features">Key Features</a> •
  <a href="#architecture--medallion-lakehouse-flow">Architecture</a> •
  <a href="#quickstart-in-60-seconds">Quickstart</a> •
  <a href="#agentic-mcp-integration">Agentic MCP</a> •
  <a href="#configuration-matrix">Configuration</a> •
  <a href="#contributing">Contributing</a>
</p>

</div>

---

## 🌟 Overview

Data pipelines break every day due to schema drift, messy unstructured payloads, missing values, duplicate records, and silent upstream format modifications. **Self-Healing Pipeline System Configuration** eliminates data downtime by introducing an autonomous self-healing data ingestion engine for modern Lakehouses.

Built with **PySpark**, **Delta Lake**, and **Apache Airflow**, this system ingests disparate multi-format sources (CSV, JSON, TXT, and application logs), dynamically parses fields via declarative source contracts, auto-repairs dirty and missing values in-flight, quarantines corrupted records into an isolated dead-letter queue (DLQ), and publishes ACID-guaranteed Medallion layers (Bronze, Silver, Gold).

Pipeline health, lineage, and runtime quality events are continuously exposed through an interactive **Streamlit Control Room** and a **FastMCP (Model Context Protocol)** server, allowing AI assistants (Claude, Gemini, Cursor) to monitor, inspect, and diagnose pipelines in real time.

---

## ✨ Key Features

| Capability | Description |
| :--- | :--- |
| 🛡️ **Autonomous Self-Healing** | In-flight schema reconciliation, null-like token cleansing (`"null"`, `"N/A"`, `"NaN"`, `"undefined"`), regex integer extraction, dynamic median imputation, and automated deduplication. |
| ⚡ **Medallion Architecture** | Strict ACID lakehouse stages: **Bronze** (raw historical record), **Silver** (cleaned & healed), and **Gold** (curated business aggregates like `customer_segments` and `executive_summary`). |
| 🛑 **Quarantine Dead-Letter Queue** | Corrupt and irrecoverable records are automatically quarantined into `data/reject` with explicit failure reasoning (`missing_id`, `negative_age`) without halting the pipeline. |
| 🤖 **AI-Ready FastMCP Server** | Native [Model Context Protocol](https://modelcontextprotocol.io/) server exposing pipeline snapshots, run statuses, active alerts, and customer segments to LLMs and agentic hosts. |
| 🔄 **Dual Airflow Orchestration** | Includes both a scheduled batch DAG (`0 5 * * *`) and an event-driven DAG featuring real-time file-sensing triggers. |
| 📊 **Real-Time Observability** | Interactive Streamlit Control Center dashboard and modern React 18/Vite operations client tracking record throughput, lineage trees, and quality events. |
| 🔒 **Idempotency & Lineage** | Fingerprinted file hashing (name + file size + modification time) prevents redundant batch reprocessing; complete dataset lineage is captured per run. |
| 🚨 **Automated Webhook Alerts** | Real-time Slack/Discord/Teams webhook notifications triggered whenever the batch reject rate exceeds configurable thresholds. |
| 🐳 **One-Click Docker Stack** | Fully orchestrated container environment containing PostgreSQL, Airflow, Spark Runner, Streamlit Dashboard, MCP Server, and Demo Data Simulator. |

---

## 🏗️ Architecture & Medallion Lakehouse Flow

```mermaid
flowchart TD
    subgraph Sources["📥 Ingestion Zone (Unstructured / Multi-Format)"]
        A1["📄 Raw CSV"]
        A2["📜 Unstructured JSON"]
        A3["📝 Log Files (.log)"]
        A4["📋 Clinical Notes (.txt)"]
    end

    subgraph ParserEngine["⚙️ Declarative Contract Parsing"]
        B["source_contracts.json<br/>(Alias matching & Regex extractors)"]
    end

    subgraph SelfHealing["🛡️ Self-Healing & Quality Engine"]
        C1["Schema Standardization<br/>(Column alignment & Token sanitization)"]
        C2{"Validation Check<br/>(ID present? Age >= 0?)"}
        C3["Autonomous Healing<br/>• Coalesce missing names to 'unknown'<br/>• Dynamic median age imputation<br/>• Composite deduplication (ID, Name, Age)"]
        C4["Quarantine Dead-Letter Queue<br/>(Tagged with reject_reason)"]
    end

    subgraph Medallion["💎 Medallion Lakehouse (Delta Lake / Parquet)"]
        D1["🥉 Bronze Layer<br/>Raw payloads & source metadata"]
        D2["🥈 Silver Layer<br/>Cleaned, typed, and healed records"]
        D3["🥇 Gold Layer<br/>• Executive Summary<br/>• Customer / Patient Segments"]
        D4["🚫 Reject / Quarantine Layer"]
    end

    subgraph Observability["📡 Observability & Agent Interfaces"]
        E1["📊 Streamlit Operations Dashboard<br/>(Port :8501)"]
        E2["⚛️ React 18 / Vite Frontend<br/>(Port :5173)"]
        E3["🤖 FastMCP Server<br/>(Model Context Protocol for AI Agents)"]
        E4["🚨 Webhook Alerts<br/>(Slack, Teams, PagerDuty)"]
        E5["📈 Airflow Webserver & DAGs<br/>(Port :8080)"]
    end

    Sources --> B
    B --> D1
    D1 --> C1
    C1 --> C2
    C2 -- "Valid" --> C3
    C2 -- "Invalid" --> C4
    C3 --> D2
    D2 --> D3
    C4 --> D4

    D1 & D2 & D3 & D4 --> Observability
```

---

## 🚀 Quickstart in 60 Seconds

### Option A: Complete Docker Compose Stack (Recommended)

Start the full stack with Airflow, PostgreSQL, Streamlit, FastMCP, and the continuous demo data generator:

```bash
# 1. Clone repository
git clone https://github.com/Hey-Dhruv-15/Pipeline-System-Configuration.git
cd Pipeline-System

# 2. Copy environment template
cp .env.example .env

# 3. Launch stack
docker compose up -d postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler dashboard mcp-server

# 4. Trigger one-off batch ingestion
docker compose run --rm pipeline-runner
```

#### Available Service Endpoints:
- **Streamlit Operations Center**: [`http://localhost:8501`](http://localhost:8501)
- **Apache Airflow Webserver**: [`http://localhost:8080`](http://localhost:8080) *(Credentials: `admin` / `admin`)*
- **PostgreSQL Database**: `localhost:5432` *(User: `airflow`, DB: `airflow`)*
- **FastMCP Agent Server**: Running over standard I/O and container bridge

---

### Option B: Local Python Development Setup

For local experimentation without Docker:

```bash
# 1. Create and activate virtual environment
python -m venv .venv

# On Linux/macOS:
source .venv/bin/activate
# On Windows (PowerShell):
.venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# 3. Initialize environment
cp .env.example .env

# 4. Run the Spark self-healing batch pipeline
python -m src.pipeline.data_pipeline

# 5. Launch the Streamlit Operations Dashboard
streamlit run app/dashboard.py

# 6. Launch the FastMCP Server
python -m src.mcp_server.server
```

---

## 🤖 Agentic MCP (Model Context Protocol) Integration

This repository includes a native **FastMCP** server (`src/mcp_server/server.py`) allowing AI assistants like **Claude Desktop**, **Cursor**, **Gemini**, or **ChatGPT** to supervise the pipeline.

### Available MCP Tools:

| Tool Name | Parameters | Returns | Description |
| :--- | :--- | :--- | :--- |
| `get_pipeline_overview` | `use_demo_seed: bool = True` | `dict` | Total runs, latest run status, records processed vs. rejected, active alert count, and quality event totals. |
| `get_executive_summary` | `use_demo_seed: bool = True` | `dict` | Total records, average age, segments count, and processing timestamp from the Gold layer. |
| `get_active_alerts` | `limit: int = 10` | `list[dict]` | Filtered list of warning and critical pipeline alert notifications. |
| `get_customer_segments` | `limit: int = 20` | `list[dict]` | Segment classification, record volume, and percentage breakdowns. |

### Connecting to Claude Desktop / Cursor:

Add the following configuration to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "lakehouse-pipeline": {
      "command": "python",
      "args": ["-m", "src.mcp_server.server"],
      "cwd": "/path/to/Pipeline-System",
      "env": {
        "PIPELINE_DATA_ROOT": "data",
        "PIPELINE_DATA_FORMAT": "delta"
      }
    }
  }
}
```

---

## ⚙️ Configuration Matrix

All parameters can be tuned via `.env` or system environment variables:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `PIPELINE_DATA_ROOT` | `data` | Base directory for raw, bronze, silver, gold, and metadata storage. |
| `PIPELINE_DATA_FORMAT` | `delta` | Storage engine format: `delta` (Delta Lake with ACID) or `parquet`. |
| `PIPELINE_APP_NAME` | `self_healing_lakehouse_pipeline` | Spark application identifier. |
| `PIPELINE_SOURCE_PATTERNS` | `*.csv,*.json,*.log,*.txt` | Glob patterns to discover incoming unstructured files in `data/raw`. |
| `PIPELINE_SHUFFLE_PARTITIONS` | `4` | Spark SQL shuffle partition tuning. |
| `PIPELINE_MAX_RECORDS_PER_FILE` | `50000` | Target record split per part file. |
| `PIPELINE_ALERT_WEBHOOK_URL` | `""` | Webhook URL (Slack/Teams/Discord) for automated failure alerts. |
| `PIPELINE_ALERT_REJECT_RATE_THRESHOLD` | `0.15` | Reject rate threshold ratio (15%) above which alerts fire. |
| `PIPELINE_BACKEND_GENERATION_INTERVAL_SECONDS` | `20` | Interval between generated batches in the live backend simulator. |
| `PIPELINE_BACKEND_BATCH_SIZE` | `3` | Files emitted per simulation batch. |

---

## 📂 Project Structure

```
├── .github/
│   ├── workflows/
│   │   ├── ci.yml                     # Automated CI (lint, format, test, docker check)
│   │   └── codeql.yml                 # CodeQL security scanning
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.yml             # Structured bug reporting template
│   │   ├── feature_request.yml        # Feature suggestion template
│   │   └── config.yml                 # Community discussion links
│   ├── pull_request_template.md       # PR review checklist
│   └── dependabot.yml                 # Weekly dependency updates
├── app/
│   ├── dashboard.py                   # Streamlit Operations Center UI
│   └── ui_config.json                 # Dashboard styling configuration
├── dags/
│   └── lakehouse_pipeline_dag.py      # Apache Airflow scheduled and event-driven DAGs
├── data/                              # Lakehouse data directory (raw, bronze, silver, gold, reject, metadata)
├── demo_data/                         # Seeded JSON snapshots for zero-touch demo exploration
├── docker/
│   ├── Dockerfile.airflow             # Airflow container image
│   ├── Dockerfile.app                 # Pipeline runner container image
│   ├── Dockerfile.dashboard           # Streamlit dashboard container image
│   └── requirements-*.txt             # Container-specific requirements
├── frontend/                          # Modern React 18 / Vite operational UI client
├── samples/
│   └── customer_data.csv              # Reference input sample
├── scripts/
│   ├── generate_unstructured_patients.py # Synthetic hospital record generator
│   ├── run_demo.sh / run_demo.ps1     # Automated demo runners
│   ├── setup_dev.sh / setup_dev.ps1   # Developer onboarding scripts
│   └── smoke_test.sh                  # Pre-commit smoke test
├── src/
│   ├── mcp_server/
│   │   └── server.py                  # FastMCP agent tool server
│   ├── pipeline/
│   │   ├── alerts.py                  # Webhook delivery and formatting
│   │   ├── backend_simulator.py       # Live data emitter
│   │   ├── config.py                  # Dataclass environment configuration
│   │   ├── data_pipeline.py           # PySpark batch execution entrypoint
│   │   ├── metadata.py                # Lineage hashing and metrics persistence
│   │   ├── parsers.py                 # Source contract regex & JSON extractors
│   │   ├── quality.py                 # Self-healing rules and DLQ quarantine logic
│   │   ├── repository.py              # Unified metadata reader for UI & MCP
│   │   ├── source_contracts.json      # Declarative schema contracts
│   │   └── spark.py                   # Delta Lake SparkSession factory
├── tests/                             # Comprehensive Pytest unit & integration test suite
├── .env.example                       # Documented environment template
├── .gitignore                         # Comprehensive ignore rules
├── CHANGELOG.md                       # Semantic version history
├── CODE_OF_CONDUCT.md                 # Contributor Covenant v2.1
├── CONTRIBUTING.md                    # Contributor workflow and development guide
├── docker-compose.yml                 # Complete multi-service Docker stack
├── LICENSE                            # Apache 2.0 Open Source License
├── Makefile                           # Developer task runner
├── pyproject.toml                     # PEP 517/518 build and tool configuration
├── requirements.txt                   # Production Python dependencies
├── requirements-dev.txt               # Contributor test & lint dependencies
└── SECURITY.md                        # Responsible disclosure policy
```

---

## 🧪 Testing & Verification

Run the automated test suite and style checks locally:

```bash
# Execute Pytest suite
pytest tests/ -v

# Run linting
ruff check .

# Validate formatting
black --check .

# Validate Docker Compose syntax
docker compose config
```

---

## 🗺️ Roadmap & Good First Issues

We welcome community contributions! Here are areas planned for upcoming releases:

- [ ] **Apache Iceberg & Apache Hudi Support**: Provide pluggable storage table formats alongside Delta Lake.
- [ ] **ML-Driven Outlier Detection**: Incorporate dynamic Isolation Forest / Z-score outlier detection for numeric values.
- [ ] **Slack / Teams Native Templates**: Rich Block Kit message cards for pipeline alert events.
- [ ] **OpenTelemetry Integration**: Export distributed traces and Spark metrics to Prometheus / Grafana.
- [ ] **Great Expectations Compatibility**: Export self-healing execution logs into Great Expectations data docs.

Check out our [Good First Issues](https://github.com/Hey-Dhruv-15/Pipeline-System-Configuration/labels/good%20first%20issue) to get started!

---

## 🤝 Contributing

Contributions make the open-source community an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

Please review our [Contributing Guide](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) before submitting your first pull request.

---

## 📜 License

Distributed under the **Apache License, Version 2.0**. See [`LICENSE`](LICENSE) for more information.

---

## 📬 Contact & Maintainers

- **Maintainer**: Dhruva S
- **Email**: [dhruvkuruvilla@gmail.com](mailto:dhruvkuruvilla@gmail.com)
- **Repository**: [https://github.com/Hey-Dhruv-15/Pipeline-System-Configuration](https://github.com/Hey-Dhruv-15/Pipeline-System-Configuration)
