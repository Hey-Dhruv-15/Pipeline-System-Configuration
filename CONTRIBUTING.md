# Contributing to Self-Healing Pipeline System Configuration

Thank you for your interest in contributing to the **Self-Healing Pipeline System Configuration** project! We are building an autonomous, resilient Lakehouse pipeline ecosystem and welcome developers of all skill levels.

---

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [How Can I Contribute?](#how-can-i-contribute)
   - [Reporting Bugs](#reporting-bugs)
   - [Suggesting Enhancements](#suggesting-enhancements)
   - [Adding New Self-Healing Rules](#adding-new-self-healing-rules)
   - [Adding Source Contracts](#adding-source-contracts)
   - [Improving Documentation](#improving-documentation)
3. [Development Setup](#development-setup)
   - [Prerequisites](#prerequisites)
   - [Local Environment](#local-environment)
   - [Docker Development Stack](#docker-development-stack)
4. [Git Workflow & Branching Strategy](#git-workflow--branching-strategy)
5. [Commit Message Guidelines](#commit-message-guidelines)
6. [Testing & Quality Checks](#testing--quality-checks)
7. [Submitting a Pull Request](#submitting-a-pull-request)
8. [Community & Recognition](#community--recognition)

---

## Code of Conduct

All contributors and maintainers are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md). Please read it before participating.

---

## How Can I Contribute?

### Reporting Bugs
If you encounter a bug, broken parser, or unexpected pipeline failure:
1. Search the [GitHub Issues](https://github.com/laasyaarun18-gif/Pipeline-System/issues) to ensure it hasn't already been reported.
2. If new, open an issue using the **Bug Report Template**.
3. Include sample input files (sanitized, without PII), log outputs, and system details.

### Suggesting Enhancements
Feature requests are welcome! Whether it's adding Delta Lake change data capture (CDC), expanding MCP tools, integrating Iceberg/Hudi, or adding Slack/Teams alerting:
- Open a discussion or create an issue using the **Feature Request Template**.
- Outline the user story, rationale, and proposed architectural design.

### Adding New Self-Healing Rules
One of the most impactful ways to contribute is adding new self-healing transformers to `src/pipeline/quality.py`:
1. Identify common data quality anomalies (e.g., date format deviations, malformed phone numbers/emails, outlier clamping).
2. Add the transformation rule to `src/pipeline/quality.py`.
3. Add corresponding unit tests in `tests/test_quality_logic.py`.
4. Update the quality event counter in `src/pipeline/metadata.py` so the Streamlit dashboard and MCP server track the healing event.

### Adding Source Contracts
To support new file extensions or schemas:
1. Open `src/pipeline/source_contracts.json`.
2. Define the new source specification (aliases, extraction regexes, and defaults).
3. Add parser support in `src/pipeline/parsers.py`.
4. Add sample records under `demo_data/` or `samples/`.

---

## Development Setup

### Prerequisites
- **Git** 2.30+
- **Python** 3.10 or 3.11
- **Docker & Docker Compose** (v2+)
- **Node.js** 18+ (for frontend development)

### Local Environment
Clone the repository and set up a virtual environment:

```bash
git clone https://github.com/laasyaarun18-gif/Pipeline-System.git
cd Pipeline-System

# Create virtual environment
python -m venv .venv

# Activate on Linux/macOS:
source .venv/bin/activate
# Activate on Windows:
.venv\Scripts\Activate.ps1

# Install core and dev dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Copy environment variables
cp .env.example .env
```

### Docker Development Stack
To run the full stack locally (Postgres, Airflow, Spark Runner, Streamlit, MCP Server, Simulator):

```bash
# Start background services
docker compose up -d postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler dashboard mcp-server

# Trigger an on-demand batch run
docker compose run --rm pipeline-runner
```

---

## Git Workflow & Branching Strategy

1. **Fork** the repository to your own GitHub account.
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/Pipeline-System.git
   cd Pipeline-System
   ```
3. Create a feature branch with a descriptive name:
   ```bash
   git checkout -b feat/add-regex-date-healing
   # or
   git checkout -b fix/parser-json-whitespace
   ```
4. Ensure your local branch is kept in sync with `upstream/main`.

---

## Commit Message Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short summary>

[optional body]

[optional footer(s)]
```

### Allowed Types:
- `feat`: A new feature or self-healing rule
- `fix`: A bug fix in pipeline, parsers, or UI
- `docs`: Documentation changes
- `test`: Adding or updating test cases
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `perf`: Performance optimizations (e.g., Spark partition tuning)
- `chore`: Build scripts, CI workflow, or dependency bumps

### Examples:
- `feat(quality): add email validation and masking self-healing rule`
- `fix(parsers): correctly handle trailing commas in json logs`
- `docs(readme): add architecture sequence diagram`
- `test(config): add unit tests for environment override resolution`

---

## Testing & Quality Checks

Before submitting your pull request, run the test and linting suite:

```bash
# Run unit tests
pytest tests/ -v

# Run linting with ruff
ruff check .

# Validate formatting
black --check .

# Validate Docker Compose configuration
docker compose config
```

---

## Submitting a Pull Request

1. Push your branch to your GitHub fork:
   ```bash
   git push origin feat/your-feature-name
   ```
2. Navigate to the upstream repository and click **New Pull Request**.
3. Fill out the PR template completely:
   - What changed?
   - Why was this change made?
   - How was it tested?
   - Any breaking changes?
4. Link any related issues (`Fixes #123`).
5. Ensure all CI checks pass. Maintainers will review your PR and provide constructive feedback!

---

## Community & Recognition

- **First-time contributor?** Look for issues labeled [`good first issue`](https://github.com/laasyaarun18-gif/Pipeline-System/labels/good%20first%20issue).
- All contributors will be listed in our project `README.md` and release notes!

Thank you for helping make data pipelines autonomous, self-healing, and rock-solid!
