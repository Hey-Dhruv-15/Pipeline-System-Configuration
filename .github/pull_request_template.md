## Description
<!-- Provide a brief description of the changes introduced in this PR. -->

## Type of Change
- [ ] 🐛 Bug fix (non-breaking change fixing an issue)
- [ ] ✨ New feature (non-breaking addition of a self-healing rule, parser, or MCP tool)
- [ ] 💥 Breaking change (fix or feature causing existing behavior to change)
- [ ] 📝 Documentation update
- [ ] ⚡ Performance improvement
- [ ] 🧪 Tests addition / update
- [ ] 🔧 Refactoring or CI / build maintenance

## Related Issues
<!-- Link related issues here (e.g., Fixes #123) -->

## Self-Healing / Lakehouse Verification Checklist
- [ ] Added or updated unit tests under `tests/`
- [ ] Tested locally with PySpark or Docker runner
- [ ] Validated idempotency (multiple runs with identical files do not corrupt Delta tables)
- [ ] Verified dead-letter queue (quarantine / reject) behavior
- [ ] Checked FastMCP server tools if pipeline API or metadata changed
- [ ] Verified Streamlit dashboard and UI observability metrics

## Screenshots / CLI Output (if applicable)
<!-- Attach screenshots of the dashboard or terminal output here -->

## Checklist
- [ ] My code follows the project's style guidelines (`black`, `ruff`)
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have updated the documentation / README if needed
- [ ] All CI checks are passing
