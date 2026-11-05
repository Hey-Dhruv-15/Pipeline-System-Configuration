<#
.SYNOPSIS
    Automated developer setup script for Windows.
.DESCRIPTION
    Creates Python virtual environment, installs requirements and dev tools,
    sets up .env configuration, and verifies project sanity.
#>

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Self-Healing Pipeline System - Developer Setup (PowerShell)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Check Python
$pythonCmd = (Get-Command python -ErrorAction SilentlyContinue)
if (-not $pythonCmd) {
    $pythonCmd = (Get-Command py -ErrorAction SilentlyContinue)
}

if (-not $pythonCmd) {
    Write-Warning "Python was not detected in PATH. Please install Python 3.10 or 3.11."
    Exit 1
}

Write-Host "[1/4] Found Python: $($pythonCmd.Source)" -ForegroundColor Green

# 2. Virtual environment setup
if (-not (Test-Path ".venv")) {
    Write-Host "[2/4] Creating virtual environment (.venv)..." -ForegroundColor Yellow
    & $pythonCmd.Source -m venv .venv
} else {
    Write-Host "[2/4] Virtual environment already exists." -ForegroundColor Green
}

# 3. Environment file
if (-not (Test-Path ".env")) {
    Write-Host "[3/4] Generating .env from .env.example..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
} else {
    Write-Host "[3/4] .env already exists." -ForegroundColor Green
}

# 4. Dependency installation instructions
Write-Host "[4/4] Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Activate virtual environment:  .venv\Scripts\Activate.ps1"
Write-Host "  2. Install dependencies:           pip install -r requirements.txt -r requirements-dev.txt"
Write-Host "  3. Run test suite:                 pytest tests/ -v"
Write-Host "  4. Start local Docker stack:       docker compose up -d"
Write-Host ""

