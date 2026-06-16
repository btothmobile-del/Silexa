# Hírolvasó indítása
Set-Location $PSScriptRoot

if (-not (Test-Path "venv")) {
    Write-Host "Virtuális környezet létrehozása..." -ForegroundColor Cyan
    python -m venv venv
}

Write-Host "Csomagok telepítése..." -ForegroundColor Cyan
.\venv\Scripts\pip install -r requirements.txt --quiet

Write-Host "Szerver indítása: http://localhost:8000" -ForegroundColor Green
Start-Process "http://localhost:8000"
.\venv\Scripts\uvicorn main:app --reload --port 8000
