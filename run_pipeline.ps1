# run_pipeline.ps1
# Retail Sales Lakehouse - One-click pipeline runner (Windows PowerShell)
# Runs: Docker Postgres -> Bronze -> Silver -> Gold -> Schema -> Load -> SQL validation

$ErrorActionPreference = "Stop"

Write-Host "== Retail Sales Lakehouse Pipeline =="

# 1) Ensure Postgres is up
Write-Host "`n[1/7] Starting Postgres (Docker)..."
docker compose up -d | Out-Null

# Optional: small wait for DB readiness
Start-Sleep -Seconds 2

Write-Host "[1/7] Postgres status:"
docker ps --filter "name=retail-postgres"

# 2) Generate Bronze
Write-Host "`n[2/7] Generating Bronze data..."
python src/00_ingest_bronze/generate_bronze.py

# 3) Bronze -> Silver
Write-Host "`n[3/7] Transform Bronze -> Silver..."
python src/10_bronze_to_silver/bronze_to_silver.py

# 4) Silver -> Gold
Write-Host "`n[4/7] Transform Silver -> Gold..."
python src/20_silver_to_gold/silver_to_gold.py

# 5) Apply schema
Write-Host "`n[5/7] Applying warehouse schema..."
python src/30_load/apply_schema.py

# 6) Load Gold marts
Write-Host "`n[6/7] Loading Gold marts to Postgres..."
python src/30_load/load_gold_to_postgres.py

# 7) Validation SQL
Write-Host "`n[7/7] Running validation & analysis SQL..."
Get-Content .\sql\01_validation_and_analysis.sql | docker exec -i retail-postgres psql -U de_user -d retail

Write-Host "`n✅ Pipeline finished successfully."
