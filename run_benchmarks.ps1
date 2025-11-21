# Run benchmark tests for lsearch-core stratified indexing

Write-Host "╔═══════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   Lightning Search Stratified Index Benchmark   ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Small test (just to verify it works)
Write-Host "Running small precision tests..." -ForegroundColor Yellow
cargo test precision --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Precision tests failed!" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Precision tests passed" -ForegroundColor Green
Write-Host ""

# Medium dataset benchmark (~100MB)
Write-Host "Running medium dataset benchmark (~100MB)..." -ForegroundColor Yellow
Write-Host "(This will take a few minutes)" -ForegroundColor Gray
cargo test medium_dataset -- --ignored --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Medium benchmark failed!" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Medium benchmark completed" -ForegroundColor Green
Write-Host ""

# Optional: Large dataset benchmark (~500MB)
$runLarge = Read-Host "Run large dataset benchmark (~500MB)? This will take ~10-15 minutes (y/N)"
if ($runLarge -eq "y" -or $runLarge -eq "Y") {
    Write-Host "Running large dataset benchmark..." -ForegroundColor Yellow
    cargo test large_dataset -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Large benchmark failed!" -ForegroundColor Red
        exit 1
    }
    Write-Host "✓ Large benchmark completed" -ForegroundColor Green
}

Write-Host ""
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "All benchmarks completed successfully! 🎉" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Cyan
