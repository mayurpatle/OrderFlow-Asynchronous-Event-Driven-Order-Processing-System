### Command for live database watched used in Session 9 for  tracing the  compensation of payment failed  track ; 

Live  Databse Tracker : 

# Loop forever, querying inventory state every 2 seconds
while ($true) {
Clear-Host
Write-Host "=== INVENTORY STATE ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Stock items:" -ForegroundColor Yellow
docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT sku, available_quantity, reserved_quantity FROM stock_items ORDER BY sku;"
Write-Host ""
Write-Host "Reservations by status:" -ForegroundColor Yellow
docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT status, COUNT(*) FROM reservations GROUP BY status;"
Start-Sleep -Seconds 2
}

