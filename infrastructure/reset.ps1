Write-Host "=== Resetting OrderFlow — coordinated full reset ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "1/2: Truncating all service databases..." -ForegroundColor Yellow
docker exec orderflow-postgres psql -U orderflow -d orders -c "TRUNCATE TABLE orders;" | Out-Null
docker exec orderflow-postgres psql -U orderflow -d inventory -c "TRUNCATE TABLE reservations, reservation_items, stock_items;" | Out-Null
docker exec orderflow-postgres psql -U orderflow -d payments -c "TRUNCATE TABLE payments;" | Out-Null
docker exec orderflow-postgres psql -U orderflow -d shipping -c "TRUNCATE TABLE shipments;" | Out-Null
Write-Host "    All four service DBs truncated." -ForegroundColor Green

Write-Host ""
Write-Host "2/2: Recreating all Kafka topics..." -ForegroundColor Yellow
$topics = @(
    "order.created",
    "inventory.reserved",
    "inventory.reservation_failed",
    "payment.completed",
    "payment.failed",
    "shipping.dispatched",
    "order.cancelled"
)
foreach ($topic in $topics) {
    docker exec orderflow-kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic $topic 2>$null
    docker exec orderflow-kafka kafka-topics --bootstrap-server kafka:9092 --create --topic $topic --partitions 3 --replication-factor 1 | Out-Null
    Write-Host "    Reset: $topic" -ForegroundColor Green
}

Write-Host ""
Write-Host "=== Reset complete. All services start from zero. ===" -ForegroundColor Cyan