### General DB Queries

Orders Table :
docker exec -it orderflow-postgres psql -U orderflow -d orders -c "SELECT * from orders;"

Inventory :
for reservations : 

docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT * from reservations;"

for reservation items : 

docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT * from reservation_items;"

Payment  DB : 

docker exec -it orderflow-postgres psql -U orderflow -d payments -c "SELECT COUNT(*) from payments;"

Shipping DB : 

docker exec -it orderflow-postgres psql -U orderflow -d shipping -c "SELECT * from shipments;"

Stock Item DB : 

docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT * from stock_items;"

------------------------------------------------

### Pick one order, trace it across all services

$orderId = "709d95c9-11c4-4246-8625-684e63fd4269"

Write-Host "`n=== Order in order-service ===" -ForegroundColor Cyan
docker exec -it orderflow-postgres psql -U orderflow -d orders -c "SELECT id, customer_id, total_cents, status, created_at FROM orders WHERE id = '$orderId';"

Write-Host "`n=== Reservation in inventory-service ===" -ForegroundColor Cyan
docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT id, order_id, status, created_at FROM reservations WHERE order_id = '$orderId';"

Write-Host "`n=== Reservation items in inventory-service ===" -ForegroundColor Cyan
docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT ri.sku, ri.quantity FROM reservation_items ri JOIN reservations r ON ri.reservation_id = r.id WHERE r.order_id = '$orderId';"

Write-Host "`n=== Payment in payment-service ===" -ForegroundColor Cyan
docker exec -it orderflow-postgres psql -U orderflow -d payments -c "SELECT id, order_id, amount_cents, status, gateway_reference, failure_code FROM payments WHERE order_id = '$orderId';"

Write-Host "`n=== Shipment in shipping-service ===" -ForegroundColor Cyan
docker exec -it orderflow-postgres psql -U orderflow -d shipping -c "SELECT id, order_id, carrier, tracking_number, status, estimated_delivery_at FROM shipments WHERE order_id = '$orderId';"

--------------------------------------------------------

### Send 20 Orders query  : 

1..1050 | ForEach-Object {
$body = "{`"customerId`":`"cust-analytics-$_`",`"items`":[{`"sku`":`"SKU-001`",`"quantity`":1,`"unitPriceCents`":2999}]}"
Invoke-RestMethod -Uri http://localhost:8081/api/orders -Method POST -ContentType "application/json" -Body $body -ErrorAction SilentlyContinue | Out-Null
Start-Sleep -Milliseconds 150
}
Write-Host "Sent 20 orders."

---------------------------------------------------------

### Powershell 7 command : 

winget install --id Microsoft.PowerShell --source winget

----------------------------------------------------------

### 2000 Concurrent Order request 

$total = 2000
$throttle = 50

$pool = [runspacefactory]::CreateRunspacePool(1, $throttle)
$pool.Open()
$jobs = @()

1..$total | ForEach-Object {
$ps = [powershell]::Create()
$ps.RunspacePool = $pool
[void]$ps.AddScript({
param($i)
$body = "{`"customerId`":`"cust-conc-$i`",`"items`":[{`"sku`":`"SKU-001`",`"quantity`":1,`"unitPriceCents`":2999}]}"
try {
Invoke-RestMethod -Uri http://localhost:8081/api/orders -Method POST -ContentType "application/json" -Body $body -ErrorAction Stop | Out-Null
} catch {}
}).AddArgument($_)
$jobs += [pscustomobject]@{ PS = $ps; Handle = $ps.BeginInvoke() }
}

# Wait for all to finish
foreach ($j in $jobs) { $j.PS.EndInvoke($j.Handle); $j.PS.Dispose() }
$pool.Close()
$pool.Dispose()
Write-Host "Fired $total concurrent orders (50 at a time)."


--------------------------------------------------------------




