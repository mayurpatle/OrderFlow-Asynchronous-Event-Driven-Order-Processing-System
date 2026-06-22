### Concurrency Issue for 2000 Concurrent Users 

System was facing problem for 2000 Concurrent users I sollved it  by  Atomic SQL Query Update 

"I fixed the oversell with an atomic conditional UPDATE — verified exactly 1000 reservations under 2000 concurrent orders. But the first version threw an exception to signal out-of-stock, and that exception, crossing the @Transactional boundary, marked the transaction rollback-only — which made the Kafka listener fail its offset commit and redeliver, so rejected orders got processed multiple times and my rejection metric inflated to 2128 instead of 1000. The fix was to stop using exceptions for expected business outcomes: out-of-stock isn't an error, it's a normal result, so I return a result object instead of throwing. No exception crosses the transaction boundary, the listener commits cleanly, no redelivery. It also happens to be better design — exceptions shouldn't model expected control flow."

### The moment  of Truth : 
Run this command to check the for stock item sku 

docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT sku, available_quantity, reserved_quantity, (available_quantity + reserved_quantity) AS total FROM stock_items WHERE sku='SKU-001';"

### Reservation Count :

docker exec -it orderflow-postgres psql -U orderflow -d inventory -c "SELECT status, COUNT(*) FROM reservations GROUP BY status;"

### Analytics : 
Invoke-RestMethod -Uri http://localhost:8086/metrics | ConvertTo-Json -Depth 5

-----------------------------------------------------------------

Found a concurrency bug through load testing — not theory, you caused a 100% oversell and measured it
Root-caused it as a lost-update race in unguarded read-modify-write, and understood why @Transactional didn't prevent it (isolation ≠ locking)
Fixed it with an atomic conditional UPDATE — lock-free, single-statement, impossible to oversell
Hit a second, subtler bug — exception-across-transaction-boundary marking the listener rollback-only, causing redelivery and inflated metrics
Root-caused and fixed that too — replaced exception-as-control-flow with a result object, which is both correct and better design
Verified both fixes under the identical 2000-order load that broke the original

--------------------------------------------------------------------


"I load-tested my inventory service with 2000 concurrent orders against 1000 units and found my reservation logic oversold by 100% — a classic lost-update race in read-modify-write. I understood why my existing @Transactional didn't help: at READ_COMMITTED isolation, transactions don't prevent two readers from both updating. I fixed it with an atomic conditional UPDATE that does check-and-decrement in one statement, verified exactly 1000 reservations under the same load. Then I hit a second issue — I'd thrown an exception to signal out-of-stock, which marked the Kafka listener's transaction rollback-only and caused redelivery, inflating my rejection count to 2128. I realized out-of-stock is an expected outcome, not an error, so I returned a result object instead of throwing. Both correct and better design — exceptions shouldn't model normal control flow. Final run: every counter reconciled, zero oversell, clean logs."




