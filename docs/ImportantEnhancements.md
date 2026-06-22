## Future Important  Enhancements  

### Session  7 :   
 
When Inventory service was  added  we  build  a consumer that consumes  order.created event and  it checke the  availability of  the  stock if it  is  available  in the StockItemRepository then create a inventory.reserved event and  add the  reservation in the Reservation Entity and  the reservations Items  in the  RservationItem Repo but  if  the  stock are not  available  and the  there  is  event  in the  inventory.failed then thre should be  notifcation service  consuming that and  mailing  the  used initialy our notification service used  to mail after every order created 

### Session  8  :  

Till here we  have    not  added the  actual cost  for  the order  and c we are also not added the actual 
Add the  Actual Cost  of the order
Add Real  Payment Gateway

### Session 9  : 

if the payment fails release the event to the inventory failed topic  so that the notificationn service could mail the user regarding the failed event andthe reason why the payment failed

### Session 10 shipping service 

we can add more feature like shipping cancelled , shiping delivered and  send event  to the  specific  topics  

### Session 12 

- currently the the  outbox  pattern is implemented in only the Orders Service Future enhancement  will be  to implement  outbox pattern in all other services inventory  payment shipping notification 
- Implement  the CDC over polling for the outbox  
- 





## Future : 

### 1. High-Concurrency Stock & Inventory Management
   At peak scale (like a Big Billion Days sale or a flash sale), standard database locking on inventory tables causes massive bottlenecks and database gridlocks.

Redis-Based Distributed Locks / Lua Scripting: Instead of hitting the database to check if an item is in stock, giants use Redis. They use Lua scripts to execute "check-and-deduct" operations atomically inside Redis memory.

Inventory Bucketing (Database Sharding): If an item has 1,000 units in stock, the database row is split into 10 buckets of 100 units each. Transactions randomly pick a bucket to update. This reduces row-level lock contention by 10x.

Virtual Cart Reservation: When a user adds an item to their cart on Blinkit, the inventory is softly held for 5–10 minutes. If they don't check out, a TTL (Time-to-Live) expires, and a Kafka event releases the stock back to the pool.

### 2.Resiliency & Blast Radius Control
In a massive microservices ecosystem, if the Payment Gateway drops, it shouldn't take down the entire app.

Circuit Breakers & Rate Limiters: Using tools like Resilience4j or Envoy proxies, services drop failing requests early. If the Recommendation Service fails, the home page still loads—it just shows generic products instead of personalized ones.

Bulkheading: Isolating resources. For example, assigning separate thread pools or database connection pools for critical flows (Checkout) vs. non-critical flows (Reviews).

Saga Pattern (Orchestration vs. Choreography): Since you already have Kafka, you are likely using Choreography-based Sagas (services reacting to events). Giants like Amazon often use an Orchestrator (like AWS Step Functions or Temporal) for the checkout flow. If a payment succeeds but inventory fulfillment fails, the Orchestrator explicitly triggers compensating transactions to refund the user.

### 3.Delivery & Logistics Routing (Zomato/Blinkit Specific)
For quick-commerce and food delivery, spatial data and real-time matching are critical.

Geospatial Indexing (H3 / S2 Frameworks): Uber, Zomato, and Swiggy divide the map into hexagonal cells using Uber’s H3 spatial index. This allows them to quickly aggregate available delivery partners and restaurants in a specific hexagon without doing expensive SQL geometric queries.

CQRS (Command Query Responsibility Segregation): The service updating driver locations (write heavy) is completely separated from the service searching for nearby drivers (read heavy).

### 4.Read-Path Scalability
Hybrid Database Architecture: * Relational (PostgreSQL/MySQL): Strictly for ACID compliance in Order and Transaction histories.

NoSQL (DynamoDB/Cassandra): For the Product Catalog, Cart management, and User Sessions because they scale horizontally with predictable low latency.

Search Engine (Elasticsearch/OpenSearch): For fuzzy matching, filtering, and facets on the product search page.