# order-saga-platform-through-kafka

Four FastAPI services (Order, Inventory, Payment, Notification) talking to
each other only through Kafka events, each owning its own database, using
the **saga pattern** to keep an order's state consistent across services
without a distributed transaction.

I built this to get real hands-on practice with the parts of backend work
that don't show up in most portfolios: what happens when a multi-step
process needs to fail *partway through* and undo what it already did, how
services stay in sync without directly calling each other, and what it
actually takes to observe a system like this in production.

## Why this shape

Most "microservices demo" repos stop at "service A calls service B over
HTTP." That's not really the hard part. The hard part is: what happens
when an order reserves stock, then payment fails? Something has to notice
that and release the stock back — nobody tells the inventory service
directly "undo that," it just reacts to an event. That's the saga pattern,
and it's the actual reason companies reach for event-driven architecture
instead of one big service with everything in a single transaction.

## The flow

```
POST /orders (order-service)
        │
        ▼
   OrderCreated ──────────────► inventory-service
                                       │
                     ┌─────────────────┴─────────────────┐
                     ▼                                    ▼
            InventoryReserved                     InventoryFailed
                     │                                    │
                     ▼                                    ▼
             payment-service                      order-service
                     │                              marks CANCELLED
        ┌────────────┴────────────┐
        ▼                         ▼
PaymentCompleted            PaymentFailed
        │                         │
        ▼                         ▼
order-service               order-service
 marks COMPLETED          marks CANCELLED
        │              + publishes InventoryReleaseRequested
        │                (compensating transaction - undoes
        │                 the earlier stock reservation)
        ▼                         │
        └──────────┬──────────────┘
                    ▼
           notification-service
          (order completed / cancelled)
```

Every service reacts to events, nothing calls another service's API
directly. Order-service is the one that owns compensation logic here —
when it sees a payment failure, it's the one that tells inventory to
release the stock, since inventory has no way of knowing on its own that a
later step in the saga failed.

## Running it

```bash
docker compose up --build
```

This brings up Kafka (KRaft mode, no Zookeeper needed), Postgres (with
separate databases per service), all four services, and Prometheus +
Grafana. Give it 30-60 seconds on first boot for Kafka to finish forming
its quorum before the services fully connect.

Then:

```bash
# create an order
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-1", "sku": "SKU-WIDGET", "quantity": 2, "amount": 50.00}'

# check its status a moment later (the saga runs asynchronously)
curl http://localhost:8001/orders/<order_id>

# see the effect on stock
curl http://localhost:8002/stock

# see the payment record
curl http://localhost:8003/transactions/<order_id>

# see the notification
curl http://localhost:8004/notifications
```

To see the compensation path fire, send an order with `amount` at or above
1000 (the demo decline threshold) — payment will fail, and you'll see the
stock reservation get released back on the `/stock` endpoint.

Seeded demo stock: `SKU-WIDGET` (50 units), `SKU-GADGET` (10 units),
`SKU-OUT-OF-STOCK` (0 units, for testing the "insufficient stock" path).

## Observability

- Every service exposes `/metrics` (Prometheus format) — request counts,
  latency histograms, in-progress requests, no custom instrumentation code
  needed per route.
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (login `admin` / `admin`) — add
  Prometheus (`http://prometheus:9090`) as a data source to start building
  dashboards.
- Logs are prefixed per-service, so `docker compose logs -f` gives you a
  merged, filterable view of the whole saga as it happens across all four
  services.

## Tests

```bash
pip install -r services/order_service/requirements.txt
pytest -v
```

The real test here is `tests/test_saga_e2e.py` — it runs the entire saga
(all four services) against an in-memory event bus and SQLite, with no
Kafka or Postgres needed, and asserts:
- the happy path completes and stock/payment/notification all reflect it
- a payment failure triggers the compensating transaction and stock gets
  released back to its original quantity
- an out-of-stock order cancels immediately without ever reaching payment

All four services share the same internal package name (`app`), which is
fine in production since each runs in its own process, but meant the test
has to load each service's code in isolation and grab what it needs before
moving to the next one — see the `load_service()` helper in the test file
if you're curious how that's done.

## What I simplified on purpose

- **One Postgres container with three databases** instead of three
  separate instances — real service independence would mean separate DB
  servers (or at least separate hosting), but that's more infra than a
  demo needs.
- **Alembic migrations are only fully wired up for order-service** (see
  `services/order_service/alembic/`) as a working example; inventory and
  payment services use `create_all()` on startup instead. In a real setup
  I'd replicate the Alembic setup for all three.
- **Demo payment logic** — any order at or above $1000 "declines," which
  exists purely to make the compensation path easy to trigger without
  needing a real payment processor.
- **Notifications are logged, not actually sent** — swap `_send()` in
  `notification_service/app/handlers.py` for a real email/SMS provider
  call and nothing else changes.

## Stack

FastAPI, Kafka (KRaft mode) via aiokafka, SQLAlchemy 2.0 async + asyncpg,
Alembic, Prometheus, Grafana, Docker Compose, pytest + pytest-asyncio.
