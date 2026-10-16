# Kafka Demo Project – Order → Notification + Analytics

A simple but complete project to understand **Kafka** through a real use-case.

---

## Architecture

```
                        ┌───────────────────────┐
                        │  Notification Svc    │  port 8001
                        │  group: notification │
                        └──────────▲───────────┘
                                   │
┌──────────────┐     order.created │
│ Order Service│ ──────────────────├
│  (Producer)  │     Kafka Topic   │
│  port 8000   │ ──────────────────├
└──────────────┘                   │
                        ┌──────────▼───────────┐
                        │  Analytics Service   │  port 8003
                        │  group: analytics    │
                        └──────────────────────┘
```

- **1 Producer** → Order Service
- **2 Independent Consumers** → Notification + Analytics
- Different consumer groups → each gets **all** messages

---

## What you will learn

| Concept                  | Where you see it                                |
|--------------------------|-------------------------------------------------|
| Producer                 | Order Service                                   |
| Consumer                 | Notification + Analytics                        |
| Topic                    | `order.created`                                 |
| Consumer Group           | Different groups = full copy of messages        |
| Decoupling                | Services never call each other                  |
| Idempotency              | `event_id` prevents duplicates                  |
| Message durability       | Messages stay even if consumer is down          |
| Catch-up / Recovery      | Consumer processes missed messages later        |

---

## Quick Start

### 1. Start Kafka
```bash
cd kafka-demo
docker compose up -d
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start the 3 services (3 terminals)

**Terminal 1 – Order Service (Producer)**
```bash
cd order-service
uvicorn main:app --port 8000 --reload
```

**Terminal 2 – Notification Service**
```bash
cd notification-service
uvicorn main:app --port 8001 --reload
```

**Terminal 3 – Analytics Service**
```bash
cd analytics-service
uvicorn main:app --port 8003 --reload
```

### 4. Run tests
```bash
cd scripts
python bulk_test.py --count 10
python test_recovery.py --count 5
python bulk_stress_test.py --count 30
```

---

## Analytics Endpoints

```bash
# Summary (total orders, revenue, avg order value, unique customers)
curl http://localhost:8003/analytics/summary

# Top products by revenue
curl http://localhost:8003/analytics/top-products

# Top customers by spending
curl http://localhost:8003/analytics/top-customers

# Recent events
curl http://localhost:8003/analytics/events
```

---

## Manual testing

```bash
# Create an order
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_email": "ahmed@example.com",
    "items": [
      {"name": "Laptop", "qty": 1, "price": 1200},
      {"name": "Mouse", "qty": 2, "price": 25}
    ]
  }'

# Check notification
curl http://localhost:8001/notifications

# Check analytics
curl http://localhost:8003/analytics/summary
curl http://localhost:8003/analytics/top-products
```

---

## Project Structure

```
kafka-demo/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── order-service/
│   └── main.py                 # Producer (port 8000)
├── notification-service/
│   └── main.py                 # Consumer (port 8001)
├── analytics-service/
│   └── main.py                 # Consumer (port 8003)
└── scripts/
    ├── bulk_test.py
    ├── test_recovery.py
    └── bulk_stress_test.py
```

---

Happy learning! 🚀
