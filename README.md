# Kafka Demo Project – Order → Notification + Analytics + Auth

Learn **Kafka**, **Auth (JWT)**, and **SQL/PostgreSQL** in one project.

---

## Architecture

```
┌──────────────┐
│ Auth Service │  port 8004  (register / login / JWT)
└──────────────┘

┌──────────────┐     order.created      ┌──────────────────────┐
│ Order Service│ ─────────────────────► │ Notification Service │ 8001
│  port 8000   │         Kafka          └──────────────────────┘
└──────────────┘              │
                              └──────► ┌──────────────────────┐
                                       │  Analytics Service   │ 8003
                                       └──────────────────────┘

┌──────────────┐
│  PostgreSQL  │  port 5432  (users table – practice SQL here)
└──────────────┘
```

---

## Services

| Service        | Port | Role                                      |
|----------------|------|-------------------------------------------|
| Order          | 8000 | Producer – creates orders, publishes events |
| Notification   | 8001 | Consumer – creates notifications          |
| Analytics      | 8003 | Consumer – summary, top products/customers |
| Auth           | 8004 | Register, login, JWT, validate token      |
| Kafka          | 9092 | Message broker                            |
| PostgreSQL     | 5432 | DB for Auth (and SQL practice)            |

---

## Quick Start

### 1. Infrastructure
```bash
docker compose up -d
```

### 2. Install deps
```bash
pip install -r requirements.txt
```

### 3. Start services

```bash
# Auth (optional: point at Postgres)
cd auth-service
# Windows:  $env:DATABASE_URL="postgresql://kafka:kafka@localhost:5432/kafka_demo"
# Linux:    export DATABASE_URL="postgresql://kafka:kafka@localhost:5432/kafka_demo"
uvicorn main:app --port 8004 --reload

# Order
cd order-service && uvicorn main:app --port 8000 --reload

# Notification
cd notification-service && uvicorn main:app --port 8001 --reload

# Analytics
cd analytics-service && uvicorn main:app --port 8003 --reload
```

Auth works with SQLite by default if you skip `DATABASE_URL`.

---

## Auth API

```bash
# Register
curl -X POST http://localhost:8004/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"secret123","full_name":"Alice"}'

# Login
curl -X POST http://localhost:8004/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"secret123"}'

# Me
curl http://localhost:8004/auth/me -H "Authorization: Bearer <token>"
```

---

## Learn SQL

See **[docs/SQL_LEARNING.md](docs/SQL_LEARNING.md)**.

```bash
docker exec -it kafka-demo-postgres psql -U kafka -d kafka_demo
\dt
SELECT * FROM users;
```

---

## Project Structure

```
kafka-demo/
├── docker-compose.yml
├── requirements.txt
├── docs/SQL_LEARNING.md
├── auth-service/main.py          # port 8004
├── order-service/main.py
├── notification-service/main.py
├── analytics-service/main.py
└── scripts/
```

Happy learning! 🚀
