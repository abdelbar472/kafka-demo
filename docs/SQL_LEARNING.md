# SQL Learning Guide (with this project)

Use the Postgres container from `docker-compose.yml`.

## 1. Start Postgres

```bash
docker compose up -d postgres
```

## 2. Open psql inside the container

```bash
docker exec -it kafka-demo-postgres psql -U kafka -d kafka_demo
```

## 3. Core commands inside psql

```sql
\l                  -- list databases
\dt                 -- list tables
\d users            -- describe users table
\q                  -- quit
```

## 4. Practice path (do in order)

### Level 1 – SELECT basics
```sql
SELECT * FROM users;
SELECT email, full_name FROM users;
SELECT email FROM users WHERE is_active = TRUE;
SELECT * FROM users ORDER BY created_at DESC LIMIT 5;
```

### Level 2 – Filtering & sorting
```sql
SELECT * FROM users WHERE email LIKE '%@example.com';
SELECT * FROM users WHERE created_at > NOW() - INTERVAL '7 days';
SELECT email FROM users ORDER BY email ASC;
```

### Level 3 – Aggregates
```sql
SELECT COUNT(*) AS total_users FROM users;
SELECT COUNT(*) FILTER (WHERE is_active) AS active_users FROM users;
SELECT DATE(created_at) AS day, COUNT(*) AS signups
FROM users
GROUP BY day
ORDER BY day DESC;
```

### Level 4 – UPDATE
```sql
UPDATE users SET full_name = 'Test User' WHERE email = 'alice@example.com';
SELECT * FROM users WHERE email = 'alice@example.com';
```

### Level 5 – Joins (later, when orders use Postgres)
```sql
-- SELECT u.email, o.id AS order_id, o.total
-- FROM users u
-- JOIN orders o ON o.user_email = u.email
-- ORDER BY o.created_at DESC
-- LIMIT 10;
```

## 5. Point Auth Service at Postgres

```bash
# Windows PowerShell
$env:DATABASE_URL="postgresql://kafka:kafka@localhost:5432/kafka_demo"
$env:JWT_SECRET="kafka-demo-secret"

cd auth-service
uvicorn main:app --port 8004 --reload
```

```bash
# Linux / macOS
export DATABASE_URL="postgresql://kafka:kafka@localhost:5432/kafka_demo"
export JWT_SECRET="kafka-demo-secret"
cd auth-service
uvicorn main:app --port 8004 --reload
```

Register a user via API, then query the `users` table with SQL.

## 6. One-liners from host

```bash
docker exec -it kafka-demo-postgres psql -U kafka -d kafka_demo -c "\dt"
docker exec -it kafka-demo-postgres psql -U kafka -d kafka_demo -c "SELECT email, created_at FROM users;"
```
