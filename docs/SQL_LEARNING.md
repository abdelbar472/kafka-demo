# SQL Learning Guide

## Connect

```bash
docker compose up -d postgres

docker exec -it kafka-demo-postgres psql -U postgres
docker exec -it kafka-demo-postgres psql -U postgres -d users
docker exec -it kafka-demo-postgres psql -U postgres -d orders
docker exec -it kafka-demo-postgres psql -U postgres -d notifications
docker exec -it kafka-demo-postgres psql -U postgres -d analytics
```

## Practice

```sql
\l
\c users
\dt
SELECT * FROM users;
SELECT email, created_at FROM users ORDER BY created_at DESC;
SELECT COUNT(*) FROM users;

\c orders
SELECT user_email, total FROM orders ORDER BY created_at DESC LIMIT 10;
SELECT user_email, COUNT(*), SUM(total) FROM orders GROUP BY user_email;
```
