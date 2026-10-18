-- Runs automatically when Postgres container is first created.

CREATE TABLE IF NOT EXISTS users (
    id          VARCHAR PRIMARY KEY,
    email       VARCHAR UNIQUE NOT NULL,
    hashed_password VARCHAR NOT NULL,
    full_name   VARCHAR,
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
