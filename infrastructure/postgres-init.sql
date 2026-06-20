-- Runs automatically the first time Postgres initializes an empty data directory.
-- Creates one database per microservice — the strict database-per-service boundary.
--
-- The 'monolith' database is created by the POSTGRES_DB env var in docker-compose.
-- The four service databases are created here.
--
-- This file is mounted into /docker-entrypoint-initdb.d/ where the official
-- Postgres image runs any *.sql files exactly once, on fresh initialization.

CREATE DATABASE orders;
CREATE DATABASE inventory;
CREATE DATABASE payments;
CREATE DATABASE shipping;