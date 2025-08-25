-- Create user if not exists
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'telemetry'
   ) THEN
      CREATE ROLE telemetry LOGIN PASSWORD 'telemetry';
   END IF;
END
$$;

-- Create database if not exists
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'telemetry'
   ) THEN
      CREATE DATABASE telemetry OWNER telemetry;
   END IF;
END
$$;

-- Switch to telemetry DB
\c telemetry

-- Optional: extensions (idempotent)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
