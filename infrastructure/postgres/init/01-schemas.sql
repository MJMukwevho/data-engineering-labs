-- Runs once, the first time the PostgreSQL volume is created.
-- Curated (gold) tables from the lake are loaded into this schema.
CREATE SCHEMA IF NOT EXISTS gold;
