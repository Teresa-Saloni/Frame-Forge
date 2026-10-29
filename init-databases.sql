-- Runs once when the Postgres container is first created.
-- One Postgres, three logical databases: Airflow metadata, Marquez lineage, Iceberg REST catalog.
CREATE DATABASE airflow;
CREATE DATABASE marquez;
CREATE DATABASE iceberg;
