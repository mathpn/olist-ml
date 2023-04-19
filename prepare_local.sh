#!/bin/bash
export PYTHONPATH=.
docker run --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=olist -p 5401:5432 -d postgres
sleep 5
psql "postgresql://postgres:postgres@localhost:5401/olist" -a -f sql/create_tables.sql

python scripts/insert_data_into_db.py
