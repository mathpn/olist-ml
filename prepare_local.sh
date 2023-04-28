#!/bin/bash
export PYTHONPATH=.
docker container stop postgres_olist
docker container rm postgres_olist
docker run --name postgres_olist -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=olist\
    -p 5401:5432 \
    -v ./postgres-data:/var/lib/postgresql/data \
    -d postgres
sleep 5

python scripts/insert_data_into_db.py
