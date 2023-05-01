#!/bin/bash

echo "starting feature ingestion"
python src/etl/feature_store_ingestion.py --table client
python src/etl/feature_store_ingestion.py --table delivery
python src/etl/feature_store_ingestion.py --table payment
python src/etl/feature_store_ingestion.py --table product
python src/etl/feature_store_ingestion.py --table review
python src/etl/feature_store_ingestion.py --table sale
echo "done!"
