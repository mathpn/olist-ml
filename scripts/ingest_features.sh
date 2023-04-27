#!/bin/bash

echo "starting feature ingestion"
python etl/feature_store_ingestion.py --table client
python etl/feature_store_ingestion.py --table delivery
python etl/feature_store_ingestion.py --table payment
python etl/feature_store_ingestion.py --table product
python etl/feature_store_ingestion.py --table review
python etl/feature_store_ingestion.py --table sale
echo "done!"
