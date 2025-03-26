#!/bin/bash

echo "Starting ETL Process..."

echo "Running Extraction..."
python3 /tourism_analysis/etl/airbnb/extract/airbnb_extraction.py

if [ $? -eq 0 ]; then
    echo "Extraction completed successfully."
else
    echo "Extraction failed."
    exit 1
fi

echo "Running Transformation..."
python3 /tourism_analysis/etl/airbnb/transform/airbnb_transform.py

if [ $? -eq 0 ]; then
    echo "Transformation completed successfully."
else
    echo "Transformation failed."
    exit 1
fi

echo "ETL Process completed."