#!/bin/bash
# Run all landing to bronze jobs

echo "========================================="
echo "Starting Landing to Bronze Pipeline"
echo "========================================="

cd /mnt/c/Users/HP/Learning/DE_Development
source venv_linux/bin/activate

echo ""
echo "Job 1: Processing Inventory Data"
echo "-----------------------------------------"
python -m src.pipelines.landing_to_bronze

echo ""
echo "Job 2: Processing Sales Data"
echo "-----------------------------------------"
python -m src.pipelines.sales_to_bronze

echo ""
echo "========================================="
echo "All jobs completed!"
echo "========================================="
