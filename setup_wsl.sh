#!/bin/bash
# Setup script for WSL environment

echo "Setting up Python environment in WSL..."

# Navigate to project directory
cd /mnt/c/Users/HP/Learning/DE_Development

# Activate virtual environment
source venv_linux/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install required packages
pip install pyspark pyyaml

echo "Setup complete!"
echo "Virtual environment activated. You can now run:"
echo "python -m src.pipelines.landing_to_bronze"
