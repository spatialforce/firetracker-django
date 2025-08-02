#!/usr/bin/env bash
# Exit on error
set -o errexit

# Install system dependencies
apt-get update
apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    libgdal-dev \
    python3-dev

# Upgrade pip and install requirements
pip install --upgrade pip
pip install -r requirements.txt

# Collect static files
python manage.py collectstatic --no-input

# Apply database migrations (if needed)
# python manage.py migrate