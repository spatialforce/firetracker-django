#!/usr/bin/env bash
set -o errexit

# Install system dependencies for GDAL
apt-get update
apt-get install -y --no-install-recommends \
    binutils \
    libgdal-dev \
    gdal-bin \
    libproj-dev \
    proj-bin \
    python3-dev

# Verify GDAL installation
gdalinfo --version

# Install Python dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Apply database migrations
python manage.py migrate

# Collect static files
python manage.py collectstatic --no-input