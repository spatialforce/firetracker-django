#!/usr/bin/env bash
# Exit on any error
set -o errexit

# Install system dependencies (GDAL, GEOS, PROJ, etc.)
apt-get update
apt-get install -y \
    binutils \
    libproj-dev \
    gdal-bin \
    libgdal-dev

# Install Python dependencies
pip install -r requirements.txt

# Collect static files (if needed)
python manage.py collectstatic --no-input