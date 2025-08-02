#!/usr/bin/env bash
set -o errexit

# Add GIS repository for GDAL
add-apt-repository -y ppa:ubuntugis/ppa
apt-get update

# Install EXACT versions matching your Windows setup
apt-get install -y \
    libgdal-dev=3.4.3+dfsg-1build4 \
    gdal-bin=3.4.3+dfsg-1build4 \
    libgeos-dev=3.10.2-1 \
    proj-bin=8.2.1-1ubuntu1

# Verify installation
echo "=== GDAL VERSION ==="
gdalinfo --version
echo "=== LIBRARY PATHS ==="
ls -la /usr/lib/x86_64-linux-gnu/libgdal*
ls -la /usr/lib/x86_64-linux-gnu/libgeos*

# Python setup
pip install --upgrade pip
pip install -r requirements.txt

# Database setup
python manage.py migrate
python manage.py collectstatic --no-input