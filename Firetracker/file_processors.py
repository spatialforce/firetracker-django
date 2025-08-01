
import pandas as pd
import json
import logging
from django.contrib.gis.geos import Point, Polygon, MultiPolygon, GEOSGeometry
from django.contrib.gis.gdal import DataSource
from django.core.files.storage import default_storage
from .models import Province, District, FirePoint

logger = logging.getLogger(__name__)

def process_firepoint_file(uploaded_file):
    """Process firepoint data from CSV or Excel files"""
    try:
        # Handle both in-memory and on-disk files
        if hasattr(uploaded_file, 'temporary_file_path'):
            filepath = uploaded_file.temporary_file_path()
            df = pd.read_csv(filepath) if filepath.endswith('.csv') else pd.read_excel(filepath)
        else:
            df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        
        # Validate required columns
        required_columns = {'latitude', 'longitude', 'brightness', 'acq_date'}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")

        # Process each row
        for _, row in df.iterrows():
            FirePoint.objects.create(
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                brightness=float(row.get('brightness', 0)),
                acq_date=row['acq_date'],
                frp=float(row.get('frp', 0)),
                confidence=int(row.get('confidence', 0)),
                geometry=Point(float(row['longitude']), float(row['latitude']))
            )
        return True
    except Exception as e:
        logger.error(f"Error processing firepoints: {str(e)}", exc_info=True)
        raise  # Re-raise for handling in the view

def process_province_file(uploaded_file):
    """Process province data from GeoJSON files"""
    try:
        content = uploaded_file.read().decode('utf-8')
        data = json.loads(content)
        
        for feature in data['features']:
            props = feature['properties']
            admin1Name = props.get('admin1Name') or props.get('ADM1_EN') or props.get('NAME_1')
            admin1Pcod = props.get('admin1Pcod') or props.get('ADM1_PCODE') or props.get('PCODE_1')
            
            if not admin1Name:
                raise ValueError("Could not determine province name from properties")
            
            geometry = GEOSGeometry(json.dumps(feature['geometry']))
            if not isinstance(geometry, (Polygon, MultiPolygon)):
                raise ValueError("Province geometry must be Polygon or MultiPolygon")
            
            Province.objects.create(
                admin1Name=admin1Name,
                admin1Pcod=admin1Pcod,
                geometry=geometry
            )
        return True
    except Exception as e:
        logger.error(f"Error processing provinces: {str(e)}", exc_info=True)
        raise

def process_district_file(uploaded_file, auxiliary_files=None):
    """Process district data from Shapefiles or GeoJSON"""
    try:
        if uploaded_file.name.endswith('.geojson'):
            return _process_district_geojson(uploaded_file)
        elif uploaded_file.name.endswith('.shp'):
            if not auxiliary_files:
                raise ValueError("Shapefile upload requires auxiliary files (.shx, .dbf, etc.)")
            return _process_district_shapefile(uploaded_file, auxiliary_files)
        else:
            raise ValueError("Unsupported file format for districts")
    except Exception as e:
        logger.error(f"Error processing districts: {str(e)}", exc_info=True)
        raise

def _process_district_geojson(uploaded_file):
    """Helper for GeoJSON district processing"""
    content = uploaded_file.read().decode('utf-8')
    data = json.loads(content)
    
    for feature in data['features']:
        props = feature['properties']
        admin2Name = props.get('admin2Name') or props.get('ADM2_EN') or props.get('NAME_2')
        admin2Pcod = props.get('admin2Pcod') or props.get('ADM2_PCODE') or props.get('PCODE_2')
        admin1Name = props.get('admin1Name') or props.get('ADM1_EN') or props.get('NAME_1')
        
        geometry = GEOSGeometry(json.dumps(feature['geometry']))
        if not isinstance(geometry, (Polygon, MultiPolygon)):
            raise ValueError("District geometry must be Polygon or MultiPolygon")
        
        District.objects.create(
            admin2Name=admin2Name,
            admin2Pcod=admin2Pcod,
            admin1Name=admin1Name,
            geometry=geometry
        )
    return True

def _process_district_shapefile(shp_file, auxiliary_files):
    """Helper for Shapefile district processing"""
    # Create temporary directory for shapefile components
    import tempfile
    import os
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save all shapefile components
        shp_path = os.path.join(temp_dir, shp_file.name)
        with open(shp_path, 'wb+') as destination:
            for chunk in shp_file.chunks():
                destination.write(chunk)
        
        for aux_file in auxiliary_files:
            aux_path = os.path.join(temp_dir, aux_file.name)
            with open(aux_path, 'wb+') as destination:
                for chunk in aux_file.chunks():
                    destination.write(chunk)
        
        # Process the shapefile
        ds = DataSource(shp_path)
        layer = ds[0]
        
        for feature in layer:
            District.objects.create(
                admin2Name=feature.get('admin2Name'),
                admin2Pcod=feature.get('admin2Pcod'),
                admin1Name=feature.get('admin1Name'),
                geometry=feature.geom.geos
            )
    return True