from django.contrib.gis.db import models
from django.contrib.gis.geos import Point, Polygon, MultiPolygon, GEOSGeometry
from django.db import models as django_models
from django.db import transaction
from django.core.exceptions import ValidationError
import logging
import pandas as pd
import zipfile
import tempfile
import os
import json
import time
from datetime import datetime
from django.utils.timezone import make_aware

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define the validator function first
def validate_file_extension(value):
    ext = os.path.splitext(value.name)[1].lower()
    valid_extensions = ['.csv', '.json', '.geojson', '.zip']
    if ext not in valid_extensions:
        raise ValidationError(f'Unsupported file extension. Supported formats: {", ".join(valid_extensions)}')

class Province(models.Model):
    admin1Name = models.CharField(max_length=100)
    admin1Pcod = models.CharField(max_length=20, unique=True)
    geometry = models.MultiPolygonField(srid=4326)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    def __str__(self):
        return self.admin1Name

    def clean(self):
        if Province.objects.filter(admin1Pcod=self.admin1Pcod).exclude(pk=self.pk).exists():
            raise ValidationError({'admin1Pcod': 'This province code already exists'})

    class Meta:
        verbose_name = "Province"
        verbose_name_plural = "Provinces"
        ordering = ['admin1Name']
        indexes = [models.Index(fields=['admin1Pcod'])]

class District(models.Model):
    admin2Name = models.CharField(max_length=100)
    admin2Pcod = models.CharField(max_length=20, unique=True)
    admin1Name = models.CharField(max_length=100)
    province = models.ForeignKey(Province, on_delete=models.CASCADE, null=True, blank=True)
    geometry = models.MultiPolygonField(srid=4326)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    def __str__(self):
        return f"{self.admin2Name} ({self.admin1Name})"

    def clean(self):
        if District.objects.filter(admin2Pcod=self.admin2Pcod).exclude(pk=self.pk).exists():
            raise ValidationError({'admin2Pcod': 'This district code already exists'})

    class Meta:
        verbose_name = "District"
        verbose_name_plural = "Districts"
        ordering = ['admin1Name', 'admin2Name']
        indexes = [
            models.Index(fields=['admin2Pcod']),
            models.Index(fields=['admin1Name']),
        ]

class FirePoint(models.Model):
    latitude = models.FloatField()
    longitude = models.FloatField()
    brightness = models.FloatField(null=True, blank=True)
    acq_date = models.DateTimeField()
    frp = models.FloatField(null=True, blank=True)
    confidence = models.IntegerField(
        null=True, 
        blank=True,
        choices=[(i, str(i)) for i in range(0, 101, 10)]
    )
    geometry = models.PointField(srid=4326)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    def __str__(self):
        return f"FirePoint at {self.latitude:.4f},{self.longitude:.4f} on {self.acq_date.strftime('%Y-%m-%d')}"

    def clean(self):
        if not (-90 <= self.latitude <= 90):
            raise ValidationError({'latitude': 'Latitude must be between -90 and 90'})
        if not (-180 <= self.longitude <= 180):
            raise ValidationError({'longitude': 'Longitude must be between -180 and 180'})

    class Meta:
        verbose_name = "Fire Point"
        verbose_name_plural = "Fire Points"
        ordering = ['-acq_date']
        indexes = [
            models.Index(fields=['acq_date']),
            models.Index(fields=['latitude', 'longitude']),
        ]

class GeoDataUpload(django_models.Model):
    DATA_TYPES = (
        ('firepoint', 'Fire Points'),
        ('province', 'Provinces'),
        ('district', 'Districts'),
    )
    
    UPLOAD_FORMATS = (
        ('csv', 'CSV'),
        ('json', 'GeoJSON'),
        ('shp', 'Shapefile'),
    )
    
    title = django_models.CharField(max_length=255)
    data_type = django_models.CharField(max_length=10, choices=DATA_TYPES)
    upload_format = django_models.CharField(max_length=10, choices=UPLOAD_FORMATS)
    data_file = django_models.FileField(
        upload_to='geodata_uploads/%Y/%m/%d/',
        validators=[validate_file_extension],
        help_text="Upload data file in the selected format"
    )
    processed = django_models.BooleanField(default=False)
    processing_errors = django_models.TextField(null=True, blank=True)
    records_processed = django_models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)
    processing_time = models.DurationField(null=True, blank=True)

    def __str__(self):
        return f"{self.title} ({self.get_data_type_display()})"

    def clean(self):
        super().clean()
        if not self.data_file:
            return
            
        ext = os.path.splitext(self.data_file.name)[1].lower()
        
        if self.upload_format == 'shp' and ext != '.zip':
            raise ValidationError({'data_file': 'Shapefile upload requires a ZIP archive'})
        elif self.upload_format == 'json' and ext not in ['.json', '.geojson']:
            raise ValidationError({'data_file': 'GeoJSON upload requires a .json or .geojson file'})
        elif self.upload_format == 'csv' and ext != '.csv':
            raise ValidationError({'data_file': 'CSV upload requires a .csv file'})
            
        if self.data_type == 'firepoint' and self.upload_format not in ['csv', 'json']:
            raise ValidationError({'upload_format': 'Fire Points only support CSV or GeoJSON format'})
        elif self.data_type in ['province', 'district'] and self.upload_format not in ['json', 'shp']:
            raise ValidationError({'upload_format': 'Provinces/Districts only support GeoJSON or Shapefile format'})

    def debug_geojson(self):
        """Debug method to check GeoJSON structure and property matching"""
        try:
            with open(self.data_file.path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.debug("\n=== GEOJSON DEBUG ===")
            logger.debug(f"File: {self.data_file.path}")
            logger.debug(f"Type: {data.get('type')}")
            logger.debug(f"Feature count: {len(data.get('features', []))}")
            
            if data.get('features'):
                first_feature = data['features'][0]
                logger.debug("\nFirst feature properties:")
                logger.debug(first_feature.get('properties', {}))
                
                # Test property matching
                props = first_feature.get('properties', {})
                props_lower = {k.lower(): v for k, v in props.items()}
                
                logger.debug("\nProperty matching test:")
                logger.debug(f"ADM1_PCODE found: {'adm1_pcode' in props_lower}")
                logger.debug(f"ADM1_EN found: {'adm1_en' in props_lower}")
                logger.debug(f"All lowercase props: {props_lower}")
                
                # Test geometry
                if first_feature.get('geometry'):
                    try:
                        geom = GEOSGeometry(json.dumps(first_feature['geometry']))
                        logger.debug(f"Geometry type: {geom.geom_type}")
                        logger.debug(f"Geometry valid: {geom.valid}")
                        logger.debug(f"Geometry area: {geom.area}")
                    except Exception as e:
                        logger.error(f"Geometry error: {str(e)}")
            
            return True
        except Exception as e:
            logger.error(f"Debug error: {str(e)}")
            return False

    def process(self):
        start_time = datetime.now()
        self.processing_errors = None
        self.records_processed = 0
        
        try:
            logger.info(f"Starting processing of {self.title} (ID: {self.id})")
            
            if not os.path.exists(self.data_file.path):
                raise FileNotFoundError(f"File not found at {self.data_file.path}")
            
            # Run debug first (skip for shapefiles)
            if self.upload_format != 'shp':
                self.debug_geojson()
            
            with transaction.atomic():
                if self.data_type == 'province':
                    if self.upload_format == 'shp':
                        result, count = self._process_shapefile('province')
                    else:
                        result, count = self._process_provinces_geojson_enhanced()
                elif self.data_type == 'district':
                    if self.upload_format == 'shp':
                        result, count = self._process_shapefile('district')
                    else:
                        result, count = self._process_districts_geojson_enhanced()
                elif self.data_type == 'firepoint':
                    if self.upload_format == 'csv':
                        result, count = self._process_firepoints_csv()
                    else:
                        result, count = self._process_firepoints_geojson()
                else:
                    raise ValueError(f"Unknown data type: {self.data_type}")
                
                if result:
                    self.processed = True
                    self.records_processed = count
                    self.processing_time = datetime.now() - start_time
                    self.save()
                    logger.info(f"Successfully processed {self.title}. Created {count} records")
                    return True
                return False
                
        except Exception as e:
            error_msg = f"Error processing {self.title}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.processing_errors = error_msg
            self.processed = False
            self.save()
            return False

    def _process_provinces_geojson_enhanced(self):
        """Enhanced GeoJSON processor with detailed logging"""
        try:
            # Load the GeoJSON file with detailed logging
            with open(self.data_file.path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Loaded GeoJSON file: {self.data_file.path}")
                logger.debug(f"GeoJSON type: {data.get('type')}")
                logger.debug(f"Number of features: {len(data.get('features', []))}")
            
            # Validate GeoJSON structure
            if data.get('type') != 'FeatureCollection':
                raise ValueError("GeoJSON must be a FeatureCollection")
            
            features = data.get('features', [])
            created_count = 0
            skipped_count = 0
            
            logger.info(f"Found {len(features)} features to process")
            
            for i, feature in enumerate(features):
                try:
                    logger.debug(f"\n=== Processing feature {i} ===")
                    
                    # Skip features without geometry
                    if not feature.get('geometry'):
                        logger.warning(f"Feature {i} has no geometry - skipping")
                        skipped_count += 1
                        continue
                    
                    # Extract properties with enhanced logging
                    props = feature.get('properties', {})
                    props_lower = {k.lower(): v for k, v in props.items()}
                    logger.debug(f"All properties (original case): {props}")
                    logger.debug(f"All properties (lowercase): {props_lower}")
                    
                    # Get province code and name with flexible field matching
                    pcod = props.get('ADM1_PCODE') or props_lower.get('adm1_pcode')
                    name = props.get('ADM1_EN') or props_lower.get('adm1_en')
                    
                    if not pcod:
                        logger.error(f"Feature {i} missing PCODE - available properties: {list(props.keys())}")
                        skipped_count += 1
                        continue
                    if not name:
                        logger.error(f"Feature {i} missing NAME - available properties: {list(props.keys())}")
                        skipped_count += 1
                        continue
                    
                    logger.debug(f"Extracted values - PCODE: {pcod}, NAME: {name}")
                    
                    # Process geometry with validation
                    try:
                        geometry_data = feature['geometry']
                        logger.debug(f"Geometry type from data: {geometry_data.get('type')}")
                        
                        geometry = GEOSGeometry(json.dumps(geometry_data))
                        logger.debug(f"GEOS Geometry type: {geometry.geom_type}")
                        logger.debug(f"GEOS Geometry valid: {geometry.valid}")
                        
                        # Handle both Polygon and MultiPolygon
                        if geometry.geom_type == 'Polygon':
                            logger.debug("Converting Polygon to MultiPolygon")
                            geometry = MultiPolygon([geometry])
                        elif geometry.geom_type != 'MultiPolygon':
                            logger.warning(f"Feature {i} has unsupported geometry type: {geometry.geom_type}")
                            skipped_count += 1
                            continue
                        
                        # Validate geometry
                        if not geometry.valid:
                            logger.warning(f"Feature {i} has invalid geometry - attempting to fix")
                            original_area = geometry.area
                            geometry = geometry.buffer(0)
                            logger.debug(f"Fixed geometry - Valid: {geometry.valid}, Original area: {original_area}, New area: {geometry.area}")
                            if not geometry.valid:
                                logger.error(f"Could not fix invalid geometry for feature {i}")
                                skipped_count += 1
                                continue
                        
                        # Check if province already exists
                        existing = Province.objects.filter(admin1Pcod=pcod).first()
                        if existing:
                            logger.debug(f"Province with PCODE {pcod} already exists (ID: {existing.id})")
                        
                        # Create or update province
                        province, created = Province.objects.update_or_create(
                            admin1Pcod=pcod,
                            defaults={
                                'admin1Name': name,
                                'geometry': geometry
                            }
                        )
                        
                        if created:
                            created_count += 1
                            logger.info(f"Created new province: {name} ({pcod})")
                        else:
                            logger.info(f"Updated existing province: {name} ({pcod})")
                            
                    except Exception as geom_error:
                        logger.error(f"Geometry processing error for feature {i}: {str(geom_error)}", exc_info=True)
                        skipped_count += 1
                        continue
                        
                except Exception as e:
                    logger.error(f"Error processing feature {i}: {str(e)}", exc_info=True)
                    skipped_count += 1
                    continue
            
            logger.info(f"\n=== Processing Summary ===")
            logger.info(f"Total features: {len(features)}")
            logger.info(f"Created: {created_count}")
            logger.info(f"Updated: {len(features) - created_count - skipped_count}")
            logger.info(f"Skipped: {skipped_count}")
            logger.info(f"Finished processing. Total provinces created/updated: {created_count}")
            
            return True, created_count
            
        except Exception as e:
            error_msg = f"Failed to process GeoJSON: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg)

    def _process_districts_geojson_enhanced(self):
        """Enhanced districts GeoJSON processor with flexible property matching"""
        try:
            with open(self.data_file.path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data.get('type') != 'FeatureCollection':
                raise ValueError("GeoJSON must be a FeatureCollection")
            
            features = data.get('features', [])
            created_count = 0
            skipped_count = 0
            
            logger.info(f"Found {len(features)} district features to process")
            
            for i, feature in enumerate(features):
                try:
                    logger.debug(f"\n=== Processing district feature {i} ===")
                    
                    # Skip features without geometry
                    if not feature.get('geometry'):
                        logger.warning(f"Feature {i} has no geometry - skipping")
                        skipped_count += 1
                        continue
                        
                    props = feature.get('properties', {})
                    props_lower = {k.lower(): v for k, v in props.items()}
                    logger.debug(f"All properties (original case): {props}")
                    logger.debug(f"All properties (lowercase): {props_lower}")
                    
                    # Flexible property matching for district fields
                    pcod = props.get('admin2Pcod') or props_lower.get('admin2pcod') or props_lower.get('adm2_pcode') or props_lower.get('pcode')
                    name = props.get('admin2Name') or props_lower.get('admin2name') or props_lower.get('adm2_en') or props_lower.get('name')
                    parent_pcod = props.get('admin1Pcod') or props_lower.get('admin1pcod') or props_lower.get('adm1_pcode')
                    parent_name = props.get('admin1Name') or props_lower.get('admin1name') or props_lower.get('adm1_en')
                    
                    if not pcod:
                        logger.error(f"Feature {i} missing district PCODE - available properties: {list(props.keys())}")
                        skipped_count += 1
                        continue
                    if not name:
                        logger.error(f"Feature {i} missing district NAME - available properties: {list(props.keys())}")
                        skipped_count += 1
                        continue
                    
                    logger.debug(f"Extracted values - District PCODE: {pcod}, Name: {name}")
                    logger.debug(f"Parent province - PCODE: {parent_pcod}, Name: {parent_name}")
                    
                    # Process geometry
                    try:
                        geometry_data = feature['geometry']
                        geometry = GEOSGeometry(json.dumps(geometry_data))
                        
                        if geometry.geom_type == 'Polygon':
                            geometry = MultiPolygon([geometry])
                        elif geometry.geom_type != 'MultiPolygon':
                            logger.warning(f"Feature {i} has unsupported geometry type: {geometry.geom_type}")
                            skipped_count += 1
                            continue
                        
                        if not geometry.valid:
                            logger.warning(f"Feature {i} has invalid geometry - attempting to fix")
                            geometry = geometry.buffer(0)
                            if not geometry.valid:
                                logger.error(f"Could not fix invalid geometry for feature {i}")
                                skipped_count += 1
                                continue
                        
                        # Find related province
                        province = None
                        if parent_pcod:
                            province = Province.objects.filter(admin1Pcod=parent_pcod).first()
                        elif parent_name:
                            province = Province.objects.filter(admin1Name=parent_name).first()
                        
                        if province:
                            logger.debug(f"Found parent province: {province.admin1Name} ({province.admin1Pcod})")
                        
                        # Create or update district
                        district, created = District.objects.update_or_create(
                            admin2Pcod=pcod,
                            defaults={
                                'admin2Name': name,
                                'admin1Name': parent_name or (province.admin1Name if province else ''),
                                'province': province,
                                'geometry': geometry
                            }
                        )
                        
                        if created:
                            created_count += 1
                            logger.info(f"Created new district: {name} ({pcod})")
                        else:
                            logger.info(f"Updated existing district: {name} ({pcod})")
                            
                    except Exception as geom_error:
                        logger.error(f"Geometry processing error for feature {i}: {str(geom_error)}", exc_info=True)
                        skipped_count += 1
                        continue
                        
                except Exception as e:
                    logger.error(f"Error processing district feature {i}: {str(e)}", exc_info=True)
                    skipped_count += 1
                    continue
            
            logger.info(f"\n=== District Processing Summary ===")
            logger.info(f"Total features: {len(features)}")
            logger.info(f"Created: {created_count}")
            logger.info(f"Updated: {len(features) - created_count - skipped_count}")
            logger.info(f"Skipped: {skipped_count}")
            logger.info(f"Finished processing. Total districts created/updated: {created_count}")
            
            return True, created_count
            
        except Exception as e:
            logger.error(f"Failed to process districts GeoJSON: {str(e)}", exc_info=True)
            raise

    def _process_firepoints_csv(self):
        try:
            df = pd.read_csv(self.data_file.path)
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {str(e)}")
        
        required_columns = {'latitude', 'longitude', 'acq_date'}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"CSV missing required columns: {', '.join(missing)}")
        
        firepoints = []
        for _, row in df.iterrows():
            try:
                try:
                    acq_date = pd.to_datetime(row['acq_date'])
                    if not acq_date.tzinfo:
                        acq_date = make_aware(acq_date)
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid date format in row {_}: {row['acq_date']}")
                
                fp = FirePoint(
                    latitude=float(row['latitude']),
                    longitude=float(row['longitude']),
                    acq_date=acq_date,
                    brightness=float(row.get('brightness', 0)),
                    frp=float(row.get('frp', 0)),
                    confidence=int(row.get('confidence', 0)) if str(row.get('confidence', '')).isdigit() else None,
                    geometry=Point(float(row['longitude']), float(row['latitude']))
                )
                
                fp.full_clean()
                firepoints.append(fp)
                
            except Exception as e:
                logger.error(f"Error processing row {_}: {str(e)}")
                continue
        
        try:
            batch_size = 1000
            for i in range(0, len(firepoints), batch_size):
                FirePoint.objects.bulk_create(firepoints[i:i+batch_size])
            return True, len(firepoints)
        except Exception as e:
            logger.error(f"Bulk create failed: {str(e)}")
            return False, 0

    def _process_firepoints_geojson(self):
        try:
            with open(self.data_file.path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data.get('type') != 'FeatureCollection':
                raise ValueError("GeoJSON must be a FeatureCollection")
            
            features = data.get('features', [])
            firepoints = []
            
            for i, feature in enumerate(features):
                try:
                    if not feature.get('geometry'):
                        logger.warning(f"Feature {i} has no geometry - skipping")
                        continue
                        
                    props = feature.get('properties', {})
                    coords = feature['geometry']['coordinates']
                    
                    if 'acq_date' not in props:
                        raise ValueError("Missing acq_date property")
                    
                    try:
                        acq_date = pd.to_datetime(props['acq_date'])
                        if not acq_date.tzinfo:
                            acq_date = make_aware(acq_date)
                    except (ValueError, TypeError) as e:
                        raise ValueError(f"Invalid date format: {props['acq_date']}")
                    
                    fp = FirePoint(
                        latitude=coords[1],
                        longitude=coords[0],
                        brightness=props.get('brightness'),
                        acq_date=acq_date,
                        frp=props.get('frp'),
                        confidence=int(props.get('confidence', 0)) if str(props.get('confidence', '')).isdigit() else None,
                        geometry=Point(coords[0], coords[1])
                    )
                    
                    fp.full_clean()
                    firepoints.append(fp)
                    
                except Exception as e:
                    logger.error(f"Error processing feature {i}: {str(e)}")
                    continue
            
            batch_size = 1000
            for i in range(0, len(firepoints), batch_size):
                FirePoint.objects.bulk_create(firepoints[i:i+batch_size])
            return True, len(firepoints)
            
        except Exception as e:
            logger.error(f"Failed to process GeoJSON: {str(e)}")
            raise

    def _process_shapefile(self, data_type):
        from django.contrib.gis.gdal import DataSource
        
        temp_dir = None
        try:
            # Create a uniquely named temporary directory
            temp_dir = tempfile.mkdtemp(prefix='geodata_')
            logger.info(f"Created temporary directory: {temp_dir}")
            
            # Extract the zip file
            with zipfile.ZipFile(self.data_file.path) as zip_ref:
                zip_ref.extractall(temp_dir)
                logger.info(f"Extracted shapefile to: {temp_dir}")
            
            # Find the .shp file
            shp_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.shp')]
            if not shp_files:
                raise ValueError("No .shp file found in ZIP archive")
            
            shp_path = os.path.join(temp_dir, shp_files[0])
            logger.info(f"Processing shapefile: {shp_path}")
            
            # Process the shapefile with explicit cleanup
            try:
                # Open the DataSource in a context manager if possible
                ds = DataSource(shp_path)
                layer = ds[0]
                
                if data_type == 'province':
                    result, count = self._process_shapefile_provinces(layer)
                elif data_type == 'district':
                    result, count = self._process_shapefile_districts(layer)
                else:
                    raise ValueError(f"Unknown data type for shapefile: {data_type}")
                
                # Explicitly close the DataSource and release resources
                del layer
                del ds
                
                return result, count
                
            except Exception as e:
                logger.error(f"Error processing shapefile data: {str(e)}", exc_info=True)
                raise
                
        except Exception as e:
            logger.error(f"Error processing shapefile: {str(e)}", exc_info=True)
            raise
            
        finally:
            # Robust cleanup of temporary files
            if temp_dir and os.path.exists(temp_dir):
                logger.info(f"Cleaning up temporary directory: {temp_dir}")
                self._cleanup_temp_dir(temp_dir)

    def _process_shapefile_provinces(self, layer):
        created_count = 0
        
        try:
            # Get field names - handle different GDAL versions
            if hasattr(layer, 'fields'):  # Newer GDAL versions
                if callable(layer.fields):  # If it's a method
                    field_names = layer.fields()
                else:  # If it's a property
                    field_names = layer.fields
            else:  # Older GDAL versions
                field_names = [field.name for field in layer.schema]
            
            # Create field map (lowercase field names to original names)
            field_map = {name.lower(): name for name in field_names}
            logger.debug(f"Field map: {field_map}")
            
            for i, feature in enumerate(layer):
                try:
                    if not feature.geom:
                        logger.warning(f"Feature {i} has no geometry - skipping")
                        continue
                        
                    pcod = None
                    name = None
                    
                    # Check all possible field name variations
                    for field in ['admin1pcod', 'pcod', 'adm1_pcode', 'pcode', 'adm1_pcod']:
                        if field in field_map:
                            try:
                                pcod = feature.get(field_map[field])
                                if pcod:
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting field {field}: {str(e)}")
                                continue
                    
                    for field in ['admin1name', 'name', 'adm1_en', 'adm1name', 'adm1name_en']:
                        if field in field_map:
                            try:
                                name = feature.get(field_map[field])
                                if name:
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting field {field}: {str(e)}")
                                continue
                    
                    if not pcod or not name:
                        logger.warning(f"Feature {i} missing required fields - skipping")
                        continue
                        
                    geometry = GEOSGeometry(feature.geom.wkt)
                    if not isinstance(geometry, (Polygon, MultiPolygon)):
                        logger.warning(f"Feature {i} geometry is not a Polygon/MultiPolygon - skipping")
                        continue
                    
                    # Convert Polygon to MultiPolygon if needed
                    if isinstance(geometry, Polygon):
                        geometry = MultiPolygon([geometry])
                    
                    # Validate geometry
                    if not geometry.valid:
                        logger.warning(f"Feature {i} has invalid geometry - attempting to fix")
                        geometry = geometry.buffer(0)
                    
                    province, created = Province.objects.update_or_create(
                        admin1Pcod=pcod,
                        defaults={
                            'admin1Name': name,
                            'geometry': geometry
                        }
                    )
                    if created:
                        created_count += 1
                        logger.info(f"Created new province: {name} ({pcod})")
                    else:
                        logger.info(f"Updated existing province: {name} ({pcod})")
                        
                except Exception as e:
                    logger.error(f"Error processing shapefile feature {i}: {str(e)}", exc_info=True)
                    continue
        
        except Exception as e:
            logger.error(f"Error setting up shapefile processing: {str(e)}", exc_info=True)
            raise
            
        logger.info(f"Finished processing. Total provinces created/updated: {created_count}")
        return True, created_count

    def _process_shapefile_districts(self, layer):
        created_count = 0
        
        try:
            # Get field names - handle different GDAL versions
            if hasattr(layer, 'fields'):  # Newer GDAL versions
                if callable(layer.fields):  # If it's a method
                    field_names = layer.fields()
                else:  # If it's a property
                    field_names = layer.fields
            else:  # Older GDAL versions
                field_names = [field.name for field in layer.schema]
            
            # Create field map (lowercase field names to original names)
            field_map = {name.lower(): name for name in field_names}
            logger.debug(f"Field map: {field_map}")
            
            for i, feature in enumerate(layer):
                try:
                    if not feature.geom:
                        logger.warning(f"Feature {i} has no geometry - skipping")
                        continue
                        
                    pcod = None
                    name = None
                    parent_pcod = None
                    parent_name = None
                    
                    # Check all possible field name variations
                    for field in ['admin2pcod', 'pcod', 'adm2_pcode', 'pcode', 'adm2_pcod']:
                        if field in field_map:
                            try:
                                pcod = feature.get(field_map[field])
                                if pcod:
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting field {field}: {str(e)}")
                                continue
                    
                    for field in ['admin2name', 'name', 'adm2_en', 'adm2name', 'adm2name_en']:
                        if field in field_map:
                            try:
                                name = feature.get(field_map[field])
                                if name:
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting field {field}: {str(e)}")
                                continue
                    
                    for field in ['admin1pcod', 'parentpcod', 'adm1_pcode', 'adm1_pcod']:
                        if field in field_map:
                            try:
                                parent_pcod = feature.get(field_map[field])
                                if parent_pcod:
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting field {field}: {str(e)}")
                                continue
                    
                    for field in ['admin1name', 'parentname', 'adm1_en', 'adm1name', 'adm1name_en']:
                        if field in field_map:
                            try:
                                parent_name = feature.get(field_map[field])
                                if parent_name:
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting field {field}: {str(e)}")
                                continue
                    
                    if not pcod or not name:
                        logger.warning(f"Feature {i} missing required fields - skipping")
                        continue
                        
                    geometry = GEOSGeometry(feature.geom.wkt)
                    if not isinstance(geometry, (Polygon, MultiPolygon)):
                        logger.warning(f"Feature {i} geometry is not a Polygon/MultiPolygon - skipping")
                        continue
                    
                    # Convert Polygon to MultiPolygon if needed
                    if isinstance(geometry, Polygon):
                        geometry = MultiPolygon([geometry])
                    
                    # Validate geometry
                    if not geometry.valid:
                        logger.warning(f"Feature {i} has invalid geometry - attempting to fix")
                        geometry = geometry.buffer(0)
                    
                    province = None
                    if parent_pcod:
                        province = Province.objects.filter(admin1Pcod=parent_pcod).first()
                    elif parent_name:
                        province = Province.objects.filter(admin1Name=parent_name).first()
                    
                    district, created = District.objects.update_or_create(
                        admin2Pcod=pcod,
                        defaults={
                            'admin2Name': name,
                            'admin1Name': parent_name or '',
                            'province': province,
                            'geometry': geometry
                        }
                    )
                    if created:
                        created_count += 1
                        logger.info(f"Created new district: {name} ({pcod})")
                    else:
                        logger.info(f"Updated existing district: {name} ({pcod})")
                        
                except Exception as e:
                    logger.error(f"Error processing shapefile feature {i}: {str(e)}", exc_info=True)
                    continue
        
        except Exception as e:
            logger.error(f"Error setting up shapefile processing: {str(e)}", exc_info=True)
            raise
            
        logger.info(f"Finished processing. Total districts created/updated: {created_count}")
        return True, created_count

    def _cleanup_temp_dir(self, temp_dir):
        """Robust temporary directory cleanup with retries and error handling"""
        max_attempts = 5
        delay = 0.5  # seconds between retries
        
        for attempt in range(max_attempts):
            try:
                # First try to remove all files
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            os.unlink(file_path)
                            logger.debug(f"Deleted file: {file_path}")
                        except Exception as e:
                            logger.warning(f"Could not delete file {file_path}: {str(e)}")
                            if attempt == max_attempts - 1:
                                raise
                
                # Then try to remove the directory itself
                try:
                    os.rmdir(temp_dir)
                    logger.info(f"Successfully removed temp directory: {temp_dir}")
                    return
                except OSError as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Failed to remove temp directory {temp_dir}: {str(e)}")
                        raise
            
            except Exception as e:
                if attempt == max_attempts - 1:
                    logger.error(f"Final attempt failed to cleanup temp dir {temp_dir}: {str(e)}")
                    # At this point, we've tried our best - log the error but don't crash
                    return
                time.sleep(delay)
                delay *= 2  # Exponential backoff

    class Meta:
        verbose_name = "Geo Data Upload"
        verbose_name_plural = "Geo Data Uploads"
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['data_type']),
            models.Index(fields=['processed']),
            models.Index(fields=['created_at']),
        ]