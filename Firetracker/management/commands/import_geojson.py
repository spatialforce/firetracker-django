import os
import json
from django.core.management.base import BaseCommand
from django.contrib.gis.geos import GEOSGeometry
from Firetracker.models import Province, District, FirePoint

class Command(BaseCommand):
    help = 'Import GeoJSON data into the database'
    
    def handle(self, *args, **options):
        # Get the base directory (where manage.py is)
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        DATA_DIR = os.path.join(BASE_DIR, 'data')
        
        # Define paths to match YOUR EXACT FILENAMES
        provinces_path = os.path.join(DATA_DIR, 'Province.json')
        districts_path = os.path.join(DATA_DIR, 'Districts.json')  # Changed to plural
        firepoints_path = os.path.join(DATA_DIR, 'Firepoints.json')
        
        # Debug output
        print(f"Looking for Province.json at: {provinces_path}")
        print(f"Looking for Districts.json at: {districts_path}") 
        print(f"Looking for Firepoints.json at: {firepoints_path}")
        
        # Clear existing data
        self.stdout.write("Clearing existing data...")
        Province.objects.all().delete()
        District.objects.all().delete()
        FirePoint.objects.all().delete()
        
        # ===== IMPORT PROVINCES =====
        if os.path.exists(provinces_path):
            try:
                with open(provinces_path) as f:
                    data = json.load(f)
                    count = 0
                    for feature in data['features']:
                        Province.objects.create(
                            admin1Name=feature['properties']['admin1Name'],
                            admin1Pcod=feature['properties']['admin1Pcod'],
                            geometry=GEOSGeometry(json.dumps(feature['geometry'])))
                        count += 1
                    self.stdout.write(self.style.SUCCESS(f"Successfully imported {count} provinces"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error importing provinces: {str(e)}"))
        else:
            self.stdout.write(self.style.ERROR(f"Province.json not found at: {provinces_path}"))
        
        # ===== IMPORT DISTRICTS =====
        if os.path.exists(districts_path):
            try:
                with open(districts_path) as f:
                    data = json.load(f)
                    count = 0
                    for feature in data['features']:
                        District.objects.create(
                            admin2Name=feature['properties']['admin2Name'],
                            admin2Pcod=feature['properties']['admin2Pcod'],
                            admin1Name=feature['properties']['admin1Name'],
                            geometry=GEOSGeometry(json.dumps(feature['geometry'])))
                        count += 1
                    self.stdout.write(self.style.SUCCESS(f"Successfully imported {count} districts"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error importing districts: {str(e)}"))
        else:
            self.stdout.write(self.style.ERROR(f"Districts.json not found at: {districts_path}"))
        
        # ===== IMPORT FIREPOINTS =====
        if os.path.exists(firepoints_path):
            try:
                with open(firepoints_path) as f:
                    data = json.load(f)
                    count = 0
                    for feature in data['features']:
                        FirePoint.objects.create(
                            latitude=feature['properties']['latitude'],
                            longitude=feature['properties']['longitude'],
                            brightness=feature['properties']['brightness'],
                            acq_date=feature['properties']['acq_date'],
                            frp=feature['properties']['frp'],
                            confidence=feature['properties']['confidence'],
                            geometry=GEOSGeometry(json.dumps(feature['geometry'])))
                        count += 1
                    self.stdout.write(self.style.SUCCESS(f"Successfully imported {count} firepoints"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error importing firepoints: {str(e)}"))
        else:
            self.stdout.write(self.style.ERROR(f"Firepoints.json not found at: {firepoints_path}"))