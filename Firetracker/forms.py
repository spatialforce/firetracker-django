# forms.py
from django import forms
from .models import GeoDataUpload
import os

class GeoDataUploadForm(forms.ModelForm):
    DATA_TYPE_CHOICES = [
        ('firepoint', 'Fire Points'),
        ('province', 'Provinces'),
        ('district', 'Districts'),
    ]
    
    UPLOAD_FORMAT_CHOICES = [
        ('json', 'GeoJSON'),
        ('shp', 'Shapefile'),
    ]
    
    data_type = forms.ChoiceField(
        choices=DATA_TYPE_CHOICES,
        label="Data Type",
        help_text="Select the type of geographic data you're uploading"
    )
    
    upload_format = forms.ChoiceField(
        choices=UPLOAD_FORMAT_CHOICES,
        label="File Format",
        help_text="Select the format of your data file"
    )
    
    class Meta:
        model = GeoDataUpload
        fields = ['title', 'data_type', 'upload_format', 'data_file']
        labels = {
            'data_file': 'Data File',
        }
        help_texts = {
            'data_file': 'Upload GeoJSON (.json/.geojson) or Shapefile ZIP (.zip)'
        }

    def clean(self):
        cleaned_data = super().clean()
        data_type = cleaned_data.get('data_type')
        upload_format = cleaned_data.get('upload_format')
        data_file = cleaned_data.get('data_file')
        
        if not data_file:
            return cleaned_data
            
        ext = os.path.splitext(data_file.name)[1].lower()
        
        # Validate CSV
        if upload_format == 'csv':
            if ext != '.csv':
                raise forms.ValidationError("CSV upload requires a .csv file")
            if data_type != 'firepoint':
                raise forms.ValidationError("CSV format only supported for FirePoints")
        
        # Validate GeoJSON
        elif upload_format == 'json':
            if ext not in ['.json', '.geojson']:
                raise forms.ValidationError("GeoJSON upload requires a .json or .geojson file")
        
        # Validate Shapefile
        elif upload_format == 'shp':
            if ext != '.zip':
                raise forms.ValidationError("Shapefile upload requires a ZIP archive")
            if data_type not in ['province', 'district']:
                raise forms.ValidationError("Shapefiles only supported for Provinces and Districts")
        
        return cleaned_data