# serializers.py
from rest_framework import serializers
from django.contrib.gis.geos import Point
from .models import Province, FirePoint

class ProvinceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Province
        fields = '__all__'
        geo_field = 'geometry'

class FirePointSerializer(serializers.ModelSerializer):
    class Meta:
        model = FirePoint
        fields = '__all__'
        geo_field = 'geometry'
    
    def create(self, validated_data):
        # Convert lat/long to Point if not already done
        if 'geometry' not in validated_data:
            validated_data['geometry'] = Point(
                validated_data['longitude'],
                validated_data['latitude']
            )
        return super().create(validated_data)