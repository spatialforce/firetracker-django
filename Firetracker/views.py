
from django.contrib.auth.decorators import user_passes_test
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from django.core.serializers import serialize
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.views.decorators.cache import never_cache
from django.contrib.gis.geos import GEOSGeometry
from .models import Province, District, FirePoint
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def is_superuser(user):
    """Check if user is authenticated superuser"""
    return user.is_authenticated and user.is_superuser

def create_json_response(data, status=200):
    """Helper function to create consistent JSON responses"""
    response = JsonResponse({
        'status': 'success',
        'data': data,
        'timestamp': datetime.now().isoformat()
    }, status=status)
    
    # Set cache control headers
    response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    return response

def create_error_response(message, status=500):
    """Helper function for error responses"""
    logger.error(f"API Error: {message}")
    return JsonResponse({
        'status': 'error',
        'message': message,
        'timestamp': datetime.now().isoformat()
    }, status=status)

@never_cache
@require_GET
def provinces_json(request):
    """Endpoint for province GeoJSON data"""
    try:
        provinces = Province.objects.annotate(geojson=AsGeoJSON('geometry')).values(
            'id', 'admin1Name', 'admin1Pcod', 'geojson'
        )
        return create_json_response(list(provinces))
        
    except Exception as e:
        return create_error_response(str(e))

@never_cache
@require_GET
def districts_json(request):
    """Endpoint for district GeoJSON data"""
    try:
        districts = District.objects.annotate(geojson=AsGeoJSON('geometry')).values(
            'id', 'admin2Name', 'admin2Pcod', 'admin1Name', 'geojson'
        )
        return create_json_response(list(districts))
        
    except Exception as e:
        return create_error_response(str(e))

@never_cache
@require_GET
def firepoints_json(request):
    """Endpoint for firepoint GeoJSON data"""
    try:
        # Optional query parameters for filtering
        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        min_confidence = request.GET.get('min_confidence')
        
        # Base queryset
        firepoints = FirePoint.objects.annotate(geojson=AsGeoJSON('geometry'))
        
        # Apply filters if provided
        if date_from:
            firepoints = firepoints.filter(acq_date__gte=date_from)
        if date_to:
            firepoints = firepoints.filter(acq_date__lte=date_to)
        if min_confidence:
            firepoints = firepoints.filter(confidence__gte=min_confidence)
            
        # Final queryset with selected fields
        firepoints = firepoints.values(
            'id', 'latitude', 'longitude', 'brightness', 
            'acq_date', 'frp', 'confidence', 'geojson'
        ).order_by('-acq_date')
        
        return create_json_response(list(firepoints))
        
    except Exception as e:
        return create_error_response(str(e))

@never_cache
@require_GET
@user_passes_test(is_superuser)
def data_status(request):
    """Endpoint for checking data freshness (admin only)"""
    try:
        latest_data = {
            'provinces': Province.objects.latest('id').id if Province.objects.exists() else 0,
            'districts': District.objects.latest('id').id if District.objects.exists() else 0,
            'firepoints': FirePoint.objects.latest('id').id if FirePoint.objects.exists() else 0
        }
        return create_json_response(latest_data)
        
    except Exception as e:
        return create_error_response(str(e))

@never_cache
@require_GET
def data_overview(request):
    """Endpoint for basic data statistics"""
    try:
        stats = {
            'province_count': Province.objects.count(),
            'district_count': District.objects.count(),
            'firepoint_count': FirePoint.objects.count(),
            'latest_firepoint_date': FirePoint.objects.latest('acq_date').acq_date.isoformat() 
                if FirePoint.objects.exists() else None
        }
        return create_json_response(stats)
        
    except Exception as e:
        return create_error_response(str(e))