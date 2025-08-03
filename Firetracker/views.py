from django.contrib.auth.decorators import user_passes_test, login_required
from django.http import JsonResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST, require_http_methods
from django.core.serializers import serialize
from django.contrib.gis.db.models.functions import AsGeoJSON
from django.views.decorators.cache import never_cache
from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import cache
from django.conf import settings
from .models import Province, District, FirePoint
import json
from datetime import datetime, timedelta
import logging
from functools import wraps

logger = logging.getLogger(__name__)

# =====================
# DECORATORS & HELPERS
# =====================

def api_response(format='json'):
    """Decorator to standardize API responses"""
    def decorator(view_func):
        @wraps(view_func)
        def wrapped_view(request, *args, **kwargs):
            try:
                result = view_func(request, *args, **kwargs)
                
                if format == 'json':
                    if isinstance(result, dict):
                        response_data = {
                            'status': 'success',
                            'data': result,
                            'timestamp': datetime.now().isoformat(),
                            'version': settings.API_VERSION
                        }
                        response = JsonResponse(response_data)
                    else:
                        return result  # Assume it's already a response
                
                # Set cache headers
                response['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                response['Pragma'] = 'no-cache'
                response['Expires'] = '0'
                return response
                
            except Exception as e:
                logger.error(f"API Error in {view_func.__name__}: {str(e)}", exc_info=True)
                error_data = {
                    'status': 'error',
                    'message': str(e),
                    'code': getattr(e, 'code', 500),
                    'timestamp': datetime.now().isoformat()
                }
                return JsonResponse(error_data, status=500)
        return wrapped_view
    return decorator

def is_superuser(user):
    """Check if user is authenticated superuser"""
    return user.is_authenticated and user.is_superuser

# =====================
# CORE VIEWS
# =====================

@never_cache
@require_GET
@api_response()
def home(request):
    """Root endpoint with API documentation"""
    return {
        'api': {
            'name': 'Firetracker API',
            'version': settings.API_VERSION,
            'documentation': f'{settings.DOCS_URL}/api' if hasattr(settings, 'DOCS_URL') else None
        },
        'endpoints': {
            'provinces': {
                'path': '/api/provinces/',
                'methods': ['GET'],
                'description': 'Returns GeoJSON of all provinces'
            },
            'districts': {
                'path': '/api/districts/',
                'methods': ['GET'],
                'description': 'Returns GeoJSON of all districts'
            },
            'firepoints': {
                'path': '/api/firepoints/',
                'methods': ['GET'],
                'parameters': {
                    'date_from': 'YYYY-MM-DD',
                    'date_to': 'YYYY-MM-DD',
                    'min_confidence': '0-100'
                }
            },
            'data_status': {
                'path': '/api/data-status/',
                'methods': ['GET'],
                'access': 'admin-only'
            }
        }
    }

# =====================
# DATA ENDPOINTS
# =====================

@never_cache
@require_GET
@api_response()
def provinces_json(request):
    """Endpoint for province GeoJSON data"""
    cache_key = 'provinces_geojson'
    cached_data = cache.get(cache_key)
    
    if cached_data and not settings.DEBUG:
        return cached_data
        
    provinces = Province.objects.annotate(geojson=AsGeoJSON('geometry')).values(
        'id', 'admin1Name', 'admin1Pcod', 'geojson'
    )
    
    result = list(provinces)
    cache.set(cache_key, result, timeout=3600)  # Cache for 1 hour
    return result

@never_cache
@require_GET
@api_response()
def districts_json(request):
    """Endpoint for district GeoJSON data"""
    cache_key = f'districts_geojson_{request.GET.urlencode()}'
    cached_data = cache.get(cache_key)
    
    if cached_data and not settings.DEBUG:
        return cached_data
        
    districts = District.objects.annotate(geojson=AsGeoJSON('geometry')).values(
        'id', 'admin2Name', 'admin2Pcod', 'admin1Name', 'geojson'
    )
    
    result = list(districts)
    cache.set(cache_key, result, timeout=3600)
    return result

@never_cache
@require_GET
@api_response()
def firepoints_json(request):
    """Endpoint for firepoint GeoJSON data with filtering"""
    # Generate cache key based on query parameters
    params = request.GET.dict()
    cache_key = f'firepoints_{hash(frozenset(params.items()))}'
    
    # Return cached response if available
    cached_data = cache.get(cache_key)
    if cached_data and not settings.DEBUG:
        return cached_data
    
    # Build queryset
    firepoints = FirePoint.objects.annotate(geojson=AsGeoJSON('geometry'))
    
    # Apply filters
    date_from = request.GET.get('date_from')
    date_to = request.GET.get('date_to')
    min_confidence = request.GET.get('min_confidence')
    
    if date_from:
        firepoints = firepoints.filter(acq_date__gte=date_from)
    if date_to:
        firepoints = firepoints.filter(acq_date__lte=date_to)
    if min_confidence:
        firepoints = firepoints.filter(confidence__gte=min_confidence)
    
    # Get last 30 days by default
    if not any([date_from, date_to, min_confidence]):
        default_date = datetime.now() - timedelta(days=30)
        firepoints = firepoints.filter(acq_date__gte=default_date)
    
    # Prepare response
    result = list(firepoints.values(
        'id', 'latitude', 'longitude', 'brightness',
        'acq_date', 'frp', 'confidence', 'geojson'
    ).order_by('-acq_date'))
    
    # Cache for 15 minutes (shorter TTL due to frequent updates)
    cache.set(cache_key, result, timeout=900)
    return result

# =====================
# ADMIN ENDPOINTS
# =====================

@never_cache
@require_GET
@user_passes_test(is_superuser)
@api_response()
def data_status(request):
    """Admin endpoint for data freshness check"""
    return {
        'provinces': {
            'count': Province.objects.count(),
            'latest': Province.objects.latest('updated_at').updated_at.isoformat() if Province.objects.exists() else None
        },
        'districts': {
            'count': District.objects.count(),
            'latest': District.objects.latest('updated_at').updated_at.isoformat() if District.objects.exists() else None
        },
        'firepoints': {
            'count': FirePoint.objects.count(),
            'latest': FirePoint.objects.latest('acq_date').acq_date.isoformat() if FirePoint.objects.exists() else None,
            'last_24h': FirePoint.objects.filter(
                acq_date__gte=datetime.now()-timedelta(days=1)
            ).count()
        }
    }

# =====================
# NEW FEATURES
# =====================

@never_cache
@require_GET
@api_response()
def data_overview(request):
    """Public statistics endpoint"""
    cache_key = 'data_overview'
    cached_data = cache.get(cache_key)
    
    if cached_data and not settings.DEBUG:
        return cached_data
    
    stats = {
        'province_count': Province.objects.count(),
        'district_count': District.objects.count(),
        'firepoint_count': FirePoint.objects.count(),
        'latest_firepoint_date': FirePoint.objects.latest('acq_date').acq_date.isoformat() 
            if FirePoint.objects.exists() else None,
        'recent_firepoints': FirePoint.objects.filter(
            acq_date__gte=datetime.now()-timedelta(days=7)
        ).count()
    }
    
    cache.set(cache_key, stats, timeout=3600)
    return stats

@never_cache
@require_http_methods(["POST"])
@csrf_exempt
@api_response()
def webhook_receiver(request):
    """Endpoint for receiving data update webhooks"""
    if request.method == 'POST':
        try:
            payload = json.loads(request.body)
            # Validate payload
            if payload.get('secret') != settings.WEBHOOK_SECRET:
                raise PermissionError("Invalid webhook secret")
            
            # Process update
            cache.clear()  # Clear all API caches
            logger.info("Webhook received - caches cleared")
            
            return {'status': 'cache_cleared'}
            
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON payload")
        except Exception as e:
            logger.error(f"Webhook error: {str(e)}")
            raise