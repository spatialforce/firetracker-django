# Firetracker-backendd/urls.py
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from Firetracker import views  # Import views from your app

urlpatterns = [
    # Admin interface at /admin/
    path('admin/', admin.site.urls),
    
    # API endpoints under /api/
    path('api/', include('Firetracker.urls')),
    
    # Root URL shows API documentation
    path('', views.api_documentation, name='api-docs'),
]

# Serve static files during development
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
