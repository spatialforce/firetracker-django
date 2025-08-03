# Firetracker/urls.py
from django.urls import path
from . import views
from django.views.decorators.cache import never_cache

from django.urls import path
from . import views
from django.views.decorators.cache import never_cache

urlpatterns = [
    # Root endpoint
    path('', views.home, name='home'),
    
    # Data endpoints
    path('api/provinces/', never_cache(views.provinces_json), name='provinces_json'),
    path('api/districts/', never_cache(views.districts_json), name='districts_json'),
    path('api/firepoints/', never_cache(views.firepoints_json), name='firepoints_json'),
    path('api/data-status/', never_cache(views.data_status), name='data_status'),
]