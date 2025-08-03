from django.urls import path
from . import views
from django.views.decorators.cache import never_cache

urlpatterns = [
  
    path('api/provinces/', never_cache(views.provinces_json), name='provinces_json'),
    path('api/districts/', never_cache(views.districts_json), name='districts_json'),
    path('api/firepoints/', never_cache(views.firepoints_json), name='firepoints_json'),
    path('api/data-status/', never_cache(views.data_status), name='data_status'),
]