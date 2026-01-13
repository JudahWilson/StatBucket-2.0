from django.urls import path
from . import views 
# For css and js
from django.conf.urls.static import static
from django.conf import settings

# URL Configuration
urlpatterns = [
    # path('', views.index, name='index'),
]

# CSS and JS usually hosted from cloud server. in that case you may want to prefix below line with--if settings.DEBUG:
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
