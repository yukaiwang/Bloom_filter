from . import views
from django.conf.urls import include, url
from django.views.generic import TemplateView
from django.core.urlresolvers import reverse


urlpatterns = [
	url(r'^$', views.initialize_filter, name='initialize_filter'),
	url(r'^search$', views.search, name='search'),
]