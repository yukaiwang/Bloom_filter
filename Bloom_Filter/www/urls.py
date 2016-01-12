from . import views
from django.conf.urls import include, url
from django.views.generic import TemplateView
from django.core.urlresolvers import reverse


urlpatterns = [
	url(r'^$', views.index, name='index'),
	url(r'^search$', views.search, name='search'),
]