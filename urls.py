from django.conf.urls import url

from . import views

urlpatterns = [
#    url(r'^$', views.index, name='index'),
    url(r'^c$', views.compare, name='compare'), 
    url(r'^$', views.index, name='index'), #/difference_engine/ goes here.
    url(r'^(?P<resource_id_1>[^/]+)/vs/(?P<resource_id_2>[^/]+)$', views.compare, name='compare'), 
    #url(r'^(?P<table_name>[^/]+)/csv$', views.csv_view, name='show_csv'),
    #url(r'^(?P<table_name>[^/]+)/push$', views.export_table_to_ckan, name='export_to_ckan'),
    #url(r'data$', views.data, name='data'),
    #    url(r'^(?P<resource_id>[^/]+)/(?P<field>.*)/(?P<search_term>.*)$', views.results, name='results'),
    ]
