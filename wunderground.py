#!/usr/bin/env python
import urllib2
import json
from wundersettings import *
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from pprint import pprint

esIndex = city + '-weather-%(date)s' % {"date": datetime.utcnow().strftime("%Y.%m.%d") }
esIndexSettings = {
                   "settings": {
                     "number_of_shards": 5,
                     "number_of_replicas": 0,
                    }
                  }

#grab the data from wunderground and dump it into a dict
wunderUrl = 'http://api.wunderground.com/api/' + apiKey + '/conditions/alerts/q/' + state + '/' + city + '.json'
f = urllib2.urlopen(wunderUrl)
jsonString = f.read()
currentWeather = json.loads(jsonString)
f.close()

#build up an object to insert into elasticsearch
esObject = {
            '_index': esIndex,
            '_type': 'weather',
            '_source': currentWeather
           }

if esObject['_source']['current_observation']['observation_epoch']:
  esObject['_source']['@timestamp'] = datetime.utcfromtimestamp(float(esObject['_source']['current_observation']['local_epoch']))
else:
  esObject['_source']['@timestamp'] = datetime.utcnow()

if esObject['_source']['current_observation']['observation_location']['longitude']:
  esObject['_source']['current_observation']['observation_location']['bettermap_field'] = [
    float(esObject['_source']['current_observation']['observation_location']['longitude']),
    float(esObject['_source']['current_observation']['observation_location']['latitude'])
  ]

if esObject['_source']['current_observation']['relative_humidity']:
  try:
    esObject['_source']['current_observation']['relative_humidity_percent'] = float(esObject['_source']['current_observation']['relative_humidity'][:-1])
  except:
    pass

#clean up data since Wunderground responds with unicode numbers occasionally
#this sucks
floats = [
          'dewpoint_c',
          'dewpoint_f',
          'feelslike_c',
          'feelslike_f',
          'heat_index_c',
          'heat_index_f',
          'observation_location',
          'observation_location',
          'precip_1hr_in',
          'precip_1hr_metric',
          'precip_today_in',
          'precip_today_metric',
          'pressure_mb',
          'solarradiation',
          'temp_c' ,
          'temp_f',
          'visibility_km',
          'wind_degrees',
          'wind_gust_kph',
          'wind_gust_mph',
          'wind_kph',
          'wind_mph',
          'UV',
          'visibility_mi',
]

for entry in floats:
 try:
   esObject['_source']['current_observation'][entry] = float(esObject['_source']['current_observation'][entry])
 except:
   pass

try:
  esObject['_source']['current_observation']['observation_location']['longitude'] = float(esObject['_source']['current_observation']['observation_location']['longitude'])
  esObject['_source']['current_observation']['observation_location']['latitude']  = float(esObject['_source']['current_observation']['observation_location']['latitude'])
except:
  pass

#making a bulkObject list so it will be easier to do multiple cities later on
bulkObject = []
bulkObject.append(esObject)

es = Elasticsearch([esHost], sniff_on_start=True)
es.indices.create(index=esIndex, body=esIndexSettings, ignore=400)

if len(bulkObject) > 0:
  helpers.bulk(es, bulkObject)

exit()
