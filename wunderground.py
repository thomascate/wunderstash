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

if esObject['_source']['current_observation']['local_epoch']:
  esObject['_source']['@timestamp'] = datetime.utcfromtimestamp(float(esObject['_source']['current_observation']['local_epoch']))
else:
  esObject['_source']['@timestamp'] = datetime.utcnow()

#making a bulkObject list so it will be easier to do multiple cities later on
bulkObject = []
bulkObject.append(esObject)

es = Elasticsearch([esHost], sniff_on_start=True)
es.indices.create(index=esIndex, body = esIndexSettings, ignore=400)

if len(bulkObject) > 0:
  helpers.bulk(es, bulkObject)

exit()
