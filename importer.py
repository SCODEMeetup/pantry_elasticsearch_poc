from ssl import create_default_context
from uuid import uuid4 as uuid4

import pandas as pd
import urllib3
from elasticsearch import Elasticsearch, helpers

urllib3.disable_warnings()

context = create_default_context(capath=".")
update_mappings = False
# update_mappings = False
# purge = True
purge = False
load = True
# load = False
use_uuid = True
# use_uuid = False
elastic_hosts = [
    '127.0.0.1',
]
username=''
password=''

index_name = 'pantry_list'
pantry_list = pd.read_csv(filepath_or_buffer='pantrytrak_locations.csv', parse_dates=True, index_col='loc_id')

pantry_list_mapping = {
    "pantry": {
        "properties": {
            "Year": {"type": "integer"},
            "loc_name": {"type": "keyword"},
            "loc_nickname": {"type": "keyword"},
            "address1": {"type": "text"},
            "address2": {"type": "text"},
            "city": {"type": "keyword"},
            "state": {"type": "text"},
            "zip": {"type": "integer"},
            "country_fips": {"type": "text"},
            "geofips_tract": {"type": "float"},  # TODO: can we convert this somehow?
            "geofips_bg": {"type": "float"},  # TODO: can we convert this somehow?
            "location": {"type": "geo_point"},  # TODO: can we convert this somehow?
            "crh_zone_name": {"type": "keyword"},
            "crh_zone_id": {"type": "float"},
        }
    }
}


def load_docs(pantry_list):
    for x, row in pantry_list.iterrows():
        if use_uuid:
            doc_id = uuid4().int
        else:
            doc_id = x

        fields = ['loc_name', 'loc_nickname', 'address1', 'location']
        values = [
            row['loc_name'],
            row['loc_nickname'],
            row['address1'],
            {
                'lat': row['pt_latitude'],
                'lon': row['pt_longitude']
            }
        ]

        yield doc_id, dict(zip(fields, values))


def bulk(es, data):
    k = ({
        "_index": index_name,
        "_type": "pantry",
        "_source": doc,
    } for doc_id, doc in load_docs(data))
    helpers.bulk(es, k)


def get_doc_count(es, index_name):
    stats = es.indices.stats(index=index_name)
    doc_count = stats['indices'][index_name]['primaries']['docs']['count']
    return doc_count


es = Elasticsearch(
    elastic_hosts,
    port=9200,
    #use_ssl=True,
    ssl_context=context,
    #http_auth=(username, password),
    retry_on_timeout=True,
    randomize_hosts=True,
)

if es.indices.exists(index_name) and purge:
    print("Deleting {}".format(index_name))
    es.indices.delete(index_name)

if not es.indices.exists(index_name):
    index_params = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
            }
        },
        "mappings": pantry_list_mapping

    }
    print("Creating {} index".format(index_name))
    es.indices.create(index=index_name, body=index_params)

if update_mappings:
    es.indices.put_mapping(doc_type='pantry', index=index_name, body=pantry_list_mapping)

if load:
    bulk(es, pantry_list)
