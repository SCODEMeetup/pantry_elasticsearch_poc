from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch()

index = 'pantry_list'
lat = 40.022079
lon = -82.977878
distance = '5km'

# TODO: There is probably a nicer way to do this via the dsl
raw_query = {
    "query": {
        "bool": {
            "must": {
                "match_all": {}
            },
            "filter": {
                "geo_distance": {
                    "distance": distance,
                    "location": {
                        "lat": lat,
                        "lon": lon,
                    }
                }
            }
        }
    }
}
search = Search.from_dict(raw_query).using(es)
search.doc_type("pantry")
search.index(index)
search.source(include=['loc_name', 'city', 'state', 'address1', 'zip'])
search.sort('score')
res = search.execute()

print("Found {} documents matching located around latitude {} longitude {}".format(len(res.hits), lat, lon))
for resp in res.hits:
    print("score: {} name: {} address1: {} located in {}".format(resp.meta.score, resp.loc_name, resp.address1,
                                                                 resp.city))
