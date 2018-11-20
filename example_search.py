from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch()

index = 'pantry_list'
lat = 40.022079
lon = -82.977878
distance = '5mi'

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
    },
    "sort": [
        {
            "_geo_distance": {
                "location": {
                    "lat": lat,
                    "lon": lon
                },
                "order": "asc",
                "unit": "mi",
                "distance_type": "plane"
            }
        }
    ]
}
search = Search.from_dict(raw_query).using(es)
search.doc_type("pantry")
search.index(index)
res = search.execute()

print("Found {} documents matching located around latitude {} longitude {}".format(len(res.hits), lat, lon))
# The closer meta.sort gets to 0 the closer it is to the lat/lon
for resp in res.hits:
    print("score: {} name: {} address1: {} located in {}".format(resp.meta.sort[0], resp.loc_name, resp.address1,
                                                                 resp.city))
