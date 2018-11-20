import pandas as pd
from elasticsearch import Elasticsearch

es = Elasticsearch()

pantry_list = pd.read_csv(filepath_or_buffer='pantrytrak_locations.csv', parse_dates=True, index_col='loc_id')
# service_types = pd.read_csv(filepath_or_buffer='service_types.csv', parse_dates=True, index_col='service_id')
# trak_agency_schedule_dates = pd.read_csv(filepath_or_buffer='trak_agency_schedule_dates.csv', parse_dates=True, index_col='tasd_id')
# trak_agency_schedule_geographies = pd.read_csv(filepath_or_buffer='trak_agency_schedule_geographies.csv', parse_dates=True, index_col='tasg_id')
# trak_agency_schedule_hours = pd.read_csv(filepath_or_buffer='trak_agency_schedule_hours.csv', parse_dates=True, index_col='tash_id')
# location_agency_crosswalk = pd.read_csv(filepath_or_buffer='location_agency_crosswalk.csv', parse_dates=True, index_col='pt_loc_id')

# https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html
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
if not es.indices.exists('pantry_list'):
    es.indices.create('pantry_list')
    # TODO: Will this mapping ever not exist or be updated?
    es.indices.put_mapping(doc_type='pantry', index='pantry_list', body=pantry_list_mapping)

# TODO, we are getting some data load failures. Probably just need to get more specific with pandas
for doc_id, row in pantry_list.iterrows():
    doc = {
        'loc_name': row["loc_name"],
        'loc_nickname': row["loc_nickname"],
        'address1': row["address1"],
        # 'address2': row["address2"],
        'city': row["city"],
        'state': row["state"],
        'zip': row["zip"],
        # 'country_fips': row["country_fips"],
        # 'geofips_tract': row["geofips_tract"],
        # 'geofips_bg': row["geofips_bg"],
        'location': {
            'lat': row["pt_latitude"],
            'lon': row["pt_longitude"],
        },
        # 'crh_zone_name': row["crh_zone_name"], # getting nans
        # 'crh_zone_id': row["crh_zone_id"], # getting nans
    }
    res = es.index(index="pantry_list", doc_type='pantry', id=doc_id, body=doc)
    if res['result'] == 'created':
        print("added record id {} for {}".format(doc_id, row["loc_name"]))
    if res['result'] == 'updated':
        print("updated record id {} for {}".format(doc_id, row["loc_name"]))
