# pantry_elasticsearch_poc

## Example usage

1. Install [docker](https://www.docker.com/get-started)
1. Install python ( recommend [anaconda](https://www.anaconda.com/download/))
1. install python modules
   * python / venv
      1. python -m venv csv_to_elastic_geo
      1. venv/bin/activate
      1. pip install -r requirements.txt
   * conda 
      1. conda create -n csv_to_elastic_geo
      1. activate csv_to_elastic_geo
      1. pip install -r requirements.txt
1. docker-compose up
1. python importer.py

Note that [docker-compose.yml](docker-compose.yml) will run elasticsearch and kibana so you can go fiddle around with things. This is **not** secured in any way!

## API usage

This is TBD but I'd like to see [community-services-locator-api](https://github.com/SCODEMeetup/community-services-locator-api) and [mofb-api](https://github.com/SCODEMeetup/mofb-api) make use of elasticsearch over the ckan / other site as it should provide better response times and in general be more flexible.