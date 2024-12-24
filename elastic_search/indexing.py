from elasticsearch import Elasticsearch
from configs.elastic_configurations import ELASTIC_PORT

es = Elasticsearch(ELASTIC_PORT)

index_mappings = {
    "historic_data": {
        "mappings": {
            "properties": {
                "eventid": { "type": "long" },
                "year": { "type": "integer" },
                "month": { "type": "integer" },
                "country": { "type": "keyword" },
                "region": { "type": "keyword" },
                "city": { "type": "keyword" },
                "date": { "type": "date", "format": "yyyy-MM-dd" },
                "latitude": { "type": "double" },
                "longitude": { "type": "double" },
                "AttackType": { "type": "keyword" },
                "Group": { "type": "keyword" },
                "killed": { "type": "double" },
                "wounded": { "type": "integer" },
                "Target": { "type": "text" },
                "Summary": { "type": "text" },
                "target_type": { "type": "keyword" },
                "Weapon_type": { "type": "keyword" }
            }
        }
    },
    "realtime_news": {
        "mappings": {
            "properties": {
                "country": { "type": "keyword" },
                "city": { "type": "keyword" },
                "region": { "type": "keyword" },
                "killed": { "type": "integer" },
                "wounded": { "type": "integer" },
                "AttackType": { "type": "keyword" },
                "Weapon_type": { "type": "keyword" },
                "year": { "type": "integer" },
                "month": { "type": "integer" },
                "Target": { "type": "text" },
                "Group": { "type": "keyword" },
                "Summary": { "type": "text" }
            }
        }
    },
    "combined_data": {
        "mappings": {
            "properties": {
            }
        }
    }
}

for index, mapping in index_mappings.items():
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping, ignore=400)
        print(f"Index '{index}' created.")