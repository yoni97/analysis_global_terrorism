from bson import ObjectId
from urllib.parse import unquote

def clean_filter_value(filter_value):
    return unquote(filter_value).strip()

def convert_objectid(data):
    if isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, dict):
        return {key: convert_objectid(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid(item) for item in data]
    else:
        return data

def check_if_exsist(region_filter, country_filter):
    if region_filter:
        region_filter = clean_filter_value(region_filter)
    if country_filter:
        country_filter = clean_filter_value(country_filter)
    return region_filter, country_filter