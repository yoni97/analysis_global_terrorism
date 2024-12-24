import folium
from folium.plugins import MarkerCluster
from db_repo.upload_to_pandas import upload_to_pandas

def calc_total_wounded(df):
    df['total_wounded_score'] = df['killed'] * 2 + df['wounded']
    df['total_wounded'] = df['killed'] + df['wounded']
    return df

def create_map_and_marker(df):
    map_center = [df['latitude'].mean(), df['longitude'].mean()]
    folium_map = folium.Map(location=map_center, zoom_start=2)
    marker_cluster = MarkerCluster().add_to(folium_map)

    return marker_cluster, folium_map