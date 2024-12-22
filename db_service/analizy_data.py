import pandas as pd
import folium
from folium.plugins import MarkerCluster
from bson import ObjectId
from configs.mongodb import terrorism_actions

def convert_objectid(data):
    if isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, dict):
        return {key: convert_objectid(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid(item) for item in data]
    else:
        return data

# Part A:
#   1: Question 1:
def get_top_attack_types(top_n=50):
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)
        df['killed'] = pd.to_numeric(df['killed'], errors='coerce').fillna(0)
        df['wounded'] = pd.to_numeric(df['wounded'], errors='coerce').fillna(0)
        df['total_wounded_score'] = df['killed'] * 2 + df['wounded'] * 1
        attack_types = df.groupby('AttackType')['total_wounded_score'].sum().reset_index()

        attack_types = attack_types.sort_values(by='total_wounded_score', ascending=False)
        if top_n:
            attack_types = attack_types.head(top_n)
        results = attack_types.to_dict(orient='records')

        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}

#   2:  Question 2:
def get_average_wounded_by_region(show_top_5=False):
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)

        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])
        df['total_wounded_score'] = df['killed'] * 2 + df['wounded']
        avg_wounded_by_region = df.groupby('region')['total_wounded_score'].mean().reset_index()
        df['total_wounded'] = df['killed'] + df['wounded']
        total_wounded_by_region = df.groupby('region')['total_wounded'].sum().reset_index()

        # חישוב מספר האירועים לפי אזור
        total_events_by_region = df.groupby('region').size().reset_index(name='event_count')

        avg_wounded_by_region = avg_wounded_by_region.merge(total_wounded_by_region, on='region')
        avg_wounded_by_region = avg_wounded_by_region.merge(total_events_by_region, on='region')

        avg_wounded_by_region['percentage_wounded'] = (
            avg_wounded_by_region['total_wounded'] / avg_wounded_by_region['event_count']
        )

        if show_top_5:
            avg_wounded_by_region = avg_wounded_by_region.nlargest(5, 'total_wounded_score')

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        # Marker Cluster for better visualization
        marker_cluster = MarkerCluster().add_to(folium_map)

        # הוספת נקודות למפה
        for _, row in df.iterrows():
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=row['total_wounded_score'] / 10,
                color='red' if row['total_wounded_score'] > 50 else 'blue',
                fill=True,
                fill_color='orange' if row['total_wounded_score'] > 50 else 'lightblue',
                fill_opacity=0.6,
                popup=f"Region: {row['region']}<br>Score: {row['total_wounded_score']}"
            ).add_to(marker_cluster)

        folium_map.save('wounded_by_region_map.html')
        results = avg_wounded_by_region[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(
            orient='records'
        )

        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}

#   3: Question 3:
def get_top_groups_by_victims(top_n=5):
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)
        df['killed'] = pd.to_numeric(df['killed'], errors='coerce').fillna(0)
        df['wounded'] = pd.to_numeric(df['wounded'], errors='coerce').fillna(0)
        df['total_wounded_score'] = df['killed'] * 2 + df['wounded'] * 1

        total_wounded_by_group = df.groupby('Group')['total_wounded_score'].sum().reset_index()

        total_wounded_by_group = total_wounded_by_group.sort_values(by='total_wounded_score', ascending=False)
        top_groups = total_wounded_by_group.head(top_n)
        results = top_groups[['Group', 'total_wounded_score']].to_dict(orient='records')

        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}

#   4: Question 6:
def get_terror_incidents_change(show_top_5=True):
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['year', 'latitude', 'longitude'])

        incident_counts = df.groupby(['region', 'year']).size().reset_index(name='incident_count')

        incident_counts['prev_incident_count'] = incident_counts.groupby('region')['incident_count'].shift(1)
        incident_counts['percentage_change'] = (
            (incident_counts['incident_count'] - incident_counts['prev_incident_count']) / incident_counts['prev_incident_count']
        ) * 100

        incident_counts = incident_counts.dropna(subset=['percentage_change'])

        # הוספת את ה- latitude וה- longitude עבור כל אזור ושנה
        incident_counts = pd.merge(incident_counts, df[['region', 'year', 'latitude', 'longitude']], on=['region', 'year'], how='left')

        if show_top_5:
            top_5 = incident_counts.groupby('region')['percentage_change'].mean().nlargest(5).index
            incident_counts = incident_counts[incident_counts['region'].isin(top_5)]

        result = incident_counts[['region', 'year', 'percentage_change', 'latitude', 'longitude']].to_dict(orient='records')

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        # Marker Cluster for better visualization
        marker_cluster = MarkerCluster().add_to(folium_map)

        # יצירת נקודות מפה עבור כל אזור
        for _, row in incident_counts.iterrows():
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=row['percentage_change'] / 10,  # גודל הנקודה מבוסס על אחוז השינוי
                color='red' if row['percentage_change'] > 50 else 'blue',
                fill=True,
                fill_color='orange' if row['percentage_change'] > 50 else 'lightblue',
                fill_opacity=0.6,
                popup=f"Region: {row['region']}<br>Change: {row['percentage_change']}%"
            ).add_to(marker_cluster)
        folium_map.save('terror_incidents_change_map.html')
        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}

#   5: Question 8:
def get_top_active_groups(filter_by='region', filter_value=None, top_n=5):
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)
        print("Columns in the DataFrame:", df.columns)
        print("First few rows of data:", df.head())
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude', 'region', 'Group'])

        if filter_value:
            df = df[df[filter_by] == filter_value]

        # חישוב מספר האירועים לפי קבוצה ואזור
        group_counts = df.groupby([filter_by, 'Group']).size().reset_index(name='event_count')

        # מיון קבוצות לפי כמות אירועים בכל אזור
        top_groups_by_region = group_counts.groupby(filter_by).apply(
            lambda x: x.nlargest(top_n, 'event_count')
        ).reset_index(drop=True)

        # זיהוי הקבוצה הכי פעילה לכל אזור
        most_active_group = group_counts.groupby(filter_by).apply(
            lambda x: x.nlargest(1, 'event_count')
        ).reset_index(drop=True)

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        marker_cluster = MarkerCluster().add_to(folium_map)

        for region, group in most_active_group.iterrows():
            region_data = top_groups_by_region[top_groups_by_region[filter_by] == group[filter_by]]

            popup_text = f"""
            <b>{filter_by.capitalize()}: {group[filter_by]}</b><br>
            <b>Most Active Group: {group['Group']}</b><br>
            <b>Top {top_n} Groups:</b><br>
            {region_data[['Group', 'event_count']].to_html(index=False, escape=False)}
            """
            folium.Marker(
                location=[df[df[filter_by] == group[filter_by]]['latitude'].mean(),
                          df[df[filter_by] == group[filter_by]]['longitude'].mean()],
                popup=popup_text
            ).add_to(marker_cluster)

        folium_map.save('top_active_groups_map.html')

        return {
            "status": "success",
            "data": top_groups_by_region.to_dict(orient='records'),
            "most_active_group": most_active_group.to_dict(orient='records')
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}





