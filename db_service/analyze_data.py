import logging
import pandas as pd
import folium
from flask import render_template
from folium.plugins import MarkerCluster
from bson import ObjectId
from configs.mongodb import terrorism_actions
from db_repo.upload_to_pandas import upload_to_pandas


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
        df = upload_to_pandas()
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
        df = upload_to_pandas()

        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])

        df['total_wounded_score'] = df['killed'] * 2 + df['wounded']
        df['total_wounded'] = df['killed'] + df['wounded']

        region_stats = df.groupby('region').agg(
            total_wounded_score=('total_wounded_score', 'mean'),
            total_wounded=('total_wounded', 'sum'),
            event_count=('region', 'size')
        ).reset_index()

        region_stats['percentage_wounded'] = region_stats['total_wounded'] / region_stats['event_count']

        if show_top_5:
            region_stats = region_stats.nlargest(5, 'total_wounded_score')

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        marker_cluster = MarkerCluster().add_to(folium_map)

        def add_marker(row):
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=row['total_wounded_score'] / 10,
                color='red' if row['total_wounded_score'] > 50 else 'blue',
                fill=True,
                fill_color='orange' if row['total_wounded_score'] > 50 else 'lightblue',
                fill_opacity=0.6,
                popup=f"Region: {row['region']}<br>Score: {row['total_wounded_score']}"
            ).add_to(marker_cluster)

        df.apply(add_marker, axis=1)
        folium_map.save('wounded_by_region_map.html')

        results = region_stats[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(orient='records')
        return {
            "status": "success",
            "data": results,  # נתוני אזורים
            "map_url": "/static/wounded_by_region_map.html"  # קישור למפה
        }

    except Exception as e:
        logging.error(f"Error in get_average_wounded_by_region: {str(e)}")  # הוספת לוג של השגיאה
        return {"status": "error", "message": str(e)}

# def get_average_wounded_by_region(show_top_5=False):
#     try:
#         df = upload_to_pandas()
#
#         for col in ['killed', 'wounded', 'latitude', 'longitude']:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#
#         df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])
#
#         df['total_wounded_score'] = df['killed'] * 2 + df['wounded']
#         df['total_wounded'] = df['killed'] + df['wounded']
#
#         region_stats = df.groupby('region').agg(
#             total_wounded_score=('total_wounded_score', 'mean'),
#             total_wounded=('total_wounded', 'sum'),
#             event_count=('region', 'size')
#         ).reset_index()
#
#         region_stats['percentage_wounded'] = region_stats['total_wounded'] / region_stats['event_count']
#
#         if show_top_5:
#             region_stats = region_stats.nlargest(5, 'total_wounded_score')
#
#         map_center = [df['latitude'].mean(), df['longitude'].mean()]
#         folium_map = folium.Map(location=map_center, zoom_start=2)
#
#         marker_cluster = MarkerCluster().add_to(folium_map)
#
#         def add_marker(row):
#             folium.CircleMarker(
#                 location=[row['latitude'], row['longitude']],
#                 radius=row['total_wounded_score'] / 10,
#                 color='red' if row['total_wounded_score'] > 50 else 'blue',
#                 fill=True,
#                 fill_color='orange' if row['total_wounded_score'] > 50 else 'lightblue',
#                 fill_opacity=0.6,
#                 popup=f"Region: {row['region']}<br>Score: {row['total_wounded_score']}"
#             ).add_to(marker_cluster)
#
#         df.apply(add_marker, axis=1)
#
#         folium_map.save('wounded_by_region_map.html')
#
#         results = region_stats[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(orient='records')
#         return results
#
#     except Exception as e:
#         return {"status": "error", "message": str(e)}

# def get_average_wounded_by_region(show_top_5):
#     try:
#         df = upload_to_pandas()
#
#         for col in ['killed', 'wounded', 'latitude', 'longitude']:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])
#         df['total_wounded_score'] = df['killed'] * 2 + df['wounded']
#         avg_wounded_by_region = df.groupby('region')['total_wounded_score'].mean().reset_index()
#         df['total_wounded'] = df['killed'] + df['wounded']
#         total_wounded_by_region = df.groupby('region')['total_wounded'].sum().reset_index()
#
#         total_events_by_region = df.groupby('region').size().reset_index(name='event_count')
#
#         avg_wounded_by_region = avg_wounded_by_region.merge(total_wounded_by_region, on='region')
#         avg_wounded_by_region = avg_wounded_by_region.merge(total_events_by_region, on='region')
#
#         avg_wounded_by_region['percentage_wounded'] = (
#             avg_wounded_by_region['total_wounded'] / avg_wounded_by_region['event_count']
#         )
#
#         if show_top_5:
#             avg_wounded_by_region = avg_wounded_by_region.nlargest(5, 'total_wounded_score')
#
#         map_center = [df['latitude'].mean(), df['longitude'].mean()]
#         folium_map = folium.Map(location=map_center, zoom_start=2)
#
#         # Marker Cluster for better visualization
#         marker_cluster = MarkerCluster().add_to(folium_map)
#
#         for _, row in df.iterrows():
#             folium.CircleMarker(
#                 location=[row['latitude'], row['longitude']],
#                 radius=row['total_wounded_score'] / 10,
#                 color='red' if row['total_wounded_score'] > 50 else 'blue',
#                 fill=True,
#                 fill_color='orange' if row['total_wounded_score'] > 50 else 'lightblue',
#                 fill_opacity=0.6,
#                 popup=f"Region: {row['region']}<br>Score: {row['total_wounded_score']}"
#             ).add_to(marker_cluster)
#
#         folium_map.save('wounded_by_region_map.html')
#         results = avg_wounded_by_region[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(
#             orient='records')
#
#         # return render_template('wounded_by_region_map.html', data=results)
#         folium_map.save('wounded_by_region_map.html')
#         results = avg_wounded_by_region[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(
#             orient='records'
#         )
#
#         return results
#     except Exception as e:
#         return {"status": "error", "message": str(e)}


# def get_average_wounded_by_region(show_top_5=False):
#     try:
#         df = upload_to_pandas()
#
#         for col in ['killed', 'wounded', 'latitude', 'longitude']:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#
#         df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])
#
#         df['total_wounded_score'] = df['killed'] * 2 + df['wounded']
#         avg_wounded_by_region = df.groupby('region')['total_wounded_score'].mean().reset_index()
#         avg_wounded_by_region.rename(columns={'total_wounded_score': 'average_wounded_score'}, inplace=True)
#
#         map_center = [df['latitude'].mean(), df['longitude'].mean()]
#         folium_map = folium.Map(location=map_center, zoom_start=2)
#         marker_cluster = MarkerCluster().add_to(folium_map)
#
#         for _, row in df.iterrows():
#             folium.CircleMarker(
#                 location=[row['latitude'], row['longitude']],
#                 radius=row['total_wounded_score'] / 10,
#                 color='red' if row['total_wounded_score'] > 50 else 'blue',
#                 fill=True,
#                 fill_color='orange' if row['total_wounded_score'] > 50 else 'lightblue',
#                 fill_opacity=0.6,
#                 popup=f"Region: {row['region']}<br>Score: {row['total_wounded_score']}"
#             ).add_to(marker_cluster)
#
#         folium_map.save('static/wounded_by_region_map.html')
#         results = avg_wounded_by_region.to_dict(orient='records')
#
#         return render_template('wounded_by_region_map.html', data=results)
#
#     except Exception as e:
#         return {"status": "error", "message": str(e)}


#   3: Question 3:
def get_top_groups_by_victims(top_n=5):
    try:
        df = upload_to_pandas()

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

        incident_counts = pd.merge(incident_counts, df[['region', 'year', 'latitude', 'longitude']], on=['region', 'year'], how='left')

        if show_top_5:
            top_5 = incident_counts.groupby('region')['percentage_change'].mean().nlargest(5).index
            incident_counts = incident_counts[incident_counts['region'].isin(top_5)]

        result = incident_counts[['region', 'year', 'percentage_change', 'latitude', 'longitude']].to_dict(orient='records')

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        marker_cluster = MarkerCluster().add_to(folium_map)

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
        df = upload_to_pandas()

        print("Columns in the DataFrame:", df.columns)
        print("First few rows of data:", df.head())
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude', 'region', 'Group'])

        if filter_value:
            df = df[df[filter_by] == filter_value]

        group_counts = df.groupby([filter_by, 'Group']).size().reset_index(name='event_count')

        top_groups_by_region = group_counts.groupby(filter_by).apply(
            lambda x: x.nlargest(top_n, 'event_count')
        ).reset_index(drop=True)

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

# Part 2:
#   1: Question 11:
def get_groups_with_common_targets(filter_by='region', filter_value=None):
    try:
        df = upload_to_pandas()

        print("Columns in the DataFrame:", df.columns)
        print("First few rows of data:", df.head())
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude', 'region', 'country', 'Group', 'target'])

        if filter_value:
            df = df[df[filter_by] == filter_value]

        grouped = df.groupby([filter_by, 'target']).agg({
            'Group': lambda x: list(set(x)),
            'latitude': 'mean',
            'longitude': 'mean'
        }).reset_index()

        grouped['num_groups'] = grouped['Group'].apply(len)

        top_targets = grouped[grouped['num_groups'] == grouped['num_groups'].max()]

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        marker_cluster = MarkerCluster().add_to(folium_map)

        for _, row in grouped.iterrows():
            popup_text = f"""
            <b>{filter_by.capitalize()}: {row[filter_by]}</b><br>
            Target: {row['target']}<br>
            Groups: {', '.join(row['Group'])}<br>
            Number of Groups: {row['num_groups']}
            """
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=popup_text
            ).add_to(marker_cluster)

        folium_map.save('common_targets_map.html')
        return {
            "status": "success",
            "data": grouped.to_dict(orient='records'),
            "top_targets": top_targets.to_dict(orient='records')
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

#   Question 12: ???
def track_group_expansion_over_time():
    try:
        results = list(terrorism_actions.find({}))
        df = pd.DataFrame(results)
        df = df.dropna(subset=['region', 'Group', 'year'])
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        group_region_year = df.groupby(['Group', 'region'])['year'].min().reset_index()
        group_region_year = group_region_year.rename(columns={'year': 'first_active_year'})

        new_regions_by_group = group_region_year.sort_values(by=['Group', 'first_active_year'])
        result = new_regions_by_group.to_dict(orient='records')

        return {"status": "success", "data": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}

# 13???
def find_groups_in_same_attack():
    try:
        df = upload_to_pandas()

        if 'eventid' not in df.columns:
            raise KeyError("The column 'eventid' is missing from the dataset.")

        if 'Group' not in df.columns:
            raise KeyError("The column 'Group' is missing from the dataset.")
        df = df.dropna(subset=['eventid', 'Group'])

        grouped = df.groupby('eventid')['Group'].unique().reset_index()

        print("Grouped Data:", grouped.head())
        grouped = grouped[grouped['Group'].apply(len) > 1]

        print("Filtered Grouped Data:", grouped.head())
        result = grouped.to_dict(orient='records')
        return {"status": "success", "data": result}

    except KeyError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

#   2: Question 14:
def find_shared_attack_strategies(region_filter=None, country_filter=None):
    try:
        df = upload_to_pandas()

        df = df.dropna(subset=['region', 'country', 'AttackType', 'Group', 'latitude', 'longitude'])
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude'])

        if region_filter:
            df = df[df['region'] == region_filter]
        if country_filter:
            df = df[df['country'] == country_filter]

        attack_group_count = df.groupby(
            ['region', 'country', 'AttackType', 'latitude', 'longitude']
        ).agg(
            unique_groups=('Group', 'nunique'),
            group_names=('Group', lambda x: list(x.unique()))
        ).reset_index()

        # בחירת האזור עם מספר הקבוצות הייחודיות הגבוה ביותר לכל סוג תקיפה
        top_attack_by_region = attack_group_count.loc[
            attack_group_count.groupby(['region', 'country', 'AttackType'])['unique_groups'].idxmax()
        ]

        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        for _, row in top_attack_by_region.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=(
                    f"Region: {row['region']}<br>"
                    f"Country: {row['country']}<br>"
                    f"Attack Type: {row['AttackType']}<br>"
                    f"Unique Groups: {row['unique_groups']}<br>"
                    f"Group Names: {', '.join(row['group_names'])}"
                ),
                icon=folium.Icon(color="blue", icon="info-sign")
            ).add_to(folium_map)

        folium_map.save('shared_attack_strategies_map.html')
        result = top_attack_by_region.to_dict(orient='records')

        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

#  3: Question 15:
def find_groups_and_attack_count_by_target(region_filter=None, country_filter=None):
    try:
        df = upload_to_pandas()

        print(df.columns)
        df = df.dropna(subset=['region', 'country', 'AttackType', 'Group', 'target_type'])

        if region_filter:
            df = df[df['region'] == region_filter]
        if country_filter:
            df = df[df['country'] == country_filter]

        attack_group_count = df.groupby(['target_type', 'Group'])['eventid'].count().reset_index(name='attack_count')

        result = attack_group_count[['target_type', 'Group', 'attack_count']].to_dict(orient='records')

        return {"status": "success", "data": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}

#  4: Question 16:
def find_areas_with_high_intergroup_activity(region_filter=None, country_filter=None):
    try:
        df = upload_to_pandas()

        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['region', 'country', 'Group', 'latitude', 'longitude'])

        if region_filter:
            df = df[df['region'] == region_filter]
        if country_filter:
            df = df[df['country'] == country_filter]

        activity_by_region = df.groupby(['region', 'country', 'latitude', 'longitude'])['Group'].nunique().reset_index(name='unique_groups_count')

        top_activity_regions = activity_by_region.sort_values(by='unique_groups_count', ascending=False)

        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        for _, row in top_activity_regions.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=(
                    f"Region: {row['region']}<br>"
                    f"Country: {row['country']}<br>"
                    f"Unique Groups: {row['unique_groups_count']}"
                ),
                icon=folium.Icon(color="red", icon="info-sign")
            ).add_to(folium_map)

        folium_map.save('areas_with_high_intergroup_activity_map.html')
        result = top_activity_regions[['region', 'country', 'unique_groups_count']].to_dict(orient='records')
        return {"status": "success", "data": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}

# 5: Question 18:
def find_influential_groups(region_filter=None, country_filter=None):
    try:
        df = upload_to_pandas()

        df = df.dropna(subset=['region', 'country', 'target_type', 'Group', 'latitude', 'longitude'])
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude'])

        if region_filter:
            df = df[df['region'] == region_filter]
        if country_filter:
            df = df[df['country'] == country_filter]

        influence_data = df.groupby(
            ['Group']
        ).agg(
            unique_regions=('region', 'nunique'),
            unique_targets=('target_type', 'nunique'),
            latitude=('latitude', 'mean'),
            longitude=('longitude', 'mean')
        ).reset_index()

        influence_data['influence_score'] = influence_data['unique_regions'] + influence_data['unique_targets']

        top_influential = influence_data.sort_values(by='influence_score', ascending=False)

        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        for _, row in top_influential.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=(
                    f"Group: {row['Group']}<br>"
                    f"Unique Regions: {row['unique_regions']}<br>"
                    f"Unique Targets: {row['unique_targets']}<br>"
                    f"Influence Score: {row['influence_score']}"
                ),
                icon=folium.Icon(color="red", icon="info-sign")
            ).add_to(folium_map)

        folium_map.save('influential_groups_map.html')
        result = top_influential.to_dict(orient='records')

        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}


#   5: Question 19:
def find_groups_with_shared_targets_by_year():
    try:
        df = upload_to_pandas()

        df = df.dropna(subset=['year', 'Group', 'target_type'])
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        df = df.dropna(subset=['year'])

        shared_targets = (
            df.groupby(['year', 'target_type'])['Group']
            .apply(lambda groups: list(groups.unique()))
            .reset_index(name='groups')
        )

        shared_targets = shared_targets[shared_targets['groups'].apply(len) > 1]

        shared_targets['group_count'] = shared_targets['groups'].apply(len)
        result = shared_targets.to_dict(orient='records')

        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}