import folium
import logging
import pandas as pd
from configs.mongodb import terrorism_actions
from flask import render_template, send_file, request, jsonify
from db_repo.upload_to_pandas import upload_to_pandas
from db_service.calculation_services import create_map_and_marker, calc_total_wounded
from services.query_service import clean_filter_value


# Part A:
#   1: Question 1:
def get_top_attack_types(top_n):
    try:
        df = upload_to_pandas(terrorism_actions)
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
def get_average_wounded_by_region_event(show_top_5=False):
    try:
        df = upload_to_pandas(terrorism_actions)

        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])

        new_df = calc_total_wounded(df)

        region_stats = new_df.groupby('region').agg(
            total_wounded_score=('total_wounded_score', 'mean'),
            total_wounded=('total_wounded', 'sum'),
            event_count=('region', 'size')
        ).reset_index()

        region_stats['percentage_wounded'] = region_stats['total_wounded'] / region_stats['event_count']

        if show_top_5:
            region_stats = region_stats.nlargest(5, 'total_wounded_score')

        marker_cluster, folium_map = create_map_and_marker(new_df)

        def add_marker(row):
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=row['total_wounded_score'] / 10,
                color='red' if row['total_wounded_score'] > 50 else 'blue',
                fill=True,
                fill_color='orange' if row['total_wounded_score'] > 50 else 'lightblue',
                fill_opacity=0.6,
                popup=f"City: {row['city']}<br>Score: {row['total_wounded_score']}"
            ).add_to(marker_cluster)

        new_df.apply(add_marker, axis=1)
        map_html = folium_map._repr_html_()

        results = region_stats[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(orient='records')
        print("Results:", results)
        return results, map_html

    except Exception as e:
        logging.error(f"Error in get_average_wounded_by_region: {str(e)}")
        return {"status": "error", "message": str(e)}

# 2b:
def get_average_wounded_by_region(show_top_5=False):
    try:
        df = upload_to_pandas(terrorism_actions)

        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])

        df['total_wounded'] = df['killed'] + df['wounded']

        region_stats = df.groupby('region').agg(
            avg_wounded=('total_wounded', 'mean'),
            total_wounded=('total_wounded', 'sum'),
            event_count=('region', 'size'),
            latitude=('latitude', 'mean'),
            longitude=('longitude', 'mean')
        ).reset_index()

        region_stats['percentage_wounded'] = region_stats['total_wounded'] / region_stats['event_count']

        if show_top_5:
            region_stats = region_stats.nlargest(5, 'avg_wounded')

        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        for _, row in region_stats.iterrows():
            color = 'red' if row['avg_wounded'] > 7 else 'blue'
            fill_color = 'orange' if row['avg_wounded'] > 7 else 'lightblue'
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=row['avg_wounded'],
                color=color,
                fill=True,
                fill_color=fill_color,
                fill_opacity=1,
                popup=f"Region: {row['region']}<br>Avg Wounded: {row['avg_wounded']:.2f}<br>Total Wounded: {row['total_wounded']}<br>Percentage Wounded: {row['percentage_wounded']:.2%}"
            ).add_to(folium_map)

        folium_map.save('templates/average_wounded_by_region_map.html')

        map_html = folium_map._repr_html_()

        results = region_stats[['region', 'avg_wounded', 'percentage_wounded']].to_dict(orient='records')
        return {"status": "success", "data": results, "map_html": map_html}

    except Exception as e:
        logging.error(f"Error in get_average_wounded_by_region: {str(e)}")
        return {"status": "error", "message": str(e)}

#   3: Question 3:
def get_top_groups_by_victims(top_n=5):
    try:
        df = upload_to_pandas(terrorism_actions)

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
        df = upload_to_pandas(terrorism_actions)
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

        marker_cluster, folium_map = create_map_and_marker(df)

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

        map_html = folium_map._repr_html_()
        return result, map_html
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_terror_incidents_change1(show_top_5=True):
    try:
        df = upload_to_pandas(terrorism_actions)

        required_columns = ['year', 'latitude', 'longitude']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        df = df.dropna(subset=['latitude', 'longitude', 'year'])

        incident_counts = df.groupby(['region', 'year']).agg(
            incident_count=('year', 'size'),
            avg_latitude=('latitude', 'mean'),
            avg_longitude=('longitude', 'mean')
        ).reset_index()

        incident_counts['prev_incident_count'] = incident_counts.groupby('region')['incident_count'].shift(1)
        incident_counts['percentage_change'] = (
                                                       (incident_counts['incident_count'] - incident_counts[
                                                           'prev_incident_count']) / incident_counts[
                                                           'prev_incident_count']
                                               ) * 100

        incident_counts = incident_counts.dropna(subset=['percentage_change'])

        if show_top_5:
            top_5_regions = (
                incident_counts.groupby('region')['percentage_change']
                .mean()
                .nlargest(5)
                .index
            )
            incident_counts = incident_counts[incident_counts['region'].isin(top_5_regions)]

        # יצירת מפה
        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        for name, group_data in incident_counts.groupby('region'):
            region_lat = group_data['avg_latitude'].mean()
            region_lon = group_data['avg_longitude'].mean()
            avg_percentage_change = group_data['percentage_change'].mean()

            folium.Marker(
                location=[region_lat, region_lon],
                popup=f"Region: {name}<br>Average Change: {avg_percentage_change:.2f}%",
                icon=folium.Icon(color="blue", icon="info-sign")
            ).add_to(folium_map)

        folium_map.save('terror_incidents_change_map.html')
        map_html = folium_map._repr_html_()

        result = incident_counts[['region', 'year', 'percentage_change']].to_dict(orient='records')
        print(result)
        return result, map_html
    except Exception as e:
        print("Error in get_terror_incidents_change1:", str(e))
        return {"status": "error", "message": str(e)}


#   5: Question 8:
def get_top_active_groups(top_n, filter_by,  filter_value=None):
    try:
        if filter_by:
            filter_by = clean_filter_value(filter_by)
        if filter_value:
            filter_value = clean_filter_value(filter_value)
        df = upload_to_pandas(terrorism_actions)

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

        marker_cluster, folium_map = create_map_and_marker(df)

        for _, group in most_active_group.iterrows():
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

        map_html = folium_map._repr_html_()
        results = top_groups_by_region.to_dict(orient='records')
        return results, map_html

    except Exception as e:
        logging.error(f"Error in get_top_active_groups: {str(e)}")
        return {"status": "error", "message": str(e)}

# Part 2:
#   1: Question 11:
def get_groups_with_common_targets(filter_by='region', filter_value=None):
    try:
        if filter_by:
            filter_by = clean_filter_value(filter_by)
        if filter_value:
            filter_value = clean_filter_value(filter_value)
        df = upload_to_pandas(terrorism_actions)
        count_central = df[df['region'].str.contains("Central America", case=False, na=False)].shape[0]

        print(count_central)
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude', 'region', 'country', 'Group', 'target'])

        if filter_value:
            if filter_by not in df.columns:
                raise ValueError(f"Filter column '{filter_by}' not found in data.")
            df = df[df[filter_by] == filter_value]
            if df.empty:
                raise ValueError(f"No data found for filter value: {filter_value}")

        grouped = df.groupby([filter_by, 'target']).agg({
            'Group': lambda x: list(set(x)),
            'latitude': 'mean',
            'longitude': 'mean'
        }).reset_index()

        grouped['num_groups'] = grouped['Group'].apply(len)

        top_targets = grouped[grouped['num_groups'] == grouped['num_groups'].max()]

        marker_cluster, folium_map = create_map_and_marker(df)
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

        map_html = folium_map._repr_html_()
        results = top_targets.to_dict(orient='records')
        return {"status": "success", "data": results, "map_html": map_html}

    except Exception as e:
        logging.error(f"Error in get_groups_with_common_targets: {str(e)}")
        return {"status": "error", "message": str(e)}

#   Question 12: ???
def track_group_expansion_over_time():
    try:
        df = upload_to_pandas(terrorism_actions)
        df = df.dropna(subset=['region', 'Group', 'year'])
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        group_region_year = df.groupby(['Group', 'region'])['year'].min().reset_index()
        group_region_year = group_region_year.rename(columns={'year': 'first_active_year'})

        new_regions_by_group = group_region_year.sort_values(by=['Group', 'first_active_year'])
        result = new_regions_by_group.to_dict(orient='records')

        return {"status": "success", "data": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}

# 13
def find_groups_in_same_attack():
    try:
        df = upload_to_pandas(terrorism_actions)

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
        if region_filter:
            region_filter = clean_filter_value(region_filter)
        if country_filter:
            country_filter = clean_filter_value(country_filter)
        df = upload_to_pandas(terrorism_actions)

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
        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        if "postman" in user_agent or "application/json" in accept_header:
            return {"status": "success", "data": result}
        else:
            return send_file('shared_attack_strategies_map.html')
        # return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

#  3: Question 15:
def find_groups_and_attack_count_by_target(region_filter=None, country_filter=None):
    try:
        df = upload_to_pandas(terrorism_actions)
        df = df.dropna(subset=['region', 'country', 'AttackType', 'Group', 'target_type'])

        if region_filter:
            df = df[df['region'] == region_filter]
        if country_filter:
            df = df[df['country'] == country_filter]

        attack_group_count = df.groupby(['region', 'country', 'target_type', 'Group'])['eventid'].count().reset_index(name='attack_count')
        result = attack_group_count[['region', 'country', 'target_type', 'Group', 'attack_count']].to_dict(orient='records')

        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

#  4: Question 16:
def find_areas_with_high_intergroup_activity(region_filter=None, country_filter=None):
    try:
        if region_filter:
            region_filter = clean_filter_value(region_filter)
        if country_filter:
            country_filter = clean_filter_value(country_filter)
        df = upload_to_pandas(terrorism_actions)

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
        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        if "postman" in user_agent or "application/json" in accept_header:
            return {"status": "success", "data": result}
        else:
            return send_file('areas_with_high_intergroup_activity_map.html')
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 5: Question 18:
def find_influential_groups(region_filter=None, country_filter=None):
    try:
        if region_filter:
            region_filter = clean_filter_value(region_filter)
        if country_filter:
            country_filter = clean_filter_value(country_filter)
        df = upload_to_pandas(terrorism_actions)

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
            longitude=('longitude', 'mean'),
            country=('country', 'first'),
            region=('region', 'first')
        ).reset_index()

        influence_data['influence_score'] = influence_data['unique_regions'] + influence_data['unique_targets']

        top_influential = influence_data.sort_values(by='influence_score', ascending=False)

        folium_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=2)

        for _, row in top_influential.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=(f"Group: {row['Group']}<br>"
                       f"Unique Regions: {row['unique_regions']}<br>"
                       f"Unique Targets: {row['unique_targets']}<br>"
                       f"Influence Score: {row['influence_score']}<br>"
                       f"Region: {row['region']}<br>"
                       f"Country: {row['country']}"),
                icon=folium.Icon(color="red", icon="info-sign")
            ).add_to(folium_map)

        folium_map.save('influential_groups_map.html')

        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        if "postman" in user_agent or "application/json" in accept_header:
            result = top_influential.to_dict(orient='records')
            return {"status": "success", "data": result}
        else:
            return send_file('influential_groups_map.html')

    except Exception as e:
        return {"status": "error", "message": str(e)}

#   5: Question 19:
def find_groups_with_shared_targets_by_year():
    try:
        df = upload_to_pandas(terrorism_actions)

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