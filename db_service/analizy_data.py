import pandas as pd
import folium
from folium.plugins import MarkerCluster
from bson import ObjectId
from configs.mongodb import terrorism_actions
from scipy.stats import chi2_contingency


def convert_objectid(data):
    """
    פונקציה להמיר את ה-ObjectId למחרוזת כך שניתן יהיה להחזיר את התוצאה ב-JSON.
    """
    if isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, dict):
        return {key: convert_objectid(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid(item) for item in data]
    else:
        return data

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

def get_average_wounded_by_region(show_top_5=True):
    try:
        results = list(terrorism_actions.find({}))

        df = pd.DataFrame(results)

        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])

        df['total_wounded_score'] = df['killed'] * 2 + df['wounded']

        total_wounded_by_region = (
            df.groupby('region')['total_wounded_score']
            .sum()
            .reset_index()
            .sort_values(by='total_wounded_score', ascending=False)
        )

        if show_top_5:
            total_wounded_by_region = total_wounded_by_region.head(5)

        map_center = [df['latitude'].mean(), df['longitude'].mean()]
        folium_map = folium.Map(location=map_center, zoom_start=2)

        # Marker Cluster for better visualization
        marker_cluster = MarkerCluster().add_to(folium_map)

        for region in total_wounded_by_region['region']:
            region_data = df[df['region'] == region]
            for _, row in region_data.iterrows():
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=row['total_wounded_score'] / 10,
                    color='red',
                    fill=True,
                    fill_color='orange',
                    fill_opacity=0.6,
                    popup=f"Region: {row['region']}<br>Score: {row['total_wounded_score']}"
                ).add_to(marker_cluster)

        folium_map.save('wounded_by_region_map.html')

        results = total_wounded_by_region.to_dict(orient='records')

        return results
    except Exception as e:
        return {"status": "error", "message": str(e), 'hi':'hi'}

import pandas as pd
import folium
from folium.plugins import MarkerCluster

import pandas as pd
import folium
from folium.plugins import MarkerCluster

def get_terror_incidents_change(show_top_5=True):
    try:
        # שליפת הנתונים ממסד הנתונים
        results = list(terrorism_actions.find({}))

        # יצירת DataFrame מתוך התוצאות
        df = pd.DataFrame(results)

        # המרת שנת האירוע לפורמט מספרי עם טיפול בערכים לא תקינים
        df['year'] = pd.to_numeric(df['year'], errors='coerce')

        # המרת ערכים גיאוגרפיים לפורמט מספרי עם טיפול בערכים לא תקינים
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # הסרת שורות עם ערכים חסרים בשדות קריטיים (latitude, longitude, year)
        df = df.dropna(subset=['year', 'latitude', 'longitude'])

        # חישוב מספר הפיגועים לפי אזור ושנה
        incident_counts = df.groupby(['region', 'year']).size().reset_index(name='incident_count')

        # חישוב אחוז שינוי במספר הפיגועים בין השנים לפי אזור
        incident_counts['prev_incident_count'] = incident_counts.groupby('region')['incident_count'].shift(1)
        incident_counts['percentage_change'] = (
            (incident_counts['incident_count'] - incident_counts['prev_incident_count']) / incident_counts['prev_incident_count']
        ) * 100

        # הסרת שורות עם ערכים חסרים
        incident_counts = incident_counts.dropna(subset=['percentage_change'])

        # הוספת את ה- latitude וה- longitude עבור כל אזור ושנה
        incident_counts = pd.merge(incident_counts, df[['region', 'year', 'latitude', 'longitude']], on=['region', 'year'], how='left')

        # סינון לפי Top-5 אם נדרש
        if show_top_5:
            top_5 = incident_counts.groupby('region')['percentage_change'].mean().nlargest(5).index
            incident_counts = incident_counts[incident_counts['region'].isin(top_5)]

        # חישוב המידע הסופי
        result = incident_counts[['region', 'year', 'percentage_change', 'latitude', 'longitude']].to_dict(orient='records')

        # יצירת מפה עם Folium
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

        # שמירת המפה כקובץ HTML להצגה
        folium_map.save('terror_incidents_change_map.html')

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}




# def get_average_wounded_by_region():
#     try:
#         results = list(terrorism_actions.find({}))
#
#         df = pd.DataFrame(results)
#
#         df['killed'] = pd.to_numeric(df['killed'], errors='coerce')
#         df['wounded'] = pd.to_numeric(df['wounded'], errors='coerce')
#
#         df['total_wounded_score'] = df['killed'] * 2 + df['wounded'] * 1
#
#         df = df.dropna(subset=['total_wounded_score', 'killed', 'wounded'])
#
#         avg_wounded_by_region = df.groupby('region')['total_wounded_score'].mean().reset_index()
#
#         df['total_wounded'] = df['killed'] + df['wounded']
#         total_wounded_by_region = df.groupby('region')['total_wounded'].sum().reset_index()
#
#         total_events_by_region = df.groupby('region').size().reset_index(name='event_count')
#
#         avg_wounded_by_region = avg_wounded_by_region.merge(total_wounded_by_region, on='region')
#         avg_wounded_by_region = avg_wounded_by_region.merge(total_events_by_region, on='region')
#
#         avg_wounded_by_region['percentage_wounded'] = (
#                     avg_wounded_by_region['total_wounded'] / avg_wounded_by_region['event_count'])
#
#         results = avg_wounded_by_region[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(
#             orient='records')
#
#         return results
#     except Exception as e:
#         return {"status": "error", "message": str(e)}
def get_top_groups_by_victims(top_n=5):
    try:
        # שליפת הנתונים ממסד הנתונים
        results = list(terrorism_actions.find({}))

        # המרת התוצאות ל-Pandas DataFrame
        df = pd.DataFrame(results)

        # המרת ערכים ריקים ל-0 עבור 'killed' ו-'wounded'
        df['killed'] = pd.to_numeric(df['killed'], errors='coerce').fillna(0)
        df['wounded'] = pd.to_numeric(df['wounded'], errors='coerce').fillna(0)

        # חישוב total_wounded_score: הרוגים * 2 + פצועים * 1
        df['total_wounded_score'] = df['killed'] * 2 + df['wounded'] * 1

        # קיבוץ לפי קבוצה (Group) וחישוב סכום ה-total_wounded_score
        total_wounded_by_group = df.groupby('Group')['total_wounded_score'].sum().reset_index()

        # מיון לפי סכום total_wounded_score
        total_wounded_by_group = total_wounded_by_group.sort_values(by='total_wounded_score', ascending=False)

        # חזרה רק על חמש הקבוצות עם הכי הרבה נפגעים
        top_groups = total_wounded_by_group.head(top_n)

        # המרת התוצאה לרשימה של מילונים
        results = top_groups[['Group', 'total_wounded_score']].to_dict(orient='records')

        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}

# def get_attack_type_target_correlation():
#     try:
#         # שליפת הנתונים ממסד הנתונים
#         results = list(terrorism_actions.find({}))
#
#         # יצירת DataFrame מתוך התוצאות
#         df = pd.DataFrame(results)
#
#         # סינון רק עבור סוגי התקפה וסוגי מטרות
#         if 'AttackType' not in df.columns or 'TargetType' not in df.columns:
#             return {"status": "error", "message": "Missing required columns in the data"}
#
#         # יצירת טבלה צולבת בין סוגי ההתקפה למטרות
#         contingency_table = pd.crosstab(df['AttackType'], df['TargetType'])
#
#         # חישוב Chi-squared Test לבדוק את הקורלציה בין המשתנים
#         chi2, p_value, dof, expected = chi2_contingency(contingency_table)
#
#         # אם p-value קטן מ-0.05, יש קשר מובהק בין סוגי ההתקפה לסוגי המטרות
#         correlation_status = "Significant correlation" if p_value < 0.05 else "No significant correlation"
#
#         # החזרת תוצאות
#         return {
#             "status": "success",
#             "correlation_status": correlation_status,
#             "p_value": p_value,
#             "contingency_table": contingency_table.to_dict()
#         }
#     except Exception as e:
#         return {"status": "error", "message": str(e)}


def get_average_wounded_by_region_on_map(show_top_5=False):
    try:
        # שליפת הנתונים ממסד הנתונים
        results = list(terrorism_actions.find({}))

        # יצירת DataFrame מתוך התוצאות
        df = pd.DataFrame(results)

        # המרת שדות לפורמט מספרי
        for col in ['killed', 'wounded', 'latitude', 'longitude']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # הסרת שורות עם ערכים חסרים
        df = df.dropna(subset=['killed', 'wounded', 'latitude', 'longitude', 'region'])

        # חישוב ציון
        df['total_wounded_score'] = df['killed'] * 2 + df['wounded']

        # חישוב ממוצע הפצועים לפי אזור
        avg_wounded_by_region = df.groupby('region')['total_wounded_score'].mean().reset_index()

        # חישוב סך הפצועים לפי אזור
        df['total_wounded'] = df['killed'] + df['wounded']
        total_wounded_by_region = df.groupby('region')['total_wounded'].sum().reset_index()

        # חישוב מספר האירועים לפי אזור
        total_events_by_region = df.groupby('region').size().reset_index(name='event_count')

        # מיזוג הנתונים ליצירת מידע סופי
        avg_wounded_by_region = avg_wounded_by_region.merge(total_wounded_by_region, on='region')
        avg_wounded_by_region = avg_wounded_by_region.merge(total_events_by_region, on='region')

        # חישוב אחוז הפצועים
        avg_wounded_by_region['percentage_wounded'] = (
            avg_wounded_by_region['total_wounded'] / avg_wounded_by_region['event_count']
        )

        # סינון Top-5 אם נדרש
        if show_top_5:
            avg_wounded_by_region = avg_wounded_by_region.nlargest(5, 'total_wounded_score')

        # יצירת מפה
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

        # שמירת המפה
        folium_map.save('wounded_by_region_map.html')

        # המרת התוצאות לפורמט מילוני
        results = avg_wounded_by_region[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(
            orient='records'
        )

        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}
