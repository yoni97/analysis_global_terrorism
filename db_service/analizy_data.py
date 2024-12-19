import pandas as pd
from bson import ObjectId
from configs.mongodb import terrorism_actions

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

def get_average_wounded_by_region():
    try:
        results = list(terrorism_actions.find({}))

        df = pd.DataFrame(results)

        df['killed'] = pd.to_numeric(df['killed'], errors='coerce')
        df['wounded'] = pd.to_numeric(df['wounded'], errors='coerce')

        df['total_wounded_score'] = df['killed'] * 2 + df['wounded'] * 1

        df = df.dropna(subset=['total_wounded_score', 'killed', 'wounded'])

        avg_wounded_by_region = df.groupby('region')['total_wounded_score'].mean().reset_index()

        df['total_wounded'] = df['killed'] + df['wounded']
        total_wounded_by_region = df.groupby('region')['total_wounded'].sum().reset_index()

        total_events_by_region = df.groupby('region').size().reset_index(name='event_count')

        avg_wounded_by_region = avg_wounded_by_region.merge(total_wounded_by_region, on='region')
        avg_wounded_by_region = avg_wounded_by_region.merge(total_events_by_region, on='region')

        avg_wounded_by_region['percentage_wounded'] = (
                    avg_wounded_by_region['total_wounded'] / avg_wounded_by_region['event_count'])

        results = avg_wounded_by_region[['region', 'total_wounded_score', 'percentage_wounded']].to_dict(
            orient='records')

        return results
    except Exception as e:
        return {"status": "error", "message": str(e)}