import csv
from datetime import datetime
from configs.database_url import DATA_PATH
from configs.mongodb import terrorism_actions

def read_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row

def convert_date(date_str):
    try:
        return datetime.strptime(date_str, '%d-%b-%y')
    except ValueError:
        return None

def create_terror_actions_collection(df):
   terrorism_actions.drop()
   print('table dropped successfully')
   terror_list = []

   for row in read_csv(f'.{DATA_PATH}'):

       new_terror = {
           'eventid': row.get('eventid', None),
           'year': row.get('Year', None),
           'month': row.get('Month', None),
           'country': row.get('Country', None),
           'region': row.get('Region', None),
           'city': row.get('city', None),
           'date': row.get('Date', None),
           'latitude': row.get('latitude', None),
           'longitude': row.get('longitude', None),
           'AttackType': row.get('AttackType', None),
           'Group': row.get('Group', None),
           'killed': row.get('Killed', None),
           'wounded': row.get('Wounded', None),
           'target': row.get('Target', None),
           'summary': row.get('Summary', None),
           'target_type': row.get('Target_type', None),
           'total_wounded_score': row.get('severity', None),
           'weapon_type': row.get('Weapon_type', None),
       }
       terror_list.append(new_terror)

   try:
       terrorism_actions.insert_many(terror_list)
       print('data inserted successfully')
   except Exception as e:
       print(f"Error inserting new_terror: {e}")

   terrorism_actions.create_index('region')
   terrorism_actions.create_index('country')

if __name__ == '__main__':
    create_terror_actions_collection()