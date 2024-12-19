import csv
from configs.mongodb import terrorism_actions

def read_csv(csv_path):
   with open(csv_path, 'r') as file:
       csv_reader = csv.DictReader(file)
       for row in csv_reader:
           yield row

def init_car_accidents_big_data():
   terrorism_actions.drop()
   print('table dropped successfully')
   terror_list = []
   accidents_list = []

   for row in read_csv('../pandas_analytics/normalized_data.csv'):

       new_terror = {
           'eventid': row['eventid'], 'year': row['Year'], 'month': row['Month'], 'day': row['Day'],
           'country': row['Country'], 'region': row['Region'], 'state': row['state'], 'city': row['city'],
           'latitude': row['latitude'], 'longitude': row['longitude'], 'AttackType': row['AttackType'],
           'killed': row['Killed'], 'wounded': row['Wounded'], 'target': row['Target'], 'summary': row['Summary'],
           'Group': row['Group'],'success': row['success'], 'target_type': row['Target_type'],
           'weapon_type': row['Weapon_type'], 'nperps': row['nperps'], 'date': row['Date'],
       }
       terror_list.append(new_terror)

   try:
       terror_id = terrorism_actions.insert_many(terror_list)
       print('data inserted successfully')
   except Exception as e:
       print(f"Error inserting new_terror: {e}")

if __name__ == '__main__':
    init_car_accidents_big_data()