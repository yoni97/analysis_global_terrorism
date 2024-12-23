import csv
from datetime import datetime
from configs.mongodb import terrorism_actions
from configs.files_path import random_file

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

def init_terror_actions_big_data():
   terrorism_actions.drop()
   print('table dropped successfully')
   terror_list = []

   for row in read_csv('../pandas_analytics/normalized_data.csv'):

       new_terror = {
           'eventid': row['eventid'], 'year': row['Year'], 'month': row['Month'], 'day': row['Day'],
           'country': row['Country'], 'region': row['Region'], 'city': row['city'],
           'latitude': row['latitude'], 'longitude': row['longitude'], 'AttackType': row['AttackType'],
           'Group': row['Group'],'killed': row['Killed'], 'wounded': row['Wounded'], 'target': row['Target'],
           'summary': row['Summary'], 'success': row['success'], 'target_type': row['Target_type'],
           'total_wounded_score': row['severity'],'weapon_type': row['Weapon_type'],
           'date': row['Date'], 'summary': row['Summary'],
       }
       terror_list.append(new_terror)

   try:
       terrorism_actions.insert_many(terror_list)
       print('data inserted successfully')
   except Exception as e:
       print(f"Error inserting new_terror: {e}")

   terrorism_actions.create_index('region')
   terrorism_actions.create_index('country')








# with open(random_file, mode='r', encoding='latin1') as file:
#     reader = csv.DictReader(file)
#
#     for row in reader:
#         # המרת התאריך והפרדתו לשנה, חודש ויום
#         date = convert_date(row['date'])
#         if date:
#             year = date.year
#             month = date.month
#             day = date.day
#         else:
#             year = month = day = None  # אם לא ניתן להמיר את התאריך
#
#         # יצירת השדות החדשים
#         new_row = {
#             'eventid': row['eventid'],
#             'year': year,
#             'month': month,
#             'day': day,
#             'country': row['country'],
#             'city': row['city'],
#             'Group': row['perpetrator'],
#             'killed': row['fatalities'],
#             'wounded': row['injuries'],
#             'weapon_type': row['weapon'],
#             'summary': row['description']
#         }
#
#         # הוספת השורה לאוסף במונגוDB
#         collection.insert_one(new_row)
#
# print("הנתונים נכתבו בהצלחה למונגוDB")

# def init_new_terror_actions():
#     terror_list = []
#     for row in read_csv('../csv_files/RAND_Database_of_Worldwide_Terrorism_Incidents.csv'):
#         new_terror_event = {}


# # הוספת eventid כ-_id למסמכים
# data_dict = data.to_dict(orient='records')  # המרת ה-DataFrame למילון של רשומות
# for record in data_dict:
#     record['_id'] = record['eventid']

if __name__ == '__main__':
    init_terror_actions_big_data()