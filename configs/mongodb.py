from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/?directConnection=true")
db = client['analysis_terror_actions']
terrorism_actions = db['terrorism_actions']
