import json
import requests
from groq import Groq
from dotenv import load_dotenv
from kafka import KafkaConsumer
from configs.mongodb import terrorism_actions
from configs.kafka_config import KAFKA_TOPIC, KAFKA_BROKER
from configs.API_KEYS import GROQ_API_KEY, GROQ_API_URL, OPENCAGE_API_KEY, OPENCAGE_API_URL


def classify_article(article):
    payload = {
        "model": "news-event-classification",
        "text": article["title"] + " " + article["body"]
    }
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
    response = requests.post(GROQ_API_URL, json=payload, headers=headers)

    if response.status_code == 200:
        return response.json().get("category", "general")
    else:
        print(f"Failed to classify article: {response.status_code}")
        return "general"

def save_to_mongo(article, lng, lat):
    try:
        new_article = {**article, "longitude": lng, "latitude": lat}
        if article['category'] == 'nowadays terror attack' or article['category'] == 'historical terror attack':
            new_article.pop("category", None)
            new_article["Group"] = new_article.pop("group_attacker")
            terrorism_actions.insert_one(new_article)
            print("Data inserted successfully into MongoDB")
        else:
            print('Data not inserted into MongoDB because it is not relevant')
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

load_dotenv(verbose=True)

client = Groq(
    api_key=GROQ_API_KEY,
)

def get_json_from_groq_api(article_content):
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content":  (
                    f"{json.dumps(article_content)}\n\n"
                    "This is an article. I want to analyze a few things:\n"
                    "1. In what country did it happen?\n"
                    "2. Classify the article into one of the following categories: general news, historical terror attack, or nowadays terror attack.\n\n"
                    "After analyzing, provide a JSON with the following structure:\n"
                    "{\n"
                    "    \"category\": \"str\",\n"
                    "    \"country\": \"str\",\n"
                    "    \"city\": \"str\",\n"
                    "    \"region\": \"str\",\n"                   
                    "    \"killed\": \"int\",\n"                    
                    "    \"wounded\": \"int\",\n"                    
                    "    \"AttackType\": \"str\",\n"                    
                    "    \"Weapon_type\": \"str\",\n"                    
                    "    \"year\": \"int\",\n"                    
                    "    \"month\": \"int\",\n"                    
                    "    \"Target\": \"str\",\n"                    
                    "    \"Group \": \"str\",\n" 
                    "    \"Summary\": \"str\",\n" 
                    "}\n\n"
                    "Respond with the JSON only, without any extra text."
                ),
            }
        ],
        model="llama3-8b-8192",
    )
    data = json.loads(chat_completion.choices[0].message.content)
    return data

def get_coordinates(location_name):
    params = {"q": location_name, "key": OPENCAGE_API_KEY}
    response = requests.get(OPENCAGE_API_URL, params=params)

    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            geometry = results[0].get("geometry", {})
            return geometry.get("lat"), geometry.get("lng")
    return None, None

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BROKER,
        auto_offset_reset = 'earliest',
        enable_auto_commit = False,
        value_deserializer = lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        print('n')
        article = message.value
        print(f"Processing article: {article['title']}")

        enriched_data = get_json_from_groq_api(article)
        country = enriched_data['country']
        lat, lng = get_coordinates(country)

        save_to_mongo(enriched_data, lat, lng)
        print(f"Saved article: {article['title']} to MongoDB")