import json
import requests
from groq import Groq
from dotenv import load_dotenv
from kafka import KafkaConsumer
from configs.mongodb import terrorism_actions, news
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

def save_to_db(article, lng, lat):
    try:
        new_article = {**article, "longitude": lng, "latitude": lat}
        if article['category'] != "general news":
            new_article.pop("category", None)
            new_article["Group"] = new_article.pop("group_attacker")
            if article['category'] == 'historical terror attack':
                terrorism_actions.insert_one(new_article)
            elif article['category'] == 'nowadays terror attack':
                news.insert_one(new_article)
            print("Data inserted successfully into MongoDB")
        else:
            print('Data not inserted into MongoDB because it is general news')
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

load_dotenv(verbose=True)

client = Groq(
    api_key=GROQ_API_KEY,
)

def get_json_from_groq_api(article_content):
    article_content["region"] = article_content.get("region", "Unknown")
    article_content["target_type"] = article_content.get("target_type", "Unknown")
    article_content["group_attacker"] = article_content.get("group_attacker", "Unknown")

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
                    "    \"date\": \"date\",\n"                     
                    "    \"AttackType\": \"str\",\n"                    
                    "    \"Weapon_type\": \"str\",\n"                    
                    "    \"year\": \"int\",\n"                    
                    "    \"month\": \"int\",\n"                    
                    "    \"target_type\": \"str\",\n" 
                    "    \"Target\": \"str\",\n"                     
                    "    \"group_attacker \": \"str\",\n" 
                    "    \"Summary\": \"str\",\n" 
                    "}\n\n"
                    "Respond with the JSON only, without any extra text."
                ),
            }
        ],
        model="llama3-8b-8192",
        temperature=1,
        max_tokens=4900,
        top_p=1,
        stop=None,
        response_format={"type": "json_object"},
    )

    data = json.loads(chat_completion.choices[0].message.content)
    print(data)
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
        print(message.value)
        print(f"Raw message value: {message.value}")

        article = None
        try:
            if isinstance(message.value, str):
                article = json.loads(message.value)
            else:
                article = message.value

            print(f"Processing article: {article.get('title', 'No Title')}")

        except Exception as e:
            print(f"Error processing message: {e}")

        if article:
            print(f"Processing article: {article['title']}")
            enriched_data = get_json_from_groq_api(article)
            country = enriched_data.get('country', 'Unknown')
            lat, lng = get_coordinates(country)
            save_to_db(enriched_data, lat, lng)
            print(f"Saved article: {article.get('title', 'No Title')} to Database")
        else:
            print("Failed to process message: Article could not be parsed")