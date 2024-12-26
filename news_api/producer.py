from kafka import KafkaProducer
import json
import time
import requests
from configs.API_KEYS import API_KEY_NEWS, NEWS_API_URL
from configs.kafka_config import KAFKA_BROKER, KAFKA_TOPIC

ARTICLES_PAGE = 1

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_news():
    global ARTICLES_PAGE
    payload = {
        "action": "getArticles",
        "keyword": "terror attack",
        "ignoreSourceGroupUri": "paywall/paywalled_sources",
        "articlesPage": ARTICLES_PAGE,
        "articlesCount": 100,
        "articlesSortBy": "socialScore",
        "articlesSortByAsc": False,
        "dataType": ["news", "pr"],
        "forceMaxDataTimeWindow": 31,
        "resultType": "articles",
        "apiKey": API_KEY_NEWS
    }

    response = requests.post(NEWS_API_URL, json=payload)
    if response.status_code == 200:
        try:
            data = response.json()
            print(json.dumps(data, indent=4))
            ARTICLES_PAGE += 1
            return data.get("articles", {}).get("results", [])
        except ValueError as e:
            print(f"Error parsing JSON: {e}")
            return []
    else:
        print(f"Failed to fetch news: {response.status_code}")
        return []

def send_to_kafka():
    articles = fetch_news()
    for article in articles:
        if isinstance(article, dict) and 'title' in article:
            print(f"Sent article: {article['title']} to Kafka")
            producer.send(KAFKA_TOPIC, article)
            print(article)
            print(f"Sent article: {article['title']} to Kafka")
        else:
            print(f"Article is not a dictionary or does not contain 'title': {article}")
    producer.flush()
    producer.close()

if __name__ == "__main__":
    try:
        while True:
            time.sleep(120)
    except KeyboardInterrupt:
        print("התקבלה הפסקה. המערכת תסיים את הפעולה.")

# from kafka import KafkaProducer
# import json
# import time
# import requests
# from configs.API_KEYS import API_KEY_NEWS, NEWS_API_URL
# from configs.kafka_config import KAFKA_BROKER, KAFKA_TOPIC
# from concurrent.futures import ThreadPoolExecutor
#
# ARTICLES_PAGE = 1
#
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#
# def fetch_news():
#     global ARTICLES_PAGE
#     payload = {
#         "action": "getArticles",
#         "keyword": "terror attack",
#         "ignoreSourceGroupUri": "paywall/paywalled_sources",
#         "articlesPage": ARTICLES_PAGE,
#         "articlesCount": 1,
#         "articlesSortBy": "socialScore",
#         "articlesSortByAsc": False,
#         "dataType": ["news", "pr"],
#         "forceMaxDataTimeWindow": 31,
#         "resultType": "articles",
#         "apiKey": API_KEY_NEWS
#     }
#
#     try:
#         response = requests.post(NEWS_API_URL, json=payload, timeout=20)
#         if response.status_code == 200:
#             data = response.json()
#             ARTICLES_PAGE += 1
#             return data.get("articles", {}).get("results", [])
#         else:
#             print(f"Failed to fetch news: {response.status_code}")
#             return []
#     except requests.exceptions.RequestException as e:
#         print(f"Error fetching news: {e}")
#         return []
#
# def send_to_kafka(articles):
#     for article in articles:
#         if isinstance(article, dict) and 'title' in article:
#             print(f"Sent article: {article['title']} to Kafka")
#             producer.send(KAFKA_TOPIC, article)
#         else:
#             print(f"Article is not a dictionary or does not contain 'title': {article}")
#     producer.flush()
#
# def process_articles():
#     articles = fetch_news()
#     if articles:
#         send_to_kafka(articles)
#
# if __name__ == "__main__":
#     with ThreadPoolExecutor(max_workers=1) as executor:
#         while True:
#             future = executor.submit(process_articles)
#             time.sleep(120)  # Wait 2 minutes before next request
