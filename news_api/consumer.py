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
            terrorism_actions.insert_one(article)
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
                    "    \"country_longitude\": \"int\",\n"
                    "    \"country_latitude\": \"int\",\n"                    
                    "    \"killed\": \"int\",\n"                    
                    "    \"wounded\": \"int\",\n"                    
                    "    \"AttackType\": \"str\",\n"                    
                    "    \"Weapon_type\": \"str\",\n"                    
                    "    \"year\": \"int\",\n"                    
                    "    \"month\": \"int\",\n"                    
                    "    \"Target\": \"str\",\n"                    
                    "    \"group_attacker \": \"str\",\n" 
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

        # Use Groq API to get detailed information
        enriched_data = get_json_from_groq_api(article)

        save_to_mongo(enriched_data)
        print(f"Saved article: {article['title']} to MongoDB")














# import json
# import requests
# from groq import Groq
# from dotenv import load_dotenv
# from kafka import KafkaConsumer
# from configs.mongodb import terrorism_actions
# from configs.kafka_config import KAFKA_TOPIC, KAFKA_BROKER
# from configs.API_KEYS import GROQ_API_KEY, GROQ_API_URL, OPENCAGE_API_URL
#
#
# def classify_article(article):
#     payload = {
#         "model": "news-event-classification",
#         "text": article["title"] + " " + article["body"]
#     }
#     headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
#     response = requests.post(GROQ_API_URL, json=payload, headers=headers)
#
#     if response.status_code == 200:
#         return response.json().get("category", "general")
#     else:
#         print(f"Failed to classify article: {response.status_code}")
#         return "general"
#
def get_coordinates(location_name):
    params = {"q": location_name, "key": OPENCAGE_API_KEY}
    response = requests.get(OPENCAGE_API_URL, params=params)

    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            geometry = results[0].get("geometry", {})
            return geometry.get("lat"), geometry.get("lng")
    return None, None
#
# def save_to_mongo(article, category, lat, lng):
#     new_terror = {
#         "country": article.get("country", None),
#         "city": article.get("city", None),
#         "year": article.get("year", None),
#         "latitude": lat,
#         "longitude": lng,
#         "category": category,
#         "Killed": article.get("killed", None),
#         "Wounded": article.get("wounded", None),
#         "month": article.get("month", None),
#         "Weapon_type": article.get("weapon_type", None),
#         "AttackType": article.get("AttackType", None),
#         "Target": article.get("target", None),
#         "Group": article.get("group", None),
#         "region": article.get("region", None)
#     }
#     try:
#         terrorism_actions.insert_one(new_terror)
#         print("Data inserted successfully into MongoDB")
#     except Exception as e:
#         print(f"Error inserting data into MongoDB: {e}")
#
#
#
# if __name__ == "__main__":
#     consumer = KafkaConsumer(
#         KAFKA_TOPIC,
#         bootstrap_servers=KAFKA_BROKER,
#         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#     )
#
#     for message in consumer:
#         article = message.value
#         print(f"Processing article: {article['title']}")
#
#         category = classify_article(article)
#         location_name = article.get("title", "")
#         lat, lng = get_coordinates(location_name)
#
#         save_to_mongo(article, category, lat, lng)
#         print(f"Saved article: {article['title']} to MongoDB")
#
#
#
#
# load_dotenv(verbose=True)
#
#
#
# client = Groq(
#     api_key=GROQ_API_KEY,
# )
#
# def get_json_from_groq_api(article_content):
#     chat_completion = client.chat.completions.create(
#         messages=[
#             {
#                 "role": "user",
#                 "content":  (
#                     f"{json.dumps(article_content)}\n\n"
#                     "This is an article. I want to analyze a few things:\n"
#                     "1. In what country did it happen?\n"
#                     "2. Classify the article into one of the following categories: general news, historical terror attack, or nowadays terror attack.\n\n"
#                     "After analyzing, provide a JSON with the following structure:\n"
#                     "{\n"
#                     "    \"category\": \"str\",\n"
#                     "    \"country\": \"str\",\n"
#                     "    \"city\": \"str\",\n"
#                     "    \"region\": \"str\",\n"
#                     "    \"country_longitude\": \"int\",\n"
#                     "    \"country_latitude\": \"int\",\n"
#                     "    \"killed\": \"int\",\n"
#                     "    \"wounded\": \"int\",\n"
#                     "    \"AttackType\": \"str\",\n"
#                     "    \"Weapon_type\": \"str\",\n"
#                     "    \"year\": \"int\",\n"
#                     "    \"month\": \"int\",\n"
#                     "    \"Target\": \"str\",\n"
#                     "    \"group_attacker\": \"str\",\n"
#                     "}\n\n"
#                     "Respond with the JSON only, without any extra text."
#                 ),
#             }
#         ],
#         model="llama3-8b-8192",
#     )
#     data = json.loads(chat_completion.choices[0].message.content)
#     return data

# client = Groq()
# completion = client.chat.completions.create(
#     model="llama3-8b-8192",
#     messages=[
#         {
#             "role": "user",
#             "content": "תוציא לי מהJSON הבא JSON חדש עם המפתחות הבאים:\nמדינה, מס הרוגים, מס פצועים, עיר(אם יש), שנה, חודש, יום, סוג נשק, סוג תקיפה, סוג מטרה, סוג קבוצת טרור, אזור(אם יש):\n{\n "
#                        "   \"articles\": {\n        \"page\": 1,\n        \"pages\": 2199,\n        \"totalResults\": 2199,\n        \"results\": [\n            {\n                \"uri\": \"2024-12-583281968\",\n                \"lang\": \"eng\",\n                \"isDuplicate\": false,\n                \"date\": \"2024-12-22\",\n                \"time\": \"16:40:27\",\n                \"dateTime\": \"2024-12-22T16:40:27Z\",\n                \"dateTimePub\": \"2024-12-22T16:39:48Z\",\n                \"dataType\": \"news\",\n                \"sim\": 0,\n                \"url\": \"https://www.infowars.com/posts/german-mother-mourns-loss-of-9-year-old-son-in-christmas-market-attack\",\n                \"title\": \"German Mother Mourns Loss of 9-Year-Old Son in Christmas Market Attack\",\n                \"body\": \"Germany continues to reel from a terror attack committed by Taleb A., a Saudi man who was granted German citizenship, after Taleb murdered five Germans and wounded over 200 on Friday night at the Magdeburg Christmas market. Now, the mother of the youngest victim, 9-year-old Andr\\u00e9, has posted a heart-wrenching message on Facebook following the loss of her son.\\n\\n\\\"Andr\\u00e9 didn't do anything to anyone -- why you, why?\\\" With these words, D\\u00e9sir\\u00e9e G. wrote her goodbyes to her son on Facebook. She also shared a photo of him.\\n\\n\\\"Let my little teddy bear fly around the world again,\\\" she begins her post. She wrote that Andr\\u00e9 is now with his grandparents in heaven.\\n\\n\\\"They missed you very much, as much as we miss you here now.\\\" she added.\\n\\nTaleb A. drove a rented BMW into the crowd at full speed, killing five but also injuring another 200 people. There are still dozens fighting for their lives. So far, 41 people were seriously injured in the attack,\\n\\nOne woman, Anne, who was hit by the driver and knocked unconscious, told Bild newspaper: \\\"I woke up and thought I was in a dream.\\\" She still has a bloodshot eye from attack, however, her husband suffered a far worse injury to his thigh, saying that his flesh was \\\"literally ripped out.\\\" However, they were reunited when Anne regained consciousness when volunteers helped them find each other at the scene of the attack, and both expressed their happiness at being alive.\\n\\nNotably, German Christmas markets are already treated like fortresses, but Taleb A. found the only spot in the entire market that did not have a barrier, which was there to allow emergency vehicles and personnel to gain access to the market in the case of an emergency.\\n\\nThere is currently a debate raging about the man's motives, with a number on the right trying to claim Taleb A.is a secret Islamist. However, it appears from his own messages that he was furious that Germany was not granting asylum to Saudi refugees. He is on record threatening Germany and ethnic Germans with \\\"slaughter\\\" for not helping Saudi ex-Muslims.\\n\\nSome on the left are trying to tie him to the AfD, since he expressed support for the party in one of his posts. However, skeptics cite him saying he was a \\\"leftist\\\" in a video, that the AfD is strictly against mass immigration and granting asylum to those with a criminal record such as Taleb A., and that his multiple threats against Germans stands in complete contrast to the AfD's political platform, which seeks to block foreigners such as Taleb from entering the country entirely.\\n\\nRegardless of his motives, the man has a criminal history and had an open extradition request from Saudi Arabia -- yet he was still granted asylum and citizenship. In Germany, he faced a criminal complaint dating back to 2013 for threatening behavior. According to the Spiegel, Taleb A. had was found guilty in a district court in Rostock and sentenced to a fine of 90 daily rates on Sept. 4, 2013, for \\\"disturbing the public peace by threatening to commit criminal offenses\\\".\\n\\nIn addition, despite the German authorities at the BAMF being warned about his threatening statements against Germans by X users, no action was taken.\\n\\nIn short, it was a catastrophic failure in terms of migration and security policy from the German authorities. This failure has even caught the attention of Elon Musk, who shared a thread documenting the man's disturbing history in Germany and a lack of action from authorities.\\n\\nTaleb A. worked as a specialist in psychiatry and psychotherapy at a correctional facility in Bernberg. Shortly before the attack, he was on sick leave for several weeks.\\n\\nGermans have had their houses raided for calling Economic Minister Robert Habeck an \\\"idiot,\\\" and hundreds have faced prosecution for such insults. However, Taleb A. was saying just before the attack on X \\\"I assure you that revenge will 100 percent come soon. Even if it costs me my life.\\\"\\n\\nThere was no security response to his threats. In fact, police have been instead harassing grandmas at Christmas markets, a potential source of wasted resources that could have been put to work stopping an actual terror attack.\\n\\nTaleb A.is charged with five counts of murder and 205 counts of attempted murder. While Taleb A. is alive, Germans are mourning the deaths of their loved ones. Writing to her dead son Andr\\u00e9, D\\u00e9sir\\u00e9e G. ends her farewell post by writing: \\\"You will always live on in our hearts. I promise you that.\\\"\",\n                \"source\": {\n                    \"uri\": \"infowars.com\",\n                    \"dataType\": \"news\",\n                    \"title\": \"Infowars\"\n                },\n                \"authors\": [],\n                \"image\": \"https://imagedelivery.net/aeJ6ID7yrMczwy3WKNnwxg/3168c99d-690b-4b26-b635-721cf9089700/w=800,h=450\",\n                \"eventUri\": null,\n                \"sentiment\": -0.1764705882352942,\n                \"wgt\": 92912,\n                \"relevance\": 18\n            }\n        ]\n    }\n}\nSent article: German Mother Mourns Loss of 9-Year-Old Son in Christmas Market Attack to Kafka\n{'uri': '2024-12-583281968', 'lang': 'eng', 'isDuplicate': False, 'date': '2024-12-22', 'time': '16:40:27', 'dateTime': '2024-12-22T16:40:27Z', 'dateTimePub': '2024-12-22T16:39:48Z', 'dataType': 'news', 'sim': 0, 'url': 'https://www.infowars.com/posts/german-mother-mourns-loss-of-9-year-old-son-in-christmas-market-attack', 'title': 'German Mother Mourns Loss of 9-Year-Old Son in Christmas Market Attack', 'body': 'Germany continues to reel from a terror attack committed by Taleb A., a Saudi man who was granted German citizenship, after Taleb murdered five Germans and wounded over 200 on Friday night at the Magdeburg Christmas market. Now, the mother of the youngest victim, 9-year-old André, has posted a heart-wrenching message on Facebook following the loss of her son.\\n\\n\"André didn\\'t do anything to anyone -- why you, why?\" With these words, Désirée G. wrote her goodbyes to her son on Facebook. She also shared a photo of him.\\n\\n\"Let my little teddy bear fly around the world again,\" she begins her post. She wrote that André is now with his grandparents in heaven.\\n\\n\"They missed you very much, as much as we miss you here now.\" she added.\\n\\nTaleb A. drove a rented BMW into the crowd at full speed, killing five but also injuring another 200 people. There are still dozens fighting for their lives. So far, 41 people were seriously injured in the attack,\\n\\nOne woman, Anne, who was hit by the driver and knocked unconscious, told Bild newspaper: \"I woke up and thought I was in a dream.\" She still has a bloodshot eye from attack, however, her husband suffered a far worse injury to his thigh, saying that his flesh was \"literally ripped out.\" However, they were reunited when Anne regained consciousness when volunteers helped them find each other at the scene of the attack, and both expressed their happiness at being alive.\\n\\nNotably, German Christmas markets are already treated like fortresses, but Taleb A. found the only spot in the entire market that did not have a barrier, which was there to allow emergency vehicles and personnel to gain access to the market in the case of an emergency.\\n\\nThere is currently a debate raging about the man\\'s motives, with a number on the right trying to claim Taleb A.is a secret Islamist. However, it appears from his own messages that he was furious that Germany was not granting asylum to Saudi refugees. He is on record threatening Germany and ethnic Germans with \"slaughter\" for not helping Saudi ex-Muslims.\\n\\nSome on the left are trying to tie him to the AfD, since he expressed support for the party in one of his posts. However, skeptics cite him saying he was a \"leftist\" in a video, that the AfD is strictly against mass immigration and granting asylum to those with a criminal record such as Taleb A., and that his multiple threats against Germans stands in complete contrast to the AfD\\'s political platform, which seeks to block foreigners such as Taleb from entering the country entirely.\\n\\nRegardless of his motives, the man has a criminal history and had an open extradition request from Saudi Arabia -- yet he was still granted asylum and citizenship. In Germany, he faced a criminal complaint dating back to 2013 for threatening behavior. According to the Spiegel, Taleb A. had was found guilty in a district court in Rostock and sentenced to a fine of 90 daily rates on Sept. 4, 2013, for \"disturbing the public peace by threatening to commit criminal offenses\".\\n\\nIn addition, despite the German authorities at the BAMF being warned about his threatening statements against Germans by X users, no action was taken.\\n\\nIn short, it was a catastrophic failure in terms of migration and security policy from the German authorities. This failure has even caught the attention of Elon Musk, who shared a thread documenting the man\\'s disturbing history in Germany and a lack of action from authorities.\\n\\nTaleb A. worked as a specialist in psychiatry and psychotherapy at a correctional facility in Bernberg. Shortly before the attack, he was on sick leave for several weeks.\\n\\nGermans have had their houses raided for calling Economic Minister Robert Habeck an \"idiot,\" and hundreds have faced prosecution for such insults. However, Taleb A. was saying just before the attack on X \"I assure you that revenge will 100 percent come soon. Even if it costs me my life.\"\\n\\nThere was no security response to his threats. In fact, police have been instead harassing grandmas at Christmas markets, a potential source of wasted resources that could have been put to work stopping an actual terror attack.\\n\\nTaleb A.is charged with five counts of murder and 205 counts of attempted murder. While Taleb A. is alive, Germans are mourning the deaths of their loved ones. Writing to her dead son André, Désirée G. ends her farewell post by writing: \"You will always live on in our hearts. I promise you that.\"', 'source': {'uri': 'infowars.com', 'dataType': 'news', 'title': 'Infowars'}, 'authors': [], 'image': 'https://imagedelivery.net/aeJ6ID7yrMczwy3WKNnwxg/3168c99d-690b-4b26-b635-721cf9089700/w=800,h=450', 'eventUri': None, 'sentiment': -0.1764705882352942, 'wgt': 92912, 'relevance': 18}\nSent article: German Mother Mourns Loss of 9-Year-Old Son in Christmas Market Attack to Kafka\n"
#         },
#         {
#             "role": "assistant",
#             "content": "Based on the provided JSON, I will create a new JSON object with the desired keys and values. Here is the new JSON object:\n\n```\n{\n    \" מדינה\": \"Germany\",\n    \"מס הרוגים\": 5,\n    \"מס פצועים\": 200,\n    \"עיר\": \"Magdeburg\",\n    \"שנה\": 2024,\n    \"חודש\": 12,\n    \"יום\": 22,\n    \"סוג נשק\": \"Car\",\n    \"סוג תקיפה\": \"Terror attack\",\n    \"סוג מטרה\": \"Christmas market\",\n    \"סוג קבוצת טרור\": \"Saudi\",\n    \"אזור\": null\n}\n```\n\nNote that I did not include the \"סוג קבוצת טרור\" field in the original JSON, so I left it as `null`. If you want to fill it, please let me know the correct value."
#         },
#
#         {
#             "role": "assistant",
#             "content": "Here is the code in Python:\n```\nimport json\n\n# Original JSON data\noriginal_data = json.loads('''\n{\n    \"articles\": {\n        \"page\": 1,\n        \"pages\": 2199,\n        \"totalResults\": 2199,\n        \"results\": [\n            {\n                \"uri\": \"2024-12-583281968\",\n                \"lang\": \"eng\",\n                \"isDuplicate\": false,\n                \"date\": \"2024-12-22\",\n                \"time\": \"16:40:27\",\n                \"dateTime\": \"2024-12-22T16:40:27Z\",\n                \"dateTimePub\": \"2024-12-22T16:39:48Z\",\n                \"dataType\": \"news\",\n                \"sim\": 0,\n                \"url\": \"https://www.infowars.com/posts/german-mother-mourns-loss-of-9-year-old-son-in-christmas-market-attack\",\n                \"title\": \"German Mother Mourns Loss of 9-Year-Old Son in Christmas Market Attack\",\n                \"body\": \"... (rest of the data)...\",\n                \"source\": {\n                    \"uri\": \"infowars.com\",\n                    \"dataType\": \"news\",\n                    \"title\": \"Infowars\"\n                },\n                \"authors\": [],\n                \"image\": \"https://imagedelivery.net/aeJ6ID7yrMczwy3WKNnwxg/3168c99d-690b-4b26-b635-721cf9089700/w=800,h=450\",\n                \"eventUri\": null,\n                \"sentiment\": -0.1764705882352942,\n                \"wgt\": 92912,\n                \"relevance\": 18\n            }\n        ]\n    }\n}\n''')\n\n# Extract relevant data\ndata = original_data[\"articles\"][\"results\"][0]\ncountry = \"Germany\"\nnumber_killed = 5\nnumber_wounded = 200\ncity = \"Magdeburg\"\nyear = 2024\nmonth = 12\nday = 22\nweapon = \"Car\"\nattack_type = \"Terror attack\"\ntarget = \"Christmas market\"\nterror_group = \"Saudi\"\nregion = None\n\n# Create new JSON object\nnew_data = {\n    \"מדינה\": country,\n    \"מס הרוגים\": number_killed,\n    \"מס פצועים\": number_wounded,\n    \"עיר\": city,\n    \"שנה\": year,\n    \"חודש\": month,\n    \"יום\": day,\n    \"סוג נשק\": weapon,\n    \"סוג תקיפה\": attack_type,\n    \"סוג מטרה\": target,\n    \"סוג קבוצת טרור\": terror_group,\n    \"אזור\": region\n}\n\n# Print the new JSON object\nprint(json.dumps(new_data, indent=4))\n```\n\nThis code extracts the relevant data from the original JSON object and creates a new JSON object with the desired keys and values. The new JSON object is then printed to the console using the `json.dumps()` function."
#         }
#     ],
#     temperature=1,
#     max_tokens=1024,
#     top_p=1,
#     stream=True,
#     stop=None,
# )
#
# for chunk in completion:
#     print(chunk.choices[0].delta.content or "", end="")




