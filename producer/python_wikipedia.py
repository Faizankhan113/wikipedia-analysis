import json
import requests
from sseclient import SSEClient
from confluent_kafka import Producer


# Wikipedia Stream
WIKI_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS = {
    "User-Agent": "FaizanWikiProject/1.0 (learning project)"
}


# Kafka Config
conf = {
    "bootstrap.servers": "kafka:9092",
    "client.id": 'wiki-producer'
}


producer = Producer(conf)


def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)


# Connect to Wikipedia
response = requests.get(WIKI_URL, headers=HEADERS, stream=True)
client = SSEClient(response)

print("Connected. Streaming → Kafka...")


for event in client.events():

    if event.data:

        try:
            data = json.loads(event.data)

            producer.produce(
                topic="wiki-changes",
                value=json.dumps(data),
                callback=delivery_report
            )

            producer.poll(0)


        except Exception as e:
            print("Error:", e)


producer.flush()
