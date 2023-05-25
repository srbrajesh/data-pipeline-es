#!/usr/bin/env python
# coding: utf-8

# In[1]:


import random
import time

# Generate a unique identifier for each click event.
def generate_row_key():
  return str(random.randint(1, 1000000))

# Generate the click data for a click event.
def generate_click_data():
  user_id = random.randint(1, 1000000)
  timestamp = time.time()
  url = "https://www.example.com/"
  return {
    "user_id": user_id,
    "timestamp": timestamp,
    "url": url
  }

# Generate the geo data for a click event.
def generate_geo_data():
  country = random.choice(["United States", "United Kingdom", "Canada"])
  city = random.choice(["New York City", "London", "Toronto"])
  return {
    "country": country,
    "city": city
  }

# Generate the user agent data for a click event.
def generate_user_agent_data():
  browser = random.choice(["Chrome", "Firefox", "Safari"])
  operating_system = random.choice(["Windows", "Mac", "Linux"])
  device = random.choice(["Desktop", "Mobile", "Tablet"])
  return {
    "browser": browser,
    "operating_system": operating_system,
    "device": device
  }

# Generate a clickstream event.
def generate_clickstream_event():
  row_key = generate_row_key()
  click_data = generate_click_data()
  geo_data = generate_geo_data()
  user_agent_data = generate_user_agent_data()
  return {
    "row_key": row_key,
    "click_data": click_data,
    "geo_data": geo_data,
    "user_agent_data": user_agent_data
  }

# Generate a batch of clickstream events.
def generate_batch_of_clickstream_events(batch_size):
  events = []
  for _ in range(batch_size):
    events.append(generate_clickstream_event())
  return events


# In[12]:


from pykafka import KafkaClient
from .constants import KAFKA_HOST, TOPIC_NAME

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics[TOPIC_NAME]

events = generate_batch_of_clickstream_events(100)

with topic.get_sync_producer() as producer:
    for event in events:
        encoded_message = str(event).encode("utf-8")
        producer.produce(encoded_message)


# In[ ]:




