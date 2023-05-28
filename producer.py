#!/usr/bin/env python
# coding: utf-8

# In[3]:


from kafka import KafkaProducer
import json

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Define the click event data
click_event = {
    'user_id': '123',
    'timestamp': '2023-05-14T12:34:56',
    'url': 'https://example.com',
    'ip_address': '192.168.0.1',
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Convert the click event to JSON
click_event_json = json.dumps(click_event)

# Produce the click event data to the "clickstream" topic
producer.send('pratikop', value=click_event_json.encode('utf-8'))

# Close the Kafka producer
producer.close()


# In[ ]:




