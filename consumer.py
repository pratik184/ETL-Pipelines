#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('pratikop', bootstrap_servers='localhost:9092')
for message in consumer:
    click_event = json.loads(message.value)
    user_id = click_event['user_id']
    timestamp = click_event['timestamp']
    url = click_event['url']
    ip_address = click_event['ip_address']
    user_agent = click_event['user_agent']

    # Process or print the extracted information as desired
    print(f"User ID: {user_id}, Timestamp: {timestamp}, URL: {url}, IP Address: {ip_address}, User Agent: {user_agent}")



# In[ ]:




