#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pymysql
import requests
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# MySQL connection
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = '1234'
mysql_database = 'mydatabase'
mysql_table = 'clickstreamed_data'

# Elasticsearch connection 
es_host = 'localhost'
es_port = 9200
es_index = 'clickstream_index'
es_username = 'elastic' 
es_password = 'bP=2BcdCs=FpcMAErf4o'  

# Created MySQL connection
conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_database)
cursor = conn.cursor()

# fetch data from MySQL table
query = """
SELECT url, country, 
       COUNT(DISTINCT user_id) AS unique_users, 
       COUNT(DISTINCT row_key) AS clicks, 
       AVG(TIMESTAMPDIFF(MINUTE, timestamp, NOW())) AS avg_time_spent
FROM clickstreamed_data
GROUP BY url, country;"""
cursor.execute(query)
data = cursor.fetchall()

cursor.close()
conn.close()

# Created Elasticsearch session
session = requests.Session()
session.auth = (es_username, es_password)

# Insert data into Elasticsearch
for row in data:
    #print(data)
    url = row[0]
    #print(url)
    country = row[1]
    unique_users = int(row[2]) if row[1].isdigit() else 0
    clicks = int(row[3]) if row[0].isdigit() else 0
    avg_time_spent = row[4]

    document = {
        "url": url,
        "country": country,
        "unique_users": unique_users,
        "clicks": clicks,
        "avg_time_spent": avg_time_spent
    }

    
    url = f"https://{es_host}:{es_port}/{es_index}/_doc"
    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Authorization': 'Basic ZWxhc3RpYzpiUD0yQmNkQ3M9RnBjTUFFcmY0bw==',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    "Content-Type": "application/json",
    # 'Cookie': 'JSESSIONID.74da03e4=node012it413nhu9m1hwa4475d9su30.node0; JSESSIONID.268a4ad1=node019dvokdt0cof4kcz1jijdmzgv2.node0; JSESSIONID.b1141a01=node01ewwmwnioqzvpbj214t6kjegl2.node0; screenResolution=1366x768; JSESSIONID.e9494ba3=node0noodmj90ltx41gbxe3ixuos5t3.node0',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    }
    response = requests.post(url, headers=headers, data=json.dumps(document, cls=DecimalEncoder),verify=False)
#     requests.post
    # Check the response status
    if response.status_code == 201:
        print("Row successfully inserted into Elasticsearch.")
    else:
        print("Failed to insert row into Elasticsearch.")
    #break
    

