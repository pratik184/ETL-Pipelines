#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pymysql

host = 'localhost'
user = 'root'
password = '1234'
database = 'mydatabase'
table_name = 'clickstreamed_data'



# Connect to MySQL
connection = pymysql.connect(host=host, user=user, password=password, database=database)
cursor = connection.cursor()

# Create the table
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (row_key INT PRIMARY KEY, user_id VARCHAR(10), timestamp DATETIME, url VARCHAR(100), country VARCHAR(20), city VARCHAR(20), browser VARCHAR(20), os VARCHAR(20), device VARCHAR(20))"
cursor.execute(create_table_query)
connection.commit()

# Insert data into the table
for _, row in clickstream_data.iterrows():
    insert_query = f"INSERT INTO {table_name} (row_key, user_id, timestamp, url, country, city, browser, os, device) VALUES ({row['row_key']}, '{row['user_id']}', '{row['timestamp']}', '{row['url']}', '{row['country']}', '{row['city']}', '{row['browser']}', '{row['os']}', '{row['device']}')"
    cursor.execute(insert_query)
    connection.commit()

cursor.close()
connection.close()

