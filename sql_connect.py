#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pyodbc


# In[24]:


# server = 'localhost'
# database = 'mydatabase'
# username = 'root'
# password = '1234'
# driver = 'SQL Server'
# #connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
# #connection = pyodbc.connect(connection_string)
# conn = pyodbc.connect(Driver="SQL Server",Server='127.0.0.1',Database='test',UID="root",PWD="1234")


# In[36]:


import time
from datetime import datetime
data = [
    (123, datetime.now(), 'https://example.com', 'Country 1', 'City 1', 'Browser 1', 'OS 1', 'Device 1'),
    (456, datetime.now(), 'https://example.com', 'Country 2', 'City 2', 'Browser 2', 'OS 2', 'Device 2'),
    (789, datetime.now(), 'https://example.com', 'Country 3', 'City 3', 'Browser 3', 'OS 3', 'Device 3')
]

# Column names
columns = ['user_id', 'timestamp', 'url', 'country', 'city', 'browser', 'operating_system', 'device']

# Create DataFrame
df = pd.DataFrame(data, columns=columns)


# In[37]:


df.dtypes
# df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S')


# In[38]:


import pymysql

# Connect to MySQL database
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='1234',
    database='mydatabase'
)

# # Create a cursor object to execute SQL queries
cursor = connection.cursor()

# # Example: Execute a SELECT query
# cursor.execute('SELECT * FROM your_t/able_name')

# # Fetch all rows from the result set
# rows = cursor.fetchall()

# # Process the fetched rows
# for row in rows:
#     print(row)

# # Close the cursor and connection
# cursor.close()
# connection.close()


# In[ ]:





# In[ ]:





# In[33]:


# !pip install pymysql
df.columns


# In[40]:


with connection.cursor() as cursor:
    # Generate the SQL query for inserting the rows
    query = f"INSERT INTO {table_name} (user_id, timestamp, url, country, city, browser, operating_system, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    
    # Iterate over the DataFrame rows and insert each row into the table
    for row in df.itertuples(index=False):
        cursor.execute(query, row)
    
    # Commit the changes
    connection.commit()



# In[ ]:




