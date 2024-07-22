from pyspark.sql import SparkSession
from faker import Faker
import faker_commerce
import pandas as pd
import random

NUMBER_OF_CONNECTIONS = 12

fake = Faker()
fake.add_provider(faker_commerce.Provider)

product_ids = []
product_names = []
category_ids = []
category_names = []

for i in range(1, NUMBER_OF_CONNECTIONS+1):
    product_ids.append(i)
    product_names.append(fake.ecommerce_name())

products = pd.DataFrame({
    'id' : product_ids,
    'product_name' : product_names,
},
index = product_ids)

for i in range(1, NUMBER_OF_CONNECTIONS+1):
    category_ids.append(i)
    category_names.append(fake.ecommerce_category())

categories = pd.DataFrame({
    'id' : category_ids,
    'category_name' : category_names
},
index = category_ids)

conn_product_ids = []
conn_category_ids = []
for i in range(1, NUMBER_OF_CONNECTIONS):
    conn_product_ids.append(random.choice(product_ids))
    conn_category_ids.append(random.choice(category_ids))

connections = pd.DataFrame({
    'product_id' : conn_product_ids,
    'category_id' : conn_category_ids
})
#print(conn_product_ids)
#print(conn_category_ids)

spark = SparkSession.builder.appName("mindmap").getOrCreate()

products = spark.createDataFrame(products)
categories = spark.createDataFrame(categories)
connections = spark.createDataFrame(connections)

products.show()
categories.show()
connections.show(NUMBER_OF_CONNECTIONS)

result = products.join(connections, products.id == connections.product_id, "left")
result = result.join(categories, result.category_id == categories.id, "left")
result.show(100)
result.printSchema()

result = result.select('product_name', 'category_name')
result.show(100)
result.printSchema()
