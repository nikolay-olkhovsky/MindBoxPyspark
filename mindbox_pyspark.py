from pyspark.sql import SparkSession
from faker import Faker
import faker_commerce
import pandas as pd
import random

NUMBER_OF_CONNECTIONS = 12
'''
При помощи фейкеров формирую три датафрейма:
продукты, категории, связи (промежуточная таблица)
для организации связи "многие-ко-многим"
'''
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

'''
При установке Спарка пришлось изрядно озадачиться. Попытался установить
под Windows, и уже после установки самого Спарка выяснил, что под него
нужно ставить Hadoop, который без танцев с бубном на Винде работать не
будет. В результате текст, который вы сейчас читаете написан в Linux
Ubuntu, запущенной в VM Virtual Box, где все встало и заработало без
дополнительного шаманства.

Задание оказалось интереснее, чем казалось. Фактически решение умещается
в 4 строки (74, 75, 79 и 80). Но вот пока я их запускал, узнал много
интересного.
'''
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
