from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

products_data = [(1, "Липтон"),
                 (2, "Кока-кола"),
                 (3, "Чипсы"),
                 (4, "Швабра")]

categories_data = [(1, "Напитки"),
                   (2, "Газировка"),
                   (3, "Снеки")]

product_category_data = [(1, 1),
                         (2, 1),
                         (2, 2),
                         (3, 3)]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

result_df = products_df.join(product_category_df, "product_id", "left") \
                      .join(categories_df, "category_id", "left") \
                      .select("product_name", coalesce("category_name", "Нет категории").alias("category_name"))

result_df.show()
