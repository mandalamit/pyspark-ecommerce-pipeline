from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from config.spark_config import get_spark


spark = get_spark('Silver-Orders')

# spark = SparkSession.builder \
#     .appName("Silver-Orders") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.sql.shuffle.partitions", "8") \
#     .getOrCreate()

df_bronze = spark.read.parquet("data/bronze/orders")

window = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

df_silver = df_bronze \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print("Silver Orders:")
df_silver.show()

df_silver.write \
    .mode("overwrite") \
    .parquet("data/silver/orders")

spark.stop()
