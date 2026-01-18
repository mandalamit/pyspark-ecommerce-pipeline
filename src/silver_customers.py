from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Silver-Customers") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Read from Bronze
df_bronze = spark.read.parquet("data/bronze/customers")

# Deduplicate (real-world logic)
window = Window.partitionBy("customer_id").orderBy(col("created_at").desc())

df_silver = df_bronze \
    .withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

print("Silver Data:")
df_silver.show()

# Write to Silver layer
df_silver.write \
    .mode("overwrite") \
    .parquet("data/silver/customers")

spark.stop()
