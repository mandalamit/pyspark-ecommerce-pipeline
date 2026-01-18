from pyspark.sql import SparkSession

from config.spark_config import get_spark

spark = get_spark('Orders-Ingestion')

# spark = SparkSession.builder \
#     .appName("Orders-Ingestion") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.sql.shuffle.partitions", "8") \
#     .getOrCreate()

# Sample orders data (simulate raw data)
data = [
    (101, 1, 250.0, "2024-01-05", "2024-01-05"),
    (102, 2, 450.0, "2024-01-06", "2024-01-06"),
    (103, 1, 300.0, "2024-01-07", "2024-01-07"),
    (103, 1, 300.0, "2024-01-07", "2024-01-08")  # duplicate (updated)
]

columns = ["order_id", "customer_id", "amount", "order_date", "updated_at"]

df_orders = spark.createDataFrame(data, columns)

print("Raw Orders:")
df_orders.show()

# Write to Bronze
df_orders.write \
    .mode("overwrite") \
    .parquet("data/bronze/orders")

spark.stop()
