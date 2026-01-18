from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder \
    .appName("Gold-Customer-Sales") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

customers = spark.read.parquet("data/silver/customers")
orders = spark.read.parquet("data/silver/orders")

df_joined = customers.join(
    orders,
    on="customer_id",
    how="left"
)

df_gold = df_joined.groupBy("customer_id", "name") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_amount")
    )

print("Gold Analytics:")
df_gold.show()

df_gold.write \
    .mode("overwrite") \
    .parquet("data/gold/customer_sales")

spark.stop()