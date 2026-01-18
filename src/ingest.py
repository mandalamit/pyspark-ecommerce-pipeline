from pyspark.sql import SparkSession

from config.spark_config import get_spark

# Create Spark session
spark = get_spark('Ecommerce-Ingestion')

# spark = SparkSession.builder \
#     .appName("Ecommerce-Ingestion") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.sql.shuffle.partitions", "8") \
#     .getOrCreate()

print("Spark session created successfully")

# Sample data (simulating raw CSV data)
data = [
    (1, "Amit", "amit@gmail.com", "2024-01-01"),
    (2, "Rahul", "rahul@gmail.com", "2024-01-02"),
    (3, "Priya", "priya@gmail.com", "2024-01-03")
] 

columns = ["customer_id", "name", "email", "created_at"]

# Create DataFrame
df_customers = spark.createDataFrame(data, columns)

print("Raw Data:")
df_customers.show()

# Write to Bronze layer
df_customers.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("data/bronze/customers")

print("Data written to Bronze layer successfully")

spark.stop()
