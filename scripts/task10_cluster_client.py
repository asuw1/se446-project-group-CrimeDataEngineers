from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, avg, round as spark_round

spark = SparkSession.builder \
    .appName("SE446_M2_Task10_Cluster_Client") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input_path = "hdfs:///data/chicago_crimes.csv"

print("============================================")
print("Task 10: Cluster Execution, Client Mode")
print("Author: Abdulaziz AlSenani")
print("Spark Version:", spark.version)
print("Master:", spark.sparkContext.master)
print("Input:", input_path)
print("============================================")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

df = df.select(
    col("ID"),
    col("Date"),
    col("Primary Type"),
    col("Location Description"),
    col("Arrest").cast("boolean").alias("Arrest"),
    col("Domestic").cast("boolean").alias("Domestic"),
    col("District").cast("int").alias("District"),
    col("Year").cast("int").alias("Year")
)

total_rows = df.count()
print("Real Row Count:", total_rows)

print("\nTask 10 Evidence Summary")
print("Master:", spark.sparkContext.master)
print("Dataset:", input_path)
print("Real Row Count:", total_rows)

print("\nTop 10 Crime Types on Full HDFS Dataset")
df.groupBy("Primary Type") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10) \
    .show(truncate=False)

print("\nCrime Trend Over Years on Full HDFS Dataset")
df.where(col("Year").isNotNull()) \
    .groupBy("Year") \
    .count() \
    .orderBy("Year") \
    .show(50, truncate=False)

print("\nOverall Arrest Rate on Full HDFS Dataset")
df.select(
    spark_round(avg(col("Arrest").cast("int")) * 100, 2).alias("overall_arrest_rate_percent")
).show()

spark.stop()