# ============================================
# SE446 - Milestone 2 Phase A
# Group: CrimeDataEngineers
# Task 1-4: Sulaiman AlEiteibi(ID:220391)
# ============================================

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, avg, round as spark_round
import os


input_path = sys.argv[1] if len(sys.argv) > 1 else "data/chicago_crimes_sample.csv"

spark = SparkSession.builder \
    .appName("M2_Phase_A_Sulaiman") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Version:", spark.version)
print("Master:", spark.sparkContext.master)
print("Input:", input_path)

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)
# the line below will keep the columes that are needed for phase a 
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

print("Total Rows:", df.count()) # this is task1 which counts crimes by the type and it shows top ten

print("\nTask 1: Top 10 Crime Types")
task1 = df.groupBy("Primary Type") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

task1.show(truncate=False)

# ============================================
# Task 2: Location Hotspots using Spark SQL
# Author: Sulaiman AlEiteibi (ID: 220391)
# ============================================

# Creates a temporary SQL view so we can query the DataFrame using Spark SQL
df.createOrReplaceTempView("crimes")

# Count crimes by location and show the top 10 locations
print("\nTask 2: Top 10 Location Hotspots using Spark SQL")

task2 = spark.sql("""
    SELECT `Location Description`, COUNT(*) AS total
    FROM crimes
    WHERE `Location Description` IS NOT NULL
    GROUP BY `Location Description`
    ORDER BY total DESC
    LIMIT 10
""")

task2.show(truncate=False)

# ============================================
# Task 3: Crime Trend Over Years
# Author: Sulaiman AlEiteibi (ID: 220391)
# ============================================

# Count crimes for each year and sort by year
print("\nTask 3: Crime Trend Over Years")

task3 = df.where(col("Year").isNotNull()) \
    .groupBy("Year") \
    .count() \
    .orderBy("Year")

task3.show(50, truncate=False)

# Save a simple line chart only when running locally
if spark.sparkContext.master.startswith("local"):
    import matplotlib.pyplot as plt

    os.makedirs("output/m2_phase_a", exist_ok=True)

    yearly_rows = task3.collect()
    years = [row["Year"] for row in yearly_rows]
    counts = [row["count"] for row in yearly_rows]

    plt.figure(figsize=(8, 5))
    plt.plot(years, counts, marker="o")
    plt.xlabel("Year")
    plt.ylabel("Crime Count")
    plt.title("Crime Trend Over Years")
    plt.grid(True)
    plt.savefig("output/m2_phase_a/task3_yearly_trend.png")
    plt.close()

    print("Saved chart: output/m2_phase_a/task3_yearly_trend.png")
else:
    print("Cluster mode: printed yearly table is used for Task 3 evidence.")

# ============================================
# Task 4: Arrest Rate Analysis
# Author: Sulaiman AlEiteibi (ID: 220391)
# ============================================

# Calculate the overall percentage of crimes that resulted in arrest
print("\nTask 4: Overall Arrest Rate")

overall_arrest = df.select(
    spark_round(avg(col("Arrest").cast("int")) * 100, 2).alias("overall_arrest_rate_percent")
)

overall_arrest.show()

# Calculate arrest rate for each crime type, focusing on the top 10 crime types by count
print("\nTask 4: Arrest Rate by Top 10 Crime Types")

task4 = df.groupBy("Primary Type") \
    .agg(
        count("*").alias("total_crimes"),
        spark_round(avg(col("Arrest").cast("int")) * 100, 2).alias("arrest_rate_percent")
    ) \
    .orderBy(desc("total_crimes")) \
    .limit(10)

task4.show(truncate=False)

print("\nHighest Arrest Rates among Top Crime Types")
task4.orderBy(desc("arrest_rate_percent")).show(3, truncate=False)

print("\nLowest Arrest Rates among Top Crime Types")
task4.orderBy("arrest_rate_percent").show(3, truncate=False)

spark.stop()