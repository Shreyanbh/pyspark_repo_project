# scripts/data_processing.py

#Starting a spark session

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkConf, SparkContext



# Initialize Spark session
conf = SparkConf().set("spark.hadoop.hadoop.home.dir", "C:\\hadoop\\bin")
spark = SparkSession.builder.appName("PySparkRepoProject").getOrCreate()

# Load data (from local or HDFS)
data_path = "pyspark_repo_project/data/food_consumption.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Display schema and some rows
df.printSchema()
df.show(5)

# Filter and transformation
filtered_df = df.filter(df.consumption>5)

# Save the processed data
filtered_df.write.csv("data/processed_data.csv", header=True)

# Stop Spark session
spark.stop()
