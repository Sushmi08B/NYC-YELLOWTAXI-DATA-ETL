from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

df = spark.read.parquet("/Users/chanduprasadreddypotukanuma/Downloads/Projects/clean/NYC_YELLOW_TAXI_DATA/2024/01/pickup_date=2024-01-03")



df.show(5, False)         # Show first 5 rows without truncating
df.printSchema()          # Show column names and types
df.columns                # List of all columns
