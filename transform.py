from pyspark.sql import SparkSession 
import os 
import logging 
import dotenv
from pyspark.sql.types import *
from pyspark.sql import functions as F

dotenv.load_dotenv(dotenv_path='/NYC_ETL/config.env')


logging.basicConfig(
    filename = '/NYC_ETL/logs/transform.log',
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Spark session with configurations
spark = SparkSession.builder \
    .appName("NYC_ETL") \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "5g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") \
    .getOrCreate()

nyc_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", LongType(), True),
    StructField("tpep_dropoff_datetime", LongType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])


def transform(file_path, year, month):
    df_raw = spark.read.schema(nyc_schema).format("parquet").load(file_path)
    df_count = df_raw.count()
    logging.info(f"Succesfully read the parquet file: {file_path}, count: {df_count}")

    # Only convert microseconds if it's LongType (INT64)
    df_raw = df_raw \
        .withColumn("tpep_pickup_datetime", F.from_unixtime(F.col("tpep_pickup_datetime") / 1_000_000).cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", F.from_unixtime(F.col("tpep_dropoff_datetime") / 1_000_000).cast("timestamp"))
 
    # Map store_and_fwd_flag to boolean values
    df_raw = df_raw.withColumn("store_and_fwd_flag_bool",
                F.when(F.col("store_and_fwd_flag") == "Y", True)
                .when(F.col("store_and_fwd_flag") == "N", False)
                .otherwise(None)
    )

    payment_types = [(0, 'Flex Fare trip'),
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
    ]
    payment_types_df = spark.createDataFrame(payment_types, ["payment_type", "description"])
    
    #payment_type Replace codes with mapping from dictionary 
    df_raw = df_raw.join(payment_types_df, on="payment_type", how="left")

    df_raw = df_raw.drop("payment_type")
    df_raw = df_raw.withColumnRenamed("description", "payment_type")

    # Mapping RatecodeID to descriptive values
    df_raw = df_raw.withColumn(
        'RatecodeID',
        F.when(F.col('RatecodeID') == 1, 'Standard rate')
        .when(F.col('RatecodeID') == 2, 'JFK')
        .when(F.col('RatecodeID') == 3, 'Newark')
        .when(F.col('RatecodeID') == 4, 'Nassau or Westchester')
        .when(F.col('RatecodeID') == 5, 'Negotiated fare')
        .when(F.col('RatecodeID') == 6, 'Group ride')
        .when(F.col('RatecodeID') == 99, 'Null/Unknown')
        .otherwise(None)
    )

    # reading the zones data 
    zones_df = spark.read.csv("/NYC_ETL/taxi-zone-lookup.csv", header=True, inferSchema=True)

    # Joining the zones data with the main dataframe
    df_raw = df_raw.join(zones_df, df_raw.PULocationID == zones_df.LocationID, how='left') \
        .withColumnRenamed("Zone", "Pickup_Zone") \
        .drop("LocationID", "Borough")
    df_raw = df_raw.join(zones_df, df_raw.DOLocationID == zones_df.LocationID, how='left') \
        .withColumnRenamed("Zone", "Dropoff_Zone") \
        .drop("LocationID", "Borough")
    
    # Create a new column for the trip type based on the pickup and dropoff zones
    df_raw = df_raw.withColumn("trip_type",
                F.when(F.col("Pickup_Zone") == F.col("Dropoff_Zone"), "IntraZone")
                .otherwise("InterZone")
    )
    
    #create is_airport_trip column -- PULocationID or DOLocationID in [132, 138, 1, 140] (JFK, LGA, EWR zones)
    df_raw = df_raw.withColumn("is_airport_trip",
                F.when((F.col('PULocationID').isin([132,138,1,140])) | (F.col('DOLocationID').isin([132,138, 1, 140])), True).otherwise(False))

    #create is_night_trip column -- between 8PM–6AM
    df_raw = df_raw.withColumn("is_night_trip",
                F.when(F.hour(F.col('tpep_pickup_datetime')).between(20,23)| F.hour(F.col('tpep_pickup_datetime')).between(0,6), True).otherwise(False))
    
    #create is_weekend_trip column
    # 1 = Sunday, 2 = Monday, 3 = Tuesday, 4 = Wednesday, 5 = Thursday, 6 = Friday, 7 = Saturday
    df_raw = df_raw.withColumn('is_weekend_trip',
                F.when(F.dayofweek(F.col('tpep_pickup_datetime')).isin(6,7), True).otherwise(False))

    # extracting pickup date 
    df_raw = df_raw.withColumn("pickup_date", df_raw.tpep_pickup_datetime.cast("date"))

    # create is_zero_distance_trip column
    df_raw = df_raw.withColumn("is_zero_distance_trip", F.when(F.col("trip_distance") == 0, True).otherwise(False))

    # Removing data which is not valid
    df_clean = df_raw.filter(
        (df_raw.passenger_count > 0) &  # Filter rows where passenger_count is greater than 0 
        (df_raw.fare_amount > 0) &  # Filter rows where fare_amount is greater than 0
        (df_raw.total_amount > 0) &    # Filter rows where total_amount is greater than 0
        (df_raw.trip_distance > 0)  # Filter rows where trip_distance is greater than 0
        ) 
    
    df_clean = df_clean.dropna(subset=["passenger_count"]) # Drop rows where passenger_count is null

    # Calculate trip_duration in minutes
    df_clean = df_clean.withColumn("trip_duration", F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))/ 60))

    # Filter out trips with negative or zero duration
    df_clean = df_clean.filter(F.col("trip_duration") > 0)

    # Droping duplicate rides 
    df_clean = df_clean.dropDuplicates(["tpep_pickup_datetime", "PULocationID", "DOLocationID"])

    # create average_speed column in miles per hour
    df_clean = df_clean.withColumn("average_speed", F.round(F.col("trip_distance") / (F.col("trip_duration")/60), 2))

    df_clean = df_clean \
    .withColumn("data_version", F.lit("v1.0")) \
    .withColumn("ingestion_timestamp", F.current_timestamp())


    final_columns = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_date",
    "PULocationID", "Pickup_Zone", "DOLocationID", "Dropoff_Zone", "trip_type",
    "passenger_count", "trip_distance", "is_zero_distance_trip", "trip_duration", "average_speed",
    "is_airport_trip", "is_night_trip", "is_weekend_trip", "store_and_fwd_flag_bool",
    "fare_amount", "tip_amount", "total_amount", "payment_type", "RatecodeID",
    "extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
    "Airport_fee",  "data_version", "ingestion_timestamp"]

    # Select only the final columns in the cleaned data
    df_clean = df_clean.select(final_columns)

    # unncessaru data(Invalid data) quarantine
    df_quarantine = df_raw.filter(
        (df_raw.passenger_count <= 0) | # FIlter rows where passenger_count is less than 0 
        (df_raw.fare_amount <= 0) |  # Filter rows where fare_amount is less than 0
        (df_raw.total_amount <= 0) |    # Filter rows where total_amount is less than 0
        (df_raw.trip_distance <= 0)  # Filter rows where trip_distance is less than 0
        ) 

    df_clean_count = df_clean.count()
    df_q_count = df_quarantine.count()

    # Save the cleaned data and quarantine data to the specified locations                  
    clean_path = os.path.join(os.getenv("CLEAN_LOCATION"), str(year), f"{month:02d}")
    os.makedirs(clean_path, exist_ok=True)

    logging.info("Transformation completed successfully.")
    df_clean.write.partitionBy("pickup_date").mode("overwrite").parquet(clean_path)

    logging.info(f"Succesfully transfomed and file saved to: {clean_path}, count: {df_clean_count}")

    quarantine_path = os.path.join(os.getenv("QUARANTINE_LOCATION"), str(year), f"{month:02d}")
    os.makedirs(quarantine_path, exist_ok=True)

    df_quarantine.write.partitionBy("pickup_date").mode("overwrite").parquet(quarantine_path)
    logging.info(f"Quarantined data saved to: {quarantine_path}, count: {df_q_count}")


for year in range(2024, 2025):
    for month in range(1, 13):
        file_path = os.path.join(os.getenv("RAW_LOCATION"), str(year), f"{month:02d}", f"yellow_tripdata_{year}-{month:02d}.parquet")
        logging.info(f"Processing file: {file_path}")
        if not os.path.exists(file_path):
            logging.error(f"File does not exist: {file_path}")
            continue
        transform(file_path, year, month)
        logging.info(f"Transforming data for {year}-{month:02d}")



