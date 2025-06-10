from pyspark.sql import SparkSession
import dotenv
import os 

import logging 

logging.basicConfig(
    filename='/NYC_ETL/logs/transform.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

dotenv.load_dotenv(dotenv_path='/NYC_ETL/config.env')

spark = SparkSession.builder.appName("NYC_ETL").config("spark.jars", "/Users/chanduprasadreddypotukanuma/PycharmProjects/PySparkProject/WeatherETL/postgresql-42.7.6.jar").getOrCreate()

host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
db = os.getenv("PG_DB")
url = f"jdbc:postgresql://{host}:{port}/{db}"
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")

print("JDBC URL:", url)


for year in range(2024, 2025):
    for month in range(1, 13):
        file_path = os.path.join(os.getenv("CLEAN_LOCATION"), str(year), f"{month:02d}")

        df = spark.read.parquet(file_path)

        logging.info(f"Data for {year}-{month:02d} started loading to PostgreSQL.")

        df.write.jdbc(
            url = url,
            table = "nyc_yellowtaxi_data",
            mode = "append",
            properties={
                "user": user,
                "password": password,
                "driver": "org.postgresql.Driver"
    }
        )
        logging.info(f"Data for {year}-{month:02d} loaded successfully to PostgreSQL.")
        
