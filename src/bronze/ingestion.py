import requests
from datetime import datetime
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from utils.spark_session import get_spark
from config.paths import BRONZE_PATH

def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def main():
    spark = get_spark()

    data = fetch_crypto_data()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
    ])

    df = spark.createDataFrame(data, schema=schema)

    df = df.withColumn("ingestion_timestamp", lit(datetime.utcnow()))

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("crypto_bronze")

if __name__ == "__main__":
    main()