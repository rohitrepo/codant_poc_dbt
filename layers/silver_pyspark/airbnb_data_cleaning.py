from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session with Snowflake configuration
spark = SparkSession.builder \
    .appName("AirbnbDataProcessingToSnowflake") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .getOrCreate()

# Snowflake connection configuration
sf_options = {
    "sfURL": "dbcatfo-ii99170.snowflakecomputing.com",
    "sfUser": "rohityadav121",
    "sfPassword": "Hellodbttesting@121",
    "sfDatabase": "AIRBNB_RAW",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "SYSADMIN"
}

# S3 paths (only for reading)
s3_input_bucket = "s3://raw-data-bucket/airbnb/"

# Read raw data from S3
hosts_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("is_superhost", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])
raw_hosts = spark.read.schema(hosts_schema).parquet(f"{s3_input_bucket}hosts/")

listings_schema = StructType([
    StructField("id", StringType(), True),
    StructField("listing_url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("room_type", StringType(), True),
    StructField("minimum_nights", StringType(), True),
    StructField("price", StringType(), True),
    StructField("host_id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])
raw_listings = spark.read.schema(listings_schema).parquet(f"{s3_input_bucket}listings/")

reviews_schema = StructType([
    StructField("id", StringType(), True),
    StructField("listing_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("reviewer_name", StringType(), True),
    StructField("comments", StringType(), True)
])
raw_reviews = spark.read.schema(reviews_schema).parquet(f"{s3_input_bucket}reviews/")

## 1. Clean and transform hosts data (for dim_hosts)
def process_hosts(df):
    # Select and rename columns
    hosts_clean = df.select(
        col("id").alias("HOST_ID"),
        col("name").alias("HOST_NAME"),
        col("is_superhost").alias("IS_SUPERHOST"),
        col("created_at").alias("HOST_CREATED_AT"),
        col("updated_at").alias("HOST_UPDATED_AT")
    )
    
    # Data quality checks and cleaning
    hosts_clean = hosts_clean.withColumn(
        "IS_SUPERHOST",
        when(lower(trim(col("IS_SUPERHOST"))).isin(["t", "true"]), True)
        .otherwise(False)
    )
    
    # Convert timestamp columns
    hosts_clean = hosts_clean.withColumn(
        "HOST_CREATED_AT",
        to_timestamp(col("HOST_CREATED_AT"))
    ).withColumn(
        "HOST_UPDATED_AT",
        to_timestamp(col("HOST_UPDATED_AT"))
    )
    
    # Handle nulls
    hosts_clean = hosts_clean.na.fill({
        "HOST_NAME": "Unknown",
        "IS_SUPERHOST": False
    })
    
    return hosts_clean

clean_hosts = process_hosts(raw_hosts)

## 2. Clean and transform listings data (for dim_listings)
def process_listings(df):
    # Select and rename columns
    listings_clean = df.select(
        col("id").alias("LISTING_ID"),
        col("listing_url").alias("LISTING_URL"),
        col("name").alias("LISTING_NAME"),
        col("room_type").alias("ROOM_TYPE"),
        col("minimum_nights").alias("MINIMUM_NIGHTS"),
        col("price").alias("PRICE_USD"),
        col("host_id").alias("HOST_ID"),
        col("created_at").alias("LISTING_CREATED_AT"),
        col("updated_at").alias("LISTING_UPDATED_AT")
    )
    
    # Data cleaning
    listings_clean = listings_clean.withColumn(
        "PRICE_USD",
        regexp_replace(col("PRICE_USD"), "[$,]", "").cast("double")
    ).withColumn(
        "MINIMUM_NIGHTS",
        col("MINIMUM_NIGHTS").cast("integer")
    )
    
    # Convert timestamps
    listings_clean = listings_clean.withColumn(
        "LISTING_CREATED_AT",
        to_timestamp(col("LISTING_CREATED_AT"))
    ).withColumn(
        "LISTING_UPDATED_AT",
        to_timestamp(col("LISTING_UPDATED_AT"))
    )
    
    # Handle nulls and defaults
    listings_clean = listings_clean.na.fill({
        "ROOM_TYPE": "Unknown",
        "MINIMUM_NIGHTS": 1,
        "PRICE_USD": 0.0
    })
    
    return listings_clean

clean_listings = process_listings(raw_listings)

## 3. Clean and transform reviews data (for fct_reviews)
def process_reviews(df):
    # Select and rename columns
    reviews_clean = df.select(
        col("id").alias("REVIEW_ID"),
        col("listing_id").alias("LISTING_ID"),
        col("date").alias("REVIEW_DATE"),
        col("reviewer_name").alias("REVIEWER_NAME"),
        col("comments").alias("REVIEW_COMMENT")
    )
    
    # Add sentiment analysis (simplified example)
    reviews_clean = reviews_clean.withColumn("REVIEW_COMMENT_LOWER", lower(col("REVIEW_COMMENT")))
    reviews_clean = reviews_clean.withColumn(
        "REVIEW_SENTIMENT",
        when(col("REVIEW_COMMENT_LOWER").contains("great"), "positive")
        .when(col("REVIEW_COMMENT_LOWER").contains("bad"), "negative")
        .otherwise("neutral")
    )
    
    # Convert dates
    reviews_clean = reviews_clean.withColumn(
        "REVIEW_DATE",
        to_date(col("REVIEW_DATE"))
    )
    
    # Handle nulls
    reviews_clean = reviews_clean.na.fill({
        "REVIEWER_NAME": "Anonymous",
        "REVIEW_COMMENT": "No comment",
        "REVIEW_SENTIMENT": "neutral"
    })
    
    return reviews_clean

clean_reviews = process_reviews(raw_reviews)

## 4. Write processed data directly to Snowflake tables
# Write hosts to Snowflake
clean_hosts.repartition(4).write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "RAW_HOSTS") \
    .mode("overwrite") \
    .save()

clean_listings.repartition(4).write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "RAW_LISTINGS") \
    .mode("overwrite") \
    .save()

clean_reviews.repartition(4).write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_options) \
    .option("dbtable", "RAW_REVIEWS") \
    .mode("overwrite") \
    .save()

# Stop Spark session
spark.stop()