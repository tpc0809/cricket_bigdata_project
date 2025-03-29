from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col, lower, trim, count

if __name__ == "__main__":
    # Create Spark session and reduce log verbosity.
    spark = SparkSession.builder \
        .appName("KafkaSparkIndiaPlayers") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read from Kafka with a limit on records per trigger.
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "cricket_stream") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "100000") \
        .load()

    # Define the schema (keys match the JSON payload, which uses lowercase with underscores).
    schema = StructType([
        StructField("player_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("years_active", IntegerType(), True),
        StructField("role", StringType(), True),
        StructField("player_date_of_birth", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("best_format", StringType(), True),
        StructField("batting_style", StringType(), True),
        StructField("bowling_style", StringType(), True),
        StructField("matches", IntegerType(), True),
        StructField("innings", IntegerType(), True),
        StructField("not_outs", IntegerType(), True),
        StructField("runs", IntegerType(), True),
        StructField("high_score", IntegerType(), True),
        StructField("batting_average", DoubleType(), True),
        StructField("batting_strike_rate", DoubleType(), True),
        StructField("ducks", IntegerType(), True),
        StructField("fifties", IntegerType(), True),
        StructField("hundreds", IntegerType(), True),
        StructField("double_hundreds", IntegerType(), True),
        StructField("balls_faced", IntegerType(), True),
        StructField("sixes_hit", IntegerType(), True),
        StructField("fours_hit", IntegerType(), True),
        StructField("boundary_percentage", DoubleType(), True),
        StructField("batting_impact", IntegerType(), True),
        StructField("balls_per_dismissal", IntegerType(), True),
        StructField("bowling_wickets", IntegerType(), True),
        StructField("maidens", IntegerType(), True),
        StructField("bowling_average", DoubleType(), True),
        StructField("bowling_economy_rate", DoubleType(), True),
        StructField("bowling_strike_rate", DoubleType(), True),
        StructField("overs_bowled", IntegerType(), True),
        StructField("total_balls_bowled", IntegerType(), True),
        StructField("five_wicket_hauls", IntegerType(), True),
        StructField("ten_wicket_hauls", IntegerType(), True),
        StructField("best_bowling_wickets", IntegerType(), True),
        StructField("best_bowling_runs", IntegerType(), True),
        StructField("yorkers_bowled", IntegerType(), True),
        StructField("dot_balls", IntegerType(), True),
        StructField("total_runs_conceded", IntegerType(), True),
        StructField("catches_taken", IntegerType(), True),
        StructField("run_outs", IntegerType(), True),
        StructField("wicketkeeping_dismissals", IntegerType(), True),
        StructField("fielding_dismissals", IntegerType(), True),
        StructField("boundary_stops", IntegerType(), True),
        StructField("man_of_the_match_awards", IntegerType(), True),
        StructField("matches_won_contribution", DoubleType(), True),
        StructField("win_percentage_as_captain", DoubleType(), True),
        StructField("key_strength", StringType(), True),
        StructField("impact_category", IntegerType(), True)
    ])

    # Parse the JSON payload.
    df_with_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
    df_parsed = df_with_json.select(
        "json_str",
        from_json(col("json_str"), schema).alias("data")
    )

    # Filter for records where country equals "india" (case-insensitive).
    df_india = df_parsed.filter(col("data").isNotNull()) \
        .filter(lower(trim(col("data.country"))) == "india") \
        .select("data.*")

    # Calculate the total count of India records.
    total_count_df = df_india.agg(count("*").alias("total_records"))
    total_count_query = total_count_df.writeStream \
        .format("memory") \
        .queryName("totalCountTable") \
        .outputMode("complete") \
        .trigger(once=True) \
        .option("truncate", False) \
        .start()

    # Extract a distinct sample of (player_name, country) rows (drop duplicate player names).
    sample_df = df_india.select("player_name", "country").dropDuplicates(["player_name"])
    sample_query = sample_df.writeStream \
        .format("memory") \
        .queryName("sampleTable") \
        .outputMode("append") \
        .trigger(once=True) \
        .option("truncate", False) \
        .start()

    # Wait until both streaming queries complete.
    total_count_query.awaitTermination()
    sample_query.awaitTermination()

    # Query the in-memory tables.
    total_count = spark.sql("SELECT * FROM totalCountTable")
    sample_result = spark.sql("SELECT * FROM sampleTable LIMIT 10")

    # Print final output in the desired format.
    print("Total records with country='india' is:")
    total_count.show(truncate=False)
    print("Sample 10 distinct records (player_name, country):")
    sample_result.show(truncate=False)

    spark.stop()