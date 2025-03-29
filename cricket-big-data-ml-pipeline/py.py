from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Define the full schema (50 columns)
schema = StructType([
    StructField("Player Name", StringType(), False),
    StructField("Country", StringType(), False),
    StructField("Years Active", IntegerType(), False),
    StructField("Role", StringType(), False),
    StructField("Player Date of Birth", StringType(), False),
    StructField("Age", IntegerType(), False),
    StructField("Best Format", StringType(), False),
    StructField("Batting Style", StringType(), False),
    StructField("Bowling Style", StringType(), False),
    StructField("Matches", IntegerType(), False),
    StructField("Innings", IntegerType(), False),
    StructField("Not Outs", IntegerType(), False),
    StructField("Runs", IntegerType(), False),
    StructField("High Score", IntegerType(), False),
    StructField("Batting Average", DoubleType(), False),
    StructField("Batting Strike Rate", DoubleType(), False),
    StructField("Ducks", IntegerType(), False),
    StructField("Fifties", IntegerType(), False),
    StructField("Hundreds", IntegerType(), False),
    StructField("Double Hundreds", IntegerType(), False),
    StructField("Balls Faced", IntegerType(), False),
    StructField("Sixes Hit", IntegerType(), False),
    StructField("Fours Hit", IntegerType(), False),
    StructField("Boundary Percentage", DoubleType(), False),
    StructField("Batting Impact in High-Pressure Situations", IntegerType(), False),
    StructField("Balls Per Dismissal", IntegerType(), False),
    StructField("Bowling Wickets", IntegerType(), False),
    StructField("Maidens", IntegerType(), False),
    StructField("Bowling Average", DoubleType(), False),
    StructField("Bowling Economy Rate", DoubleType(), False),
    StructField("Bowling Strike Rate", DoubleType(), False),
    StructField("Overs Bowled", IntegerType(), False),
    StructField("Total Balls Bowled", IntegerType(), False),
    StructField("Five-Wicket Hauls", IntegerType(), False),
    StructField("Ten-Wicket Hauls", IntegerType(), False),
    StructField("Best Bowling Wickets", IntegerType(), False),
    StructField("Best Bowling Runs", IntegerType(), False),
    StructField("Yorkers Bowled", IntegerType(), False),
    StructField("Dot Balls", IntegerType(), False),
    StructField("Total Runs Conceded", IntegerType(), False),
    StructField("Catches Taken", IntegerType(), False),
    StructField("Run Outs", IntegerType(), False),
    StructField("Wicketkeeping Dismissals", IntegerType(), False),
    StructField("Fielding Dismissals", IntegerType(), False),
    StructField("Boundary Stops", IntegerType(), False),
    StructField("Man of the Match Awards", IntegerType(), False),
    StructField("Matches Won Contribution (%)", IntegerType(), False),
    StructField("Win Percentage as Captain", IntegerType(), False),
    StructField("Key Strength", StringType(), False),
    StructField("Impact Category", IntegerType(), False)
])

# Create SparkSession
spark = SparkSession.builder \
    .appName("CricketPlayersStream") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cricket-players") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON messages using the full schema
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# For example, filter for players from India
df_filtered = df_parsed.filter(col("Country") == "India")

# Write the streaming output to the console
query = df_filtered.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()