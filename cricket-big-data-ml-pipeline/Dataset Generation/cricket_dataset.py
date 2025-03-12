import os
import random
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from functools import reduce

# Configure logging
logging.basicConfig(filename="cricket_data_generation.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
TOTAL_RECORDS = 123456789
UNIQUE_RECORDS = 111111111
TOTAL_DUPLICATE_RECORDS = TOTAL_RECORDS - UNIQUE_RECORDS
OUTPUT_DIRECTORY = "/Users/tpc/Desktop/cricket-data-engineering/data/"
OUTPUT_FILE_NAME = "cricket_dataset.parquet"
OUTPUT_PATH = os.path.join(OUTPUT_DIRECTORY, OUTPUT_FILE_NAME)
CHECKPOINT_DIR = "/Users/tpc/Desktop/cricket-data-engineering/checkpoint/"

# Ensure the directories exist
try:
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
except PermissionError as e:
    logging.error(f"Permission denied: {e}")
    raise

# Optimized Spark session
spark = SparkSession.builder \
    .appName("CricketDatasetGeneration") \
    .config("spark.driver.memory", "24g") \
    .config("spark.executor.memory", "24g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.memory.fraction", "0.7") \
    .config("spark.sql.files.maxPartitionBytes", "128m") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.debug.maxToStringFields", "200") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.network.timeout", "600s") \
    .config("spark.python.worker.memory", "2g") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()

# Set Spark checkpoint directory
spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

# Adjust Spark logging level
spark.sparkContext.setLogLevel("WARN")

# Clear cache to avoid stale DataFrames
spark.catalog.clearCache()

# Schema Definition (50 Columns)
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

# Player Names and Cricket-playing Countries
player_names = [
    "Virat Kohli", "Joe Root", "Steve Smith", "Kane Williamson", "Babar Azam", "David Warner",
    "Rohit Sharma", "Ben Stokes", "Faf du Plessis", "AB de Villiers", "Shakib Al Hasan",
    "Jos Buttler", "Rashid Khan", "Glenn Maxwell", "Quinton de Kock", "Trent Boult", "Pat Cummins",
    "Marnus Labuschagne", "Jofra Archer", "Shreyas Iyer", "Shaheen Afridi", "Mohammed Siraj",
    "Kusal Perera", "Jason Holder", "Chris Gayle", "Jasprit Bumrah", "Sachin Tendulkar",
    "M S Dhoni", "Yuvraj Singh", "Kevin Pietersen", "Brian Lara", "Jacques Kallis", "Kumar Sangakkara",
    "Mahela Jayawardene", "Michael Clarke", "Ricky Ponting", "Rahul Dravid", "Anil Kumble",
    "Sourav Ganguly", "Zaheer Khan", "Matthew Hayden", "Adam Gilchrist", "Shane Warne",
    "Glenn McGrath", "Brett Lee", "Lasith Malinga", "Daniel Vettori", "Graeme Smith", "Mitchell Starc",
    "Shubman Gill", "Devon Conway", "Hardik Pandya", "Ravindra Jadeja", "Mohammed Shami",
    "Suryakumar Yadav", "Heinrich Klaasen", "Anrich Nortje", "Naseem Shah", "Mitchell Marsh",
    "Kyle Mayers", "Harry Brook", "Ishan Kishan", "David Willey", "Mustafizur Rahman",
    "Kieron Pollard", "Andre Russell", "Sunil Narine", "Dwayne Bravo", "Ross Taylor",
    "Martin Guptill", "Tim Southee", "Travis Head", "Cameron Green", "Rassie van der Dussen",
    "Lungi Ngidi", "Josh Hazlewood", "Aaron Finch", "Jonny Bairstow", "Chris Woakes", "Eoin Morgan",
    "Tom Latham", "Mohammad Rizwan", "Fakhar Zaman", "Imran Tahir", "James Anderson", "Stuart Broad",
    "Ravi Ashwin", "Aiden Markram", "Usman Khawaja", "Alex Carey"
]

countries = [
    "India", "Australia", "England", "South Africa", "New Zealand", "Pakistan", "Sri Lanka",
    "West Indies", "Bangladesh", "Afghanistan", "Zimbabwe", "Ireland", "Netherlands",
    "Scotland", "UAE", "Nepal", "Oman", "Namibia", "USA", "Canada", "Hong Kong", "Papua New Guinea", "Kenya",
    "Bermuda", "Malaysia", "Singapore", "Qatar", "Germany", "Italy"
]

# Date and Record Generation Functions
def random_date(start_year=1970, end_year=2005):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def assign_years_active(age):
    return random.randint(1, max(1, age - 18)) if age > 18 else random.randint(1, 5)

def determine_best_format(role, batting_strike_rate, bowling_strike_rate):
    if role in ["Batsman", "All-rounder", "Wicketkeeper"]:
        if 30 <= batting_strike_rate <= 60:
            return "Test"
        elif 61 <= batting_strike_rate <= 90:
            return "ODI"
        elif batting_strike_rate > 90:
            return "T20"
        else:
            return "Unknown"

    elif role == "Bowler":
        bowling_strike_rate = bowling_strike_rate if bowling_strike_rate is not None else 100

        if 5 <= bowling_strike_rate <= 15:
            return "T20"
        elif 16 <= bowling_strike_rate <= 30:
            return "ODI"
        elif bowling_strike_rate > 30:
            return "Test"
        else:
            return "Unknown"

    return "Unknown"

def assign_key_strength(role):
    strengths = {
        "Batsman": ["Hand-Eye Coordination", "Footwork", "Shot Selection", "Power & Timing", "Temperament & Patience"],
        "Bowler": ["Accuracy & Line-Length Control", "Variation & Deception", "Stamina & Endurance", "Aggression & Mind Games"],
        "All-rounder": ["Versatility", "Adaptability", "Consistency", "Game Awareness", "Match Impact"],
        "Wicketkeeper": ["Speed & Agility", "Throwing Accuracy", "Diving & Athleticism", "Glove Work", "Quick Reflexes"],
        "Captain": ["Composure Under Pressure", "Decision-Making", "Teamwork & Leadership", "Strategic Thinking"]
    }
    return random.choice(strengths.get(role, ["Unknown"])).strip()

def calculate_high_score(double_hundreds, hundreds, fifties, runs, fours_hit, sixes_hit):
    if double_hundreds > 0:
        return int(random.randint(200, max(200, min(456, runs // 2))))
    elif hundreds > 0:
        return int(random.randint(100, 199))
    elif fifties > 0:
        return int(random.randint(50, 99))
    else:
        return int(random.randint(10, 49)) if fours_hit > 0 and sixes_hit > 0 else int(random.randint(3, 10))

def generate_complete_record(index):
    player_name = player_names[index % len(player_names)]
    country = str(random.choice(countries))
    dob = str(random_date().strftime('%Y-%m-%d'))
    age = int((datetime.now() - datetime.strptime(dob, '%Y-%m-%d')).days // 365)
    years_active = int(assign_years_active(age))
    role = str(random.choices(["Batsman", "Bowler", "All-rounder", "Wicketkeeper"], weights=[0.4, 0.3, 0.2, 0.1], k=1)[0])
    batting_style = str(random.choice(["Right-Handed", "Left-Handed"]))
    bowling_style = str(random.choice(["Right-Arm Fast", "Left-Arm Fast", "Right-Arm Fast Medium", "Left-Arm Fast Medium", "Right-Arm Medium", "Left-Arm Medium", "Left-Arm Orthodox", "Left-Arm Spin", "Off Spin", "Leg Spin"]))

    # Batting Stats
    matches = int(random.randint(50, 1000))
    innings = int(random.randint(matches, int(matches * 1.5)))
    not_outs = int(innings * random.randint(2, 30) / 100)
    not_outs = max(0, min(innings - 1, not_outs))
    ducks = int(max(0, innings // random.randint(10, 20)))
    fours_hit = int(random.randint(50, 500)) if role in ["Batsman", "All-rounder", "Wicketkeeper"] else int(random.randint(10, 50))
    sixes_hit = int(random.randint(10, 200)) if role in ["Batsman", "All-rounder", "Wicketkeeper"] else int(random.randint(0, 30))
    runs = int(random.randint(2, 7) * ((fours_hit * 4) + (sixes_hit * 6)))
    batting_average = float(round(runs / max(1, innings - not_outs), 2)) if runs is not None and innings > 0 else 0.0
    balls_faced = int(max(runs, random.randint(int(runs * 0.75) if runs is not None else 1, runs * 2 if runs is not None else 200)))
    batting_strike_rate = float(round((runs / max(1, balls_faced)) * 100, 2)) if runs is not None and balls_faced > 0 else 0.0
    boundary_percentage = float(round(min(((fours_hit * 4 + sixes_hit * 6) / runs) * 100, 100), 2))

    # ✅ Fifties, Hundreds, and Double Hundreds
    if role in ["Batsman", "All-rounder", "Wicketkeeper"] and runs > 0:
        fifties = min(int(innings * random.uniform(0.3, 0.5)), int((runs // 1000) * 8))
        hundreds = min(int(innings * random.uniform(0.2, 0.3)), int((runs // 1000) * 3))
        double_hundreds = min(int(innings * random.uniform(0.0, 0.05)), int((runs // 3000) * 2))
    else:
        fifties = 0
        hundreds = 0
        double_hundreds = 0

    # Performance Stats
    high_score = calculate_high_score(double_hundreds, hundreds, fifties, runs, fours_hit, sixes_hit)
    batting_impact_high_pressure = int(random.randint(0, 100))
    balls_per_dismissal = balls_faced // max(1, innings - not_outs) if balls_faced > 0 else 0

    # Bowling Stats
    if role in ["Bowler", "All-rounder"]:
        overs_bowled = int(random.randint(1000, 25000))
        total_balls_bowled = int(overs_bowled * 6)
        bowling_wickets = int(random.randint(20, 850)) if overs_bowled > 10000 else int(random.randint(1, 249))
        five_wicket_hauls = int(random.randint(1, 14)) if bowling_wickets > 100 else 0
        ten_wicket_hauls = int(random.randint(0, 4)) if bowling_wickets > 250 else 0
        best_bowling_wickets = int(min(bowling_wickets, 7 if ten_wicket_hauls > 0 else 5 if five_wicket_hauls > 0 else random.randint(0, 4)))
        best_bowling_runs = int(random.randint(0, 120))
        yorkers_bowled = int(random.randint(overs_bowled, overs_bowled * 3))
        total_runs_conceded = int(overs_bowled * random.randint(1, 3))
        bowling_average = float(round(total_runs_conceded / max(1, bowling_wickets), 2)) if bowling_wickets > 0 else 0.0
        bowling_economy_rate = float(round(total_runs_conceded / max(1, overs_bowled), 2)) if overs_bowled > 0 else 0.0
        bowling_strike_rate = float(round(total_balls_bowled / max(1, bowling_wickets), 2)) if bowling_wickets > 0 else 0.0
        maidens = int(random.randint(int(overs_bowled // 1.3), int(overs_bowled // 1.1))) if overs_bowled > 0 else 0
        dot_balls = int(overs_bowled * random.randint(1, 3))
    else:
        overs_bowled = 0
        total_balls_bowled = 0
        bowling_wickets = 0
        five_wicket_hauls = 0
        ten_wicket_hauls = 0
        best_bowling_wickets = 0
        best_bowling_runs = 0
        yorkers_bowled = 0
        total_runs_conceded = 0
        bowling_average = 0.0
        bowling_economy_rate = 0.0
        bowling_strike_rate = 0.0
        maidens = 0
        dot_balls = 0

    # Fielding Stats
    catches_taken = int(random.randint(0, 500))
    run_outs = int(random.randint(0, 150))
    boundary_stops = int(random.randint(0, 300))
    fielding_dismissals = catches_taken + run_outs
    wicketkeeping_dismissals = int(random.randint(0, 800)) if role == "Wicketkeeper" else 0

    # Miscellaneous Stats
    man_of_the_match_awards = int(random.randint(0, min(100, matches)))
    matches_won_contribution = int(random.randint(10, min(90, matches)))
    best_format = determine_best_format(role, batting_strike_rate, bowling_strike_rate)
    win_percentage_as_captain = int(random.randint(10, 100)) if random.random() < 0.2 else 0
    key_strength = assign_key_strength(role)
    impact_category = int(random.randint(0, 100))

    record = {
        "Player Name": player_name,
        "Country": country,
        "Years Active": years_active,
        "Role": role,
        "Player Date of Birth": dob,
        "Age": age,
        "Best Format": best_format,
        "Batting Style": batting_style,
        "Bowling Style": bowling_style,
        "Matches": matches,
        "Innings": innings,
        "Not Outs": not_outs,
        "Runs": runs,
        "High Score": high_score,
        "Batting Average": batting_average,
        "Batting Strike Rate": batting_strike_rate,
        "Ducks": ducks,
        "Fifties": fifties,
        "Hundreds": hundreds,
        "Double Hundreds": double_hundreds,
        "Balls Faced": balls_faced,
        "Sixes Hit": sixes_hit,
        "Fours Hit": fours_hit,
        "Boundary Percentage": boundary_percentage,
        "Batting Impact in High-Pressure Situations": batting_impact_high_pressure,
        "Balls Per Dismissal": balls_per_dismissal,
        "Bowling Wickets": bowling_wickets,
        "Maidens": maidens,
        "Bowling Average": bowling_average,
        "Bowling Economy Rate": bowling_economy_rate,
        "Bowling Strike Rate": bowling_strike_rate,
        "Overs Bowled": overs_bowled,
        "Total Balls Bowled": total_balls_bowled,
        "Five-Wicket Hauls": five_wicket_hauls,
        "Ten-Wicket Hauls": ten_wicket_hauls,
        "Best Bowling Wickets": best_bowling_wickets,
        "Best Bowling Runs": best_bowling_runs,
        "Yorkers Bowled": yorkers_bowled,
        "Dot Balls": dot_balls,
        "Total Runs Conceded": total_runs_conceded,
        "Catches Taken": catches_taken,
        "Run Outs": run_outs,
        "Wicketkeeping Dismissals": wicketkeeping_dismissals,
        "Fielding Dismissals": fielding_dismissals,
        "Boundary Stops": boundary_stops,
        "Man of the Match Awards": man_of_the_match_awards,
        "Matches Won Contribution (%)": matches_won_contribution,
        "Win Percentage as Captain": win_percentage_as_captain,
        "Key Strength": key_strength,
        "Impact Category": impact_category
    }

    return Row(**record)

# Generate Unique Records DataFrame
def generate_unique_records(batch_size=250000):
    unique_records = []
    logging.info("🔄 Generating Unique Records...")

    for i in range(UNIQUE_RECORDS):
        unique_records.append(generate_complete_record(i))

        if (i + 1) % batch_size == 0:
            logging.info(f"Generated {i + 1} unique records...")
            batch_df = spark.createDataFrame(unique_records, schema)

            # Append data safely without deleting existing files
            try:
                batch_df.write.mode("append").parquet(OUTPUT_PATH, compression="snappy")
            except Exception as e:
                logging.error(f"Error writing batch {i + 1} to Parquet: {e}")

            unique_records.clear()

    if unique_records:
        batch_df = spark.createDataFrame(unique_records, schema)
        try:
            batch_df.write.mode("append").parquet(OUTPUT_PATH, compression="snappy")
        except Exception as e:
            logging.error(f"Error writing final batch to Parquet: {e}")

    return spark.read.parquet(OUTPUT_PATH).checkpoint()

# Generate Duplicate Records DataFrame
def generate_duplicates(unique_df, duplicate_needed=TOTAL_DUPLICATE_RECORDS, batch_size=250000):
    logging.info("🔄 Generating Duplicate Records...")
    duplicates = []

    num_batches = duplicate_needed // batch_size
    remaining = duplicate_needed % batch_size

    for _ in range(num_batches):
        sample_df = unique_df.sample(withReplacement=True, fraction=0.1).limit(batch_size)
        duplicates.append(sample_df)

    if remaining > 0:
        sample_df = unique_df.sample(withReplacement=True, fraction=0.1).limit(remaining)
        duplicates.append(sample_df)

    if duplicates:
        return reduce(lambda df1, df2: df1.union(df2), duplicates)
    else:
        return spark.createDataFrame([], schema).checkpoint()

# Save final DataFrame to a single Parquet file
def save_final_dataset(unique_df, duplicate_df):
    final_df = unique_df.union(duplicate_df)
    logging.info("💾 Saving the final dataset to a single Parquet file...")

    num_records = final_df.count()
    logging.info(f"Number of records in final DataFrame: {num_records}")

    try:
        final_df = final_df.repartition(1).checkpoint()
        final_df.write.mode("overwrite").parquet(OUTPUT_PATH, compression="snappy")
    except Exception as e:
        logging.error(f"Error writing final DataFrame to Parquet: {e}")
        raise

    return final_df

if __name__ == "__main__":
    try:
        logging.info("Generating Unique Records...")
        unique_records_generated = generate_unique_records()

        logging.info("Generating Duplicate Records...")
        duplicate_records_generated = generate_duplicates(unique_records_generated)

        logging.info("Saving the final dataset...")
        final_df = save_final_dataset(unique_records_generated, duplicate_records_generated)

        logging.info("✅ Data generation completed successfully!")

    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")
    finally:
        spark.stop()