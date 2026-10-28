# apps/utils.py
import os
import glob
import shutil
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import random
from config import FIRST_NAMES, LAST_NAMES, OUTPUT_DIR, DELIMITER


def save_and_rename(df, filename):
    """
    Saves Spark DF to single CSV with pipe delimiter, then renames to .del format.
    """
    temp_path = os.path.join(OUTPUT_DIR, "temp_data")

    # Write to temp folder
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", DELIMITER) \
        .option("quote", "") \
        .option("escape", "\\") \
        .csv(temp_path)

    # Find the part file
    files = glob.glob(os.path.join(temp_path, "part-*.csv"))
    if files:
        target_name = os.path.join(OUTPUT_DIR, filename)
        shutil.move(files[0], target_name)
        print(f"  ✓ Created: {filename}")

        # Clean up temp directory
        shutil.rmtree(temp_path)
    else:
        print(f"  ✗ Failed to create: {filename}")


@udf(returnType=StringType())
def random_tz_name():
    """Generate random Tanzanian name"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)

    # 15% chance of middle name
    if random.random() < 0.15:
        middle = random.choice(FIRST_NAMES)
        return f"{first} {middle} {last}"

    return f"{first} {last}"


@udf(returnType=StringType())
def random_phone():
    """Generate Tanzanian phone number"""
    # Format: +255 7XX XXX XXX or +255 6XX XXX XXX
    prefix = random.choice(["71", "74", "75", "76", "77", "78", "68", "69"])
    number = ''.join([str(random.randint(0, 9)) for _ in range(7)])
    return f"+255{prefix}{number}"


@udf(returnType=StringType())
def random_tin():
    """Generate Tanzanian TIN (Tax Identification Number)"""
    # Format: XXX-XXX-XXX
    parts = [''.join([str(random.randint(0, 9)) for _ in range(3)]) for _ in range(3)]
    return '-'.join(parts)


def format_date(day, month, year):
    """Format date as YYYY-MM-DD for Doris date columns."""
    return f"{year}-{month:02d}-{day:02d}"


def generate_reference_number(prefix, sequence):
    """Generate transaction reference number"""
    return f"{prefix}{sequence:010d}"
