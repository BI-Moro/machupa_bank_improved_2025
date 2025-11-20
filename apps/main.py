# apps/main.py
import sys
import os
import datetime
from pyspark.sql import SparkSession
from gen_static import generate_static, generate_users
from gen_entities import generate_customers, generate_accounts, generate_loan_accounts
from gen_transactions import generate_monthly_transactions, generate_loan_transactions
from config import *


def init_spark():
    """Initialize Spark Session"""
    return SparkSession.builder \
        .appName("TCB_Massive_Data_Generator") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def file_exists(year, month, file_type="deposit"):
    """Check if transaction file exists for given month"""
    filename = f"{file_type}_txn_history__txn_{year}_{month:02d}.del"
    filepath = os.path.join(OUTPUT_DIR, filename)
    return os.path.exists(filepath)


def generate_entities_if_missing(spark):
    """Generate static data and entities if they don't exist"""

    # Check for static data
    if not os.path.exists(os.path.join(OUTPUT_DIR, "branches_information.del")):
        print("\n" + "=" * 60)
        print("PHASE 1: GENERATING STATIC REFERENCE DATA")
        print("=" * 60)
        generate_static(spark)
        generate_users(spark)
    else:
        print("\n✓ Static data already exists")

    # Check for customers
    if not os.path.exists(os.path.join(OUTPUT_DIR, "customer_information.del")):
        print("\n" + "=" * 60)
        print(f"PHASE 2: GENERATING {NUM_CUSTOMERS:,} CUSTOMERS")
        print("=" * 60)
        generate_customers(spark)
    else:
        print("✓ Customer data already exists")

    # Check for accounts
    if not os.path.exists(os.path.join(OUTPUT_DIR, "dp_account_information.del")):
        print("\n" + "=" * 60)
        print("PHASE 3: GENERATING DEPOSIT & LOAN ACCOUNTS")
        print("=" * 60)
        generate_accounts(spark)
        generate_loan_accounts(spark)
    else:
        print("✓ Account data already exists")


def generate_missing_transactions(spark):
    """Generate transactions for all missing months"""

    print("\n" + "=" * 60)
    print("PHASE 4: CHECKING & GENERATING TRANSACTION HISTORY")
    print("=" * 60)

    now = datetime.datetime.now()
    current_year = now.year
    current_month = now.month

    work_done = False
    total_generated = 0

    # Loop through all years from START_YEAR to current
    for year in range(START_YEAR, current_year + 1):

        # Determine month range for this year
        start_month = 1
        end_month = 12

        if year == current_year:
            end_month = current_month  # Only up to current month for current year

        for month in range(start_month, end_month + 1):

            # Check if deposit transactions exist
            if not file_exists(year, month, "deposit"):
                print(f"\n→ MISSING: {year}-{month:02d} | Generating...")

                try:
                    # Generate deposit/withdrawal transactions
                    generate_monthly_transactions(spark, year, month)

                    # Generate loan transactions
                    generate_loan_transactions(spark, year, month)

                    work_done = True
                    total_generated += 1

                    print(f"  ✓ Completed {year}-{month:02d}")

                except Exception as e:
                    print(f"  ✗ Error generating {year}-{month:02d}: {str(e)}")
                    continue

            else:
                print(f"✓ {year}-{month:02d} exists - skipping")

    return work_done, total_generated


def print_summary(spark):
    """Print summary of generated data"""
    print("\n" + "=" * 60)
    print("DATA GENERATION SUMMARY")
    print("=" * 60)

    # Count files
    del_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.del')]

    print(f"\nTotal .del files generated: {len(del_files)}")
    print(f"\nKey Files:")

    key_files = [
        "customer_information.del",
        "dp_account_information.del",
        "loan_account_information.del",
        "branches_information.del",
        "products_information.del",
        "GL_ACCOUNT.del"
    ]

    for kf in key_files:
        if kf in del_files:
            filepath = os.path.join(OUTPUT_DIR, kf)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  ✓ {kf}: {size_mb:.2f} MB")

    # Count transaction files
    txn_files = [f for f in del_files if "txn_history" in f]
    print(f"\nTransaction History Files: {len(txn_files)}")

    total_txn_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f))
        for f in txn_files
    ) / (1024 * 1024 * 1024)

    print(f"Total Transaction Data: {total_txn_size:.2f} GB")


def main():
    """Main orchestration function"""

    spark = init_spark()

    print("\n" + "=" * 60)
    print("TCB BANK - MASSIVE DATA GENERATOR")
    print(f"Started: {datetime.datetime.now()}")
    print("=" * 60)
    print(f"Target: {TARGET_MONTHLY_TXNS:,} transactions/month")
    print(f"Period: {START_YEAR} to {CURRENT_YEAR}")
    print(f"Customers: {NUM_CUSTOMERS:,}")
    print("=" * 60)

    try:
        # Phase 1-3: Generate entities if missing
        generate_entities_if_missing(spark)

        # Phase 4: Generate missing transactions
        work_done, total_generated = generate_missing_transactions(spark)

        if not work_done:
            print("\n" + "=" * 60)
            print("✓ ALL DATA UP TO DATE - NO GENERATION NEEDED")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print(f"✓ SUCCESSFULLY GENERATED {total_generated} MONTHS OF DATA")
            print("=" * 60)

        # Print summary
        print_summary(spark)

    except Exception as e:
        print(f"\n✗ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()
        print(f"\n{'=' * 60}")
        print(f"Completed: {datetime.datetime.now()}")
        print("=" * 60)


if __name__ == "__main__":
    main()