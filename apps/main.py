# apps/main.py
import sys
import os
import datetime
from pyspark.sql import SparkSession
from gen_static import generate_static, generate_users
from gen_entities import generate_customers, generate_accounts, generate_loan_accounts
from gen_transactions import (
    generate_transactions_for_period,
    generate_loan_transactions_for_period,
    period_suffix,
)
from config import *

TRANSACTION_FILE_PREFIXES = {
    "deposit": "fact_deposit_transactions",
    "gl": "fact_gl_transactions",
    "loan": "fact_loan_transactions",
}

REQUIRED_STATIC_FILES = [
    "dim_business_unit.del",
    "dim_account_status.del",
    "dim_chart_of_accounts.del",
    "dim_general_ledger.del",
    "dim_product.del",
    "dim_currency.del",
    "dim_exchange_rate.del",
    "fact_exchange_rates.del",
    "dim_channel.del",
]

REQUIRED_ACCOUNT_FILES = [
    "dim_account.del",
    "dim_deposit_account.del",
]


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


def get_generation_window():
    """Resolve the short generation window from config/env values."""
    if GENERATION_DAYS < 1:
        raise ValueError("GENERATION_DAYS must be at least 1")

    if GENERATION_END_DATE:
        end_date = datetime.datetime.strptime(GENERATION_END_DATE, "%Y-%m-%d").date()
    else:
        end_date = datetime.datetime.now().date()

    start_date = end_date - datetime.timedelta(days=GENERATION_DAYS - 1)
    return start_date, end_date


def file_exists(start_date, end_date, file_type="deposit"):
    """Check if a transaction file exists for the selected date range."""
    file_prefix = TRANSACTION_FILE_PREFIXES[file_type]
    filename = f"{file_prefix}__txn_{period_suffix(start_date, end_date)}.del"
    filepath = os.path.join(OUTPUT_DIR, filename)
    return os.path.exists(filepath)


def generate_entities_if_missing(spark):
    """Generate static data and entities if they don't exist"""

    # Check for static data
    missing_static_files = [
        filename
        for filename in REQUIRED_STATIC_FILES
        if not os.path.exists(os.path.join(OUTPUT_DIR, filename))
    ]

    if missing_static_files:
        print("\n" + "=" * 60)
        print("PHASE 1: GENERATING STATIC REFERENCE DATA")
        print("=" * 60)
        print(f"Missing static files: {', '.join(missing_static_files)}")
        generate_static(spark)
    else:
        print("\n✓ Static data already exists")

    if not os.path.exists(os.path.join(OUTPUT_DIR, "dim_user_employee.del")):
        generate_users(spark)
    else:
        print("✓ User data already exists")

    # Check for customers
    customer_file_missing = not os.path.exists(os.path.join(OUTPUT_DIR, "dim_customer.del"))
    customer_stage_missing = not os.path.exists(os.path.join(OUTPUT_DIR, "internal/customers.parquet"))

    if customer_file_missing or customer_stage_missing:
        print("\n" + "=" * 60)
        print(f"PHASE 2: GENERATING {NUM_CUSTOMERS:,} CUSTOMERS")
        print("=" * 60)
        if customer_stage_missing:
            print("Missing customer stage: internal/customers.parquet")
        generate_customers(spark)
    else:
        print("✓ Customer data already exists")

    # Check for accounts
    missing_account_files = [
        filename
        for filename in REQUIRED_ACCOUNT_FILES
        if not os.path.exists(os.path.join(OUTPUT_DIR, filename))
    ]
    account_stage_missing = not os.path.exists(os.path.join(OUTPUT_DIR, "internal/accounts.parquet"))
    loan_stage_missing = not os.path.exists(os.path.join(OUTPUT_DIR, "internal/loan_accounts.parquet"))

    if missing_account_files or account_stage_missing or loan_stage_missing:
        print("\n" + "=" * 60)
        print("PHASE 3: GENERATING DEPOSIT & LOAN ACCOUNTS")
        print("=" * 60)
        if missing_account_files:
            print(f"Missing account files: {', '.join(missing_account_files)}")
        if account_stage_missing:
            print("Missing account stage: internal/accounts.parquet")
        if loan_stage_missing:
            print("Missing loan account stage: internal/loan_accounts.parquet")
        generate_accounts(spark)
        generate_loan_accounts(spark)
    else:
        print("✓ Account data already exists")


def generate_missing_transactions(spark, start_date, end_date):
    """Generate transactions for the configured short date range if missing."""

    print("\n" + "=" * 60)
    print("PHASE 4: CHECKING & GENERATING SHORT TRANSACTION HISTORY")
    print("=" * 60)
    print(f"Window: {start_date} to {end_date} ({GENERATION_DAYS} days)")

    required_files = ["deposit", "gl", "loan"]
    missing_files = [
        file_type
        for file_type in required_files
        if not file_exists(start_date, end_date, file_type)
    ]

    if not missing_files:
        print(f"✓ {period_suffix(start_date, end_date)} exists - skipping")
        return False, 0

    print(f"\n→ MISSING: {', '.join(missing_files)} | Generating...")

    try:
        generate_transactions_for_period(spark, start_date, end_date)
        generate_loan_transactions_for_period(spark, start_date, end_date)
        print(f"  ✓ Completed {start_date} to {end_date}")
        return True, GENERATION_DAYS

    except Exception as e:
        print(f"  ✗ Error generating {start_date} to {end_date}: {str(e)}")
        raise


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
        "dim_customer.del",
        "dim_account.del",
        "dim_deposit_account.del",
        "dim_business_unit.del",
        "dim_product.del",
        "dim_general_ledger.del",
        "dim_currency.del",
        "dim_channel.del",
        "dim_user_employee.del",
    ]

    for kf in key_files:
        if kf in del_files:
            filepath = os.path.join(OUTPUT_DIR, kf)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"  ✓ {kf}: {size_mb:.2f} MB")

    # Count transaction files
    txn_files = [f for f in del_files if f.startswith("fact_") and "__txn_" in f]
    print(f"\nTransaction History Files: {len(txn_files)}")

    total_txn_size = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f))
        for f in txn_files
    ) / (1024 * 1024 * 1024)

    print(f"Total Transaction Data: {total_txn_size:.2f} GB")


def main():
    """Main orchestration function"""

    start_date, end_date = get_generation_window()
    spark = init_spark()

    print("\n" + "=" * 60)
    print("TCB BANK - MASSIVE DATA GENERATOR")
    print(f"Started: {datetime.datetime.now()}")
    print("=" * 60)
    print(f"Target window: {start_date} to {end_date} ({GENERATION_DAYS} days)")
    print(f"Customers: {NUM_CUSTOMERS:,}")
    print("=" * 60)

    try:
        # Phase 1-3: Generate entities if missing
        generate_entities_if_missing(spark)

        # Phase 4: Generate missing transactions
        work_done, total_generated = generate_missing_transactions(spark, start_date, end_date)

        if not work_done:
            print("\n" + "=" * 60)
            print("✓ ALL DATA UP TO DATE - NO GENERATION NEEDED")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print(f"✓ SUCCESSFULLY GENERATED {total_generated} DAYS OF DATA")
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
