# apps/gen_entities.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config import *
from utils import save_and_rename, random_tz_name, random_phone, random_tin
import random


def generate_customers(spark):
    """Generate realistic customer base with proper segmentation"""
    print(f"--- Generating {NUM_CUSTOMERS} Customers ---")

    # Generate customer IDs
    df_cust = spark.range(1, NUM_CUSTOMERS + 1).toDF("CUST_ID")

    # Assign customer segments
    segment_udf = F.udf(lambda: random.choices(
        list(CUSTOMER_SEGMENTS.keys()),
        weights=list(CUSTOMER_SEGMENTS.values())
    )[0], StringType())

    # Assign branches with realistic distribution (more in Dar es Salaam)
    branch_weights = [3 if "Dar es Salaam" in b[3] else 1 for b in BRANCHES]
    branch_udf = F.udf(lambda: random.choices(
        [b[0] for b in BRANCHES],
        weights=branch_weights
    )[0], IntegerType())

    df_cust = df_cust \
        .withColumn("CUST_NO", F.format_string("%010d", F.col("CUST_ID"))) \
        .withColumn("CUST_NM", random_tz_name()) \
        .withColumn("CUST_TY_ID", F.when(F.rand() > 0.85, "843").otherwise("701")) \
        .withColumn("CUST_CAT", F.when(F.col("CUST_TY_ID") == "843", "COR").otherwise("PER")) \
        .withColumn("CUST_SHORT_NM", F.split(F.col("CUST_NM"), " ")[0]) \
        .withColumn("RESIDENT_CNTRY_ID", F.lit(COUNTRY_ID)) \
        .withColumn("RESIDENT_FG", F.lit("N")) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("MAIN_BRANCH_ID", branch_udf()) \
        .withColumn("OPEN_RSN_ID", F.lit("501")) \
        .withColumn("CREATE_DT", F.lit(f"01/01/{START_YEAR}")) \
        .withColumn("TIN", random_tin()) \
        .withColumn("EMAIL_ADDR", F.lower(F.concat(
        F.regexp_replace(F.col("CUST_SHORT_NM"), " ", "."),
        F.lit("@example.com")
    ))) \
        .withColumn("SEGMENT", segment_udf()) \
        .withColumn("RISK_ID", F.lit("")) \
        .withColumn("INDUSTRY_ID", F.when(F.col("CUST_TY_ID") == "843", "3620").otherwise("")) \
        .withColumn("VERSION_NO", F.lit("1")) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    # Persist for reuse
    df_cust.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/internal/customers.parquet")

    # Save to .del file
    save_and_rename(df_cust.select(
        "CUST_ID", "CUST_NO", "CUST_TY_ID", "CUST_NM", "CUST_CAT", "CUST_SHORT_NM",
        "RESIDENT_CNTRY_ID", "RESIDENT_FG", "REC_ST", "MAIN_BRANCH_ID", "OPEN_RSN_ID",
        "CREATE_DT", "TIN", "EMAIL_ADDR", "RISK_ID", "INDUSTRY_ID", "VERSION_NO",
        "ROW_TS", "USER_ID"
    ), "customer_information.del")

    print(f"✓ Generated {df_cust.count()} customers")


def generate_accounts(spark):
    """Generate multiple accounts per customer based on segments"""
    print("--- Generating Deposit Accounts ---")

    # Load customers
    df_cust = spark.read.parquet(f"{OUTPUT_DIR}/internal/customers.parquet")

    accounts_list = []

    # Generate savings accounts (every customer gets one)
    df_savings = df_cust.select("CUST_ID", "CUST_NM", "MAIN_BRANCH_ID", "SEGMENT") \
        .withColumn("ACCT_ID", F.col("CUST_ID") + 1000000) \
        .withColumn("PROD_ID", F.when(F.col("SEGMENT") == "RETAIL", 10)
                    .when(F.col("SEGMENT") == "SME", 11)
                    .otherwise(12)) \
        .withColumn("ACCT_NO", F.concat(
        F.substring(F.format_string("%03d", F.abs(F.col("MAIN_BRANCH_ID"))), 2, 2),
        F.lit("02"),
        F.format_string("%08d", F.col("CUST_ID"))
    ))

    accounts_list.append(df_savings)

    # Generate current accounts (50% of customers)
    df_current = df_cust.filter(F.rand() > 0.5) \
        .select("CUST_ID", "CUST_NM", "MAIN_BRANCH_ID", "SEGMENT") \
        .withColumn("ACCT_ID", F.col("CUST_ID") + 2000000) \
        .withColumn("PROD_ID", F.when(F.col("SEGMENT") == "RETAIL", 20)
                    .when(F.col("SEGMENT") == "SME", 21)
                    .otherwise(22)) \
        .withColumn("ACCT_NO", F.concat(
        F.substring(F.format_string("%03d", F.abs(F.col("MAIN_BRANCH_ID"))), 2, 2),
        F.lit("09"),
        F.format_string("%08d", F.col("CUST_ID"))
    ))

    accounts_list.append(df_current)

    # Generate fixed deposits (20% of customers)
    df_fixed = df_cust.filter(F.rand() > 0.8) \
        .select("CUST_ID", "CUST_NM", "MAIN_BRANCH_ID", "SEGMENT") \
        .withColumn("ACCT_ID", F.col("CUST_ID") + 3000000) \
        .withColumn("PROD_ID", F.lit(30 + (F.rand() * 4).cast("int"))) \
        .withColumn("ACCT_NO", F.concat(
        F.substring(F.format_string("%03d", F.abs(F.col("MAIN_BRANCH_ID"))), 2, 2),
        F.lit("03"),
        F.format_string("%08d", F.col("CUST_ID"))
    ))

    accounts_list.append(df_fixed)

    # Union all accounts
    df_accounts = accounts_list[0]
    for df in accounts_list[1:]:
        df_accounts = df_accounts.union(df)

    # Add common fields
    df_accounts = df_accounts \
        .withColumn("PROD_CAT_TY", F.when(F.col("PROD_ID") < 30, "DP").otherwise("TDEP")) \
        .withColumn("CRNCY_ID", F.lit(CURRENCY_ID)) \
        .withColumn("ACCT_NM", F.col("CUST_NM")) \
        .withColumn("STATUS_EFFECTIVE_DT", F.lit(f"01/01/{START_YEAR}")) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("VERSION_NO", F.lit("1")) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    # Persist for transactions
    df_accounts.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/internal/accounts.parquet")

    # Save to .del file
    save_and_rename(df_accounts.select(
        "ACCT_ID", "ACCT_NO", "PROD_CAT_TY", "PROD_ID", "CRNCY_ID",
        "ACCT_NM", "STATUS_EFFECTIVE_DT", "CUST_ID", "MAIN_BRANCH_ID",
        "REC_ST", "VERSION_NO", "ROW_TS", "USER_ID"
    ), "dp_account_information.del")

    print(f"✓ Generated {df_accounts.count()} deposit accounts")


def generate_loan_accounts(spark):
    """Generate loan accounts for eligible customers"""
    print("--- Generating Loan Accounts ---")

    # Load customers - 30% have loans
    df_cust = spark.read.parquet(f"{OUTPUT_DIR}/internal/customers.parquet") \
        .filter(F.rand() > 0.7)

    # Assign loan products based on segment
    loan_prod_udf = F.udf(lambda seg: random.choice(
        [80, 81, 82, 83] if seg == "RETAIL" else
        [90, 91, 92, 93] if seg == "SME" else
        [110, 111]
    ), IntegerType())

    df_loans = df_cust.select("CUST_ID", "CUST_NM", "MAIN_BRANCH_ID", "SEGMENT") \
        .withColumn("ACCT_ID", F.col("CUST_ID") + 5000000) \
        .withColumn("PROD_ID", loan_prod_udf(F.col("SEGMENT"))) \
        .withColumn("ACCT_NO", F.concat(
        F.substring(F.format_string("%03d", F.abs(F.col("MAIN_BRANCH_ID"))), 2, 2),
        F.lit("14"),
        F.format_string("%08d", F.col("CUST_ID"))
    )) \
        .withColumn("PROD_CAT_TY", F.lit("LN")) \
        .withColumn("CRNCY_ID", F.lit(CURRENCY_ID)) \
        .withColumn("ACCT_NM", F.col("CUST_NM")) \
        .withColumn("STATUS_EFFECTIVE_DT", F.lit(f"01/01/{START_YEAR}")) \
        .withColumn("REC_ST", F.lit("L")) \
        .withColumn("VERSION_NO", F.lit("1")) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    # Persist for transactions
    df_loans.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/internal/loan_accounts.parquet")

    # Save to .del file
    save_and_rename(df_loans.select(
        "ACCT_ID", "ACCT_NO", "PROD_CAT_TY", "PROD_ID", "CRNCY_ID",
        "ACCT_NM", "STATUS_EFFECTIVE_DT", "CUST_ID", "MAIN_BRANCH_ID",
        "REC_ST", "VERSION_NO", "ROW_TS", "USER_ID"
    ), "loan_account_information.del")

    print(f"✓ Generated {df_loans.count()} loan accounts")