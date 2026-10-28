# apps/gen_entities.py
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from config import *
from utils import save_and_rename, random_tz_name, random_tin
import random


def _segment_id_expr():
    return F.when(F.col("SEGMENT") == "RETAIL", 1) \
        .when(F.col("SEGMENT") == "SME", 2) \
        .when(F.col("SEGMENT") == "CORPORATE", 3) \
        .otherwise(4)


def _account_dimension(df_accounts):
    return df_accounts.select(
        F.col("ACCT_ID").cast("long").alias("account_id"),
        F.col("ACCT_NO").alias("account_number"),
        F.col("CUST_ID").cast("long").alias("customer_id"),
        F.col("ACCT_NM").alias("account_name"),
        F.when(F.col("PROD_CAT_TY") == "LN", "Loan").otherwise("Deposit").alias("product_category"),
        F.col("PROD_CAT_TY").alias("product_sub_category"),
        F.col("ACCT_NO").alias("legacy_account_number"),
        F.col("PROD_ID").cast("string").alias("product_id"),
        F.col("MAIN_BRANCH_ID").cast("int").alias("branch_id"),
        F.col("CRNCY_ID").cast("int").alias("currency_id"),
        F.lit(-99).cast("int").alias("opening_authorizer_id"),
        F.col("REC_ST").alias("record_status"),
        F.lit(1).cast("int").alias("current_risk_id"),
        F.lit(None).cast("int").alias("previous_risk_id"),
        F.to_date(F.col("STATUS_EFFECTIVE_DT")).alias("status_change_date"),
        F.round(F.rand() * 5_000_000 + 20_000, 2).alias("current_balance"),
        F.round(F.rand() * 4_500_000 + 10_000, 2).alias("available_balance"),
        F.when(F.col("PROD_CAT_TY") == "LN", F.round(F.rand() * 50_000_000 + 1_000_000, 2))
        .otherwise(F.lit(0.0)).alias("credit_limit"),
        F.when(F.col("PROD_CAT_TY") == "DP", F.round(F.rand() * 500_000, 2))
        .otherwise(F.lit(0.0)).alias("overdraft_limit"),
        F.lit(0).cast("int").alias("pending_modification_flag"),
        F.lit(0).cast("int").alias("auto_reclassification_flag"),
        F.to_date(F.col("STATUS_EFFECTIVE_DT")).alias("account_opening_date"),
        F.add_months(F.to_date(F.col("STATUS_EFFECTIVE_DT")), 12).alias("last_kyc_review_date"),
        F.add_months(F.to_date(F.col("STATUS_EFFECTIVE_DT")), 24).alias("scheduled_review_date"),
        F.current_timestamp().alias("authorization_timestamp"),
        F.lit("MONTHLY").alias("interest_accrual_frequency"),
        F.lit("MONTHLY").alias("statement_frequency"),
        F.lit(-99).cast("int").alias("user_id"),
        F.lit("SYSTEM").alias("created_by"),
        F.current_timestamp().alias("last_updated_ts"),
    )


def _save_dim_account(df_accounts):
    save_and_rename(_account_dimension(df_accounts), "dim_account.del")


def generate_customers(spark):
    """Generate realistic customer base using the Doris warehouse schema."""
    print(f"--- Generating {NUM_CUSTOMERS} Customers ---")

    df_cust = spark.range(1, NUM_CUSTOMERS + 1).toDF("CUST_ID")

    segment_udf = F.udf(lambda: random.choices(
        list(CUSTOMER_SEGMENTS.keys()),
        weights=list(CUSTOMER_SEGMENTS.values())
    )[0])

    branch_weights = [3 if "Dar es Salaam" in b[3] else 1 for b in BRANCHES]
    branch_udf = F.udf(lambda: random.choices(
        [b[0] for b in BRANCHES],
        weights=branch_weights
    )[0], IntegerType())

    gender_udf = F.udf(lambda: random.choice(["M", "F"]))

    df_cust = df_cust \
        .withColumn("CUST_NO", F.format_string("%010d", F.col("CUST_ID"))) \
        .withColumn("CUST_NM", random_tz_name()) \
        .withColumn("CUST_TY_ID", F.when(F.rand() > 0.85, F.lit(843)).otherwise(F.lit(701))) \
        .withColumn("CUST_CAT", F.when(F.col("CUST_TY_ID") == 843, "COR").otherwise("PER")) \
        .withColumn("CUST_SHORT_NM", F.split(F.col("CUST_NM"), " ")[0]) \
        .withColumn("RESIDENT_CNTRY_ID", F.lit(int(COUNTRY_ID))) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("MAIN_BRANCH_ID", branch_udf()) \
        .withColumn("OPEN_RSN_ID", F.lit(501)) \
        .withColumn("CREATE_DT", F.to_date(F.lit(f"{START_YEAR}-01-01"))) \
        .withColumn("TIN", random_tin()) \
        .withColumn("EMAIL_ADDR", F.lower(F.concat(
            F.regexp_replace(F.col("CUST_SHORT_NM"), " ", "."),
            F.lit("@example.com")
        ))) \
        .withColumn("SEGMENT", segment_udf()) \
        .withColumn("INDUSTRY_ID", F.when(F.col("CUST_TY_ID") == 843, F.lit(3620)).otherwise(F.lit(None).cast("int"))) \
        .withColumn("GENDER", gender_udf()) \
        .withColumn("MONTHLY_INCOME", F.round(F.rand() * 2_500_000 + 250_000, 2)) \
        .withColumn("ANNUAL_INCOME", F.col("MONTHLY_INCOME") * 12) \
        .withColumn("NET_WORTH", F.round(F.rand() * 100_000_000 + 1_000_000, 2))

    df_cust.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/internal/customers.parquet")

    df_dim_customer = df_cust.select(
        F.col("CUST_ID").cast("long").alias("customer_id"),
        F.col("CUST_NO").alias("customer_number"),
        F.col("CUST_NM").alias("customer_full_name"),
        F.col("CUST_SHORT_NM").alias("customer_short_name"),
        F.col("CUST_CAT").alias("customer_category"),
        F.col("CUST_TY_ID").cast("int").alias("customer_type_id"),
        F.date_sub(F.col("CREATE_DT"), (F.rand() * 18_980 + 6_570).cast("int")).alias("date_of_birth"),
        F.col("EMAIL_ADDR").alias("contact_details"),
        F.lit(None).cast("string").alias("marital_status"),
        F.col("GENDER").alias("gender"),
        F.lit("sw-TZ").alias("locale_code"),
        F.col("OPEN_RSN_ID").cast("int").alias("opening_reason_id"),
        F.col("RESIDENT_CNTRY_ID").cast("int").alias("residence_country_id"),
        F.col("MAIN_BRANCH_ID").cast("int").alias("home_branch_id"),
        F.col("INDUSTRY_ID").cast("int").alias("industry_sector_id"),
        F.lit(None).cast("int").alias("institution_id"),
        F.col("CUST_SHORT_NM").alias("swift_short_name"),
        F.col("ANNUAL_INCOME").alias("annual_income"),
        F.col("MONTHLY_INCOME").alias("monthly_income"),
        F.col("NET_WORTH").alias("net_worth"),
        F.round(F.rand() * 5_000_000, 2).alias("credit_limit_amount"),
        F.lit(1).cast("int").alias("risk_rating_id"),
        F.lit(1).cast("int").alias("kyc_level"),
        F.lit(1).cast("int").alias("source_of_funds_id"),
        F.lit(None).cast("int").alias("tax_exemption_id"),
        F.lit(None).cast("int").alias("sourced_campaign_id"),
        F.lit(1).cast("int").alias("is_resident_flag"),
        F.lit(1).cast("int").alias("email_statement_opt_in_flag"),
        F.lit(0).cast("int").alias("is_converted_customer_flag"),
        F.lit(0).cast("int").alias("insider_relationship_flag"),
        F.lit(0).cast("int").alias("aml_failed_flag"),
        F.lit(0).cast("int").alias("exception_requested_flag"),
        F.col("CREATE_DT").alias("customer_created_date"),
        F.col("CREATE_DT").alias("customer_activated_date"),
        F.lit(None).cast("date").alias("customer_deactivated_date"),
        F.lit(None).cast("date").alias("last_statement_date"),
        F.col("TIN").alias("tax_identification_number"),
        F.lit(None).cast("int").alias("primary_relationship_manager_id"),
        F.lit(None).cast("int").alias("secondary_relationship_manager_id"),
        _segment_id_expr().cast("int").alias("customer_segment_id"),
        F.col("MAIN_BRANCH_ID").cast("int").alias("responsible_cost_centre_id"),
        F.col("REC_ST").alias("record_status"),
        F.lit(-99).cast("int").alias("user_id"),
        F.lit("SYSTEM").alias("created_by"),
        F.col("CREATE_DT").alias("created_date"),
        F.col("CREATE_DT").alias("activation_date"),
        F.lit(int(CURRENCY_ID)).cast("int").alias("base_currency_id"),
        F.lit(None).cast("int").alias("credit_rating_agency_id"),
        F.lit(None).cast("date").alias("deactivation_date"),
        F.lit(None).cast("int").alias("rejection_caution_type_id"),
        F.lit(None).cast("int").alias("caution_reason_id"),
    )

    save_and_rename(df_dim_customer, "dim_customer.del")

    print(f"✓ Generated {df_cust.count()} customers")


def generate_accounts(spark):
    """Generate deposit accounts using the Doris warehouse schema."""
    print("--- Generating Deposit Accounts ---")

    df_cust = spark.read.parquet(f"{OUTPUT_DIR}/internal/customers.parquet")

    accounts_list = []

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

    df_accounts = accounts_list[0]
    for df in accounts_list[1:]:
        df_accounts = df_accounts.union(df)

    df_accounts = df_accounts \
        .withColumn("PROD_CAT_TY", F.when(F.col("PROD_ID") < 30, "DP").otherwise("TDEP")) \
        .withColumn("CRNCY_ID", F.lit(int(CURRENCY_ID))) \
        .withColumn("ACCT_NM", F.col("CUST_NM")) \
        .withColumn("STATUS_EFFECTIVE_DT", F.lit(f"{START_YEAR}-01-01")) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("VERSION_NO", F.lit(1)) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    df_accounts.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/internal/accounts.parquet")

    _save_dim_account(df_accounts)

    df_deposit_account = df_accounts.select(
        F.col("ACCT_ID").cast("long").alias("deposit_account_id"),
        F.col("ACCT_ID").cast("long").alias("account_id"),
        F.col("ACCT_NO").alias("account_number"),
        F.col("ACCT_NM").alias("account_name"),
        F.col("CRNCY_ID").cast("int").alias("currency_id"),
        F.col("PROD_ID").cast("long").alias("product_id"),
        F.col("CUST_ID").cast("long").alias("primary_customer_id"),
        F.col("MAIN_BRANCH_ID").cast("int").alias("branch_id"),
        F.col("MAIN_BRANCH_ID").cast("int").alias("responsible_cost_centre_id"),
        F.lit(None).cast("long").alias("portfolio_id"),
        F.lit(-99).cast("int").alias("relationship_manager_id"),
        F.lit(None).cast("long").alias("introducer_id"),
        F.lit(501).cast("int").alias("opening_reason_id"),
        F.lit(1).cast("int").alias("source_of_funds_id"),
        F.lit(None).cast("long").alias("sourced_campaign_id"),
        F.lit(1).cast("int").alias("risk_factor_score"),
        F.to_date(F.col("STATUS_EFFECTIVE_DT")).alias("account_opening_date"),
        F.to_date(F.col("STATUS_EFFECTIVE_DT")).alias("account_available_date"),
        F.lit(None).cast("date").alias("account_closed_date"),
        F.lit(None).cast("date").alias("reward_program_start_date"),
        F.lit(None).cast("long").alias("last_cheque_serial_number"),
        F.col("VERSION_NO").cast("int").alias("source_version_number"),
    )

    save_and_rename(df_deposit_account, "dim_deposit_account.del")

    print(f"✓ Generated {df_accounts.count()} deposit accounts")


def generate_loan_accounts(spark):
    """Generate loan accounts and append them to dim_account."""
    print("--- Generating Loan Accounts ---")

    df_cust = spark.read.parquet(f"{OUTPUT_DIR}/internal/customers.parquet") \
        .filter(F.rand() > 0.7)

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
        .withColumn("CRNCY_ID", F.lit(int(CURRENCY_ID))) \
        .withColumn("ACCT_NM", F.col("CUST_NM")) \
        .withColumn("STATUS_EFFECTIVE_DT", F.lit(f"{START_YEAR}-01-01")) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("VERSION_NO", F.lit(1)) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    df_loans.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/internal/loan_accounts.parquet")

    try:
        df_accounts = spark.read.parquet(f"{OUTPUT_DIR}/internal/accounts.parquet")
        _save_dim_account(df_accounts.unionByName(df_loans))
    except Exception:
        _save_dim_account(df_loans)

    print(f"✓ Generated {df_loans.count()} loan accounts")
