# apps/gen_transactions.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config import *
from utils import save_and_rename
import calendar
import random


def generate_monthly_transactions(spark, year, month):
    """
    Generate 5M+ realistic transactions for a specific month
    Includes: deposits, withdrawals, transfers, mobile money, fees
    """
    print(f"--- Generating Transactions for {year}-{month:02d} ---")

    # Load accounts
    df_accts = spark.read.parquet(f"{OUTPUT_DIR}/internal/accounts.parquet")
    num_days = calendar.monthrange(year, month)[1]

    # Calculate transactions per account to reach 5M
    # With ~750K accounts, we need ~7 txns per account per month
    txns_per_account = 7

    # Generate transaction base
    df_txns = df_accts.select("ACCT_ID", "ACCT_NO", "ACCT_NM", "MAIN_BRANCH_ID", "CUST_ID") \
        .withColumn("txn_seq", F.explode(F.array([F.lit(i) for i in range(txns_per_account)])))

    # Assign transaction types with realistic distribution
    def assign_txn_type():
        rand = random.random()
        cumulative = 0
        for category, types in TRANSACTION_TYPES.items():
            for txn_type, weight in types.items():
                cumulative += weight
                if rand <= cumulative:
                    return (txn_type, "CR" if category == "DEPOSITS" else "DR")
        return ("CASH_DEPOSIT", "CR")

    txn_type_udf = F.udf(assign_txn_type, StructType([
        StructField("type", StringType()),
        StructField("dr_cr", StringType())
    ]))

    # Assign channels
    def assign_channel():
        return random.choices(
            [ch_id for ch_id, (_, _) in CHANNELS.items()],
            weights=[weight for _, (_, weight) in CHANNELS.items()]
        )[0]

    channel_udf = F.udf(assign_channel, IntegerType())

    # Assign amounts based on transaction type
    def assign_amount(txn_type):
        if txn_type in ["SALARY_CREDIT"]:
            return random.uniform(500_000, 3_000_000)
        elif txn_type in ["ATM_WITHDRAWAL", "CASH_WITHDRAWAL"]:
            return random.uniform(20_000, 400_000)
        elif txn_type in ["MOBILE_MONEY_IN", "MOBILE_MONEY_OUT"]:
            return random.uniform(5_000, 200_000)
        elif txn_type in ["TRANSFER_IN", "TRANSFER_OUT"]:
            return random.uniform(50_000, 5_000_000)
        else:
            return random.uniform(10_000, 1_000_000)

    amount_udf = F.udf(assign_amount, DoubleType())

    # Build transaction details
    df_txns = df_txns \
        .withColumn("day", (F.rand() * num_days).cast("int") + 1) \
        .withColumn("TRAN_DT", F.concat_ws("/",
                                           F.format_string("%02d", F.col("day")),
                                           F.lit(f"{month:02d}"),
                                           F.lit(str(year))
                                           )) \
        .withColumn("VALUE_DT", F.col("TRAN_DT")) \
        .withColumn("txn_info", txn_type_udf()) \
        .withColumn("txn_type", F.col("txn_info.type")) \
        .withColumn("DR_CR_IND", F.col("txn_info.dr_cr")) \
        .withColumn("TXN_AMT", F.round(amount_udf(F.col("txn_type")), 2)) \
        .withColumn("CHANNEL_ID", channel_udf()) \
        .withColumn("ACCT_HIST_ID", F.expr("uuid()")) \
        .withColumn("TRAN_REF_TXT", F.concat(
        F.lit(f"{year}{month:02d}"),
        F.substring(F.col("ACCT_HIST_ID"), 1, 10)
    ))

    # Generate transaction descriptions
    desc_map = {
        "CASH_DEPOSIT": "Cash Deposit at Branch",
        "CASH_WITHDRAWAL": "Cash Withdrawal at Branch",
        "ATM_WITHDRAWAL": "ATM Withdrawal",
        "TRANSFER_IN": "Internal Transfer Credit",
        "TRANSFER_OUT": "Internal Transfer Debit",
        "SALARY_CREDIT": "Salary Credit",
        "MOBILE_MONEY_IN": "Mobile Money Deposit",
        "MOBILE_MONEY_OUT": "Mobile Money Withdrawal",
        "INTEREST_CREDIT": "Interest Credit",
    }

    desc_udf = F.udf(lambda t: desc_map.get(t, "Transaction"), StringType())
    df_txns = df_txns.withColumn("TRAN_DESC", desc_udf(F.col("txn_type")))

    # Add contra accounts (GL accounts)
    def get_contra_gl(txn_type):
        if "CASH" in txn_type or "ATM" in txn_type:
            return "1-060-00-1101"
        elif "MOBILE_MONEY" in txn_type:
            return random.choice(["1-060-00-1221", "1-060-00-1222", "1-060-00-1223"])
        elif "INTEREST" in txn_type:
            return "1-060-00-3001"
        else:
            return "1-060-00-2001"

    contra_udf = F.udf(get_contra_gl, StringType())
    df_txns = df_txns.withColumn("CONTRA_ACCT_NO", contra_udf(F.col("txn_type")))

    # Add standard fields
    df_txns = df_txns \
        .withColumn("TXN_CRNCY_ID", F.lit(CURRENCY_ID)) \
        .withColumn("ACCT_CRNCY_ID", F.lit(CURRENCY_ID)) \
        .withColumn("ACCT_AMT", F.col("TXN_AMT")) \
        .withColumn("CONTRA_ACCT_AMT", F.col("TXN_AMT")) \
        .withColumn("EXCH_RATE", F.lit(1)) \
        .withColumn("TOTAL_CHRG_AMT", F.lit(0)) \
        .withColumn("TOTAL_TAX_AMT", F.lit(0)) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("VERSION_NO", F.lit("1")) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM")) \
        .withColumn("ORIGIN_BU_ID", F.col("MAIN_BRANCH_ID"))

    # Select final columns for deposit_txn_history
    df_deposit_txn = df_txns.select(
        "ACCT_HIST_ID", "ACCT_ID", "ACCT_NO", "CONTRA_ACCT_NO",
        "TRAN_DT", "VALUE_DT", "TXN_AMT", "ACCT_AMT", "DR_CR_IND",
        "TRAN_DESC", "TRAN_REF_TXT", "CHANNEL_ID", "ORIGIN_BU_ID",
        "TXN_CRNCY_ID", "ACCT_CRNCY_ID", "EXCH_RATE",
        "TOTAL_CHRG_AMT", "TOTAL_TAX_AMT", "REC_ST", "VERSION_NO",
        "ROW_TS", "USER_ID"
    )

    # Save deposit transactions
    save_and_rename(df_deposit_txn, f"deposit_txn_history__txn_{year}_{month:02d}.del")

    # Generate corresponding GL transactions
    df_gl_txn = df_txns.select(
        F.col("ACCT_HIST_ID").alias("ACCT_HIST_ID"),
        F.col("CONTRA_ACCT_NO").alias("GL_ACCT_NO"),
        F.col("TRAN_DT"),
        F.col("VALUE_DT"),
        F.col("TXN_AMT"),
        F.col("ACCT_AMT"),
        F.when(F.col("DR_CR_IND") == "DR", "CR").otherwise("DR").alias("DR_CR_IND"),
        F.col("TRAN_DESC"),
        F.col("TRAN_REF_TXT"),
        F.col("CHANNEL_ID"),
        F.col("ORIGIN_BU_ID"),
        F.col("TXN_CRNCY_ID"),
        F.col("ACCT_CRNCY_ID"),
        F.col("EXCH_RATE"),
        F.col("TOTAL_CHRG_AMT"),
        F.col("TOTAL_TAX_AMT"),
        F.col("REC_ST"),
        F.col("VERSION_NO"),
        F.col("ROW_TS"),
        F.col("USER_ID")
    )

    # Save GL transactions
    save_and_rename(df_gl_txn, f"gl_txn_history__txn_{year}_{month:02d}.del")

    print(f"✓ Generated {df_deposit_txn.count():,} transactions for {year}-{month:02d}")


def generate_loan_transactions(spark, year, month):
    """Generate loan repayments and disbursements"""
    print(f"--- Generating Loan Transactions for {year}-{month:02d} ---")

    try:
        df_loans = spark.read.parquet(f"{OUTPUT_DIR}/internal/loan_accounts.parquet")
    except:
        print("  ⚠ No loan accounts found, skipping")
        return

    num_days = calendar.monthrange(year, month)[1]

    # Generate monthly repayments (assume 2 repayments per loan per month)
    df_loan_txns = df_loans \
        .withColumn("payment_num", F.explode(F.array([F.lit(1), F.lit(2)]))) \
        .withColumn("day", (F.col("payment_num") * 15)) \
        .withColumn("TRAN_DT", F.concat_ws("/",
                                           F.format_string("%02d", F.col("day")),
                                           F.lit(f"{month:02d}"),
                                           F.lit(str(year))
                                           )) \
        .withColumn("VALUE_DT", F.col("TRAN_DT")) \
        .withColumn("PRINCIPAL_AMT", F.round(F.rand() * 500_000 + 50_000, 2)) \
        .withColumn("INT_AMT", F.round(F.col("PRINCIPAL_AMT") * 0.15 / 12, 2)) \
        .withColumn("LN_ACCT_HIST_ID", F.expr("uuid()")) \
        .withColumn("TRAN_REF_TXT", F.concat(
        F.lit(f"LN{year}{month:02d}"),
        F.substring(F.col("LN_ACCT_HIST_ID"), 1, 8)
    )) \
        .withColumn("TRAN_DESC", F.lit("Loan Repayment - Principal")) \
        .withColumn("DR_CR_IND", F.lit("CR")) \
        .withColumn("TXN_AMT", F.col("PRINCIPAL_AMT") + F.col("INT_AMT")) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("VERSION_NO", F.lit("1")) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    # Select columns
    df_loan_txns = df_loan_txns.select(
        F.col("LN_ACCT_HIST_ID"), F.col("ACCT_ID").alias("LN_ACCT_ID"),
        "ACCT_NO", "TRAN_DT", "VALUE_DT", "PRINCIPAL_AMT", "INT_AMT",
        "TXN_AMT", "DR_CR_IND", "TRAN_DESC", "TRAN_REF_TXT",
        "REC_ST", "VERSION_NO", "ROW_TS", "USER_ID"
    )

    # Save loan transactions
    save_and_rename(df_loan_txns, f"loan_txn_history__txn_{year}_{month:02d}.del")

    print(f"✓ Generated {df_loan_txns.count():,} loan transactions for {year}-{month:02d}")