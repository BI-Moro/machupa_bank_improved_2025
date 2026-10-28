# apps/gen_transactions.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config import *
from utils import save_and_rename
import calendar
import math
import random
from datetime import date, timedelta

GL_ACCOUNT_ID_BY_NO = {
    gl_account_number: gl_account_id
    for accounts in GL_ACCOUNTS.values()
    for gl_account_id, gl_account_number, _gl_account_name in accounts
}


def period_suffix(start_date, end_date):
    return f"{start_date:%Y%m%d}_{end_date:%Y%m%d}"


def _period_day_count(start_date, end_date):
    if end_date < start_date:
        raise ValueError("GENERATION_END_DATE cannot be before the start date")
    return (end_date - start_date).days + 1


def _period_dates(start_date, end_date):
    return [
        (start_date + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(_period_day_count(start_date, end_date))
    ]


def _date_from_index(period_dates):
    expr = F.lit(period_dates[-1])
    for index in range(len(period_dates) - 2, -1, -1):
        expr = F.when(F.col("date_index") == index, F.lit(period_dates[index])).otherwise(expr)
    return expr


def _scaled_count(monthly_count, day_count):
    return max(1, math.ceil(monthly_count * day_count / 30))


gl_account_id_udf = F.udf(lambda gl_no: GL_ACCOUNT_ID_BY_NO.get(gl_no, -1), LongType())


def generate_monthly_transactions(spark, year, month):
    """Generate the existing monthly deposit/withdrawal transaction files."""
    last_day = calendar.monthrange(year, month)[1]
    generate_transactions_for_period(
        spark,
        date(year, month, 1),
        date(year, month, last_day),
        file_suffix=f"{year}_{month:02d}",
        txns_per_account=7,
    )


def generate_transactions_for_period(spark, start_date, end_date, file_suffix=None, txns_per_account=None):
    """Generate deposit, withdrawal, transfer, mobile money, fee, and GL transactions for a date range."""
    day_count = _period_day_count(start_date, end_date)
    file_suffix = file_suffix or period_suffix(start_date, end_date)
    txns_per_account = txns_per_account or _scaled_count(7, day_count)
    period_dates = _period_dates(start_date, end_date)

    print(f"--- Generating Transactions from {start_date} to {end_date} ({day_count} days) ---")

    # Load accounts
    df_accts = spark.read.parquet(f"{OUTPUT_DIR}/internal/accounts.parquet")

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
        .withColumn("date_index", (F.rand() * len(period_dates)).cast("int")) \
        .withColumn("TRAN_DT", F.to_date(_date_from_index(period_dates))) \
        .withColumn("VALUE_DT", F.col("TRAN_DT")) \
        .withColumn("txn_info", txn_type_udf()) \
        .withColumn("txn_type", F.col("txn_info.type")) \
        .withColumn("DR_CR_IND", F.col("txn_info.dr_cr")) \
        .withColumn("TXN_AMT", F.round(amount_udf(F.col("txn_type")), 2)) \
        .withColumn("CHANNEL_ID", channel_udf()) \
        .withColumn("ACCT_HIST_ID", F.pmod(
            F.xxhash64(F.col("ACCT_ID"), F.col("txn_seq"), F.col("TRAN_DT"), F.lit(file_suffix)),
            F.lit(9223372036854775807)
        ).cast("long")) \
        .withColumn("TRAN_REF_TXT", F.concat(
        F.lit(file_suffix.replace("_", "")),
        F.substring(F.col("ACCT_HIST_ID").cast("string"), 1, 10)
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
    df_txns = df_txns.withColumn("CONTRA_ACCT_NO", contra_udf(F.col("txn_type"))) \
        .withColumn("CONTRA_GL_ACCT_ID", gl_account_id_udf(F.col("CONTRA_ACCT_NO")))

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

    # Select final columns for fact_deposit_transactions
    df_deposit_txn = df_txns.select(
        F.col("ACCT_HIST_ID").cast("long").alias("deposit_transaction_id"),
        F.col("ACCT_ID").cast("long").alias("deposit_account_id"),
        F.col("TRAN_DT").cast("timestamp").alias("transaction_date"),
        F.col("ACCT_NO").alias("account_number"),
        F.col("CONTRA_ACCT_NO").alias("contra_account_number"),
        F.col("VALUE_DT").cast("timestamp").alias("value_date"),
        F.col("TXN_AMT").cast("double").alias("transaction_amount"),
        F.col("ACCT_AMT").cast("double").alias("account_currency_amount"),
        F.col("CONTRA_ACCT_AMT").cast("double").alias("contra_account_amount"),
        F.col("TOTAL_CHRG_AMT").cast("double").alias("total_charges_amount"),
        F.col("TOTAL_TAX_AMT").cast("double").alias("total_tax_amount"),
        F.lit(0.0).alias("internal_system_cost"),
        F.col("EXCH_RATE").cast("double").alias("exchange_rate_applied"),
        F.round(F.rand() * 5_000_000 + 20_000, 2).alias("post_txn_cleared_balance"),
        F.round(F.rand() * 5_500_000 + 20_000, 2).alias("post_txn_total_balance"),
        F.when(F.col("DR_CR_IND") == "DR", "D").otherwise("C").alias("debit_credit_indicator"),
        F.col("TRAN_DESC").alias("transaction_narrative"),
        F.col("TRAN_REF_TXT").alias("transaction_reference"),
        F.lit(None).cast("double").alias("cheque_number"),
        F.col("ACCT_NM").alias("counterparty_name"),
        F.lit(None).cast("string").alias("memo_type_code"),
        F.lit("N").alias("backdate_status"),
        F.col("REC_ST").alias("record_status"),
        F.lit("N").alias("passbook_flag"),
        F.col("TXN_CRNCY_ID").cast("double").alias("transaction_currency_id"),
        F.col("ACCT_CRNCY_ID").cast("double").alias("account_currency_id"),
        F.col("ACCT_CRNCY_ID").cast("double").alias("contra_currency_id"),
        F.lit(1.0).alias("contra_account_type"),
        F.col("CHANNEL_ID").cast("double").alias("channel_id"),
        F.col("ORIGIN_BU_ID").cast("double").alias("origin_branch_id"),
        F.lit(None).cast("double").alias("event_type_id"),
        F.lit(None).cast("double").alias("parent_event_id"),
        F.lit(None).cast("double").alias("source_of_funds_id"),
        F.lit(None).cast("double").alias("charge_type_id"),
        F.lit(None).cast("double").alias("tax_type_id"),
        F.col("ACCT_HIST_ID").cast("double").alias("journal_batch_id"),
        F.col("ACCT_HIST_ID").cast("double").alias("event_batch_id"),
        F.lit(None).cast("double").alias("charge_batch_id"),
        F.lit(None).cast("double").alias("tax_batch_id"),
        F.lit(-99.0).alias("maker_user_id"),
        F.lit(-99.0).alias("checker_user_id"),
        F.lit("SYSTEM").alias("last_modified_by"),
        F.lit("SYSTEM").alias("system_created_by"),
        F.col("TRAN_DT").cast("timestamp").alias("system_creation_date"),
        F.current_timestamp().alias("system_creation_ts"),
        F.col("ROW_TS").alias("row_timestamp"),
        F.col("VERSION_NO").cast("double").alias("record_version"),
        F.lit("deposit_account_history").alias("cdc_source_table"),
        F.lit(None).cast("string").alias("cdc_scn"),
        F.lit("I").alias("cdc_operation_type"),
        F.current_timestamp().cast("string").alias("cdc_operation_ts"),
        F.current_timestamp().cast("string").alias("cdc_current_ts"),
        F.lit(None).cast("string").alias("cdc_row_id"),
        F.lit("SYSTEM").alias("cdc_username"),
    )

    # Save deposit transactions
    save_and_rename(df_deposit_txn, f"fact_deposit_transactions__txn_{file_suffix}.del")

    # Generate corresponding GL transactions
    df_gl_txn = df_txns.select(
        F.col("ACCT_HIST_ID").cast("long").alias("transaction_history_id"),
        F.col("TRAN_DT").alias("transaction_date"),
        F.col("CONTRA_GL_ACCT_ID").cast("long").alias("gl_account_id"),
        F.col("CONTRA_ACCT_NO").alias("gl_account_number"),
        F.col("ACCT_AMT").cast("decimal(38,10)").alias("account_currency_amount"),
        F.col("TXN_AMT").cast("decimal(38,10)").alias("transaction_amount"),
        F.when(F.col("DR_CR_IND") == "DR", "CR").otherwise("DR").alias("dr_cr_indicator"),
        F.round(F.rand() * 100_000_000, 2).cast("decimal(38,10)").alias("system_running_balance"),
        F.col("ACCT_CRNCY_ID").cast("long").alias("account_currency_id"),
        F.col("TXN_CRNCY_ID").cast("long").alias("transaction_currency_id"),
        F.col("ACCT_CRNCY_ID").cast("long").alias("contra_account_currency_id"),
        F.col("EXCH_RATE").cast("decimal(38,12)").alias("exchange_rate"),
        F.col("VALUE_DT").alias("value_date"),
        F.col("TRAN_DT").alias("create_date"),
        F.lit(None).cast("long").alias("event_id"),
        F.col("ACCT_HIST_ID").cast("long").alias("tran_journal_id"),
        F.col("ACCT_HIST_ID").cast("long").alias("event_journal_id"),
        F.lit(None).cast("long").alias("event_charge_journal_id"),
        F.lit(None).cast("long").alias("event_tax_journal_id"),
        F.col("ORIGIN_BU_ID").cast("long").alias("origin_branch_id"),
        F.col("ORIGIN_BU_ID").cast("long").alias("contra_branch_id"),
        F.col("CHANNEL_ID").cast("long").alias("channel_id"),
        F.col("ACCT_NO").alias("contra_account_number"),
        F.lit(1).cast("long").alias("contra_account_type_id"),
        F.col("CONTRA_ACCT_AMT").cast("decimal(38,10)").alias("contra_account_amount"),
        F.col("TOTAL_CHRG_AMT").cast("decimal(38,10)").alias("total_charge_amount"),
        F.col("TOTAL_TAX_AMT").cast("decimal(38,10)").alias("total_tax_amount"),
        F.lit(0).cast("decimal(38,10)").alias("event_cost"),
        F.lit(0).cast("decimal(38,10)").alias("total_cost"),
        F.col("TRAN_REF_TXT").alias("transaction_reference"),
        F.lit(None).cast("long").alias("cheque_number"),
        F.col("TRAN_DESC").alias("transaction_description"),
        F.lit(-99).cast("long").alias("supervisor_id"),
        F.lit(-99).cast("long").alias("originator_id"),
        F.col("USER_ID").alias("user_id"),
        F.lit("SYSTEM").alias("created_by"),
        F.col("REC_ST").alias("record_status"),
        F.col("VERSION_NO").cast("long").alias("version_number"),
        F.lit("Y").alias("balance_update_status"),
        F.lit("N").alias("bank_recon_flag"),
        F.lit(None).cast("long").alias("bank_recon_id"),
        F.col("ROW_TS").alias("row_timestamp"),
        F.current_timestamp().alias("source_create_timestamp"),
        F.current_timestamp().alias("etl_load_date"),
    )

    # Save GL transactions
    save_and_rename(df_gl_txn, f"fact_gl_transactions__txn_{file_suffix}.del")

    print(f"✓ Generated {df_deposit_txn.count():,} transactions for {start_date} to {end_date}")


def generate_loan_transactions(spark, year, month):
    """Generate the existing monthly loan transaction files."""
    last_day = calendar.monthrange(year, month)[1]
    generate_loan_transactions_for_period(
        spark,
        date(year, month, 1),
        date(year, month, last_day),
        file_suffix=f"{year}_{month:02d}",
        payments_per_loan=2,
    )


def generate_loan_transactions_for_period(spark, start_date, end_date, file_suffix=None, payments_per_loan=None):
    """Generate loan repayments and disbursements for a date range."""
    day_count = _period_day_count(start_date, end_date)
    file_suffix = file_suffix or period_suffix(start_date, end_date)
    payments_per_loan = payments_per_loan or _scaled_count(2, day_count)
    period_dates = _period_dates(start_date, end_date)

    print(f"--- Generating Loan Transactions from {start_date} to {end_date} ({day_count} days) ---")

    try:
        df_loans = spark.read.parquet(f"{OUTPUT_DIR}/internal/loan_accounts.parquet")
    except:
        print("  ⚠ No loan accounts found, skipping")
        return

    # Generate repayments scaled to the selected date range.
    df_loan_txns = df_loans \
        .withColumn("payment_num", F.explode(F.array([F.lit(i + 1) for i in range(payments_per_loan)]))) \
        .withColumn("date_index", (F.rand() * len(period_dates)).cast("int")) \
        .withColumn("TRAN_DT", F.to_date(_date_from_index(period_dates))) \
        .withColumn("VALUE_DT", F.col("TRAN_DT")) \
        .withColumn("PRINCIPAL_AMT", F.round(F.rand() * 500_000 + 50_000, 2)) \
        .withColumn("INT_AMT", F.round(F.col("PRINCIPAL_AMT") * 0.15 / 12, 2)) \
        .withColumn("LN_ACCT_HIST_ID", F.pmod(
            F.xxhash64(F.col("ACCT_ID"), F.col("payment_num"), F.col("TRAN_DT"), F.lit(file_suffix)),
            F.lit(9223372036854775807)
        ).cast("long")) \
        .withColumn("TRAN_REF_TXT", F.concat(
        F.lit(f"LN{file_suffix.replace('_', '')}"),
        F.substring(F.col("LN_ACCT_HIST_ID").cast("string"), 1, 8)
    )) \
        .withColumn("TRAN_DESC", F.lit("Loan Repayment - Principal")) \
        .withColumn("DR_CR_IND", F.lit("CR")) \
        .withColumn("TXN_AMT", F.col("PRINCIPAL_AMT") + F.col("INT_AMT")) \
        .withColumn("REC_ST", F.lit("A")) \
        .withColumn("VERSION_NO", F.lit("1")) \
        .withColumn("ROW_TS", F.current_timestamp()) \
        .withColumn("USER_ID", F.lit("SYSTEM"))

    # Select columns for fact_loan_transactions
    df_loan_txns = df_loan_txns.select(
        F.col("TRAN_DT").alias("transaction_date"),
        F.col("ACCT_ID").cast("long").alias("loan_account_id"),
        F.col("LN_ACCT_HIST_ID").cast("long").alias("loan_transaction_id"),
        F.col("VALUE_DT").alias("value_date"),
        F.col("MAIN_BRANCH_ID").cast("int").alias("origin_branch_id"),
        F.lit(-99).cast("int").alias("maker_user_id"),
        F.lit(-99).cast("int").alias("checker_user_id"),
        F.lit(9).cast("int").alias("channel_id"),
        F.lit(None).cast("int").alias("event_type_id"),
        F.lit(int(CURRENCY_ID)).cast("int").alias("transaction_currency_id"),
        F.lit(None).cast("int").alias("tax_reference_id"),
        F.lit(None).cast("int").alias("charge_reference_id"),
        F.lit(None).cast("int").alias("memo_type_code"),
        F.col("PRINCIPAL_AMT").cast("decimal(18,2)").alias("principal_amount"),
        F.col("INT_AMT").cast("decimal(18,2)").alias("interest_amount"),
        F.lit(0).cast("decimal(18,2)").alias("insurance_amount"),
        F.col("TXN_AMT").cast("decimal(18,2)").alias("original_txn_amount"),
        F.col("TXN_AMT").cast("decimal(18,2)").alias("account_currency_amount"),
        F.round(F.rand() * 20_000_000, 2).cast("decimal(18,2)").alias("principal_remaining_bal"),
        F.round(F.rand() * 25_000_000, 2).cast("decimal(18,2)").alias("total_outstanding_bal"),
        F.lit(0).cast("decimal(18,2)").alias("total_tax_amount"),
        F.lit(0).cast("decimal(18,2)").alias("total_charges_amount"),
        F.lit(1).cast("decimal(18,8)").alias("exchange_rate_applied"),
        F.lit(0).cast("decimal(18,2)").alias("transaction_cost"),
        F.when(F.col("DR_CR_IND") == "DR", "D").otherwise("C").alias("debit_credit_indicator"),
        F.col("ACCT_NO").alias("account_number"),
        F.col("TRAN_DESC").alias("transaction_narrative"),
        F.col("TRAN_REF_TXT").alias("transaction_reference"),
        F.lit(None).cast("string").alias("contra_account_number"),
        F.col("LN_ACCT_HIST_ID").cast("long").alias("journal_batch_id"),
        F.lit(None).cast("long").alias("cheque_number"),
        F.lit(None).cast("long").alias("parent_transaction_id"),
        F.lit(1).cast("int").alias("is_main_transaction"),
        F.col("REC_ST").alias("record_status"),
        F.lit("N").alias("backdate_status"),
    )

    # Save loan transactions
    save_and_rename(df_loan_txns, f"fact_loan_transactions__txn_{file_suffix}.del")

    print(f"✓ Generated {df_loan_txns.count():,} loan transactions for {start_date} to {end_date}")
