# apps/gen_static.py
from pyspark.sql import functions as F
from config import *
from utils import save_and_rename


def generate_static(spark):
    """Generate static reference data using the Doris warehouse schema."""
    print("--- Generating Static Reference Data ---")

    current_ts = F.current_timestamp()

    # 1. Business Units / Branches
    print("  -> Business Units")
    branch_data = []
    for branch_id, branch_name, branch_no, city, _phone_code in BRANCHES:
        branch_data.append((
            branch_id,
            branch_no,
            "2010-01-01",
            None,
            branch_name,
            "Retail Branch",
            "Urban" if city == "Dar es Salaam" else "Regional",
            f"{abs(branch_id):03d}",
            branch_no[:2],
            None,
            None,
            None,
            city,
            None,
            None,
            None,
            None,
            None,
            None,
            int(COUNTRY_ID),
            500_000_000.00,
            50_000_000.00,
            1,
            "A",
            int(CURRENCY_ID),
            int(CURRENCY_ID),
            None,
            None,
            1,
        ))

    df_branch = spark.createDataFrame(branch_data, schema="""
        business_unit_id bigint, business_unit_number string, opening_date string,
        closing_date string, branch_name string, business_unit_type_description string,
        urban_rural_classification string, branch_gl_code string, gl_prefix_code string,
        parent_business_unit_id int, district_id int, city_id int, bu_address_line_1 string,
        bu_address_line_2 string, bu_address_line_3 string, bu_address_line_4 string,
        postal_code string, latitude double, longitude double, country_id int,
        insurance_vault_limit_amount double, insurance_drawer_limit_amount double,
        has_vault_flag int, record_status string, local_currency_id int, base_currency_id int,
        branch_manager_id int, assistant_branch_manager_id int, branch_risk_rating_id int
    """).withColumn("opening_date", F.to_date("opening_date")) \
        .withColumn("closing_date", F.to_date("closing_date"))

    save_and_rename(df_branch, "dim_business_unit.del")

    # 2. Account statuses
    print("  -> Account Status")
    df_status = spark.createDataFrame([
        ("A", "Active"),
        ("I", "Inactive"),
        ("L", "Closed"),
        ("D", "Dormant"),
    ], ["status_code", "status_description"])

    save_and_rename(df_status, "dim_account_status.del")

    # 3. Chart of Accounts
    print("  -> Chart Of Accounts")
    df_chart = spark.createDataFrame([
        (1, "TCB Main Chart of Accounts"),
    ], ["chart_id", "chart_name"])

    save_and_rename(df_chart, "dim_chart_of_accounts.del")

    # 4. General Ledger
    print("  -> General Ledger")
    category_names = {
        "CASH": ("ASSET", "Cash and cash equivalents"),
        "DEPOSITS": ("LIAB", "Customer deposits"),
        "LOANS": ("ASSET", "Loan receivables"),
        "INCOME": ("INC", "Income accounts"),
        "CHARGES": ("LIAB", "Tax and charges payable"),
        "MOBILE_MONEY": ("ASSET", "Mobile money clearing"),
    }

    gl_data = []
    for category, accounts in GL_ACCOUNTS.items():
        category_code, category_description = category_names.get(category, (category, category.title()))
        for gl_account_id, gl_account_number, gl_account_name in accounts:
            gl_data.append((
                gl_account_id,
                gl_account_number,
                gl_account_name,
                gl_account_name,
                category_code,
                category_description,
                "BAL",
                gl_account_number.split("-")[-1],
                "1",
                "1",
                None,
                None,
                None,
                None,
                1,
                0,
                0,
                1,
                1,
                1,
                1,
                "A",
                "SYSTEM",
                -99,
                "2018-01-01 00:00:00",
            ))

    df_gl = spark.createDataFrame(gl_data, schema="""
        gl_account_id bigint, gl_account_number string, gl_account_name string,
        gl_account_name_local string, gl_category_code string, gl_category_description string,
        balance_code string, ledger_number string, gl_structure_id string,
        chart_of_accounts_id string, alternate_gl_number string, suspense_category string,
        branch_id bigint, sub_ledger_type_id int, is_postable_flag int, is_control_account int,
        is_suspense_account int, allows_credit_entries int, allows_debit_entries int,
        allows_balance_swing int, allows_bulk_posting int, record_status string,
        created_by string, user_id int, created_date string
    """).withColumn("created_date", F.to_timestamp("created_date")) \
        .withColumn("last_updated_ts", current_ts)

    save_and_rename(df_gl, "dim_general_ledger.del")

    # 5. Products
    print("  -> Products")
    prod_data = []
    for category, products in PRODUCTS.items():
        for product_id, product_code, product_description, product_category_code, _interest_rate in products:
            prod_data.append((
                str(product_id),
                product_code,
                product_description,
                product_category_code,
                category.replace("_", " ").title(),
                category.replace("_", " ").title(),
                None,
                int(CURRENCY_ID),
                product_description,
                "2018-01-01",
                None,
                None,
                18,
                "A",
                "MONTHLY",
                1,
                "MONTHLY",
                30,
                "YEARLY",
                "MONTHLY",
                "MONTHLY",
                "MONTH",
                3,
            ))

    df_prod = spark.createDataFrame(prod_data, schema="""
        product_id string, product_code string, product_name_description string,
        product_category_code string, product_category string, product_type string,
        product_template_id int, currency_id int, product_name_local string,
        product_launch_date string, product_expiry_date string, product_withdrawal_date string,
        minimum_customer_age int, record_status string, statement_frequency_type string,
        statement_frequency_interval int, account_cycle_type string, account_cycle_total_days int,
        review_cycle_type string, interest_accrual_frequency_type string,
        charge_frequency_type string, new_customer_period_type string,
        new_customer_period_duration int
    """).withColumn("product_launch_date", F.to_date("product_launch_date")) \
        .withColumn("product_expiry_date", F.to_date("product_expiry_date")) \
        .withColumn("product_withdrawal_date", F.to_date("product_withdrawal_date")) \
        .withColumn("sys_row_timestamp", current_ts)

    save_and_rename(df_prod, "dim_product.del")

    # 6. Currency
    print("  -> Currency")
    currency_data = [(
        int(CURRENCY_ID),
        CURRENCY_CODE,
        CURRENCY_CODE,
        "Tanzania Shilling",
        "TZS",
        "Local",
        "Shilling",
        "Senti",
        1.0,
        "ACT/365",
        0.01,
        2,
        "HALF_UP",
        int(CURRENCY_ID),
        0,
        "A",
    )]

    df_currency = spark.createDataFrame(currency_data, [
        "currency_id", "currency_iso_code", "currency_code", "currency_name",
        "currency_symbol", "currency_category", "major_unit", "minor_unit",
        "current_spot_exchange_rate", "default_accrual_basis", "smallest_unit_value",
        "rounding_precision_digits", "currency_rounding_method", "iso_numeric_code",
        "settlement_due_period_days", "record_status"
    ])

    save_and_rename(df_currency, "dim_currency.del")

    # 7. Exchange Rates
    print("  -> Exchange Rates")
    fx_data = [
        (1, 1, 841, "MULTIPLY", "A", 1, 2350.00),  # USD
        (2, 1, 826, "MULTIPLY", "A", 1, 3100.00),  # GBP
        (3, 1, 978, "MULTIPLY", "A", 1, 2550.00),  # EUR
        (4, 1, 404, "MULTIPLY", "A", 1, 17.50),  # KES
        (5, 1, 800, "MULTIPLY", "A", 1, 0.65),  # UGX
        (6, 1, 646, "MULTIPLY", "A", 1, 1.85),  # RWF
    ]

    df_fx_dim = spark.createDataFrame(fx_data, [
        "exchange_rate_id", "rate_template_id", "currency_id", "conversion_method",
        "record_status", "version_no", "exchange_rate_value"
    ]).withColumn("source_row_ts", current_ts) \
        .withColumn("created_date", current_ts) \
        .withColumn("sys_create_timestamp", current_ts) \
        .withColumn("created_by", F.lit("SYSTEM")) \
        .withColumn("user_id", F.lit("SYSTEM"))

    save_and_rename(df_fx_dim.select(
        "exchange_rate_id", "rate_template_id", "currency_id", "conversion_method",
        "record_status", "source_row_ts", "created_date", "sys_create_timestamp",
        "created_by", "user_id", "version_no"
    ), "dim_exchange_rate.del")

    df_fx_fact = df_fx_dim.select(
        F.col("exchange_rate_id").alias("exchange_rate_history_id"),
        F.col("exchange_rate_id").alias("exchange_rate_pair_id"),
        F.to_timestamp(F.lit("2026-01-01 00:00:00")).alias("valid_from_timestamp"),
        F.lit(1).alias("exchange_rate_type_id"),
        F.col("exchange_rate_value"),
        F.round(F.lit(1.0) / F.col("exchange_rate_value"), 8).alias("inverse_rate_value"),
        F.lit(None).cast("timestamp").alias("valid_to_timestamp"),
        F.lit(0.0).alias("spread_percentage"),
        F.lit("Manual").alias("rate_source"),
        F.col("record_status"),
        F.to_date(F.lit("2026-01-01")).alias("system_creation_date"),
        F.lit("SYSTEM").alias("system_created_by"),
        F.to_date(F.lit("2026-01-01")).alias("start_effective_date"),
        F.lit(None).cast("date").alias("end_effective_date"),
    )

    save_and_rename(df_fx_fact, "fact_exchange_rates.del")

    # 8. Channels
    print("  -> Channels")
    channel_data = [(channel_id, channel_name) for channel_id, (channel_name, _weight) in CHANNELS.items()]
    df_channels = spark.createDataFrame(channel_data, ["channel_id", "channel_name"])

    save_and_rename(df_channels, "dim_channel.del")

    print("✓ Static Data Generation Complete")


def generate_users(spark):
    """Generate bank users/employees using the Doris warehouse schema."""
    print("--- Generating Users ---")

    user_data = [
        (-99, "SYSTEM", "SYSTEM", "System", None, "User", "System User", "System User", -78, None, None, 1, "System", -78, 5, 0, 0, 1, 1, 0, 0, "2010-01-01", None, None, 0, "A"),
        (1, "ADMIN", "ADMIN001", "Bank", None, "Administrator", "Bank Administrator", "Bank Administrator", -78, -99, None, 1, "Staff", -78, 5, 0, 0, 1, 1, 0, 0, "2018-01-01", None, None, 0, "A"),
        (2, "TELLER01", "TELL001", "Grace", None, "Mushi", "Grace Mushi", "Grace Mushi", -78, 4, None, 2, "Staff", -78, 3, 0, 0, 0, 1, 0, 0, "2018-01-01", None, None, 0, "A"),
        (3, "TELLER02", "TELL002", "John", None, "Lyimo", "John Lyimo", "John Lyimo", -77, 4, None, 2, "Staff", -77, 3, 0, 0, 0, 1, 0, 0, "2018-01-01", None, None, 0, "A"),
        (4, "MGR01", "MGR001", "Peter", None, "Massawe", "Peter Massawe", "Peter Massawe", -78, 1, None, 3, "Staff", -78, 4, 1, 0, 1, 1, 0, 0, "2018-01-01", None, None, 0, "A"),
    ]

    df_users = spark.createDataFrame(user_data, schema="""
        user_id bigint, user_login_name string, employee_external_id string, first_name string,
        middle_name string, last_name string, full_name string,
        relationship_manager_preferred_name string, home_branch_id int, supervisor_user_id int,
        job_title_id int, primary_role_id int, user_category string, cost_center_id int,
        security_clearance_level int, is_relationship_officer_flag int,
        is_collection_officer_flag int, is_supervisor_flag int, is_internal_employee_flag int,
        is_account_locked_flag int, is_introducer_flag int, employment_start_date string,
        employment_end_date string, last_login_timestamp string, failed_login_attempts int,
        record_status string
    """).withColumn("employment_start_date", F.to_date("employment_start_date")) \
        .withColumn("employment_end_date", F.to_date("employment_end_date")) \
        .withColumn("last_login_timestamp", F.to_timestamp("last_login_timestamp"))

    save_and_rename(df_users, "dim_user_employee.del")
    print("✓ Generated users")
