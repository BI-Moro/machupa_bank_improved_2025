# apps/gen_static.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config import *
from utils import save_and_rename


def generate_static(spark):
    """Generate all static reference data"""
    print("--- Generating Static Reference Data ---")

    # 1. Branches Information
    print("  → Branches")
    branch_data = []
    for b in BRANCHES:
        branch_data.append((
            b[0],  # BU_ID
            b[1],  # BU_NM
            b[2],  # BU_NO
            COUNTRY_ID,  # CNTRY_ID
            b[3],  # CITY
            b[4],  # TELEPHONE_NO (area code)
            "A",  # REC_ST
            "1",  # VERSION_NO
            "SYSTEM",  # USER_ID
            "01/01/2010",  # OPEN_DT
            CURRENCY_ID,  # LOCAL_CRNCY_ID
            CURRENCY_ID,  # BASE_CRNCY_ID
        ))

    df_branch = spark.createDataFrame(branch_data, [
        "BU_ID", "BU_NM", "BU_NO", "CNTRY_ID", "CITY", "TELEPHONE_NO",
        "REC_ST", "VERSION_NO", "USER_ID", "OPEN_DT", "LOCAL_CRNCY_ID", "BASE_CRNCY_ID"
    ])

    save_and_rename(df_branch, "branches_information.del")

    # 2. GL Accounts
    print("  → GL Accounts")
    gl_data = []
    for category, accounts in GL_ACCOUNTS.items():
        for acc in accounts:
            gl_data.append((
                acc[0],  # GL_ACCT_ID
                acc[1],  # GL_ACCT_NO
                acc[2],  # ACCT_DESC
                "Y",  # POST_FG
                "A",  # REC_ST
                "1",  # VERSION_NO
                "SYSTEM",  # USER_ID
            ))

    df_gl = spark.createDataFrame(gl_data, [
        "GL_ACCT_ID", "GL_ACCT_NO", "ACCT_DESC", "POST_FG", "REC_ST", "VERSION_NO", "USER_ID"
    ])

    save_and_rename(df_gl, "GL_ACCOUNT.del")

    # 3. Products
    print("  → Products")
    prod_data = []
    for category, products in PRODUCTS.items():
        for prod in products:
            prod_data.append((
                prod[0],  # PROD_ID
                prod[1],  # PROD_CD
                prod[2],  # PROD_DESC
                prod[3],  # PROD_CAT_TY
                CURRENCY_ID,  # CRNCY_ID
                prod[4],  # Interest Rate
                "A",  # REC_ST
                "1",  # VERSION_NO
                "SYSTEM",  # USER_ID
                "01/01/2018",  # START_DT
            ))

    df_prod = spark.createDataFrame(prod_data, [
        "PROD_ID", "PROD_CD", "PROD_DESC", "PROD_CAT_TY", "CRNCY_ID",
        "INT_RATE", "REC_ST", "VERSION_NO", "USER_ID", "START_DT"
    ])

    save_and_rename(df_prod, "products_information.del")

    # 4. Currency Information
    print("  → Currency")
    currency_data = [(
        CURRENCY_ID,  # CRNCY_ID
        CURRENCY_CODE,  # CRNCY_CD
        "TZS",  # CRNCY_CD_ISO
        "Tanzania Shilling",  # CRNCY_NM
        "TZS",  # CRNCY_SYM
        ",",  # THOUSAND_SYM
        ".",  # DECIMAL_SYM
        "2",  # DENOM_CNT
        "A",  # REC_ST
        "1",  # VERSION_NO
        "SYSTEM",  # USER_ID
    )]

    df_currency = spark.createDataFrame(currency_data, [
        "CRNCY_ID", "CRNCY_CD", "CRNCY_CD_ISO", "CRNCY_NM", "CRNCY_SYM",
        "THOUSAND_SYM", "DECIMAL_SYM", "DENOM_CNT", "REC_ST", "VERSION_NO", "USER_ID"
    ])

    save_and_rename(df_currency, "currency_information.del")

    # 5. Exchange Rates (for USD, GBP, EUR, KES, UGX, RWF)
    print("  → Exchange Rates")
    fx_data = [
        (1, "852", "841", 2350.00, "11", "A"),  # TZS to USD
        (2, "852", "826", 3100.00, "11", "A"),  # TZS to GBP
        (3, "852", "978", 2550.00, "11", "A"),  # TZS to EUR
        (4, "852", "404", 17.50, "11", "A"),  # TZS to KES
        (5, "852", "800", 0.65, "11", "A"),  # TZS to UGX
        (6, "852", "646", 1.85, "11", "A"),  # TZS to RWF
    ]

    df_fx = spark.createDataFrame(fx_data, [
        "EXCH_RATE_ID", "FROM_CRNCY_ID", "TO_CRNCY_ID", "EXCH_RATE", "RATE_TY_ID", "REC_ST"
    ])

    save_and_rename(df_fx, "exchange_rate_information_history.del")

    # 6. Channels
    print("  → Channels")
    channel_data = [
        (9, "BRANCH", "Branch Banking", "A"),
        (81, "API", "API/Mobile Money", "A"),
        (7, "MOBILE_BANKING", "Mobile Banking", "A"),
        (6, "ATM", "ATM", "A"),
        (82, "INTERNET_BANKING", "Internet Banking", "A"),
    ]

    df_channels = spark.createDataFrame(channel_data, [
        "CHANNEL_ID", "CHANNEL_CD", "CHANNEL_DESC", "REC_ST"
    ])

    save_and_rename(df_channels, "channels_terminal_information.del")

    print("✓ Static Data Generation Complete")


def generate_users(spark):
    """Generate bank users/employees"""
    print("--- Generating Users ---")

    user_data = [
        (-99, "SYSTEM", "System", "User", "SYSTEM", "system@tcbbank.co.tz", -78, "A"),
        (1, "ADMIN", "Bank", "Administrator", "ADMIN001", "admin@tcbbank.co.tz", -78, "A"),
        (2, "TELLER01", "Grace", "Mushi", "TELL001", "grace.mushi@tcbbank.co.tz", -78, "A"),
        (3, "TELLER02", "John", "Lyimo", "TELL002", "john.lyimo@tcbbank.co.tz", -77, "A"),
        (4, "MGR01", "Peter", "Massawe", "MGR001", "peter.massawe@tcbbank.co.tz", -78, "A"),
    ]

    df_users = spark.createDataFrame(user_data, [
        "SYSUSER_ID", "LOGIN_ID", "FIRST_NM", "LAST_NM", "EMP_NO",
        "EMAIL_ADDR", "MAIN_BRANCH_ID", "REC_ST"
    ])

    save_and_rename(df_users, "users_information.del")
    print("✓ Generated users")