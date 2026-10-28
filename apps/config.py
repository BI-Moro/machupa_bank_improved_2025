# apps/config.py
import os

# Paths
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/bitnami/spark/output")
DELIMITER = ","

# Doris
DORIS_HOST = os.getenv("DORIS_HOST", "192.168.201.12")
DORIS_PORT = int(os.getenv("DORIS_PORT", "9030"))
DORIS_HTTP_PORT = int(os.getenv("DORIS_HTTP_PORT", "8030"))
DORIS_DATABASE = os.getenv("DORIS_DATABASE", "maduro_one_one")
DORIS_USER = os.getenv("DORIS_USER", "root")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD", "")

# Context
CURRENCY_ID = "852"  # TZS
CURRENCY_CODE = "TZS"
COUNTRY_ID = "676"  # Tanzania
BANK_PREFIX = "TCB"

# Scale - Targeting 5M transactions/month
TARGET_MONTHLY_TXNS = 5_000_000
NUM_CUSTOMERS = 500_000
NUM_ACTIVE_CUSTOMERS = 350_000  # 70% active
START_YEAR = 2018
CURRENT_YEAR = 2025
GENERATION_DAYS = int(os.getenv("GENERATION_DAYS", "4"))
GENERATION_END_DATE = os.getenv("GENERATION_END_DATE", "")  # Optional YYYY-MM-DD

# Branches (Major TCB branches across Tanzania)
BRANCHES = [
    # Dar es Salaam Region
    (-78, "MKWEPU", "110", "Dar es Salaam", "021"),
    (-77, "KARIAKOO", "120", "Dar es Salaam", "021"),
    (-76, "METROPOLITAN", "130", "Dar es Salaam", "021"),
    (-75, "MAGOMENI", "140", "Dar es Salaam", "021"),
    (-74, "MBAGALA", "150", "Dar es Salaam", "021"),
    (-73, "TEMEKE", "160", "Dar es Salaam", "021"),
    (-72, "KINONDONI", "170", "Dar es Salaam", "021"),
    (-71, "ILALA", "180", "Dar es Salaam", "021"),

    # Other Major Cities
    (-47, "ARUSHA MAIN", "210", "Arusha", "027"),
    (-46, "ARUSHA NJIRO", "220", "Arusha", "027"),
    (-24, "MWANZA MAIN", "310", "Mwanza", "028"),
    (-23, "MWANZA NYAMAGANA", "320", "Mwanza", "028"),
    (-13, "DODOMA", "410", "Dodoma", "026"),
    (-12, "MOROGORO", "510", "Morogoro", "023"),
    (-11, "MBEYA", "610", "Mbeya", "025"),
    (-10, "TANGA", "710", "Tanga", "027"),
    (-9, "MOSHI", "810", "Moshi", "027"),
    (-8, "IRINGA", "910", "Iringa", "026"),
]

# Product Catalog (Realistic TCB Products)
PRODUCTS = {
    # Deposit Products
    "SAVINGS": [
        (10, "SAV-REG", "Regular Savings Account", "DP", 0.03),  # 3% interest
        (11, "SAV-STU", "Student Savings", "DP", 0.04),
        (12, "SAV-WOM", "Women's Savings", "DP", 0.035),
        (13, "SAV-KIDS", "Junior Savings", "DP", 0.045),
    ],
    "CURRENT": [
        (20, "CHK-PER", "Personal Current Account", "DP", 0.0),
        (21, "CHK-BUS", "Business Current Account", "DP", 0.0),
        (22, "CHK-PREM", "Premium Current Account", "DP", 0.01),
    ],
    "FIXED_DEPOSIT": [
        (30, "FD-3M", "3-Month Fixed Deposit", "TDEP", 0.06),
        (31, "FD-6M", "6-Month Fixed Deposit", "TDEP", 0.07),
        (32, "FD-12M", "12-Month Fixed Deposit", "TDEP", 0.08),
        (33, "FD-24M", "24-Month Fixed Deposit", "TDEP", 0.09),
    ],

    # Loan Products
    "PERSONAL_LOANS": [
        (80, "LN-PER", "Personal Loan", "LN", 0.15),
        (81, "LN-SAL", "Salary Advance", "LN", 0.12),
        (82, "LN-EDU", "Education Loan", "LN", 0.10),
        (83, "LN-MED", "Medical Loan", "LN", 0.11),
    ],
    "BUSINESS_LOANS": [
        (90, "LN-SME", "SME Business Loan", "LN", 0.16),
        (91, "LN-AGR", "Agriculture Loan", "LN", 0.13),
        (92, "LN-TRD", "Trade Finance", "LN", 0.14),
        (93, "LN-WKC", "Working Capital Loan", "LN", 0.15),
    ],
    "MICRO_LOANS": [
        (100, "MCR-IND", "Individual Micro Loan", "LN", 0.18),
        (101, "MCR-GRP", "Group Micro Loan", "LN", 0.17),
        (102, "MCR-WOM", "Women Micro Loan", "LN", 0.16),
    ],
    "MORTGAGES": [
        (110, "MTG-RES", "Residential Mortgage", "LN", 0.12),
        (111, "MTG-COM", "Commercial Mortgage", "LN", 0.13),
    ],
}

# Transaction Types and Weights (for realistic distribution)
TRANSACTION_TYPES = {
    "DEPOSITS": {
        "CASH_DEPOSIT": 0.25,  # 25% of transactions
        "TRANSFER_IN": 0.15,
        "SALARY_CREDIT": 0.05,
        "MOBILE_MONEY_IN": 0.10,
        "INTEREST_CREDIT": 0.02,
    },
    "WITHDRAWALS": {
        "CASH_WITHDRAWAL": 0.20,  # 20% of transactions
        "ATM_WITHDRAWAL": 0.15,
        "TRANSFER_OUT": 0.05,
        "MOBILE_MONEY_OUT": 0.03,
    }
}

# Channel Distribution
CHANNELS = {
    9: ("BRANCH", 0.30),  # 30% branch transactions
    81: ("API", 0.25),  # 25% API/Mobile
    7: ("MOBILE_BANKING", 0.25),  # 25% mobile banking
    6: ("ATM", 0.15),  # 15% ATM
    82: ("INTERNET_BANKING", 0.05),  # 5% internet banking
}

# GL Account Structure
GL_ACCOUNTS = {
    "CASH": [
        (-1, "1-060-00-1101", "Cash in Hand - Branch"),
        (-2, "1-060-00-1102", "Cash in Vault - Head Office"),
        (-3, "1-060-00-1103", "ATM Cash"),
    ],
    "DEPOSITS": [
        (-10, "1-060-00-2001", "Savings Deposits"),
        (-11, "1-060-00-2002", "Current Account Deposits"),
        (-12, "1-060-00-2003", "Fixed Deposits"),
    ],
    "LOANS": [
        (-20, "1-060-08-1196", "Personal Loans Receivable"),
        (-21, "1-060-08-1197", "Business Loans Receivable"),
        (-22, "1-060-08-1198", "Micro Loans Receivable"),
        (-23, "1-060-08-1199", "Mortgage Loans Receivable"),
    ],
    "INCOME": [
        (-30, "1-060-00-3001", "Interest Income - Loans"),
        (-31, "1-060-00-3002", "Fee Income"),
        (-32, "1-060-00-3003", "Commission Income"),
    ],
    "CHARGES": [
        (-40, "1-000-00-4014", "Service Charges"),
        (-41, "1-060-00-2071", "VAT Payable"),
        (-42, "1-060-00-2072", "Excise Duty Payable"),
    ],
    "MOBILE_MONEY": [
        (-50, "1-060-00-1221", "Tigo Pesa Clearing"),
        (-51, "1-060-00-1222", "M-Pesa Clearing"),
        (-52, "1-060-00-1223", "Airtel Money Clearing"),
        (-53, "1-060-00-1224", "Halopesa Clearing"),
    ]
}

# Transaction Amount Ranges (in TZS)
AMOUNT_RANGES = {
    "MICRO": (1_000, 50_000),  # Small transactions
    "SMALL": (50_000, 500_000),  # Medium transactions
    "MEDIUM": (500_000, 5_000_000),  # Large transactions
    "LARGE": (5_000_000, 50_000_000),  # Very large transactions
}

# Customer Segments
CUSTOMER_SEGMENTS = {
    "RETAIL": 0.70,  # 70% retail customers
    "SME": 0.20,  # 20% SME
    "CORPORATE": 0.05,  # 5% corporate
    "GOVERNMENT": 0.05,  # 5% government/institutional
}

# Tanzanian Names Pool
FIRST_NAMES = [
    "Ramadhan", "Asha", "Baraka", "Neema", "Godfrey", "Rose", "John", "Mary",
    "Hassan", "Fatuma", "Mohamed", "Amina", "Joseph", "Grace", "Peter", "Anna",
    "Ibrahim", "Zainab", "Said", "Halima", "Emmanuel", "Joyce", "Frank", "Lucy",
    "Rashidi", "Mariam", "Hamisi", "Sophia", "Daniel", "Elizabeth", "Michael", "Sarah",
    "Bakari", "Naomi", "Ali", "Rachel", "David", "Ruth", "James", "Esther", "Juma",
]

LAST_NAMES = [
    "Mushi", "Mhina", "Lyimo", "Massawe", "Haule", "Kikwete", "Magufuli", "Mbowe",
    "Sumaye", "Lowassa", "Mwinyi", "Mkapa", "Nyerere", "Kawawa", "Jumbe", "Karume",
    "Mwakyembe", "Ndugai", "Lema", "Lissu", "Slaa", "Makinda", "Mwakasege", "Kinana",
    "Mpango", "Ndumbaro", "Sitta", "Lukuvi", "Kairuki", "Mollel", "Merinyo", "Mrema", "Marijani", "Wakili"
]
