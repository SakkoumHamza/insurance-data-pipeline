import snowflake.connector as sf
import os
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv('SNOW_USER')
PASSWORD = os.getenv('PASSWORD')
ACCOUNT = os.getenv('ACCOUNT')

print("✅ ACCOUNT =", ACCOUNT)
print("✅ USER =", USER)

conn = sf.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse="INSURANCE_WH",
    database="INSURANCE_DB",
    schema="PUBLIC",
    role="ACCOUNTADMIN"  # You can change to SYSADMIN if limited access
)

cursor = conn.cursor()

print("🔗 Connected to Snowflake")

# --- Step 1: Create role if not exists
cursor.execute("""
CREATE ROLE IF NOT EXISTS AIRBYTE_ROLE;
""")

# --- Step 2: Create warehouse
cursor.execute("""
CREATE WAREHOUSE IF NOT EXISTS INSURANCE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
""")

# --- Step 3: Create database
cursor.execute("CREATE DATABASE IF NOT EXISTS INSURANCE_DB;")

# --- Step 4: Create schemas
cursor.execute("CREATE SCHEMA IF NOT EXISTS INSURANCE_DB.RAW ;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS INSURANCE_DB.ANALYTICS;")

# --- Step 5: Create user if not exists
cursor.execute(f"""
CREATE USER IF NOT EXISTS AIRBYTE_USER
  PASSWORD = '{PASSWORD}'
  DEFAULT_ROLE = AIRBYTE_ROLE
  DEFAULT_WAREHOUSE = INSURANCE_WH
  DEFAULT_NAMESPACE = INSURANCE_DB.RAW
  MUST_CHANGE_PASSWORD = FALSE;
""")

# --- Step 6: Grants
grants = [
    # Warehouse
    "GRANT USAGE, OPERATE ON WAREHOUSE INSURANCE_WH TO ROLE AIRBYTE_ROLE;",

    # Database
    "GRANT USAGE ON DATABASE INSURANCE_DB TO ROLE AIRBYTE_ROLE;",

    # Schema
    "GRANT USAGE, CREATE TABLE ON SCHEMA INSURANCE_DB.RAW TO ROLE AIRBYTE_ROLE;",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA INSURANCE_DB.RAW TO ROLE AIRBYTE_ROLE;",
    "GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA INSURANCE_DB.RAW TO ROLE AIRBYTE_ROLE;",

    # (Optional - only if you have auto-increment IDs)
    "GRANT USAGE ON ALL SEQUENCES IN SCHEMA INSURANCE_DB.RAW TO ROLE AIRBYTE_ROLE;",
    "GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA INSURANCE_DB.RAW TO ROLE AIRBYTE_ROLE;",

    # Role assignment
    "GRANT ROLE AIRBYTE_ROLE TO USER AIRBYTE_USER;"
]


for grant in grants:
    cursor.execute(grant)

print("✅ Snowflake setup completed successfully!")

cursor.close()
conn.close()
