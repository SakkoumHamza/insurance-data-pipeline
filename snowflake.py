import snowflake.connector as sf
import os
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv('SNOW_USER')
PASSWORD = os.getenv('PASSWORD')
ACCOUNT = os.getenv('ACCOUNT')

print("✅ ACCOUNT =", ACCOUNT)  # Debug check
print("✅ USER =", USER)  # Debug check
print("✅ PASS =", PASSWORD)  # Debug check

conn = sf.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse="INSURANCE_WH",
    database="INSURANCE_DB",
    schema="PUBLIC"
)

try:
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION();")
    print("✅ Connected to Snowflake successfully!")
    for row in cursor:
        print(row)

finally:
    cursor.close()
    conn.close()
