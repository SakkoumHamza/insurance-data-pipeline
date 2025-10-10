from faker import Faker
import random, time , pymongo, os
from dotenv import load_dotenv

fake = Faker()
load_dotenv()
# Connect to MongoDB Atlas
client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = client["insurance"]
customers_col = db["customers"]
claims_col = db["claims"]

while True:
    customer = {
        "customer_id": f"CUST-{random.randint(100,999)}",
        "name": fake.name(),
        "state": fake.state_abbr(),
        "policy_type": random.choice(["Auto", "Home", "Health"])
    }

    claim = {
        "claim_id": f"CLM-{random.randint(1000,9999)}",
        "customer_id": customer["customer_id"],
        "date": fake.date_between(start_date='-1y', end_date='today').isoformat(),
        "amount": round(random.uniform(100, 20000), 2),
        "claim_type": random.choice(["Accident", "Theft", "Fire"]),
        "is_fraud": random.random() < 0.05
    }

    # Insert into MongoDB
    customers_col.insert_one(customer)
    claims_col.insert_one(claim)

    print(f"Inserted claim {claim['claim_id']} for customer {customer['customer_id']}")

    time.sleep(2)