# Re-running the full setup to regenerate all required DataFrames first

import pandas as pd
import numpy as np
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import os
import re

fake = Faker()
np.random.seed(42)
random.seed(42)

# Constants
num_messages = 200_000
start_date = datetime(2024, 11, 15)
end_date = datetime(2025, 1, 15)
message_interval = (end_date - start_date) / num_messages

# Lookup tables
aggregators = [
    {"aggregator_id": "AGG1", "aggregator_name": "JPN_Agg1", "country": "Japan", "tier": "tier-1", "trust_score": 0.95},
    {"aggregator_id": "AGG2", "aggregator_name": "UK_Agg1", "country": "United Kingdom", "tier": "tier-1", "trust_score": 0.90},
    {"aggregator_id": "AGG3", "aggregator_name": "IMImobile", "country": "United Kingdom", "tier": "tier-2", "trust_score": 0.98},
]

customers = [
    {"customer_id": "C1", "customer_name": "Webex Connect", "vertical": "SaaS", "country": "United Kingdom"},
    {"customer_id": "C2", "customer_name": "RetailX", "vertical": "Retail", "country": "France"},
    {"customer_id": "C3", "customer_name": "HealthPro", "vertical": "Healthcare", "country": "Germany"},
]

error_codes = [
    {"error_code": "E100", "description": "Temporary failure", "is_regex_flag": False, "severity": "low"},
    {"error_code": "E403", "description": "Regex content spam", "is_regex_flag": True, "severity": "high"},
    {"error_code": "E500", "description": "Blocked by aggregator", "is_regex_flag": False, "severity": "medium"},
    {"error_code": "E0", "description": "No error", "is_regex_flag": False, "severity": "low"},
]

regex_patterns = [
    r"\bOTP\b",
    r"\b\d{6}\b",  # numeric OTP
    r"(free|win|gift|urgent|click here|act now)",
    r"http[s]?://",  # suspicious links
    r"\b[A-Z]{4,}\b"  # all-caps spammy words
]

# Generate messages and billing metadata
messages = []
billing_metadata = []
current_time = start_date

for i in range(num_messages):
    message_id = str(uuid.uuid4())
    timestamp = current_time
    current_time += message_interval

    customer_pool = (
        [customers[0]] * random.randint(15, 22) +  # Webex Connect
        [customers[1]] * random.randint(8, 12) +   # RetailX
        [customers[2]] * random.randint(5, 10)     # HealthPro
    )
    customer = random.choice(customer_pool)

    aggregator_pool = (
        [aggregators[2]] * random.randint(15, 20) +  # IMImobile (most used)
        [aggregators[0]] * random.randint(8, 12) +   # JPN_Agg1
        [aggregators[1]] * random.randint(6, 10)     # UK_Agg1
    )
    aggregator = random.choice(aggregator_pool)

    country_pool = (
        ["United Kingdom"] * random.randint(18, 25) +
        ["USA"] * random.randint(12, 18) +
        ["Japan"] * random.randint(10, 15) +
        ["Germany"] * random.randint(8, 12) +
        ["India"] * random.randint(8, 12) +
        ["Brazil"] * random.randint(6, 10) +
        ["Palestine"] * random.randint(5, 8) +
        ["Nigeria"] * random.randint(3, 6)
    )
    destination_country = random.choice(country_pool)
    sender_id = fake.user_name()
    content_type = random.choices(["normal", "otp", "spam"], weights=[0.85, 0.1, 0.05])[0]

    channel_pool = (
        ["SMS"] * random.randint(60, 75) +
        ["WhatsApp"] * random.randint(15, 25) +
        ["RCS"] * random.randint(5, 15)
    )
    channel_type = random.choice(channel_pool)


    if content_type == "otp":
        otp_templates = [
            f"Your OTP is {random.randint(100000, 999999)}",
            f"Use {random.randint(100000, 999999)} to verify your login",
            f"Security code: {random.randint(100000, 999999)}. Do not share it."
        ]
        message_content = random.choice(otp_templates)

    elif content_type == "spam":
        spam_templates = [
            "WIN A FREE GIFT NOW!",
            "Click here to claim your reward",
            "ACT NOW: Get exclusive access!",
            "Visit http://offers-secure.com/claim",
            "Check your reward: http://now-win.biz",
            "Click https://login-alert.net to confirm",
            "GIFT ALERT: You're selected. Click now!"
        ]

        message_content = random.choice(spam_templates)

    else:
        message_content = fake.sentence()


    regex_hit = None
    for pattern in regex_patterns:
        if re.search(pattern, message_content, re.IGNORECASE):
            regex_hit = pattern
            break

    error_choice = random.choices(error_codes, weights=[0.05, 0.02, 0.03, 0.9])[0] if regex_hit else random.choices(error_codes, weights=[0.03, 0.01, 0.02, 0.94])[0]

    delivery_status = "Delivered" if error_choice["error_code"] == "E0" else "Failed"

    is_fraud = (
        content_type in ["otp", "spam"] and error_choice["is_regex_flag"]
        and random.random() < random.uniform(0.8, 0.95)  # Adds unpredictability
    )

    fraud_type = "otp_abuse" if "OTP" in message_content else ("spoofed_id" if content_type == "spam" else "none")

    messages.append({
        "message_id": message_id,
        "customer_id": customer["customer_id"],
        "timestamp": timestamp,
        "mobile_number": fake.phone_number(),
        "destination_country": destination_country,
        "sender_id": sender_id,
        "message_content": message_content,
        "regex_pattern_match": regex_hit,
        "error_code": error_choice["error_code"],
        "delivery_status": delivery_status,
        "is_fraud": is_fraud,
        "fraud_type": fraud_type if is_fraud else "none",
        "channel_type": channel_type
    })

    billing_metadata.append({
        "message_id": message_id,
        "aggregator_id": aggregator["aggregator_id"],
        "route_hops": random.randint(1, 3),
        "hop_details": f"{aggregator['aggregator_name']} -> {destination_country} Gateway",
        "total_cost_gbp": round(random.uniform(0.0015, 0.0050), 5),
        "direct_route": random.choice([True, False]),
        "fraud_reported_by_agg": is_fraud and random.choice([True, False]),
        "aggregator_error_code": error_choice["error_code"] if delivery_status == "Failed" else None
    })

# Convert to DataFrames
messages_df = pd.DataFrame(messages)
billing_df = pd.DataFrame(billing_metadata)
aggregators_df = pd.DataFrame(aggregators)
customers_df = pd.DataFrame(customers)
error_codes_df = pd.DataFrame(error_codes)

# Save as CSVs
os.makedirs("synthetic_data", exist_ok=True)
messages_df.to_csv("synthetic_data/messages.csv", index=False)
billing_df.to_csv("synthetic_data/billing_metadata.csv", index=False)
aggregators_df.to_csv("synthetic_data/aggregators.csv", index=False)
customers_df.to_csv("synthetic_data/customers.csv", index=False)
error_codes_df.to_csv("synthetic_data/error_codes.csv", index=False)

# Prepare for visualisation
merged_df = pd.merge(messages_df, billing_df, on="message_id", how="inner")
merged_df['hour'] = merged_df['timestamp'].dt.floor('H')

# 2. Fraud Volume Over Time
plt.figure()
merged_df[merged_df['is_fraud']].groupby('hour').size().plot(title='Fraud Messages Over Time', color='red')
plt.xlabel("Hour")
plt.ylabel("Fraud Messages")
plt.grid(True)
plt.tight_layout()
plt.show()

# 3. Error Code Breakdown
plt.figure()
merged_df['error_code'].value_counts().plot(kind='bar', title='Error Code Breakdown')
plt.xlabel("Error Code")
plt.ylabel("Count")
plt.grid(True)
plt.tight_layout()
plt.show()

# 4. Cost per Aggregator
plt.figure()
merged_df.groupby('aggregator_id')['total_cost_gbp'].mean().plot(kind='bar', title='Avg Cost per Aggregator')
plt.xlabel("Aggregator")
plt.ylabel("Avg Cost (£)")
plt.grid(True)
plt.tight_layout()
plt.show()

# 5. Message Volume by Country
plt.figure()
merged_df['destination_country'].value_counts().plot(kind='bar', title='Message Volume by Country')
plt.xlabel("Country")
plt.ylabel("Messages")
plt.grid(True)
plt.tight_layout()
plt.show()

# 6. Fraud Share by Aggregator
plt.figure()
fraud_split = merged_df[merged_df['is_fraud']].groupby('aggregator_id').size()
fraud_split.plot(kind='pie', autopct='%1.1f%%', title='Fraud Share by Aggregator')
plt.ylabel("")
plt.tight_layout()
plt.show()