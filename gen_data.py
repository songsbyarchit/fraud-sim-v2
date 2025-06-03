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
num_messages = 200000
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
    {"customer_id": "C1", "customer_name": "RetailX", "vertical": "Retail", "country": "France"},
    {"customer_id": "C2", "customer_name": "HealthPro", "vertical": "Healthcare", "country": "Germany"},
    {"customer_id": "C3", "customer_name": "AutoSys", "vertical": "SaaS", "country": "USA"},
    {"customer_id": "C4", "customer_name": "LoanQuick", "vertical": "Finance", "country": "India"},
    {"customer_id": "C5", "customer_name": "EventComms", "vertical": "Media", "country": "Brazil"},
    {"customer_id": "C6", "customer_name": "VerifyMe", "vertical": "Security", "country": "United Kingdom"},
    {"customer_id": "C7", "customer_name": "PulseAlerts", "vertical": "Healthcare", "country": "Japan"}
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

    # Inject mild or sharp traffic spikes based on day
    is_christmas = current_time.month == 12 and current_time.day == 25
    is_new_year = current_time.month == 1 and current_time.day == 1
    days_to_christmas = (datetime(2024, 12, 25) - current_time).days
    days_to_new_year = (datetime(2025, 1, 1) - current_time).days

    # Baseline jitter
    base_jitter = random.gauss(mu=message_interval.total_seconds()/60, sigma=1.5)

    # Gradual spike up to holidays (more traffic + fraud)
    if 0 < days_to_christmas <= 5 or 0 < days_to_new_year <= 5:
        base_jitter *= random.uniform(0.6, 0.9)  # condense timestamps (more msgs/hour)

    # Sharp spike on the day
    if is_christmas or is_new_year:
        base_jitter *= random.uniform(0.2, 0.4)

    # Simulated surge: randomly inject bursty patterns ~2% of time
    if random.random() < 0.02:
        base_jitter *= random.uniform(0.3, 0.5)

    timestamp = current_time + timedelta(minutes=base_jitter)
    current_time = timestamp

    current_time += message_interval

    customer_pool = (
    [customers[0]] * random.randint(12, 18) +   # RetailX
    [customers[1]] * random.randint(10, 14) +   # HealthPro
    [customers[2]] * random.randint(8, 12) +    # AutoSys
    [customers[3]] * random.randint(7, 10) +    # LoanQuick
    [customers[4]] * random.randint(6, 9) +     # EventComms
    [customers[5]] * random.randint(6, 9) +     # VerifyMe
    [customers[6]] * random.randint(4, 7)       # PulseAlerts
)

    customer = random.choice(customer_pool)

    # Slightly bias fraud for RetailX near holidays
    if customer["customer_name"] == "RetailX" and (is_christmas or is_new_year or random.random() < 0.05):
        content_type = random.choices(["otp", "spam"], weights=[0.3, 0.7])[0]  # Higher spam risk

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

    sender_id = random.choice([
        fake.user_name(),
        f"{fake.country_code()}-{fake.last_name()}",
        f"{random.randint(10000, 99999)}",
        fake.company_email().split("@")[0]
    ])

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


    regex_matches = [p for p in regex_patterns if re.search(p, message_content, re.IGNORECASE)]
    regex_hit = random.choice(regex_matches) if regex_matches else None

    error_choice = random.choices(error_codes, weights=[0.05, 0.02, 0.03, 0.9])[0] if regex_hit else random.choices(error_codes, weights=[0.03, 0.01, 0.02, 0.94])[0]

    delivery_status = "Delivered" if error_choice["error_code"] == "E0" else "Failed"

    fraud_base_prob = 0.98 if is_christmas or is_new_year else 0.3
    non_regex_prob = 0.15 if is_christmas or is_new_year else 0.05

    fraud_boost = (
        random.random() < fraud_base_prob if content_type in ["otp", "spam"] and error_choice["is_regex_flag"]
        else random.random() < non_regex_prob  # allow some false negatives in regex to still be flagged
    )

    is_fraud = fraud_boost

    fraud_type = (
        random.choice(["otp_abuse", "code_misuse"]) if "OTP" in message_content else
        random.choice(["spoofed_id", "link_fraud", "brand_impersonation"]) if content_type == "spam" else
        "none"
    )

    base_rate = 0.0015 if aggregator['tier'] == "tier-2" else 0.0025
    country_multiplier = 1.3 if destination_country in ["Nigeria", "Palestine", "India"] else 1.0
    hop_multiplier = 1 + 0.05 * random.randint(0, 2)

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
        "hop_details": " -> ".join(filter(None, [
            aggregator['aggregator_name'],
            random.choice(["EU Transit", "SG Relay", "ME Hub", None]),
            f"{destination_country} Gateway"
        ])),
        "hop_details": f"{aggregator['aggregator_name']} -> {destination_country} Gateway",
        "total_cost_gbp": round(base_rate * country_multiplier * hop_multiplier, 5),
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
merged_df['hour'] = merged_df['timestamp'].dt.floor('D')

# Combined Dashboard of All Key Metrics
fig, axs = plt.subplots(3, 2, figsize=(15, 12))
fig.suptitle('Fraud, Cost, and Delivery Insights', fontsize=16)

# 1. Message Volume Over Time
merged_df.groupby('hour').size().plot(ax=axs[0, 0], title='Message Volume Over Time')
axs[0, 0].set_xlabel("Hour")
axs[0, 0].set_ylabel("Messages")
axs[0, 0].grid(True)

# 2. Fraud Volume Over Time
merged_df[merged_df['is_fraud']].groupby('hour').size().plot(ax=axs[0, 1], color='red', title='Fraud Messages Over Time')
axs[0, 1].set_xlabel("Hour")
axs[0, 1].set_ylabel("Fraud Count")
axs[0, 1].grid(True)

# 3. Error Code Breakdown
merged_df['error_code'].value_counts().plot(kind='bar', ax=axs[1, 0], title='Error Code Breakdown')
axs[1, 0].set_xlabel("Error Code")
axs[1, 0].set_ylabel("Count")
axs[1, 0].grid(True)

# 4. Avg Cost per Aggregator
merged_df.groupby('aggregator_id')['total_cost_gbp'].mean().plot(kind='bar', ax=axs[1, 1], title='Avg Cost per Aggregator')
axs[1, 1].set_xlabel("Aggregator")
axs[1, 1].set_ylabel("Avg £")
axs[1, 1].grid(True)

# 5. Message Volume by Country
merged_df['destination_country'].value_counts().plot(kind='bar', ax=axs[2, 0], title='Messages by Country')
axs[2, 0].set_xlabel("Country")
axs[2, 0].set_ylabel("Messages")
axs[2, 0].grid(True)

# 6. Fraud Split by Aggregator
merged_df[merged_df['is_fraud']].groupby('aggregator_id').size().plot(kind='pie', ax=axs[2, 1], autopct='%1.1f%%', title='Fraud by Aggregator')
axs[2, 1].set_ylabel("")

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.show()