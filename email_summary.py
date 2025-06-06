import smtplib
from openai import OpenAI
from dotenv import load_dotenv
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from datetime import datetime, timedelta

# Load env
load_dotenv()
EMAIL = os.getenv("EMAIL_ADDRESS")
PASSWORD = os.getenv("EMAIL_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Load data
messages = pd.read_csv("synthetic_data/messages.csv", parse_dates=["timestamp"])
billing = pd.read_csv("synthetic_data/billing_metadata.csv")
df = pd.merge(messages, billing, on="message_id")
df["date"] = df["timestamp"].dt.date

# Loop through each day
start_date = datetime(2024, 12, 15).date()
end_date = datetime(2025, 1, 15).date()

for today in pd.date_range(start=start_date, end=end_date):
    today = today.date()
    yesterday = today - timedelta(days=1)
    week_start = today - timedelta(days=6)
    prev_week_start = today - timedelta(days=13)
    prev_week_end = today - timedelta(days=7)

    # Metric calcs
    today_data = df[df["date"] == today]
    yesterday_data = df[df["date"] == yesterday]

    today_fraud = today_data["is_fraud"].sum()
    yesterday_fraud = yesterday_data["is_fraud"].sum()
    day_fraud_change = ((today_fraud - yesterday_fraud) / max(yesterday_fraud, 1)) * 100

    today_volume = len(today_data)
    yesterday_volume = len(yesterday_data)
    day_volume_change = ((today_volume - yesterday_volume) / max(yesterday_volume, 1)) * 100

    this_week = df[(df["date"] >= week_start) & (df["date"] <= today)]
    prev_week = df[(df["date"] >= prev_week_start) & (df["date"] <= prev_week_end)]
    week_fraud_change = ((this_week["is_fraud"].sum() - prev_week["is_fraud"].sum()) / max(prev_week["is_fraud"].sum(), 1)) * 100

    # Trends
    top_countries = today_data[today_data["is_fraud"]].groupby("destination_country").size().sort_values(ascending=False)
    country_trend = f"Top countries: {top_countries.head(2).index.tolist()}"

    agg = today_data[today_data["is_fraud"]].groupby("aggregator_id").size().sort_values(ascending=False)
    agg_trend = f"Most flagged aggregator: {agg.idxmax()} with {agg.max()} cases." if not agg.empty else "No fraud cases."

    channel = today_data[today_data["is_fraud"]].groupby("channel_type").size().sort_values(ascending=False)
    channel_summary = ", ".join([f"{k}: {v}" for k, v in channel.items()])
    top_channel = channel.idxmax() if not channel.empty else "N/A"

    # Format readable date
    date_str = today.strftime('%b %-d, %Y')

    # Construct OpenAI prompt
    prompt = f"""
    
    You are a expert data analyst  who simplifies insights.

    Using the following fraud metrics for {date_str}, write 2 bullet points under each of the 3 sections below:

    - Country Trends
    - Aggregator Trends
    - Channel Trends

    Each bullet point should be one clear technical insight. Use the EXACT HTML formatting below:
    - Make section titles bold using <b>.
    - Use <br> to separate bullets.
    - Use <br><br> only between sections, not after every bullet.


    Metrics:
    - Day-on-day fraud % change: {round(day_fraud_change, 2)}%
    - Day-on-day volume % change: {round(day_volume_change, 2)}%
    - Week-on-week fraud % change: {round(week_fraud_change, 2)}%
    - Country trend: {country_trend}
    - Aggregator trend: {agg_trend}
    - Channel volumes: {channel_summary}
    - Top channel: {top_channel}
    """

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )

    summary = response.choices[0].message.content.strip()
    
    # Build HTML email
    html = f"""
    <html>
    <body>
    <p><b>📊 {date_str} Fraud Summary:</b></p>
    <ul>
    <li><b>Fraud % change (vs yesterday):</b> {round(day_fraud_change, 2)}%</li>
    <li><b>Message volume change (vs yesterday):</b> {round(day_volume_change, 2)}%</li>
    </ul>

    {summary}

    <p><b>🔗 Full dashboard:</b><br>
    <a href="https://lookerstudio.google.com/reporting/93c5bbb9-ae27-4e95-8deb-080188c075bc">View in Looker Studio</a></p>

    <p style="font-size:12px; color:gray;"><i>This summary was AI-generated. Please verify insights before making operational decisions.</i></p>
    <p style="font-size:12px; color:gray;"><i>💬 Got feedback? <a href="https://forms.gle/cUjxdvzGh2NY4jB79" target="_blank">Leave it here</a></i></p>

    </body>
    </html>
    """

    # Setup and send email
    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL
    msg["To"] = "archit.sachdeva007@gmail.com"
    msg["Subject"] = f"Fraud Analyst Daily Report — {date_str}"
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL, PASSWORD)
        server.send_message(msg)

    print(f"✅ Email sent for {date_str}")

    with open("email_summaries.txt", "a", encoding="utf-8") as f:
        f.write(f"--- {date_str} Fraud Summary ---\n")
        f.write(f"{summary}\n\n")
