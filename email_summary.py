import smtplib
from openai import OpenAI
from dotenv import load_dotenv
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from datetime import datetime, timedelta
import re

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

    # Cost calculations
    today_cost = today_data["total_cost_gbp"].sum()
    yesterday_cost = yesterday_data["total_cost_gbp"].sum()
    day_cost_change = ((today_cost - yesterday_cost) / max(yesterday_cost, 1)) * 100

    this_week_cost = this_week["total_cost_gbp"].sum()
    prev_week_cost = prev_week["total_cost_gbp"].sum()
    week_cost_change = ((this_week_cost - prev_week_cost) / max(prev_week_cost, 1)) * 100

    # Cost split by category
    agg_costs = today_data.groupby("aggregator_id")["total_cost_gbp"].sum().sort_values(ascending=False)
    country_costs = today_data.groupby("destination_country")["total_cost_gbp"].sum().sort_values(ascending=False)
    channel_costs = today_data.groupby("channel_type")["total_cost_gbp"].sum().sort_values(ascending=False)

    # Daily avg cost breakdown
    avg_costs = today_data.groupby(["aggregator_id", "destination_country", "channel_type"])["total_cost_gbp"].mean().reset_index()
    avg_costs.columns = ["aggregator_id", "destination_country", "channel_type", "avg_cost_gbp"]
    avg_costs = avg_costs.sort_values("avg_cost_gbp", ascending=False)

    yest_agg = yesterday_data.groupby("aggregator_id")["total_cost_gbp"].sum()
    today_agg = agg_costs
    agg_delta = ((today_agg - yest_agg) / yest_agg.replace(0, 1)).sort_values(ascending=False)

    # Format readable date
    date_str = today.strftime('%b %-d, %Y')

    # Construct OpenAI prompt
    prompt = f"""
    
    You are a clear, human-sounding analyst writing for other analysts. Vary tone based on the data: highlight sharp spikes, call out stability, and use natural phrasing like “eased off” or “held steady” where fitting. Avoid sounding robotic.

    Using the following fraud metrics for {date_str}, write exactly 2 bullet points under each of the 3 sections below:

    - Country Trends
    - Aggregator Trends
    - Channel Trends

    Round all numbers to 2 decimal places, but do not pad (e.g. 2.4 is fine, don’t force 2.40).
    Each bullet point must be concise (30–40 words), data-backed, and consistent in length. Where appropriate, include suggested next steps or actions (e.g. “Review traffic from X”, “Escalate to risk team”).
    - Make section titles bold using <b>.

    Use exactly one <br> after each bullet point.
    Use exactly one <br><br> between sections.
    Do not write section headers inside bullet lines.

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
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    summary = response.choices[0].message.content.strip()

    # Enforce clean HTML formatting: section titles must be isolated, followed by exactly 2 bullet points with <br>
    # 1. Add line breaks after section titles if missing
    summary = re.sub(r'(<b>.*?</b>)(?!<br><br>)', r'\1<br><br>', summary)

    # 2. Remove any section headers inside bullet lines
    summary = re.sub(r'- .*?(Country|Aggregator|Channel) (Trends|Cost Trends):', '-', summary, flags=re.IGNORECASE)

    # 3. Collapse excessive <br>
    summary = re.sub(r'(<br>\s*){3,}', '<br><br>', summary)

    # 4. Trim trailing breaks
    summary = re.sub(r'(<br>\s*)+$', '', summary.strip())

    # --- Cost-focused prompt ---
    cost_prompt = f"""

    You are a financial analyst writing a clear, natural internal summary. Vary tone with the data: highlight major cost swings, use phrases like “held flat” or “climbed steeply” where appropriate. Avoid robotic language.

    Using the following cost data for {date_str}, write exactly 2 bullet points under each of the 3 sections:

    - Country Cost Trends
    - Aggregator Cost Trends
    - Channel Cost Trends

    Round all numbers to 2 decimal places, but do not pad (e.g. 2.4 is fine, don’t force 2.40).
    Each bullet point must be concise (30–40 words), data-backed, and consistent in length. Where appropriate, include suggested next steps or actions (e.g. “Review traffic from X”, “Escalate to risk team”).
    - Section titles in <b>

    Use exactly one <br> after each bullet point.
    Use exactly one <br><br> between sections.
    Do not write section headers inside bullet lines.

    Metrics:
    - Day-on-day cost % change: {round(day_cost_change, 2)}%
    - Week-on-week cost % change: {round(week_cost_change, 2)}%
    - Top aggregators by cost: {agg_costs.head(3).to_dict()}
    - Country cost leaders: {country_costs.head(3).to_dict()}
    - Channel cost leaders: {channel_costs.head(3).to_dict()}
    - Highest avg cost breakdown: {avg_costs.head(2).to_dict()}
    - Aggregator deltas: {agg_delta.head(2).to_dict()}
    """

    cost_response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": cost_prompt}]
    )

    cost_summary = cost_response.choices[0].message.content.strip()

    # Enforce clean HTML formatting: section titles must be on their own, followed by bullets only
    cost_summary = re.sub(r'(<b>.*?</b>)(?!<br><br>)', r'\1<br><br>', cost_summary)

    # Remove section headers incorrectly placed inside bullet lines
    cost_summary = re.sub(r'- .*?(Country|Aggregator|Channel) (Cost )?Trends:?', '-', cost_summary, flags=re.IGNORECASE)

    # Collapse excessive line breaks
    cost_summary = re.sub(r'(<br>\s*){3,}', '<br><br>', cost_summary)

    # Remove trailing breaks
    cost_summary = re.sub(r'(<br>\s*)+$', '', cost_summary.strip())

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
    <a href="https://lookerstudio.google.com/u/0/reporting/93c5bbb9-ae27-4e95-8deb-080188c075bc/page/lKxMF">View in Looker Studio</a></p>

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

    with open("email_summaries.txt", "a", encoding="utf-8") as f:
        f.write(f"--- {date_str} Fraud Summary ---\n")
        f.write(f"{summary}\n\n")

    cost_html = f"""
    <html>
    <body>
    <p><b>💷 {date_str} Financial Summary:</b></p>
    <ul>
    <li><b>Cost % change (vs yesterday):</b> {round(day_cost_change, 2)}%</li>
    <li><b>Cost % change (vs last week):</b> {round(week_cost_change, 2)}%</li>
    </ul>

    {cost_summary}

    <p><b>🔗 Full dashboard:</b><br>
    <a href="https://lookerstudio.google.com/u/0/reporting/93c5bbb9-ae27-4e95-8deb-080188c075bc/page/p_y847pqq4sd">View in Looker Studio</a></p>

    <p style="font-size:12px; color:gray;"><i>This summary was AI-generated. Please verify insights before making financial decisions.</i></p>
    </body>
    </html>
    """

    cost_msg = MIMEMultipart("alternative")
    cost_msg["From"] = EMAIL
    cost_msg["To"] = "archit.sachdeva007@gmail.com"
    cost_msg["Subject"] = f"Financial Daily Report — {date_str}"
    cost_msg.attach(MIMEText(cost_html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL, PASSWORD)
        server.send_message(msg)
        server.send_message(cost_msg)

    print(f"✅ Both emails sent for {date_str}")

    with open("cost_summaries.txt", "a", encoding="utf-8") as f:
        f.write(f"--- {date_str} Financial Summary ---\n")
        f.write(f"{cost_summary}\n\n")