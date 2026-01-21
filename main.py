#!/usr/bin/env python
# coding: utf-8

# In[3]:


import os
import json
import difflib
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import pandas as pd

# --------------------------------------
# CONFIG
# --------------------------------------

URLS = {
    "COMPET_DYC": "https://www.dynacare.ca/",
    "COMPET_AHL": "https://alphalabs.ca/",
    "COMPET_MOW": "https://medlabsofwindsor.com/",
    "COMPET_MHL": "https://www.mhlab.ca/",
    "COMPET_SWH": "https://switchhealth.ca/",
    "COMPET_BIO": "https://bio-test.ca/",
    # "COMPET_LL": "https://www.lifelabs.com/",
    "SOB": "https://www.ontario.ca/page/ohip-schedule-benefits-and-fees",
    "NEWS_1": "https://news.ontario.ca/moh/en",
    "NEWS_2": "https://gov.on.ca",
    "NEWS_3": "https://www.ontario.ca/document/ohip-infobulletins-2025",
    "NEWS_3_2026": "https://www.ontario.ca/document/ohip-infobulletins-2026",
    "NEWS_4": "https://www.regulatoryregistry.gov.on.ca/",
    "NEWS_5": "https://www.ontario.ca/laws/regulation/900552",
    # "NEWS_6": "https://www.ontariohealth.ca/news.html",
    "ONT_1": "https://www.ontario.ca/page/ontarios-primary-care-action-plan-1-year-progress-update",
}

SNAPSHOT_CSV = "snapshot_data.csv"
SUMMARY_CSV = "Alarm_url_changes.csv"
DIFF_ARCHIVE_CSV = "Alarm_url_diff_archive.csv"

TIMEOUT = 30

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

os.makedirs(os.path.dirname(SNAPSHOT_CSV) or ".", exist_ok=True)

print(f"Loaded {len(URLS)} URLs")

# --------------------------------------
# URL GUARD (prevents urldefense / sendgrid tracking links)
# --------------------------------------

def validate_urls(urls: dict):
    bad = []
    for name, url in urls.items():
        u = (url or "").lower()
        if "urldefense.com" in u or "ct.sendgrid.net/ls/click" in u:
            bad.append((name, url))
    if bad:
        print("BAD URLS FOUND. Replace with the real website URLs:")
        for name, url in bad:
            print(" -", name, url)
        raise SystemExit(2)

validate_urls(URLS)

# --------------------------------------
# DISCORD ALERTS (Webhook)
# --------------------------------------

# Set this in Railway (or your env):
# DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/....
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
#DISCORD_WEBHOOK_URL = os.getenv("https://discord.com/api/webhooks/1463375488851378403/vnqZ8AVDmGT_6jz8Q9-yAyxWjjPIv3KcLEyfhZn6gQt0NcvxrnmvYqVMNz5VmNRcNP7b", "").strip()

def send_discord_webhook(message: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        print("Discord skipped. Missing DISCORD_WEBHOOK_URL.")
        return False

    payload = {"content": message[:1900]}  # keep under Discord limits
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=30)
        if r.status_code >= 400:
            print(f"Discord failed {r.status_code}: {r.text[:800]}")
            return False
        print("Discord sent ok")
        return True
    except Exception as e:
        print(f"Discord exception: {e}")
        return False


def safe_url(u: str) -> str:
    u = str(u or "")
    return u.replace("https://", "hxxps://").replace("http://", "hxxp://")


def build_discord_message(df_summary_run, df_diff_archive_run, run_time_str):
    changed = df_summary_run[df_summary_run["change_flag"] == "CHANGED"]
    errored = df_summary_run[df_summary_run["change_flag"] == "ERROR"]

    lines = []
    lines.append(f"URL Monitor Alert {run_time_str}")
    lines.append(f"Changed: {len(changed)} | Errors: {len(errored)}")
    lines.append("")

    if len(changed) > 0:
        lines.append("CHANGED")
        for _, row in changed.iterrows():
            lines.append(
                f"- {row['alarm_name']} changes={row['change_count']} url={safe_url(row['url'])}"
            )
        lines.append("")

    if len(errored) > 0:
        lines.append("ERROR")
        for _, row in errored.iterrows():
            lines.append(
                f"- {row['alarm_name']} error={row['change_count']} url={safe_url(row['url'])}"
            )
        lines.append("")

    # Optional: include 1 diff excerpt per changed alarm, trimmed
    if (len(changed) > 0) and (len(df_diff_archive_run) > 0):
        lines.append("DIFF EXCERPTS (trimmed)")
        for alarm in changed["alarm_name"].tolist():
            sub = df_diff_archive_run[df_diff_archive_run["alarm_name"] == alarm].tail(1)
            if sub.empty:
                continue
            before_txt = str(sub["before"].values[0])[:600]
            after_txt = str(sub["after"].values[0])[:600]
            lines.append(f"Alarm: {alarm}")
            lines.append("Before:")
            lines.append(before_txt)
            lines.append("After:")
            lines.append(after_txt)
            lines.append("")

    return "\n".join(lines)

# --------------------------------------
# EMAIL ALERTS (SendGrid)
# --------------------------------------

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "").strip()
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "").strip()

ALERT_TO_EMAILS_RAW = os.getenv("ALERT_TO_EMAILS", "").strip()
if not ALERT_TO_EMAILS_RAW:
    ALERT_TO_EMAILS_RAW = os.getenv("ALERT_TO_EMAIL", "").strip()

ALERT_TO_EMAILS = [e.strip() for e in ALERT_TO_EMAILS_RAW.split(",") if e.strip()]

def send_sendgrid_email(subject, body_text):
    if not SENDGRID_API_KEY or not ALERT_FROM_EMAIL or not ALERT_TO_EMAILS:
        print("Email skipped. Missing SendGrid configuration.")
        return False

    payload = {
        "personalizations": [{"to": [{"email": e} for e in ALERT_TO_EMAILS]}],
        "from": {"email": ALERT_FROM_EMAIL},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}],
    }

    try:
        r = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=30,
        )

        if r.status_code >= 400:
            print(f"SendGrid failed {r.status_code}: {r.text[:800]}")
            return False

        print("SendGrid sent ok:", subject)
        return True

    except Exception as e:
        print(f"SendGrid exception: {e}")
        return False


def build_email_body(df_summary_run, df_diff_archive_run, run_time_str):
    lines = []
    lines.append(f"Run time: {run_time_str}")
    lines.append("")

    changed = df_summary_run[df_summary_run["change_flag"] == "CHANGED"]
    errored = df_summary_run[df_summary_run["change_flag"] == "ERROR"]

    lines.append(f"Changed alarms: {len(changed)}")
    lines.append(f"Errored alarms: {len(errored)}")
    lines.append("")

    if not changed.empty:
        lines.append("CHANGED")
        for _, row in changed.iterrows():
            lines.append(
                f"- {row['alarm_name']}  changes={row['change_count']}  url={safe_url(row['url'])}"
            )
        lines.append("")

    if not errored.empty:
        lines.append("ERROR")
        for _, row in errored.iterrows():
            lines.append(
                f"- {row['alarm_name']}  error={row['change_count']}  url={safe_url(row['url'])}"
            )
        lines.append("")

    if not df_diff_archive_run.empty and not changed.empty:
        lines.append("DIFF EXCERPTS")
        for alarm in changed["alarm_name"].tolist():
            sub = df_diff_archive_run[df_diff_archive_run["alarm_name"] == alarm].tail(1)
            if sub.empty:
                continue

            before_txt = str(sub["before"].values[0])[:1500]
            after_txt = str(sub["after"].values[0])[:1500]

            lines.append(f"Alarm: {alarm}")
            lines.append("Before (excerpt):")
            lines.append(before_txt)
            lines.append("")
            lines.append("After (excerpt):")
            lines.append(after_txt)
            lines.append("")
            lines.append("----------------------------------------")

    lines.append("")
    lines.append("Files written:")
    lines.append(f"- {SUMMARY_CSV}")
    lines.append(f"- {DIFF_ARCHIVE_CSV}")
    lines.append(f"- {SNAPSHOT_CSV}")

    return "\n".join(lines)

# --------------------------------------
# HTTP SESSION
# --------------------------------------

def build_session():
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

session = build_session()

# --------------------------------------
# HELPERS
# --------------------------------------

def normalize_content(raw_text):
    raw_text = (raw_text or "").strip()

    try:
        return json.loads(raw_text)
    except Exception:
        pass

    soup = BeautifulSoup(raw_text, "html.parser")
    lines = [l.strip() for l in soup.get_text("\n").splitlines() if l.strip()]
    return "\n".join(lines)


def diff_to_rows(before, after, max_field_len=4000):
    rows = []
    before_lines = (before or "").splitlines()
    after_lines = (after or "").splitlines()

    line_no = 0
    before_buf = None

    for d in difflib.ndiff(before_lines, after_lines):
        code = d[0]
        text = d[2:]

        if code == " ":
            line_no += 1
            before_buf = None
        elif code == "-":
            before_buf = text
        elif code == "+":
            b = before_buf or ""
            rows.append({
                "line_no": line_no,
                "before": b[:max_field_len],
                "after": text[:max_field_len],
            })
            before_buf = None

    return rows


def archive_url(url: str) -> str:
    return "https://web.archive.org/web/0/" + url


def fetch_text(url):
    resp = session.get(url, timeout=TIMEOUT)

    if resp is None:
        raise RuntimeError("No response")

    if resp.status_code == 403:
        print("403 blocked. Trying archive:", url)
        ar = requests.get(archive_url(url), headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        ar.raise_for_status()
        return ar.text

    resp.raise_for_status()
    return resp.text

# --------------------------------------
# DATAFRAMES
# --------------------------------------

df_snapshot = pd.DataFrame(columns=["run_time", "alarm_name", "url", "content"])
df_summary = pd.DataFrame(columns=["run_time", "alarm_name", "url", "change_flag", "change_count"])
df_diff_archive = pd.DataFrame(columns=["run_time", "alarm_name", "url", "line_no", "before", "after"])

if os.path.isfile(SNAPSHOT_CSV):
    df_snapshot = pd.read_csv(SNAPSHOT_CSV)

# --------------------------------------
# MAIN LOOP
# --------------------------------------

run_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for alarm, url in URLS.items():
    try:
        raw_text = fetch_text(url)
        current_norm = normalize_content(raw_text)

        prev_row = df_snapshot[df_snapshot["alarm_name"] == alarm].tail(1)
        prev_text = None
        if not prev_row.empty:
            prev_text = prev_row["content"].values[0]

        if prev_text is None:
            change_flag = "FIRST_RUN"
            change_count = 0
            changes = []
        else:
            if str(prev_text) != str(current_norm):
                changes = diff_to_rows(str(prev_text), str(current_norm))
                change_flag = "CHANGED"
                change_count = len(changes)
            else:
                change_flag = "NO_CHANGE"
                change_count = 0
                changes = []

        df_snapshot = pd.concat(
            [
                df_snapshot,
                pd.DataFrame([{
                    "run_time": run_time_str,
                    "alarm_name": alarm,
                    "url": url,
                    "content": str(current_norm),
                }]),
            ],
            ignore_index=True,
        )

        df_summary = pd.concat(
            [
                df_summary,
                pd.DataFrame([{
                    "run_time": run_time_str,
                    "alarm_name": alarm,
                    "url": url,
                    "change_flag": change_flag,
                    "change_count": change_count,
                }]),
            ],
            ignore_index=True,
        )

        if changes:
            df_diff_archive = pd.concat(
                [
                    df_diff_archive,
                    pd.DataFrame([{
                        "run_time": run_time_str,
                        "alarm_name": alarm,
                        "url": url,
                        "line_no": None,
                        "before": "\n".join(c["before"] for c in changes),
                        "after": "\n".join(c["after"] for c in changes),
                    }]),
                ],
                ignore_index=True,
            )

        print(f"{alarm}: {change_flag} ({change_count})")

    except Exception as e:
        df_summary = pd.concat(
            [
                df_summary,
                pd.DataFrame([{
                    "run_time": run_time_str,
                    "alarm_name": alarm,
                    "url": url,
                    "change_flag": "ERROR",
                    "change_count": str(e),
                }]),
            ],
            ignore_index=True,
        )
        print(f"{alarm}: ERROR {e}")

# --------------------------------------
# SAVE FILES
# --------------------------------------

df_snapshot.to_csv(SNAPSHOT_CSV, index=False)
df_summary.to_csv(SUMMARY_CSV, index=False)
df_diff_archive.to_csv(DIFF_ARCHIVE_CSV, index=False)

# --------------------------------------
# SEND ALERTS (Email + Discord)
# --------------------------------------

df_summary_run = df_summary[df_summary["run_time"] == run_time_str]
df_diff_archive_run = df_diff_archive[df_diff_archive["run_time"] == run_time_str]

has_changed = (df_summary_run["change_flag"] == "CHANGED").any()
has_error = (df_summary_run["change_flag"] == "ERROR").any()

if has_changed or has_error:
    subject = f"URL Monitor Alert {run_time_str}"

    # Email
    body = build_email_body(df_summary_run, df_diff_archive_run, run_time_str)
    send_sendgrid_email(subject, body)

    # Discord
    discord_msg = build_discord_message(df_summary_run, df_diff_archive_run, run_time_str)
    send_discord_webhook(discord_msg)
else:
    print("No changes, no alerts")

