#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
     "COMPET_LL": "https://www.lifelabs.com",
    "SOB": "https://www.ontario.ca/page/ohip-schedule-benefits-and-fees",
    "NEWS_1": "https://news.ontario.ca/moh/en",
    "NEWS_2": "https://gov.on.ca",
    "NEWS_3": "https://www.ontario.ca/document/ohip-infobulletins-2025",
    "NEWS_4": "https://www.regulatoryregistry.gov.on.ca/",
    "NEWS_5": "https://www.ontario.ca/laws/regulation/900552",
    "NEWS_6": "https://www.ontariohealth.ca/news",
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
    "Accept": "*/*",
}

os.makedirs(os.path.dirname(SNAPSHOT_CSV) or ".", exist_ok=True)

print(f"Loaded {len(URLS)} URLs")

# --------------------------------------
# EMAIL ALERTS (SendGrid)
# --------------------------------------

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "").strip()
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "").strip()
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL", "").strip()

def send_sendgrid_email(subject, body_text):
    if not (SENDGRID_API_KEY and ALERT_FROM_EMAIL and ALERT_TO_EMAIL):
        print("Email skipped. Missing SENDGRID_API_KEY or ALERT_FROM_EMAIL or ALERT_TO_EMAIL")
        return False

    payload = {
        "personalizations": [{"to": [{"email": ALERT_TO_EMAIL}]}],
        "from": {"email": ALERT_FROM_EMAIL},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}],
    }

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
        raise RuntimeError(f"SendGrid error {r.status_code}: {r.text[:800]}")

    return True

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
            lines.append(f"- {row['alarm_name']}  changes={row['change_count']}  url={row['url']}")
        lines.append("")

    if not errored.empty:
        lines.append("ERROR")
        for _, row in errored.iterrows():
            lines.append(f"- {row['alarm_name']}  error={row['change_count']}  url={row['url']}")
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
    )
    adapter = HTTPAdapter(max_retries=retries)
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
        parsed = json.loads(raw_text)
        return parsed
    except Exception:
        pass

    soup = BeautifulSoup(raw_text, "html.parser")
    lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
    return "\n".join(lines)

def diff_to_rows(before, after, max_field_len=4000):
    before_lines = (before or "").splitlines()
    after_lines = (after or "").splitlines()

    rows = []
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
            b = before_buf if before_buf else ""
            if len(b) > max_field_len:
                b = b[:max_field_len] + "...[truncated]"
            if len(text) > max_field_len:
                text = text[:max_field_len] + "...[truncated]"
            rows.append({"line_no": line_no, "before": b, "after": text})
            before_buf = None

    return rows

# --------------------------------------
# DATAFRAMES
# --------------------------------------

df_snapshot = pd.DataFrame(columns=["run_time", "alarm_name", "url", "content"])
df_summary = pd.DataFrame(columns=["run_time", "alarm_name", "url", "change_flag", "change_count"])
df_diff_archive = pd.DataFrame(columns=["run_time", "alarm_name", "url", "line_no", "before", "after"])

if os.path.isfile(SNAPSHOT_CSV):
    df_snapshot = pd.read_csv(SNAPSHOT_CSV)

# --------------------------------------
# MAIN LOOP (ONE RUN_TIME FOR ENTIRE RUN)
# --------------------------------------

run_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for alarm, url in URLS.items():
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        current_norm = normalize_content(resp.text)

        prev_row = df_snapshot[df_snapshot["alarm_name"] == alarm].tail(1)
        prev_text = None

        if not prev_row.empty:
            prev_raw = prev_row["content"].values[0]
            try:
                prev_text = json.loads(prev_raw)
            except Exception:
                prev_text = prev_raw

        if prev_text is None:
            change_flag = "FIRST_RUN"
            change_count = 0
            changes = []
        else:
            if isinstance(current_norm, dict):
                if prev_text != current_norm:
                    change_flag = "CHANGED"
                    change_count = 1
                    changes = [{
                        "line_no": 0,
                        "before": json.dumps(prev_text, ensure_ascii=False),
                        "after": json.dumps(current_norm, ensure_ascii=False)
                    }]
                else:
                    change_flag = "NO_CHANGE"
                    change_count = 0
                    changes = []
            else:
                if prev_text != current_norm:
                    changes = diff_to_rows(str(prev_text), str(current_norm))
                    change_flag = "CHANGED"
                    change_count = len(changes)
                else:
                    change_flag = "NO_CHANGE"
                    change_count = 0
                    changes = []

        snapshot_row = {
            "run_time": run_time_str,
            "alarm_name": alarm,
            "url": url,
            "content": json.dumps(current_norm, ensure_ascii=False) if isinstance(current_norm, dict) else str(current_norm),
        }
        df_snapshot = pd.concat([df_snapshot, pd.DataFrame([snapshot_row])], ignore_index=True)

        summary_row = {
            "run_time": run_time_str,
            "alarm_name": alarm,
            "url": url,
            "change_flag": change_flag,
            "change_count": change_count,
        }
        df_summary = pd.concat([df_summary, pd.DataFrame([summary_row])], ignore_index=True)

        if changes:
            if isinstance(current_norm, dict):
                before_blob = json.dumps(prev_text, ensure_ascii=False)
                after_blob = json.dumps(current_norm, ensure_ascii=False)
                line_no_val = 0
            else:
                before_blob = "\n".join([c["before"] for c in changes])[:20000]
                after_blob = "\n".join([c["after"] for c in changes])[:20000]
                line_no_val = None

            diff_row = {
                "run_time": run_time_str,
                "alarm_name": alarm,
                "url": url,
                "line_no": line_no_val,
                "before": before_blob,
                "after": after_blob,
            }
            df_diff_archive = pd.concat([df_diff_archive, pd.DataFrame([diff_row])], ignore_index=True)

        print(f"{alarm}: {change_flag} ({change_count} changes)")

    except Exception as e:
        summary_row = {
            "run_time": run_time_str,
            "alarm_name": alarm,
            "url": url,
            "change_flag": "ERROR",
            "change_count": str(e),
        }
        df_summary = pd.concat([df_summary, pd.DataFrame([summary_row])], ignore_index=True)
        print(f"{alarm}: ERROR - {e}")

# --------------------------------------
# SAVE CSVs
# --------------------------------------

df_snapshot.to_csv(SNAPSHOT_CSV, index=False)
df_summary.to_csv(SUMMARY_CSV, index=False)
df_diff_archive.to_csv(DIFF_ARCHIVE_CSV, index=False)

print("\n=== DIFF ARCHIVE (tail) ===")
print(df_diff_archive.tail())

# --------------------------------------
# SEND ONE EMAIL IF ANYTHING CHANGED OR ERRORED
# --------------------------------------

df_summary_run = df_summary[df_summary["run_time"] == run_time_str]
df_diff_archive_run = df_diff_archive[df_diff_archive["run_time"] == run_time_str]

send_email = (df_summary_run["change_flag"] == "CHANGED").any() or (df_summary_run["change_flag"] == "ERROR").any()

if send_email:
    subject = f"URL Monitor Alert {run_time_str}"
    body = build_email_body(df_summary_run, df_diff_archive_run, run_time_str)
    try:
        ok = send_sendgrid_email(subject, body)
        if ok:
            print("Email sent")
    except Exception as e:
        print(f"Email failed: {e}")
else:
    print("No changes, no email")

