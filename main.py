#!/usr/bin/env python
# coding: utf-8

# In[17]:


import os
import json
import difflib
import base64
import re
from datetime import datetime
from io import StringIO

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import pandas as pd

# --------------------------------------
# CONFIG
# --------------------------------------

SHEET_ID = "1aXZ06m5r9voN0tU9Ip6VUtr7BYiYaqTZuwZsUYeMKvo"
SHEET_NAME = "LIVe"
SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"

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

# --------------------------------------
# BLOCK + NOISE FILTERS
# --------------------------------------

BLOCK_PAGE_PATTERNS = [
    r"\blog in to continue\b",
    r"\bfor full account access\b",
    r"\bplease log in\b",
    r"\baccess denied\b",
    r"\brequest blocked\b",
    r"\bforbidden\b",
    r"\bverify you are human\b",
    r"\bcaptcha\b",
    r"\bchecking your browser\b",
    r"\bjust a moment\b",
    r"\benable cookies\b",
    r"\bunusual traffic\b",
    r"\btemporarily unavailable\b",
]

NOISE_LINE_PATTERNS = [
    r"\blog in to continue\b",
    r"\bplease\b",
    r"\blog in\b",
    r"\bfor full account access\b",
    r"\bcookie\b",
    r"\baccept all\b",
    r"\baccept cookies\b",
    r"\bmanage cookies\b",
    r"\bprivacy policy\b",
    r"\bterms of use\b",
    r"\bskip to content\b",
    r"\bjavascript\b",
    r"\byour browser\b",
    r"\bsubscribe\b",
    r"\bsign in\b",
    r"\bregister\b",
]

_BLOCK_RE = re.compile("|".join(BLOCK_PAGE_PATTERNS), flags=re.IGNORECASE)
_NOISE_RE = re.compile("|".join(NOISE_LINE_PATTERNS), flags=re.IGNORECASE)

def is_blocked_page(text: str) -> bool:
    t = (text or "").strip()
    if t == "":
        return True
    if _BLOCK_RE.search(t):
        return True
    return False

def strip_noise_lines(text: str) -> str:
    lines = [l.strip() for l in (text or "").splitlines()]
    keep = []
    for l in lines:
        if l == "":
            continue
        if len(l) <= 1:
            continue
        if _NOISE_RE.search(l):
            continue
        keep.append(l)
    return "\n".join(keep)

# --------------------------------------
# URL GUARD
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

# --------------------------------------
# LOAD URLS FROM PUBLIC GOOGLE SHEET
# --------------------------------------

def load_urls_from_google_sheet(csv_url: str) -> dict:
    r = requests.get(csv_url, timeout=30)
    r.raise_for_status()

    ct = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        raise ValueError("Google Sheet returned HTML. Set sharing to Anyone with the link, Viewer.")

    df = pd.read_csv(StringIO(r.text))

    df.columns = [str(c).strip() for c in df.columns]
    required = {"Alarm", "url"}
    missing = [c for c in required if c not in set(df.columns)]
    if missing:
        raise ValueError(f"Sheet missing columns: {missing}. Needed: Alarm, url")

    df["Alarm"] = df["Alarm"].astype(str).str.strip()
    df["url"] = df["url"].astype(str).str.strip()

    df = df[(df["Alarm"] != "") & (df["url"] != "")]
    df = df.drop_duplicates(subset=["Alarm"], keep="last")

    urls = dict(zip(df["Alarm"].tolist(), df["url"].tolist()))
    if len(urls) == 0:
        raise ValueError("Sheet returned zero rows after cleanup")

    return urls

URLS = load_urls_from_google_sheet(SHEET_CSV_URL)
print(f"Loaded {len(URLS)} URLs from Google Sheet tab {SHEET_NAME}")

validate_urls(URLS)

# --------------------------------------
# DISCORD ALERTS (3 channels via 3 webhooks)
# --------------------------------------

DISCORD_WEBHOOK_URL_ALL = os.getenv("DISCORD_WEBHOOK_URL_ALL", "").strip()
DISCORD_WEBHOOK_URL_CHANGED = os.getenv("DISCORD_WEBHOOK_URL_CHANGED", "").strip()
DISCORD_WEBHOOK_URL_ERROR = os.getenv("DISCORD_WEBHOOK_URL_ERROR", "").strip()

def send_discord_webhook(webhook_url: str, message: str) -> bool:
    if webhook_url == "":
        print("Discord skipped. Webhook missing.")
        return False

    payload = {"content": message[:1900]}
    try:
        r = requests.post(webhook_url, json=payload, timeout=30)
        print("Discord status:", r.status_code)
        if r.status_code >= 400:
            print("Discord failed:", r.text[:800])
            return False
        print("Discord sent ok")
        return True
    except Exception as e:
        print("Discord exception:", e)
        return False

def safe_url(u: str) -> str:
    u = str(u or "")
    return u.replace("https://", "hxxps://").replace("http://", "hxxp://")

def build_discord_all_message(df_summary_run, run_time_str):
    changed = df_summary_run[df_summary_run["change_flag"] == "CHANGED"]
    errored = df_summary_run[df_summary_run["change_flag"] == "ERROR"]
    blocked = df_summary_run[df_summary_run["change_flag"] == "BLOCKED"]
    nochange = df_summary_run[df_summary_run["change_flag"] == "NO_CHANGE"]
    first = df_summary_run[df_summary_run["change_flag"] == "FIRST_RUN"]

    lines = []
    lines.append(f"RUN {run_time_str}")
    lines.append(
        f"Changed {len(changed)} | Errors {len(errored)} | Blocked {len(blocked)} | No change {len(nochange)} | First {len(first)}"
    )
    return "\n".join(lines)

def build_discord_changed_message(df_summary_run, run_time_str):
    changed = df_summary_run[df_summary_run["change_flag"] == "CHANGED"]
    lines = [f"CHANGED {run_time_str}", ""]
    for _, row in changed.iterrows():
        lines.append(f"- {row['alarm_name']} changes={row['change_count']} url={safe_url(row['url'])}")
    return "\n".join(lines)

def build_discord_error_message(df_summary_run, run_time_str):
    errored = df_summary_run[df_summary_run["change_flag"] == "ERROR"]
    blocked = df_summary_run[df_summary_run["change_flag"] == "BLOCKED"]
    lines = [f"ISSUES {run_time_str}", ""]

    for _, row in blocked.iterrows():
        lines.append(f"- {row['alarm_name']} blocked={row['change_count']} url={safe_url(row['url'])}")

    for _, row in errored.iterrows():
        lines.append(f"- {row['alarm_name']} error={row['change_count']} url={safe_url(row['url'])}")

    return "\n".join(lines)

# --------------------------------------
# EMAIL ALERTS (SendGrid) with CSV attachment
# --------------------------------------

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "").strip()
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "").strip()

ALERT_TO_EMAILS_RAW = os.getenv("ALERT_TO_EMAILS", "").strip()
if ALERT_TO_EMAILS_RAW == "":
    ALERT_TO_EMAILS_RAW = os.getenv("ALERT_TO_EMAIL", "").strip()

ALERT_TO_EMAILS = [e.strip() for e in ALERT_TO_EMAILS_RAW.split(",") if e.strip()]

def _build_sendgrid_attachments(paths):
    items = []
    if paths is None:
        return items

    for path in paths:
        if path is None:
            continue
        path = str(path).strip()
        if path == "":
            continue
        if os.path.isfile(path) is False:
            print("Attachment missing:", path)
            continue

        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        items.append({
            "content": b64,
            "type": "text/csv",
            "filename": os.path.basename(path),
            "disposition": "attachment",
        })

    return items

def send_sendgrid_email(subject, body_text, attach_paths=None):
    if SENDGRID_API_KEY == "" or ALERT_FROM_EMAIL == "" or len(ALERT_TO_EMAILS) == 0:
        print("Email skipped. Missing SendGrid configuration.")
        return False

    payload = {
        "personalizations": [{"to": [{"email": e} for e in ALERT_TO_EMAILS]}],
        "from": {"email": ALERT_FROM_EMAIL},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}],
    }

    attachments = _build_sendgrid_attachments(attach_paths)
    if len(attachments) > 0:
        payload["attachments"] = attachments

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
    blocked = df_summary_run[df_summary_run["change_flag"] == "BLOCKED"]

    lines.append(f"Changed alarms: {len(changed)}")
    lines.append(f"Blocked alarms: {len(blocked)}")
    lines.append(f"Errored alarms: {len(errored)}")
    lines.append("")

    lines.append("SUMMARY TABLE")
    try:
        df_show = df_summary_run[["alarm_name", "change_flag", "change_count", "url"]].copy()
        lines.append(df_show.to_string(index=False, max_colwidth=140))
    except Exception as e:
        lines.append(f"Summary table render error: {e}")
    lines.append("")

    if blocked.empty is False:
        lines.append("BLOCKED (snapshot skipped)")
        for _, row in blocked.iterrows():
            lines.append(f"- {row['alarm_name']}  reason={row['change_count']}  url={safe_url(row['url'])}")
        lines.append("")

    if changed.empty is False:
        lines.append("CHANGED")
        for _, row in changed.iterrows():
            lines.append(f"- {row['alarm_name']}  changes={row['change_count']}  url={safe_url(row['url'])}")
        lines.append("")

    if errored.empty is False:
        lines.append("ERROR")
        for _, row in errored.iterrows():
            lines.append(f"- {row['alarm_name']}  error={row['change_count']}  url={safe_url(row['url'])}")
        lines.append("")

    if df_diff_archive_run.empty is False and changed.empty is False:
        lines.append("DIFF EXCERPTS (first 25 rows per alarm)")
        for alarm in changed["alarm_name"].tolist():
            sub = df_diff_archive_run[df_diff_archive_run["alarm_name"] == alarm].sort_values("line_no").head(25)
            if sub.empty:
                continue

            lines.append("")
            lines.append(f"Alarm: {alarm}")
            for _, r in sub.iterrows():
                b = str(r["before"])[:500]
                a = str(r["after"])[:500]
                lines.append(f"- line={r['line_no']} before_len={len(b)} after_len={len(a)}")
                lines.append(f"  before: {b}")
                lines.append(f"  after : {a}")

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

    # JSON input
    try:
        obj = json.loads(raw_text)
        if isinstance(obj, (dict, list)):
            raw_text = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        pass

    soup = BeautifulSoup(raw_text, "html.parser")

    # Remove scripts/styles for cleaner text
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    cleaned = "\n".join(lines)

    # Strip known junk lines before diff
    cleaned = strip_noise_lines(cleaned)

    # Collapse extra whitespace lines
    cleaned_lines = [l.strip() for l in cleaned.splitlines() if l.strip()]
    return "\n".join(cleaned_lines)

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
                "before_len": len(b),
                "after_len": len(text),
                "delta_len": len(text) - len(b),
            })
            before_buf = None

    return rows

def archive_url(url: str) -> str:
    return "https://web.archive.org/web/0/" + url

def fetch_text(url):
    resp = session.get(url, timeout=TIMEOUT)

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

df_diff_archive = pd.DataFrame(columns=[
    "run_time", "alarm_name", "url",
    "line_no", "before", "after",
    "before_len", "after_len", "delta_len"
])

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

        # Block/login gate handling: skip diff + skip snapshot update
        if is_blocked_page(current_norm):
            df_summary = pd.concat(
                [df_summary, pd.DataFrame([{
                    "run_time": run_time_str,
                    "alarm_name": alarm,
                    "url": url,
                    "change_flag": "BLOCKED",
                    "change_count": "LOGIN_OR_BOT_GATE",
                }])],
                ignore_index=True,
            )
            print(f"{alarm}: BLOCKED (snapshot skipped)")
            continue

        prev_row = df_snapshot[df_snapshot["alarm_name"] == alarm].tail(1)
        prev_text = None
        if prev_row.empty is False:
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
            [df_snapshot, pd.DataFrame([{
                "run_time": run_time_str,
                "alarm_name": alarm,
                "url": url,
                "content": str(current_norm),
            }])],
            ignore_index=True,
        )

        df_summary = pd.concat(
            [df_summary, pd.DataFrame([{
                "run_time": run_time_str,
                "alarm_name": alarm,
                "url": url,
                "change_flag": change_flag,
                "change_count": change_count,
            }])],
            ignore_index=True,
        )

        if len(changes) > 0:
            df_diff_archive = pd.concat(
                [df_diff_archive, pd.DataFrame([{
                    "run_time": run_time_str,
                    "alarm_name": alarm,
                    "url": url,
                    "line_no": c["line_no"],
                    "before": c["before"],
                    "after": c["after"],
                    "before_len": c["before_len"],
                    "after_len": c["after_len"],
                    "delta_len": c["delta_len"],
                } for c in changes])],
                ignore_index=True,
            )

        print(f"{alarm}: {change_flag} ({change_count})")

    except Exception as e:
        df_summary = pd.concat(
            [df_summary, pd.DataFrame([{
                "run_time": run_time_str,
                "alarm_name": alarm,
                "url": url,
                "change_flag": "ERROR",
                "change_count": str(e),
            }])],
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
# SEND ALERTS (Discord + Email)
# --------------------------------------

df_summary_run = df_summary[df_summary["run_time"] == run_time_str]
df_diff_archive_run = df_diff_archive[df_diff_archive["run_time"] == run_time_str]

has_changed = (df_summary_run["change_flag"] == "CHANGED").any()
has_issue = ((df_summary_run["change_flag"] == "ERROR") | (df_summary_run["change_flag"] == "BLOCKED")).any()

send_discord_webhook(DISCORD_WEBHOOK_URL_ALL, build_discord_all_message(df_summary_run, run_time_str))

if has_changed:
    send_discord_webhook(DISCORD_WEBHOOK_URL_CHANGED, build_discord_changed_message(df_summary_run, run_time_str))

if has_issue:
    send_discord_webhook(DISCORD_WEBHOOK_URL_ERROR, build_discord_error_message(df_summary_run, run_time_str))

subject = f"URL Monitor Run {run_time_str}"
body = build_email_body(df_summary_run, df_diff_archive_run, run_time_str)

attach_list = [SUMMARY_CSV]
if os.path.isfile(DIFF_ARCHIVE_CSV):
    attach_list.append(DIFF_ARCHIVE_CSV)

send_sendgrid_email(subject, body, attach_paths=attach_list)
print("Email attempted for every run")


# In[16]:


df_snapshot


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[11]:




