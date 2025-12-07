#!/usr/bin/env python3
"""
npp_snapshot.py
Fetch NPP published reports, extract region/source program vs actual numbers,
produce charts and a short summary, then deliver via email or Telegram.

Set environment variables:
- NPP_BASE_URL (optional): default https://npp.gov.in/publishedReports
- DELIVERY_METHOD: "email" or "telegram" or "save"
If DELIVERY_METHOD == "email":
    - SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_TO
If DELIVERY_METHOD == "telegram":
    - TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

Run: python npp_snapshot.py
"""
import os
import re
import io
import sys
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.message import EmailMessage

# ---- Config ----
NPP_BASE_URL = os.getenv("NPP_BASE_URL", "https://npp.gov.in/publishedReports")
DELIVERY_METHOD = os.getenv("DELIVERY_METHOD", "save")  # "email", "telegram", or "save"
TMP_DIR = os.getenv("TMP_DIR", "/tmp/npp_snapshot")
os.makedirs(TMP_DIR, exist_ok=True)
LOG_LEVEL = logging.INFO

# email config (if used)
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

# telegram config (if used)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# thresholds
DEVIATION_ALERT_PERCENT = float(os.getenv("DEVIATION_ALERT_PERCENT", "5.0"))  # percent

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s: %(message)s")


# ---- Helpers ----
def fetch_npp_listing():
    logging.info("Fetching NPP published reports listing...")
    resp = requests.get(NPP_BASE_URL, timeout=20)
    resp.raise_for_status()
    return resp.text


def find_latest_report_url(html):
    """
    Heuristic: look for links ending .xls/.xlsx/.csv or containing 'Monthly' or 'region'
    Return full URL or None.
    """
    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip()
        # absolute or relative URL
        url = href if href.startswith("http") else requests.compat.urljoin(NPP_BASE_URL, href)
        # heuristics for file types or relevant report names
        if re.search(r"\.(xls|xlsx|csv)$", url, re.IGNORECASE):
            candidates.append((url, text))
        elif "monthly" in url.lower() or "monthly" in text.lower() or "region" in text.lower():
            candidates.append((url, text))
    # prefer xlsx/xls/csv
    if not candidates:
        logging.warning("No candidate reports found on page using heuristics.")
        return None
    # crude pick: prefer latest by link text containing a year or month
    def score(item):
        url, text = item
        score = 0
        m = re.search(r"(\d{4})", url + " " + text)
        if m:
            score += int(m.group(1))
        if "monthly" in (url+text).lower():
            score += 10
        if url.lower().endswith(".xlsx"):
            score += 5
        return score
    candidates.sort(key=score, reverse=True)
    chosen = candidates[0][0]
    logging.info(f"Chosen report URL: {chosen}")
    return chosen


def download_file(url):
    logging.info(f"Downloading {url} ...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    content = resp.content
    # filename heuristics
    fname = os.path.basename(url.split("?")[0])
    if not fname:
        fname = f"npp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    path = os.path.join(TMP_DIR, fname)
    with open(path, "wb") as f:
        f.write(content)
    logging.info(f"Saved to {path}")
    return path


def read_possible_tables(path):
    """
    Try pandas.read_excel or read_csv and return list of DataFrames.
    """
    dfs = []
    low = str(path).lower()
    try:
        if low.endswith(".csv"):
            df = pd.read_csv(path, skip_blank_lines=True)
            dfs.append(df)
        else:
            # read all sheets
            xls = pd.ExcelFile(path)
            for sheet in xls.sheet_names:
                try:
                    df = xls.parse(sheet_name=sheet, header=None)
                    dfs.append(df)
                except Exception as e:
                    logging.debug(f"sheet {sheet} parse error: {e}")
    except Exception as e:
        logging.warning(f"Failed to read file with pandas: {e}")
    return dfs


def extract_region_values_from_dfs(dfs):
    """
    Very heuristic extraction:
    Look for rows/columns where 'Southern' / 'Eastern' / 'North Eastern' / 'All India' appear,
    then find columns nearby with numeric values labelled Program, Actual, or similar.
    Returns dict: region -> {source -> (program, actual)}
    """
    regions_of_interest = {
        "Southern": ["southern", "south"],
        "Eastern": ["eastern", "east"],
        "North Eastern": ["north eastern", "north-eastern", "north eastern", "northeastern", "north east"],
        "All India": ["all india", "all-india", "all india"]
    }
    sources = ["thermal", "nuclear", "hydro", "res", "r.e.s", "renewable"]
    out = {}

    def find_region_row(df, region_kw):
        # flatten to strings and search row-wise
        for ri, row in df.iterrows():
            row_text = " ".join([str(x).strip().lower() for x in row.fillna("")])
            if region_kw in row_text:
                return ri, row_text
        return None, None

    for df in dfs:
        # convert all to string frame for searching
        df_str = df.fillna("").astype(str)
        # try to find column header that mentions 'program' or 'schedule' and 'actual'
        header_candidates = []
        for ci in range(df_str.shape[1]):
            col_text = " ".join(df_str.iloc[:3, ci].tolist()).lower()
            if "program" in col_text or "scheduled" in col_text or "schedule" in col_text or "programme" in col_text:
                header_candidates.append(("program", ci))
            if "actual" in col_text or "generation" in col_text:
                header_candidates.append(("actual", ci))
        # scan for region rows
        for rname, kws in regions_of_interest.items():
            for kw in kws:
                ri, row_text = find_region_row(df, kw)
                if ri is None:
                    continue
                # attempt to find source words in the row or following rows
                # Search a small block around the row for numeric columns
                block = df_str.iloc[max(0, ri-2):ri+6, :].reset_index(drop=True)
                # heuristics:
                # - find columns with numeric values in the block
                numeric_cols = []
                for ci in range(block.shape[1]):
                    colvals = block.iloc[:, ci].replace("", "0").str.replace(",", "").str.strip()
                    numeric_count = colvals.apply(lambda x: bool(re.match(r"^-?\d+(\.\d+)?$", x))).sum()
                    if numeric_count >= max(1, int(0.3*block.shape[0])):
                        numeric_cols.append(ci)
                # look for source names in nearby rows (left-most column)
                for r_offset in range(block.shape[0]):
                    row0 = " ".join(block.iloc[r_offset, :2].tolist()).lower()
                    for s in sources:
                        if s in row0:
                            # extract numeric values from numeric_cols
                            vals = []
                            for ci in numeric_cols[:4]:
                                v = block.iloc[r_offset, ci]
                                v = str(v).replace(",", "").strip()
                                try:
                                    vals.append(float(v))
                                except:
                                    vals.append(None)
                            # assume program then actual or vice versa; keep as best-effort
                            if len(vals) >= 2:
                                program = vals[0]
                                actual = vals[1]
                                out.setdefault(rname, {})[s.capitalize()] = (program, actual)
                # if we got some region info break
    # fallback: try All India totals if exists in the original tables (search for "All India")
    # NOTE: This is heuristic. It may miss or mis-interpret some formats.
    return out


def compute_summary(data):
    """
    data: dict region->source->(program,actual)
    compute deviations and top items
    """
    summary_lines = []
    alerts = []
    for region, sources in data.items():
        total_prog = 0.0
        total_act = 0.0
        for s, (p, a) in sources.items():
            p = p or 0.0
            a = a or 0.0
            total_prog += p
            total_act += a
            # percent deviation
            if p:
                dev = (a - p) / p * 100.0
            else:
                dev = None
            if dev is not None and abs(dev) >= DEVIATION_ALERT_PERCENT:
                alerts.append((region, s, dev, p, a))
        summary_lines.append(f"{region}: Program={total_prog:.2f} MU, Actual={total_act:.2f} MU, Diff={total_act-total_prog:.2f} MU")
    return "\n".join(summary_lines), alerts


def plot_charts(data, out_prefix):
    """
    Create charts for All-India and per-region.
    Returns list of image paths.
    """
    img_paths = []
    # All-India aggregate if exist
    # Build All-India program/actual by source if available
    all_sources = {}
    if "All India" in data:
        all_sources = data["All India"]
    else:
        # try sum across regions
        for region, sources in data.items():
            for s, (p, a) in sources.items():
                if s not in all_sources:
                    all_sources[s] = [0.0, 0.0]
                all_sources[s][0] += (p or 0.0)
                all_sources[s][1] += (a or 0.0)

    # All-India chart
    if all_sources:
        labels = list(all_sources.keys())
        program = [all_sources[s][0] if isinstance(all_sources[s], (list,tuple)) else (all_sources[s][0]) for s in labels]
        actual = [all_sources[s][1] if isinstance(all_sources[s], (list,tuple)) else (all_sources[s][1]) for s in labels]
        fig, ax = plt.subplots(figsize=(8,5))
        x = range(len(labels))
        ax.bar(x, program, alpha=0.7, label='Program')
        ax.bar(x, actual, alpha=0.7, label='Actual')
        ax.set_xticks(x); ax.set_xticklabels(labels)
        ax.set_ylabel("Generation (MU)")
        ax.set_title("All-India: Program vs Actual")
        ax.legend()
        ppath = f"{out_prefix}_all_india.png"
        plt.tight_layout(); plt.savefig(ppath); plt.close(fig)
        img_paths.append(ppath)

    # region-wise
    for region, sources in data.items():
        labels = list(sources.keys())
        program = [sources[s][0] or 0.0 for s in labels]
        actual = [sources[s][1] or 0.0 for s in labels]
        fig, ax = plt.subplots(figsize=(8,5))
        x = range(len(labels))
        ax.bar(x, program, alpha=0.7, label='Program')
        ax.bar(x, actual, alpha=0.7, label='Actual')
        ax.set_xticks(x); ax.set_xticklabels(labels)
        ax.set_ylabel("Generation (MU)")
        ax.set_title(f"{region}: Program vs Actual")
        ax.legend()
        ppath = f"{out_prefix}_{region.replace(' ','_').lower()}.png"
        plt.tight_layout(); plt.savefig(ppath); plt.close(fig)
        img_paths.append(ppath)

    return img_paths


def deliver_via_email(subject, body_text, attachments):
    if not SMTP_HOST or not EMAIL_TO or not SMTP_USER:
        logging.error("Email settings not fully configured. Skipping email delivery.")
        return False
    msg = EmailMessage()
    msg["From"] = SMTP_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(body_text)
    for path in attachments:
        with open(path, "rb") as f:
            data = f.read()
        maintype = "image"
        subtype = "png"
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=os.path.basename(path))
    logging.info("Connecting to SMTP and sending email...")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
    logging.info("Email sent.")
    return True


def deliver_via_telegram(text, attachments):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram settings not configured.")
        return False
    bot_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    # send text
    r = requests.post(f"{bot_url}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    logging.info("Telegram text sent: %s", r.status_code)
    # send attachments as images
    for path in attachments:
        with open(path, "rb") as f:
            files = {"photo": f}
            resp = requests.post(f"{bot_url}/sendPhoto", data={"chat_id": TELEGRAM_CHAT_ID}, files=files, timeout=30)
            logging.info("Telegram image send status: %s", resp.status_code)
    return True


def save_locally(text, attachments):
    out_text = os.path.join(TMP_DIR, f"snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt")
    with open(out_text, "w") as f:
        f.write(text)
    for p in attachments:
        # already saved by plotting
        pass
    logging.info("Saved snapshot text to %s and images to %s", out_text, TMP_DIR)
    return True


# ---- Main flow ----
def main():
    try:
        html = fetch_npp_listing()
        report_url = find_latest_report_url(html)
        if not report_url:
            logging.error("Could not find a report URL on NPP page.")
            # still attempt to write an empty snapshot
            save_locally("No report found", [])
            return
        path = download_file(report_url)
        dfs = read_possible_tables(path)
        if not dfs:
            logging.error("No tables extracted from downloaded file.")
            save_locally("No tables found in report", [])
            return
        data = extract_region_values_from_dfs(dfs)
        if not data:
            logging.warning("Extraction returned no structured region data. Saving raw first sheet for manual check.")
            # save first df to csv for user's manual inspection
            try:
                dfs[0].to_csv(os.path.join(TMP_DIR, "first_sheet_dump.csv"), index=False)
            except:
                pass
            save_locally("Failed to extract structured data. See raw dump in tmp.", [])
            return
        summary_text, alerts = compute_summary(data)
        header = f"NPP Daily Snapshot - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        body = header + summary_text + "\n\n"
        if alerts:
            body += "Alerts (deviations > {}%):\n".format(DEVIATION_ALERT_PERCENT)
            for a in alerts:
                region, s, dev, p, a_val = a
                body += f"- {region} {s}: deviation {dev:.2f}% (Program {p}, Actual {a_val})\n"
        # generate charts
        img_paths = plot_charts(data, os.path.join(TMP_DIR, "npp_snapshot"))
        # deliver
        if DELIVERY_METHOD == "email":
            deliver_via_email("NPP Snapshot " + datetime.utcnow().strftime("%Y-%m-%d"), body, img_paths)
        elif DELIVERY_METHOD == "telegram":
            deliver_via_telegram(body, img_paths)
        else:
            save_locally(body, img_paths)
        logging.info("Done.")
    except Exception as e:
        logging.exception("Error in snapshot agent: %s", e)


if __name__ == "__main__":
    main()
