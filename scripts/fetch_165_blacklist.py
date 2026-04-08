#!/usr/bin/env python3
"""
Fetches 165 anti-scam blacklists from Taiwan's open data platform
and generates data/blacklist.json.

Datasets:
  160055 - 165反詐騙諮詢專線_假投資(博弈)網站
  176455 - 165反詐騙諮詢專線_遭停止解析涉詐網站
"""

import json
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

# ─── Configuration ────────────────────────────────────────────────────────────
DATASET_FAKE_INVEST = "160055"   # 165反詐騙諮詢專線_假投資(博弈)網站
DATASET_FRAUD_SITES = "176455"   # 165反詐騙諮詢專線_遭停止解析涉詐網站

API_BASE = "https://data.gov.tw/api/v2/rest/dataset"
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "blacklist.json"
REQUEST_TIMEOUT = 30
# ──────────────────────────────────────────────────────────────────────────────


def get_csv_url(dataset_id: str) -> str:
    """Call data.gov.tw API to get the latest CSV download URL for a dataset."""
    url = f"{API_BASE}/{dataset_id}"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    data = resp.json()
    # data.gov.tw v2 API uses "distribution" (not "resources")
    distributions = data["result"]["distribution"]
    csv_distributions = [
        d for d in distributions
        if d.get("resourceFormat", "").upper() == "CSV"
    ]

    if not csv_distributions:
        raise ValueError(f"No CSV distribution found in dataset {dataset_id}")

    return csv_distributions[0]["resourceDownloadUrl"]


def download_csv(url: str) -> str:
    """Download CSV content as string.

    opdadm.moi.gov.tw uses an SSL cert missing Subject Key Identifier,
    which Python 3.14's stricter SSL rejects. Fall back to verify=False.
    """
    import urllib3
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.SSLError:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, verify=False)
    resp.raise_for_status()
    for enc in ["utf-8-sig", "utf-8", "big5"]:
        try:
            return resp.content.decode(enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode CSV from {url}")


def normalize_domain(raw: str) -> str:
    """Strip protocol, path, and port from a raw domain/URL string."""
    raw = raw.strip().lower()
    for prefix in ["https://", "http://"]:
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    raw = raw.split("/")[0]
    raw = raw.split(":")[0]
    return raw


def parse_fake_invest_csv(content: str) -> dict[str, str]:
    """
    Parse 假投資(博弈)網站 CSV.
    2 header rows: row 0 = English col names, row 1 = Chinese (skip).
    Domain column: WEBURL
    """
    df = pd.read_csv(StringIO(content), skiprows=[1])
    domains: dict[str, str] = {}
    for raw in df["WEBURL"].dropna():
        domain = normalize_domain(str(raw))
        if domain and "." in domain:
            domains[domain] = "假投資博弈"
    return domains


def parse_fraud_sites_csv(content: str) -> dict[str, str]:
    """
    Parse 遭停止解析涉詐網站 CSV.
    1 header row. Domain column: 網域, category column: 網站性質.
    """
    df = pd.read_csv(StringIO(content))
    domains: dict[str, str] = {}
    for _, row in df.iterrows():
        raw = str(row.get("網域", "")).strip()
        category = str(row.get("網站性質", "涉詐網站")).strip()
        domain = normalize_domain(raw)
        if domain and "." in domain and domain not in domains:
            domains[domain] = category
    return domains


def main() -> None:
    print("=== 165 Blacklist Updater ===")

    print("[1/4] Fetching 假投資 dataset URL...")
    fake_invest_url = get_csv_url(DATASET_FAKE_INVEST)
    print(f"      URL: {fake_invest_url}")

    print("[2/4] Downloading and parsing 假投資 CSV...")
    fake_invest_domains = parse_fake_invest_csv(download_csv(fake_invest_url))
    print(f"      Parsed {len(fake_invest_domains)} unique domains")

    print("[3/4] Fetching 涉詐網站 dataset URL...")
    fraud_url = get_csv_url(DATASET_FRAUD_SITES)
    print(f"      URL: {fraud_url}")

    print("[3/4] Downloading and parsing 涉詐網站 CSV...")
    fraud_domains = parse_fraud_sites_csv(download_csv(fraud_url))
    print(f"      Parsed {len(fraud_domains)} unique domains")

    all_domains = {**fraud_domains, **fake_invest_domains}
    print(f"[4/4] Total unique domains after merge: {len(all_domains)}")

    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total": len(all_domains),
        "domains": all_domains,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"      Written to {OUTPUT_PATH}")
    print("=== Done ===")


if __name__ == "__main__":
    main()
