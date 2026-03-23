#!/usr/bin/env python3
"""
Enrich an existing combined ATCC table (human + animal) WITHOUT DROPPING ROWS.

Fixes / satisfies mentor requirements:
1) Fix/standardize ATCC IDs using source_url (last path segment). Example:
   https://www.atcc.org/products/ccl-240  ->  CCL-240
2) Retrieve "Product type" from each ATCC product page (to flag/exclude non-cell-line products).
3) Retrieve "Application" from each ATCC product page.
4) Excel-safe output: prevents “broken rows / shifted columns” by cleaning newlines/tabs
   and quoting ALL fields when writing CSV.

Usage:
  python enrich_atcc_fixed.py --in "all comb ined cell line (1) .csv" --out combined_atcc_enriched.csv

Optional:
  --noncell non_cell_products.csv   (write the non-cell products to a separate file)
  --resume                          (resume from a previous run output file)
"""

import argparse
import csv
import os
import re
import time
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ----------------------------
# Config
# ----------------------------
DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS_SEC = 0.7  # be polite; increase if you get blocked
MAX_RETRIES = 4

# Treat these product types as "cell-line-ish" (tune if needed)
CELLISH_PAT = re.compile(
    r"(cell|cell line|primary cell|stem cell|hybridoma)",
    re.IGNORECASE
)

# ----------------------------
# CSV / Excel safety helpers
# ----------------------------
def clean_cell_for_csv(x):
    """Remove characters that make Excel break rows/columns when opening CSV."""
    if x is None:
        return ""
    s = str(x)
    # remove newlines/tabs that cause Excel row breaks / column shifts
    s = re.sub(r"[\r\n\t]+", " ", s)
    # collapse multiple spaces
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def sanitize_df_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Apply clean_cell_for_csv only to string/object columns for speed."""
    out = df.copy()
    for c in out.columns:
        # everything was read as dtype=str; but keep this safe anyway
        out[c] = out[c].map(clean_cell_for_csv)
    return out

def safe_to_csv(df: pd.DataFrame, path: str):
    """Write CSV in an Excel-safe way (UTF-8 BOM + quote all fields)."""
    df2 = sanitize_df_for_csv(df)
    df2.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",   # Excel-friendly UTF-8
        quoting=csv.QUOTE_ALL,  # quote every field
        escapechar="\\",
        lineterminator="\n"
    )

# ----------------------------
# Scraping helpers
# ----------------------------
def ensure_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    u = url.strip()
    if u.startswith("www."):
        u = "https://" + u
    if u.startswith("/products/"):
        u = "https://www.atcc.org" + u
    return u

def normalize_atcc_id_from_url(source_url: str) -> str:
    """
    ATCC id is last part of source_url path; normalize to uppercase with hyphens.
    Example:
      https://www.atcc.org/products/ccl-240 -> CCL-240
      https://www.atcc.org/products/crl-2539-gfp-luc2 -> CRL-2539-GFP-LUC2
    """
    if not isinstance(source_url, str) or not source_url.strip():
        return ""

    u = ensure_url(source_url)

    try:
        path = urlparse(u).path
    except Exception:
        return ""

    segs = [s for s in path.split("/") if s]
    if not segs:
        return ""
    last = segs[-1].strip().replace("_", "-").upper()
    return last

def request_with_retries(session: requests.Session, url: str) -> requests.Response | None:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200 and resp.text and len(resp.text) > 200:
                return resp
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue
            return resp
        except Exception as e:
            last_err = e
            time.sleep(1.2 * attempt)
    print(f"[WARN] Failed after retries: {url} ({last_err})")
    return None

def clean_text(x: str) -> str:
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

def extract_cell_line_name(soup: BeautifulSoup) -> str:
    """Typically the main product/cell line name is in an H1."""
    h1 = soup.find("h1")
    if h1:
        return clean_text(h1.get_text(" ", strip=True))
    if soup.title:
        return clean_text(soup.title.get_text(" ", strip=True))
    return ""

def extract_kv_blocks(soup: BeautifulSoup) -> dict:
    """Try to extract label/value pairs (Product type, Tissue, Disease, etc.)."""
    out = {}

    # 1) dl/dt/dd blocks
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        if dts and dds and len(dts) == len(dds):
            for dt, dd in zip(dts, dds):
                k = clean_text(dt.get_text(" ", strip=True)).rstrip(":")
                v = clean_text(dd.get_text(" ", strip=True))
                if k and v and k.lower() not in out:
                    out[k.lower()] = v

    # 2) "Label:" patterns
    for label_el in soup.find_all(string=re.compile(r":\s*$")):
        try:
            label = clean_text(label_el).rstrip(":")
            if not label:
                continue
            parent = label_el.parent
            val = ""
            nxt = parent.find_next_sibling()
            if nxt:
                val = clean_text(nxt.get_text(" ", strip=True))
            if val:
                key = label.lower()
                if key not in out:
                    out[key] = val
        except Exception:
            pass

    return out

def extract_product_type_and_application(soup: BeautifulSoup) -> tuple[str, str]:
    """Return (product_type, application)."""
    kv = extract_kv_blocks(soup)

    product_type = ""
    for k in ("product type", "product_type", "type"):
        if k in kv:
            product_type = kv[k]
            break

    application = ""
    for k in ("application", "applications"):
        if k in kv:
            application = kv[k]
            break

    if not application:
        hdr = soup.find(["h1", "h2", "h3", "h4"], string=re.compile(r"^Applications?$", re.I))
        if hdr:
            # grab some content after the header
            nxt = hdr.find_next()
            if nxt:
                application = clean_text(nxt.get_text(" ", strip=True))

    return clean_text(product_type), clean_text(application)

# ----------------------------
# Main enrichment logic
# ----------------------------
def enrich_table(input_csv: str, out_csv: str, noncell_csv: str | None, resume: bool):
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input not found: {input_csv}")

    # Use latin1 + python engine to survive weird bytes (your earlier UnicodeDecodeError)
    df = pd.read_csv(
        input_csv,
        dtype=str,
        keep_default_na=False,
        encoding="latin1",
        engine="python"
    )
    df.columns = [c.strip() for c in df.columns]

    if "source_url" not in df.columns:
        raise KeyError("Your input CSV must contain a 'source_url' column.")

    # Ensure needed columns exist (never drop rows)
    for col in ["atcc_id", "cell_line_name", "product_type", "application", "is_cell_line", "enrich_status"]:
        if col not in df.columns:
            df[col] = ""

    # Resume logic
    done_mask = pd.Series([False] * len(df))
    if resume and os.path.exists(out_csv):
        try:
            prev = pd.read_csv(out_csv, dtype=str, keep_default_na=False, encoding="utf-8-sig", engine="python")
            prev.columns = [c.strip() for c in prev.columns]
            if "source_url" in prev.columns and "enrich_status" in prev.columns:
                prev_map = dict(zip(prev["source_url"], prev["enrich_status"]))
                done_mask = df["source_url"].map(prev_map).fillna("").eq("ok")
        except Exception:
            # If previous output can't be read for any reason, just don't resume
            done_mask = pd.Series([False] * len(df))

    # Normalize URLs + always fix ATCC ID from source_url
    df["source_url"] = df["source_url"].apply(ensure_url)
    df["atcc_id"] = df["source_url"].apply(normalize_atcc_id_from_url)

    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})

    total = len(df)
    print(f"Loaded {total} rows from: {input_csv}")
    print("Starting enrichment (no rows will be dropped).")

    for i in range(total):
        if bool(done_mask.iloc[i]):
            continue

        url = df.at[i, "source_url"]
        if not url:
            df.at[i, "enrich_status"] = "missing_source_url"
            continue

        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        resp = request_with_retries(session, url)
        if resp is None:
            df.at[i, "enrich_status"] = "request_failed"
            continue

        if resp.status_code != 200:
            df.at[i, "enrich_status"] = f"http_{resp.status_code}"
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        page_name = extract_cell_line_name(soup)
        product_type, application = extract_product_type_and_application(soup)

        if not df.at[i, "cell_line_name"]:
            df.at[i, "cell_line_name"] = page_name

        df.at[i, "product_type"] = product_type
        df.at[i, "application"] = application

        # Flag is_cell_line based on product_type (do NOT delete anything)
        if product_type:
            df.at[i, "is_cell_line"] = "TRUE" if CELLISH_PAT.search(product_type) else "FALSE"
        else:
            df.at[i, "is_cell_line"] = "UNKNOWN"

        df.at[i, "enrich_status"] = "ok"

        # checkpoint every 25 rows
        if (i + 1) % 25 == 0:
            safe_to_csv(df, out_csv)
            print(f"[checkpoint] saved at row {i+1}/{total} -> {out_csv}")

    # Column order tweak: application before source_url
    cols = list(df.columns)
    if "application" in cols and "source_url" in cols:
        cols.remove("application")
        src_idx = cols.index("source_url")
        cols.insert(src_idx, "application")
        df = df[cols]

    # Final save (Excel-safe)
    safe_to_csv(df, out_csv)
    print(f"✅ Saved enriched output to: {out_csv}")

    # Optional: write non-cell products separately (main output still contains ALL rows)
    if noncell_csv:
        noncell = df[df["is_cell_line"].eq("FALSE")].copy()
        safe_to_csv(noncell, noncell_csv)
        print(f"✅ Saved NON-cell-line products to: {noncell_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="input_csv", required=True, help="Input combined CSV (human+animal).")
    ap.add_argument("--out", dest="out_csv", default="combined_atcc_enriched.csv", help="Output enriched CSV.")
    ap.add_argument("--noncell", dest="noncell_csv", default="non_cell_products.csv",
                    help="Where to write non-cell products (optional). Use '' to disable.")
    ap.add_argument("--resume", action="store_true",
                    help="Resume from existing --out file (skips rows with enrich_status == ok).")
    args = ap.parse_args()

    noncell_path = args.noncell_csv.strip() if isinstance(args.noncell_csv, str) else None
    if noncell_path == "":
        noncell_path = None

    enrich_table(
        input_csv=args.input_csv,
        out_csv=args.out_csv,
        noncell_csv=noncell_path,
        resume=args.resume
    )

if __name__ == "__main__":
    main()

