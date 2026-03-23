import re
import csv
import time
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

# ================== CONFIG ==================

BASE_URL = "https://www.atcc.org/cell-products/human-cells?t=productTab&numberOfResults=24"
HEADLESS = True
OUTFILE = "all_human_cell_lines.csv"

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 0.5  # between product page requests
MAX_PAGES_HARD_CAP = 200      # safety

PUBMED_ID_RE = re.compile(r"\b\d{7,8}\b")
ATCC_ID_RE = re.compile(r"\b[A-Z]{2,4}-\d{2,5}(\.\d+)?\b")


# ================== SELENIUM: PAGINATE & COLLECT LINKS ==================

def get_all_listing_links():
    """
    Drive ATCC's pagination:

    - Start on BASE_URL (page 1).
    - On each page:
        * collect product URLs from visible cards
        * click the NEXT control (if enabled) via JS
    - Stop when:
        * NEXT can't be found,
        * or NEXT is disabled,
        * or we hit MAX_PAGES_HARD_CAP.
    """

    chrome_opts = Options()
    if HEADLESS:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_opts)
    driver.get(BASE_URL)
    time.sleep(5)

    all_links = set()
    page = 1

    while page <= MAX_PAGES_HARD_CAP:
        print(f"[Listing] On page {page}")

        # --- collect links on this page ---
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        before = len(all_links)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # product detail links
            if "/products/" in href and "human-cells" not in href:
                if href.startswith("/"):
                    href = "https://www.atcc.org" + href
                all_links.add(href)
        added = len(all_links) - before
        print(f"[Listing] Page {page}: {added} new links (total {len(all_links)})")

        # --- find NEXT button ---
        # We'll try several selectors and then fall back to JS.
        next_element = None

        candidate_selectors = [
            "a[aria-label='Next']",
            "button[aria-label='Next']",
            "a[aria-label='Next page']",
            "button[aria-label='Next page']",
            "a.pagination-next",
            "button.pagination-next",
            "li.next a",
            "li[aria-label='Next'] a",
        ]

        for sel in candidate_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                elems = []
            for e in elems:
                try:
                    if not e.is_displayed():
                        continue
                    cls = (e.get_attribute("class") or "").lower()
                    aria_dis = (e.get_attribute("aria-disabled") or "").lower()
                    if "disabled" in cls or aria_dis == "true":
                        continue
                    next_element = e
                    break
                except StaleElementReferenceException:
                    continue
            if next_element:
                break

        # If we still didn't find, try text-based search
        if not next_element:
            try:
                text_candidates = driver.find_elements(By.XPATH, "//a|//button")
            except Exception:
                text_candidates = []
            for e in text_candidates:
                try:
                    txt = e.text.strip().lower()
                    if "next" in txt and e.is_displayed() and e.is_enabled():
                        cls = (e.get_attribute("class") or "").lower()
                        aria_dis = (e.get_attribute("aria-disabled") or "").lower()
                        if "disabled" not in cls and aria_dis != "true":
                            next_element = e
                            break
                except StaleElementReferenceException:
                    continue

        if not next_element:
            print("[Listing] No NEXT control found; assuming last page.")
            break

        # --- click NEXT via JavaScript for reliability ---
        try:
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", next_element
            )
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", next_element)
        except (ElementClickInterceptedException, StaleElementReferenceException) as e:
            print(f"[Listing] NEXT click failed ({type(e).__name__}); stopping.")
            break

        page += 1
        time.sleep(4)  # wait for page change

        # quick heuristic: if this page produced 0 new links, we're probably done
        if added == 0:
            print("[Listing] No new links on this page; assuming last page.")
            break

    driver.quit()
    print(f"[Listing] Finished pagination. Total unique product URLs: {len(all_links)}")
    return sorted(all_links)


# ================== DETAIL PAGE HELPERS ==================

def fetch_soup(url: str):
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.ok and r.text.strip():
            return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException:
        return None
    return None


def get_label_value(soup: BeautifulSoup, labels):
    if isinstance(labels, str):
        labels = [labels]
    labels_norm = [l.lower() for l in labels]

    for tag in soup.find_all(True):
        text = tag.get_text(strip=True)
        if not text:
            continue
        low = text.lower()
        if low in labels_norm:
            sib = tag.find_next_sibling()
            if sib and sib.get_text(strip=True):
                return sib.get_text(" ", strip=True)
            parent = tag.parent
            if parent:
                txt = parent.get_text(" ", strip=True)
                for ln in labels_norm:
                    if txt.lower().startswith(ln):
                        txt = txt[len(ln):].lstrip(": \u00a0-")
                if txt:
                    return txt
    return None


def extract_pubmed_ids(soup: BeautifulSoup):
    ids = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "pubmed.ncbi.nlm.nih.gov" in href or "ncbi.nlm.nih.gov/pubmed" in href:
            m = PUBMED_ID_RE.search(href)
            if m:
                ids.add(m.group(0))
    for block in soup.find_all(["p", "li", "div"]):
        txt = block.get_text(" ", strip=True)
        if "pubmed" in txt.lower():
            for m in PUBMED_ID_RE.finditer(txt):
                ids.add(m.group(0))
    return ";".join(sorted(ids)) if ids else None


def extract_other_cell_line_info(soup: BeautifulSoup):
    texts = []
    for header in soup.find_all(["h2", "h3", "strong", "b"]):
        htxt = header.get_text(" ", strip=True).lower()
        if any(k in htxt for k in ("derivation", "history", "origin", "parent")):
            segs = []
            sib = header.find_next_sibling()
            while sib and sib.name in ("p", "div", "ul", "ol"):
                segs.append(sib.get_text(" ", strip=True))
                sib = sib.find_next_sibling()
            if segs:
                texts.append(" ".join(segs))
    if not texts:
        texts.append(soup.get_text(" ", strip=True))

    hits = []
    seen = set()
    for txt in texts:
        for m in re.finditer(r"ATCC\s*[A-Z0-9.\-]+", txt):
            val = m.group(0).strip()
            if val not in seen:
                seen.add(val)
                hits.append(val)
        if re.search(r"derived from|clonal derivative|transfected with|immortalized with",
                     txt, flags=re.I):
            if txt not in seen:
                seen.add(txt)
                hits.append(txt)

    return "; ".join(hits[:8]) if hits else None


def extract_established_from(soup: BeautifulSoup):
    for lbl in ["Derivation", "History", "Passage history", "Cell line description"]:
        v = get_label_value(soup, lbl)
        if v:
            return v
    age = get_label_value(soup, ["Age", "Age at sampling"])
    tissue = get_label_value(soup, ["Tissue", "Tissue of Origin"])
    parts = []
    if age:
        parts.append(f"Age: {age}")
    if tissue:
        parts.append(f"Tissue: {tissue}")
    return "; ".join(parts) if parts else None


def scrape_product(url: str):
    soup = fetch_soup(url)
    if not soup:
        return None

    text = soup.get_text(" ", strip=True)
    m = ATCC_ID_RE.search(text)
    atcc_id = m.group(0) if m else None

    h1 = soup.find("h1")
    name = h1.get_text(" ", strip=True) if h1 else None

    organism = get_label_value(soup, ["Organism", "Species"])
    gender = get_label_value(soup, ["Gender", "Sex", "Sex of cell"])
    dev_stage = get_label_value(soup, ["Developmental stage", "Age", "Age at sampling"])
    tissue = get_label_value(soup, ["Tissue", "Tissue of Origin", "Derived from site"])
    cell_type = get_label_value(soup, ["Cell type", "Morphology"])
    disease = get_label_value(soup, ["Disease", "Diseases"])
    established_from = extract_established_from(soup)
    other_line = extract_other_cell_line_info(soup)
    pubmed_ids = extract_pubmed_ids(soup)

    return {
        "source_url": url,
        "atcc_id": atcc_id,
        "cell_line_name": name,
        "established_from": established_from,
        "organism_species": organism,
        "organism_gender": gender,
        "development_stage": dev_stage,
        "organ_tissue": tissue,
        "cell_type": cell_type,
        "other_cell_line": other_line,
        "disease": disease,
        "pubmed_ids": pubmed_ids,
    }


# ================== MAIN ==================

def main():
    print("=== Collecting all Human Cells product URLs across pages ===")
    product_links = get_all_listing_links()
    if not product_links:
        print("No product URLs found; check if the page structure changed.")
        return

    print(f"Total product URLs to scrape: {len(product_links)}")

    rows = []
    total = len(product_links)
    for i, url in enumerate(product_links, start=1):
        print(f"[{i}/{total}] Scraping {url}")
        data = scrape_product(url)
        if data:
            rows.append(data)
        else:
            print(f"   !! Failed to scrape {url}")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    if not rows:
        print("No product details scraped; nothing to write.")
        return

    fieldnames = list(rows[0].keys())
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"✅ Saved {len(rows)} records to {OUTFILE}")


if __name__ == "__main__":
    main()
