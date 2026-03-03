"""
SSL Finder Scraper v4.1
Two modes:
  full   - Scrape ALL opportunities (runs once daily in morning)
  quick  - Check page 1 newest-first for NEW opportunities only (runs every 15 min)

Usage:
  python scraper.py full
  python scraper.py quick
"""

import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://montgomerycountymd.galaxydigital.com"
LISTING_URL = f"{BASE_URL}/need/?s=1&dir=DESC&orderby=need_id"
SSL_LISTING_URL = f"{BASE_URL}/need/?s=1&need_init_id=2962&dir=DESC&orderby=need_id"
DETAIL_URL = f"{BASE_URL}/need/detail/?need_id={{}}"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "opportunities.json"
KNOWN_IDS_FILE = OUTPUT_DIR / "known_ids.json"
MAX_OPPORTUNITIES = 100  # Set to 0 for unlimited
QUICK_PAGES = 3  # How many pages to check in quick mode
REQUEST_DELAY = 0.3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

# ── Cleaning helpers ──────────────────────────────────────────────

JUNK = [
    "Get Connected Icon", "Posted By", "Back to Volunteer Center Home",
    "Share Opportunity", "Respond as Group", "Site Supervisor",
    "Skip to main content", "Open side bar", "Collapse Menu",
]


def clean(text):
    if not text:
        return ""
    result = text
    for j in JUNK:
        result = result.replace(j, "")
    result = re.sub(r'[\t\r]+', ' ', result)
    result = re.sub(r'\n\s*\n+', '\n', result)
    result = re.sub(r' {2,}', ' ', result)
    return result.strip()


def parse_address_block(lines):
    city, zip_code, city_idx = "", "", -1
    cleaned = [clean(l) for l in lines if clean(l)]
    for i, line in enumerate(cleaned):
        m = re.match(r'^([A-Za-z\s]+),\s*MD\s*$', line, re.IGNORECASE)
        if m:
            city, city_idx = m.group(1).strip().title(), i
            continue
        m = re.match(r'^([A-Za-z\s]+),\s*MD\s+(\d{5})', line, re.IGNORECASE)
        if m:
            city, zip_code, city_idx = m.group(1).strip().title(), m.group(2), i
            continue
        m = re.match(r'^(\d{5})$', line)
        if m:
            zip_code = m.group(1)
    parts = [cleaned[i] for i in range(len(cleaned)) if i < city_idx or city_idx < 0]
    parts = [p for p in parts if len(p) >= 2]
    street = ', '.join(parts)
    full = street
    if city:
        full = f"{street}, {city}, MD" if full else f"{city}, MD"
    if zip_code:
        full += f" {zip_code}"
    return full, city, zip_code


# ── Phase 1: Collect opportunity IDs via Playwright ──────────────

async def collect_ids(url, label="ALL", max_pages=0):
    """Click through paginated listing and collect need_ids.
    max_pages=0 means unlimited."""
    all_ids = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )

        print(f"\n📋 Collecting {label} opportunity IDs...")
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        page_num = 1
        while True:
            # Extract need_ids from current page
            ids_on_page = await page.evaluate("""
                () => {
                    const links = document.querySelectorAll('a[href*="need_id="]');
                    const ids = new Set();
                    for (const a of links) {
                        const m = a.href.match(/need_id=(\\d+)/);
                        if (m) ids.add(m[1]);
                    }
                    return Array.from(ids);
                }
            """)

            new_count = 0
            for oid in ids_on_page:
                if oid not in seen:
                    seen.add(oid)
                    all_ids.append(oid)
                    new_count += 1

            if page_num % 10 == 0 or new_count == 0:
                print(f"    Page {page_num}: {new_count} new IDs (total: {len(all_ids)})")

            if new_count == 0:
                print(f"    No new IDs on page {page_num}, stopping.")
                break

            if max_pages > 0 and page_num >= max_pages:
                print(f"    Reached page limit of {max_pages}")
                break

            if MAX_OPPORTUNITIES > 0 and len(all_ids) >= MAX_OPPORTUNITIES:
                print(f"    Reached opportunity limit of {MAX_OPPORTUNITIES}")
                break

            # Click next page
            try:
                next_btn = page.locator('a:has-text(">")')
                if await next_btn.count() > 0 and await next_btn.first.is_visible():
                    await next_btn.first.click()
                    await page.wait_for_timeout(2000)
                    page_num += 1
                else:
                    next_page = page.locator(f'a:has-text("{page_num + 1}")')
                    if await next_page.count() > 0:
                        await next_page.first.click()
                        await page.wait_for_timeout(2000)
                        page_num += 1
                    else:
                        print(f"    No next button after page {page_num}")
                        break
            except Exception as e:
                print(f"    Pagination error: {e}")
                break

        await browser.close()

    print(f"  ✅ Collected {len(all_ids)} {label} IDs across {page_num} pages")
    return all_ids


# ── Phase 2: Fetch detail page via HTTP ──────────────────────────

def fetch_detail(opp_id):
    url = DETAIL_URL.format(opp_id)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        return {"id": opp_id, "url": url, "error": str(e)}

    result = {"id": opp_id, "url": url}

    # Title
    h1 = soup.find("h1")
    result["title"] = clean(h1.get_text()) if h1 else ""

    # Description
    desc = ""
    for header in soup.find_all(["h2", "h3", "h4"]):
        if "Description" in header.get_text():
            parts = []
            for sib in header.find_next_siblings():
                if sib.name in ["h2", "h3", "h4"]:
                    break
                text = sib.get_text(separator="\n").strip()
                if text:
                    parts.append(text)
            desc = "\n".join(parts)
            break
    result["description"] = clean(desc)

    # Location
    location_lines = []
    for header in soup.find_all(["h2", "h3"]):
        if header.get_text().strip() == "Location":
            table = header.find_next("table")
            if table:
                for row in table.find_all("tr"):
                    text = clean(row.get_text())
                    if text:
                        location_lines.append(text)
            break
    result["location_lines"] = location_lines

    # Organization
    org = ""
    for header in soup.find_all(["h2", "h3"]):
        if header.get_text().strip() == "Organization":
            link = header.find_next("a")
            if link:
                org = clean(link.get_text())
            break
    result["organization"] = org

    # Date/time
    page_text = soup.get_text()
    date_type = "ongoing"
    event_date = ""
    hours = ""

    date_match = re.search(r"Happens On\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})", page_text)
    if date_match:
        event_date = date_match.group(1)
        date_type = event_date

    time_matches = re.findall(
        r'\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)\s*[-\u2013to]+\s*\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)',
        page_text
    )
    if time_matches:
        hours = "; ".join(time_matches[:3])

    if "ongoing" in page_text.lower() and not event_date:
        date_type = "ongoing"

    result["date_type"] = date_type
    result["event_date"] = event_date
    result["hours"] = hours

    # Details section
    page_lower = page_text.lower()
    age_req = ""
    for header in soup.find_all(["h2", "h3"]):
        if "Details" in header.get_text() and "Description" not in header.get_text():
            table = header.find_next("table")
            if table:
                for row in table.find_all("tr"):
                    text = clean(row.get_text())
                    if "age" in text.lower() or "between" in text.lower():
                        age_req = text
            break
    result["age_requirements"] = age_req

    min_age, max_age = 0, 0
    age_nums = re.findall(r'(\d+)', age_req)
    if age_nums:
        min_age = int(age_nums[0])
        if len(age_nums) > 1:
            max_age = int(age_nums[1])
    result["min_age"] = min_age
    result["max_age"] = max_age

    result["is_family_friendly"] = "family friendly" in page_lower
    result["is_outdoors"] = "is outdoors" in page_lower
    result["is_virtual"] = "virtual opportunity" in page_lower

    # Interests - look for them as list items near the bottom of the detail content
    interests = []
    known = [
        "Recreation / Sports", "Food Prep & Delivery", "Housing / Shelter",
        "Events / Collections", "Education / Mentoring", "Arts / Culture",
        "Professional Skills", "Environment", "Health / Wellness",
        "Technology", "Animals", "Community Building", "Advocacy",
    ]
    # Find interest items - they appear as standalone list items or links
    # NOT in the nav menu, so we look specifically after the Location or Details section
    interest_section = False
    for li in soup.find_all("li"):
        text = li.get_text().strip()
        if text in known:
            interests.append(text)
    # Also check for items that are direct children of the main content
    for el in soup.find_all(class_=re.compile(r"interest|category")):
        text = el.get_text().strip()
        if text in known and text not in interests:
            interests.append(text)
    # Remove "Court Ordered" - it appears in the nav menu on every page
    interests = [i for i in interests if i != "Court Ordered"]
    result["interests"] = interests

    # SSL
    result["is_ssl"] = "mcps ssl" in page_lower or "student service learning" in page_lower

    init_title = ""
    breadcrumb = soup.find("ol") or soup.find(class_=re.compile(r"breadcrumb"))
    if breadcrumb:
        for a in breadcrumb.find_all("a"):
            if "init" in a.get("href", ""):
                init_title = clean(a.get_text())
    result["initiative_title"] = init_title

    # Contact
    contact_name, contact_email, contact_phone = "", "", ""
    for header in soup.find_all(["h2", "h3"]):
        if "Supervisor" in header.get_text() or "Contact" in header.get_text():
            table = header.find_next("table")
            if table:
                for row in table.find_all("tr"):
                    text = clean(row.get_text())
                    em = re.search(r'[\w.+-]+@[\w.-]+\.\w+', text)
                    ph = re.search(r'[\d\(\)]{3,}[\s\-\.\d\(\)ext]+\d', text)
                    if em:
                        contact_email = em.group(0)
                    elif ph:
                        contact_phone = ph.group(0).strip()
                    elif text and not contact_name and len(text) > 2:
                        contact_name = text
            break
    parts = [p for p in [contact_name, contact_email, contact_phone] if p]
    result["contact"] = " | ".join(parts)

    result["allow_teams"] = "respond as group" in page_lower
    cap = 0
    cap_match = re.search(r'(\d+)\s*(?:volunteers?\s*needed|spots?\s*(?:left|available))', page_lower)
    if cap_match:
        cap = int(cap_match.group(1))
    result["capacity"] = cap

    return result


# ── Normalize ─────────────────────────────────────────────────────

def normalize(detail):
    full_addr, city, zip_code = parse_address_block(detail.get("location_lines", []))
    is_ssl = detail.get("is_ssl", False)
    init_title = detail.get("initiative_title", "")
    if "ssl" in init_title.lower():
        is_ssl = True

    hours = detail.get("hours", "")
    if not hours and detail.get("date_type") == "ongoing":
        hours = "Ongoing"

    return {
        "id": detail["id"],
        "needtitle": detail.get("title", ""),
        "agencyname": detail.get("organization", ""),
        "needdetails": detail.get("description", ""),
        "needlinkURL": detail.get("url", ""),
        "signupURL": detail.get("url", ""),
        "needaddress": full_addr,
        "needcity": city,
        "needstate": "MD",
        "needzip": zip_code,
        "needdatetype": detail.get("date_type", "ongoing"),
        "needdate": detail.get("event_date", ""),
        "registrationclosed": "",
        "needhoursdescription": hours,
        "needagerequirements": detail.get("age_requirements", ""),
        "minAge": detail.get("min_age", 0),
        "maxAge": detail.get("max_age", 0),
        "needallowteams": detail.get("allow_teams", False),
        "needvolunteersneeded": detail.get("capacity", 0),
        "needcontact": detail.get("contact", ""),
        "interests": ", ".join(detail.get("interests", [])),
        "qualifications": "",
        "initiativetitle": "MCPS SSL" if is_ssl else init_title,
        "isSSL": is_ssl,
        "isFamilyFriendly": detail.get("is_family_friendly", False),
        "isOutdoors": detail.get("is_outdoors", False),
        "isVirtual": detail.get("is_virtual", False),
        "tags": [],
        "dateadded": "",
        "dateupdated": "",
        "agencyid": "",
    }


# ── FULL MODE ─────────────────────────────────────────────────────

async def run_full():
    """Full scrape: all pages, all opportunities."""
    print("🔄 MODE: FULL SCRAPE")

    # Collect ALL ids
    all_ids = await collect_ids(LISTING_URL, "ALL", max_pages=0)

    # Collect SSL ids to tag them
    ssl_ids_list = await collect_ids(SSL_LISTING_URL, "SSL", max_pages=0)
    ssl_ids = set(ssl_ids_list)
    print(f"\n🎓 {len(ssl_ids)} SSL-approved IDs")

    # Merge and dedup
    final_ids = list(dict.fromkeys(all_ids + ssl_ids_list))
    print(f"\n📄 Fetching {len(final_ids)} detail pages...")

    results = []
    errors = 0
    for i, oid in enumerate(final_ids):
        detail = fetch_detail(oid)
        if "error" in detail:
            errors += 1
        else:
            if oid in ssl_ids:
                detail["is_ssl"] = True
            results.append(normalize(detail))

        if (i + 1) % 50 == 0 or (i + 1) == len(final_ids):
            print(f"    Progress: {i + 1}/{len(final_ids)} ({errors} errors)")
        time.sleep(REQUEST_DELAY)

    # Save known IDs for quick mode
    save_known_ids([r["id"] for r in results])

    return results


# ── QUICK MODE ────────────────────────────────────────────────────

async def run_quick():
    """Quick check: only first few pages newest-first, skip known IDs."""
    print("⚡ MODE: QUICK CHECK (newest only)")

    # Load known IDs
    known_ids = load_known_ids()
    print(f"   {len(known_ids)} previously known IDs")

    # Check first few pages for new IDs
    new_ids_list = await collect_ids(LISTING_URL, "NEW CHECK", max_pages=QUICK_PAGES)

    # Filter to only truly new ones
    new_ids = [oid for oid in new_ids_list if oid not in known_ids]

    if not new_ids:
        print("\n✅ No new opportunities found. Skipping detail fetch.")
        return None  # Signal: no changes

    print(f"\n🆕 Found {len(new_ids)} NEW opportunities!")

    # Also quick-check SSL status for new ones
    ssl_ids_list = await collect_ids(SSL_LISTING_URL, "SSL CHECK", max_pages=QUICK_PAGES)
    ssl_ids = set(ssl_ids_list)

    # Fetch details only for new ones
    print(f"\n📄 Fetching {len(new_ids)} new detail pages...")
    new_results = []
    errors = 0
    for i, oid in enumerate(new_ids):
        detail = fetch_detail(oid)
        if "error" in detail:
            errors += 1
        else:
            if oid in ssl_ids:
                detail["is_ssl"] = True
            new_results.append(normalize(detail))

        if (i + 1) % 10 == 0 or (i + 1) == len(new_ids):
            print(f"    Progress: {i + 1}/{len(new_ids)} ({errors} errors)")
        time.sleep(REQUEST_DELAY)

    # Merge with existing data
    existing = load_existing_opportunities()
    existing_map = {o["id"]: o for o in existing}

    for opp in new_results:
        existing_map[opp["id"]] = opp

    merged = list(existing_map.values())

    # Update known IDs
    save_known_ids([r["id"] for r in merged])

    return merged


# ── Data persistence helpers ──────────────────────────────────────

def load_known_ids():
    """Load set of previously seen opportunity IDs."""
    if KNOWN_IDS_FILE.exists():
        try:
            data = json.loads(KNOWN_IDS_FILE.read_text())
            return set(data.get("ids", []))
        except Exception:
            pass
    return set()


def save_known_ids(ids):
    """Save known IDs for next quick-check run."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    KNOWN_IDS_FILE.write_text(json.dumps({
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(ids),
        "ids": list(ids),
    }, indent=2))


def load_existing_opportunities():
    """Load existing opportunities.json."""
    if OUTPUT_FILE.exists():
        try:
            data = json.loads(OUTPUT_FILE.read_text())
            return data.get("opportunities", [])
        except Exception:
            pass
    return []


def write_output(results):
    """Write final output files."""
    ssl_count = sum(1 for o in results if o.get("isSSL"))

    output = {
        "metadata": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": "montgomerycountymd.galaxydigital.com",
            "total_count": len(results),
            "ssl_count": ssl_count,
        },
        "opportunities": results,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))

    (OUTPUT_DIR / "index.html").write_text(
        f'<!DOCTYPE html><html><head><title>SSL Finder API</title></head>'
        f'<body><h1>SSL Finder Data</h1>'
        f'<p>Last updated: {datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")}</p>'
        f'<p><a href="opportunities.json">opportunities.json</a></p>'
        f'<p>{len(results)} total ({ssl_count} SSL-approved)</p>'
        f'</body></html>'
    )

    return ssl_count


# ── Main ──────────────────────────────────────────────────────────

async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    mode = mode.lower().strip()

    print("=" * 60)
    print(f"SSL Finder Scraper v4.1 — {mode.upper()} mode")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    if mode == "quick":
        results = await run_quick()
        if results is None:
            print("\n💤 No changes. Exiting without commit.")
            # Touch a file so the workflow knows no commit needed
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            (OUTPUT_DIR / ".last_check").write_text(datetime.now(timezone.utc).isoformat())
            return
    else:
        results = await run_full()

    ssl_count = write_output(results)

    print(f"\n📊 Final results:")
    print(f"   Total: {len(results)}")
    print(f"   SSL: {ssl_count}")
    print(f"   Non-SSL: {len(results) - ssl_count}")

    for opp in results[:3]:
        print(f"\n   [{opp['id']}] {opp['needtitle'][:60]}")
        print(f"     Org: {opp['agencyname'][:40]} | City: {opp['needcity']} | SSL: {opp['isSSL']}")

    print(f"\n💾 Wrote {len(results)} opportunities to {OUTPUT_FILE}")
    print("✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
