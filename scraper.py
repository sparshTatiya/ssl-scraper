"""
SSL Finder Scraper v4
Uses Playwright to navigate paginated listing, then fetches detail pages via HTTP.
Combines browser-based listing extraction with fast HTTP detail scraping.
"""

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://montgomerycountymd.galaxydigital.com"
# Use table view (list icon) sorted newest first for fastest extraction
LISTING_URL = f"{BASE_URL}/need/?s=1&dir=DESC&orderby=need_id"
SSL_LISTING_URL = f"{BASE_URL}/need/?s=1&need_init_id=2962&dir=DESC&orderby=need_id"
DETAIL_URL = f"{BASE_URL}/need/detail/?need_id={{}}"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "opportunities.json"
MAX_OPPORTUNITIES = 200  # Set to 0 for unlimited
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


# ── Phase 1: Collect all opportunity IDs from listing pages ──────

async def collect_all_ids(url, label="ALL"):
    """Use Playwright to click through pages and collect all need_ids."""
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
            # Extract all need_ids from current page
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

            if page_num % 5 == 0 or new_count == 0:
                print(f"    Page {page_num}: {new_count} new IDs (total: {len(all_ids)})")

            if new_count == 0:
                print(f"    No new IDs on page {page_num}, stopping.")
                break

            if MAX_OPPORTUNITIES > 0 and len(all_ids) >= MAX_OPPORTUNITIES:
                print(f"    Reached limit of {MAX_OPPORTUNITIES}")
                break

            # Click "Next" page button
            try:
                # Look for the ">" or "Next" or next page number
                next_btn = page.locator('a:has-text(">")')
                if await next_btn.count() > 0 and await next_btn.first.is_visible():
                    await next_btn.first.click()
                    await page.wait_for_timeout(2000)
                    page_num += 1
                else:
                    # Try clicking next page number directly
                    next_page = page.locator(f'a:has-text("{page_num + 1}")')
                    if await next_page.count() > 0:
                        await next_page.first.click()
                        await page.wait_for_timeout(2000)
                        page_num += 1
                    else:
                        print(f"    No next button found after page {page_num}")
                        break
            except Exception as e:
                print(f"    Pagination error on page {page_num}: {e}")
                break

        await browser.close()

    print(f"  ✅ Collected {len(all_ids)} {label} IDs across {page_num} pages")
    return all_ids


# ── Phase 2: Fetch detail pages via HTTP (fast) ─────────────────

def fetch_detail(opp_id):
    """Fetch and parse a single detail page."""
    url = DETAIL_URL.format(opp_id)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        return {"error": str(e)}

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
        hours = "; ".join(time_matches[:3])  # Cap at 3 time ranges

    if "ongoing" in page_text.lower() and not event_date:
        date_type = "ongoing"

    result["date_type"] = date_type
    result["event_date"] = event_date
    result["hours"] = hours

    # Details section (age, family friendly, etc.)
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

    # Interests
    interests = []
    known = [
        "Recreation / Sports", "Food Prep & Delivery", "Housing / Shelter",
        "Events / Collections", "Collection Drive", "Court Ordered",
        "Education / Mentoring", "Arts / Culture", "Professional Skills",
        "Environment", "Health / Wellness", "Technology", "Animals",
        "Community Building", "Advocacy",
    ]
    for cat in known:
        if cat.lower() in page_lower:
            interests.append(cat)
    result["interests"] = interests

    # SSL status
    result["is_ssl"] = "mcps ssl" in page_lower or "student service learning" in page_lower

    # Initiative title from breadcrumb
    init_title = ""
    breadcrumb = soup.find("ol") or soup.find(class_=re.compile(r"breadcrumb"))
    if breadcrumb:
        for a in breadcrumb.find_all("a"):
            href = a.get("href", "")
            if "init" in href:
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

    # Teams & capacity
    result["allow_teams"] = "respond as group" in page_lower
    cap = 0
    cap_match = re.search(r'(\d+)\s*(?:volunteers?\s*needed|spots?\s*(?:left|available))', page_lower)
    if cap_match:
        cap = int(cap_match.group(1))
    result["capacity"] = cap

    return result


# ── Phase 3: Normalize ───────────────────────────────────────────

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


# ── Main ──────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("SSL Finder Scraper v4 (Playwright pagination + HTTP details)")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    if MAX_OPPORTUNITIES > 0:
        print(f"Limit: {MAX_OPPORTUNITIES} opportunities")
    print("=" * 60)

    # Phase 1: Collect IDs
    all_ids = await collect_all_ids(LISTING_URL, "ALL")

    ssl_ids_list = await collect_all_ids(SSL_LISTING_URL, "SSL")
    ssl_ids = set(ssl_ids_list)
    print(f"\n🎓 {len(ssl_ids)} SSL-approved IDs identified")

    # Deduplicate - use all_ids as primary, tag SSL
    final_ids = list(dict.fromkeys(all_ids + ssl_ids_list))  # preserve order, dedup
    if MAX_OPPORTUNITIES > 0:
        final_ids = final_ids[:MAX_OPPORTUNITIES]

    # Phase 2: Fetch details
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

        if (i + 1) % 25 == 0 or (i + 1) == len(final_ids):
            print(f"    Progress: {i + 1}/{len(final_ids)} ({errors} errors)")
        time.sleep(REQUEST_DELAY)

    # Stats
    ssl_count = sum(1 for o in results if o.get("isSSL"))
    print(f"\n📊 Results:")
    print(f"   Total: {len(results)}")
    print(f"   SSL: {ssl_count}")
    print(f"   Non-SSL: {len(results) - ssl_count}")
    print(f"   Errors: {errors}")

    for opp in results[:3]:
        print(f"\n   [{opp['id']}] {opp['needtitle'][:60]}")
        print(f"     Org: {opp['agencyname'][:40]} | City: {opp['needcity']} | SSL: {opp['isSSL']}")

    # Write
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
    print(f"\n💾 Wrote {len(results)} opportunities to {OUTPUT_FILE}")

    (OUTPUT_DIR / "index.html").write_text(
        f'<!DOCTYPE html><html><head><title>SSL Finder API</title></head>'
        f'<body><h1>SSL Finder Data</h1>'
        f'<p>Last updated: {datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")}</p>'
        f'<p><a href="opportunities.json">opportunities.json</a></p>'
        f'<p>{len(results)} total ({ssl_count} SSL-approved)</p>'
        f'</body></html>'
    )
    print("✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
