"""
SSL Finder Scraper v3
Scrapes ALL volunteer opportunities from Montgomery County Volunteer Center
using HTTP pagination (no browser needed).

URL patterns:
  Listing: /need/index/{offset}  (offset increments by 12)
  Newest:  /need/index/{offset}/?dir=DESC&orderby=need_id
  SSL:     /need/index/{offset}/?need_init_id=2962&s=1
  Detail:  /need/detail/?need_id={id}
"""

import json
import re
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://montgomerycountymd.galaxydigital.com"
LISTING_URL = f"{BASE_URL}/need/index/{{offset}}/?s=1&dir=DESC&orderby=need_id"
SSL_LISTING_URL = f"{BASE_URL}/need/index/{{offset}}/?need_init_id=2962&s=1&dir=DESC&orderby=need_id"
DETAIL_URL = f"{BASE_URL}/need/detail/?need_id={{id}}"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "opportunities.json"
PER_PAGE = 12
REQUEST_DELAY = 0.5  # seconds between requests to be polite
MAX_OPPORTUNITIES = 200  # Set to 0 for unlimited

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

# ── Cleaning helpers ──────────────────────────────────────────────

JUNK = [
    "Get Connected Icon", "Posted By", "Back to Volunteer Center Home",
    "Share Opportunity", "Respond as Group", "Site Supervisor",
    "Skip to main content", "Open side bar", "Collapse Menu",
    "Open top navigation menu",
]


def clean(text):
    """Remove junk phrases and collapse whitespace."""
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
    """Parse address lines into (full_address, city, zip)."""
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


# ── Listing page scraper ─────────────────────────────────────────

def get_page(url):
    """Fetch a page and return BeautifulSoup."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def scrape_listing_page(offset, ssl_only=False):
    """Scrape one page of the listing table. Returns list of {id, title, org, date, url}."""
    template = SSL_LISTING_URL if ssl_only else LISTING_URL
    url = template.format(offset=offset)

    try:
        soup = get_page(url)
    except Exception as e:
        print(f"    Error fetching offset {offset}: {e}")
        return []

    opportunities = []

    # Find all links to detail pages
    for link in soup.find_all("a", href=re.compile(r"need_id=\d+")):
        href = link.get("href", "")
        m = re.search(r"need_id=(\d+)", href)
        if not m:
            continue
        opp_id = m.group(1)

        # Get the title from the link text
        title = clean(link.get_text())
        if not title or len(title) < 3:
            continue

        # Try to find the parent row to get org and date
        row = link.find_parent("tr") or link.find_parent("div")
        org = ""
        date_text = ""
        if row:
            # Organization is usually in a smaller text under the title
            small = row.find("small") or row.find(class_=re.compile(r"agency|org"))
            if small:
                org = clean(small.get_text())
            # Date
            cells = row.find_all("td")
            for cell in cells:
                text = cell.get_text().strip()
                if "Ongoing" in text or "Happens" in text or re.search(r'\d{4}', text):
                    date_text = clean(text)

        full_url = href if href.startswith("http") else f"{BASE_URL}{href}"

        opportunities.append({
            "id": opp_id,
            "title": title,
            "organization": org,
            "date_text": date_text,
            "url": full_url,
        })

    # Deduplicate by ID within this page
    seen = set()
    unique = []
    for o in opportunities:
        if o["id"] not in seen:
            seen.add(o["id"])
            unique.append(o)
    return unique


def scrape_all_listings(ssl_only=False):
    """Scrape all pages of listings."""
    all_opps = []
    seen_ids = set()
    offset = 0
    empty_count = 0

    label = "SSL" if ssl_only else "ALL"
    print(f"\n📋 Scraping {label} listing pages...")

    while empty_count < 3:  # Stop after 3 empty pages in a row
        if MAX_OPPORTUNITIES > 0 and len(all_opps) >= MAX_OPPORTUNITIES:
            print(f"    Reached limit of {MAX_OPPORTUNITIES} opportunities")
            break

        results = scrape_listing_page(offset, ssl_only)

        new_count = 0
        for opp in results:
            if opp["id"] not in seen_ids:
                seen_ids.add(opp["id"])
                all_opps.append(opp)
                new_count += 1

        if new_count == 0:
            empty_count += 1
        else:
            empty_count = 0

        page_num = (offset // PER_PAGE) + 1
        if page_num % 10 == 0 or new_count == 0:
            print(f"    Page {page_num}: {new_count} new (total: {len(all_opps)})")

        offset += PER_PAGE
        time.sleep(REQUEST_DELAY)

    print(f"  ✅ Found {len(all_opps)} unique {label} opportunities across {offset // PER_PAGE} pages")
    return all_opps


# ── Detail page scraper ──────────────────────────────────────────

def scrape_detail(opp_id):
    """Scrape a single opportunity detail page."""
    url = DETAIL_URL.format(id=opp_id)
    try:
        soup = get_page(url)
    except Exception as e:
        print(f"    Error fetching detail {opp_id}: {e}")
        return {}

    result = {}

    # Title - h1 or h2
    h1 = soup.find("h1")
    if h1:
        result["title"] = clean(h1.get_text())

    # Description - content after "Description" header
    desc = ""
    for header in soup.find_all(["h2", "h3", "h4"]):
        if "Description" in header.get_text():
            # Collect all following siblings until next header
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

    # Details section - age, family friendly, outdoors, etc.
    details_section = {}
    for header in soup.find_all(["h2", "h3"]):
        if "Details" in header.get_text() and "Description" not in header.get_text():
            table = header.find_next("table")
            if table:
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        details_section[clean(cells[0].get_text())] = clean(cells[1].get_text())
            break

    # Extract age requirements from details
    age_req = ""
    for key, val in details_section.items():
        if "age" in val.lower() or "between" in val.lower():
            age_req = val
            break
    result["age_requirements"] = age_req

    # Is family friendly, outdoors
    page_text = soup.get_text().lower()
    result["is_family_friendly"] = "family friendly" in page_text
    result["is_outdoors"] = "is outdoors" in page_text
    result["is_virtual"] = "virtual" in page_text

    # Location section
    location_parts = []
    for header in soup.find_all(["h2", "h3"]):
        if header.get_text().strip() == "Location":
            table = header.find_next("table")
            if table:
                for row in table.find_all("tr"):
                    text = clean(row.get_text())
                    if text:
                        location_parts.append(text)
            break
    result["location_lines"] = location_parts

    # Date/time info from the table near the top
    # Look for "ongoing" or "Happens On" or time ranges
    date_type = "ongoing"
    event_date = ""
    hours = ""
    for table in soup.find_all("table"):
        text = table.get_text()
        if "ongoing" in text.lower():
            date_type = "ongoing"
        date_match = re.search(r"Happens On\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})", text)
        if date_match:
            event_date = date_match.group(1)
            date_type = event_date
        time_match = re.findall(
            r'\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)\s*[-–to]+\s*\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)',
            text
        )
        if time_match:
            hours = "; ".join(time_match)
            break

    result["date_type"] = date_type
    result["event_date"] = event_date
    result["hours"] = hours

    # Interests/categories
    interests = []
    for li in soup.find_all("li"):
        text = li.get_text().strip()
        # Interest items are usually short category names
        if text and len(text) < 50 and "/" in text or text in [
            "Recreation / Sports", "Food Prep & Delivery", "Housing / Shelter",
            "Events / Collections", "Collection Drive", "Court Ordered",
            "Education / Mentoring", "Arts / Culture", "Professional Skills",
            "Environment", "Health / Wellness", "Technology", "Animals",
            "Community Building", "Advocacy",
        ]:
            interests.append(text)
    # Also check for interest items in specific containers
    for el in soup.find_all(class_=re.compile(r"interest|category|init")):
        text = el.get_text().strip()
        if text and len(text) < 50 and text not in interests:
            interests.append(text)
    result["interests"] = interests

    # Organization
    org = ""
    for header in soup.find_all(["h2", "h3"]):
        if header.get_text().strip() == "Organization":
            link = header.find_next("a")
            if link:
                org = clean(link.get_text())
            break
    result["organization"] = org

    # SSL status
    result["is_ssl"] = "mcps ssl" in page_text or "ssl approved" in page_text or "student service learning" in page_text

    # Initiative title
    init_title = ""
    breadcrumb = soup.find("ol") or soup.find(class_=re.compile(r"breadcrumb"))
    if breadcrumb:
        for a in breadcrumb.find_all("a"):
            if "init" in a.get("href", ""):
                init_title = clean(a.get_text())
    result["initiative_title"] = init_title

    # Contact / Site Supervisor
    contact_name = ""
    contact_email = ""
    contact_phone = ""
    for header in soup.find_all(["h2", "h3"]):
        if "Supervisor" in header.get_text() or "Contact" in header.get_text():
            table = header.find_next("table")
            if table:
                for row in table.find_all("tr"):
                    text = clean(row.get_text())
                    email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w+', text)
                    phone_match = re.search(r'[\d\(\)]{3,}[\s\-\.\d\(\)ext]+\d', text)
                    if email_match:
                        contact_email = email_match.group(0)
                    elif phone_match:
                        contact_phone = phone_match.group(0).strip()
                    elif text and not contact_name and len(text) > 2:
                        contact_name = text
            break

    contact_parts = [p for p in [contact_name, contact_email, contact_phone] if p]
    result["contact"] = " | ".join(contact_parts)

    # Allow teams
    result["allow_teams"] = "respond as group" in page_text or "group" in page_text

    # Volunteers needed
    cap = 0
    cap_match = re.search(r'(\d+)\s*(?:volunteers?\s*needed|spots?\s*(?:left|available))', page_text)
    if cap_match:
        cap = int(cap_match.group(1))
    result["capacity"] = cap

    return result


# ── Normalize ─────────────────────────────────────────────────────

def normalize_opportunity(listing, detail):
    """Combine listing + detail data into final format."""
    title = detail.get("title") or listing.get("title", "")
    org = detail.get("organization") or listing.get("organization", "")

    # Parse address
    location_lines = detail.get("location_lines", [])
    full_addr, city, zip_code = parse_address_block(location_lines)

    # Build contact
    contact = detail.get("contact", "")

    # Interests
    interests_list = detail.get("interests", [])
    interests = ", ".join(interests_list) if interests_list else ""

    # SSL status
    is_ssl = detail.get("is_ssl", False)
    init_title = detail.get("initiative_title", "")
    if "ssl" in init_title.lower():
        is_ssl = True

    # Hours
    hours = detail.get("hours", "")
    if not hours and detail.get("date_type") == "ongoing":
        hours = "Ongoing"

    # Age
    age_req = detail.get("age_requirements", "")
    # Extract min/max age numbers for filtering
    min_age = 0
    max_age = 0
    age_nums = re.findall(r'(\d+)', age_req)
    if age_nums:
        min_age = int(age_nums[0])
        if len(age_nums) > 1:
            max_age = int(age_nums[1])

    return {
        "id": listing["id"],
        "needtitle": title,
        "agencyname": org,
        "needdetails": detail.get("description", ""),
        "needlinkURL": listing.get("url", ""),
        "signupURL": listing.get("url", ""),
        "needaddress": full_addr,
        "needcity": city,
        "needstate": "MD",
        "needzip": zip_code,
        "needdatetype": detail.get("date_type", "ongoing"),
        "needdate": detail.get("event_date", ""),
        "registrationclosed": "",
        "needhoursdescription": hours,
        "needagerequirements": age_req,
        "minAge": min_age,
        "maxAge": max_age,
        "needallowteams": detail.get("allow_teams", False),
        "needvolunteersneeded": detail.get("capacity", 0),
        "needcontact": contact,
        "interests": interests,
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

def main():
    print("=" * 60)
    print("SSL Finder Scraper v3 (HTTP + Pagination)")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # Step 1: Get all listing pages
    all_listings = scrape_all_listings(ssl_only=False)

    # Step 2: Also get SSL listings to tag them
    ssl_listings = scrape_all_listings(ssl_only=True)
    ssl_ids = {o["id"] for o in ssl_listings}
    print(f"\n🎓 {len(ssl_ids)} opportunities are SSL-approved")

    # Step 3: Scrape detail pages for all opportunities
    print(f"\n📄 Scraping {len(all_listings)} detail pages...")
    normalized = []
    errors = 0

    for i, listing in enumerate(all_listings):
        try:
            detail = scrape_detail(listing["id"])
            if listing["id"] in ssl_ids:
                detail["is_ssl"] = True
            opp = normalize_opportunity(listing, detail)
            normalized.append(opp)
        except Exception as e:
            print(f"    Error on {listing['id']}: {e}")
            errors += 1

        if (i + 1) % 50 == 0 or (i + 1) == len(all_listings):
            print(f"    Progress: {i + 1}/{len(all_listings)} ({errors} errors)")

        time.sleep(REQUEST_DELAY)

    # Stats
    ssl_count = sum(1 for o in normalized if o.get("isSSL"))
    print(f"\n📊 Results:")
    print(f"   Total: {len(normalized)}")
    print(f"   SSL: {ssl_count}")
    print(f"   Non-SSL: {len(normalized) - ssl_count}")
    print(f"   Errors: {errors}")

    # Sample
    for opp in normalized[:3]:
        print(f"\n   [{opp['id']}] {opp['needtitle'][:60]}")
        print(f"     Org: {opp['agencyname'][:40]}")
        print(f"     City: {opp['needcity']} | SSL: {opp['isSSL']}")

    # Write output
    output = {
        "metadata": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": "montgomerycountymd.galaxydigital.com",
            "total_count": len(normalized),
            "ssl_count": ssl_count,
        },
        "opportunities": normalized,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\n💾 Wrote {len(normalized)} opportunities to {OUTPUT_FILE}")

    # Index page
    (OUTPUT_DIR / "index.html").write_text(
        f'<!DOCTYPE html><html><head><title>SSL Finder API</title></head>'
        f'<body><h1>SSL Finder Data</h1>'
        f'<p>Last updated: {datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")}</p>'
        f'<p><a href="opportunities.json">opportunities.json</a></p>'
        f'<p>{len(normalized)} total opportunities ({ssl_count} SSL-approved)</p>'
        f'</body></html>'
    )
    print("✅ Done!")


if __name__ == "__main__":
    main()
