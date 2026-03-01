"""
SSL Opportunity Scraper v2 for Montgomery County Volunteer Center
Scrapes live volunteer opportunities from montgomerycountymd.galaxydigital.com
with proper text cleaning to remove HTML artifacts.
"""

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright

SITE_URL = "https://montgomerycountymd.galaxydigital.com/need/"
SSL_URL = "https://montgomerycountymd.galaxydigital.com/need/?s=1&need_init_id=2962"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "opportunities.json"
MAX_LOAD_MORE_CLICKS = 30

# ── Cleaning helpers ──────────────────────────────────────────────

JUNK_PHRASES = [
    "Get Connected Icon", "Posted By", "Back to Volunteer Center Home",
    "Share Opportunity", "Respond as Group", "Site Supervisor",
    "No Selection",
]

KNOWN_INTERESTS = [
    "Recreation / Sports", "Food Prep & Delivery", "Housing / Shelter",
    "Events / Collections", "Collection Drive", "Court Ordered",
    "Education / Mentoring", "Arts / Culture", "Professional Skills",
    "Environment", "Health / Wellness", "Technology",
    "Animals", "Community Building", "Advocacy",
]


def deep_clean(text):
    if not text:
        return ""
    result = text
    for j in JUNK_PHRASES:
        result = result.replace(j, "")
    result = re.sub(r'\bRespond\b', '', result)
    result = re.sub(r'\bOrganization\b', '', result)
    result = re.sub(r'[\t\r]+', ' ', result)
    result = re.sub(r'\n\s*\n', '\n', result)
    result = re.sub(r' {2,}', ' ', result)
    lines = [l.strip() for l in result.split('\n') if l.strip()]
    return ' '.join(lines).strip()


def parse_address_block(raw):
    """Returns (full_address, city, zip_code) from raw address text."""
    if not raw:
        return "", "", ""
    text = raw
    for j in JUNK_PHRASES:
        text = text.replace(j, "")
    lines = [l.strip() for l in re.split(r'[\n\r]+', text) if l.strip()]

    city, zip_code, city_line_idx = "", "", -1
    for i, line in enumerate(lines):
        m = re.match(r'^([A-Za-z\s]+),\s*MD\s*$', line, re.IGNORECASE)
        if m:
            city, city_line_idx = m.group(1).strip().title(), i
            continue
        m = re.match(r'^([A-Za-z\s]+),\s*MD\s+(\d{5})', line, re.IGNORECASE)
        if m:
            city, zip_code, city_line_idx = m.group(1).strip().title(), m.group(2), i
            continue
        m = re.match(r'^(\d{5})$', line)
        if m:
            zip_code = m.group(1)

    address_parts = []
    for i, line in enumerate(lines):
        if i >= city_line_idx >= 0:
            break
        if len(line) >= 2:
            address_parts.append(line)

    street = ', '.join(address_parts)
    full = street
    if city:
        full = f"{street}, {city}, MD" if full else f"{city}, MD"
    if zip_code:
        full += f" {zip_code}"
    return full, city, zip_code


def extract_description(raw, title):
    if not raw:
        return ""
    text = raw
    desc_marker = text.find("Description")
    if desc_marker != -1:
        text = text[desc_marker + len("Description"):]
    elif title:
        title_pos = text.find(title)
        if title_pos != -1:
            text = text[title_pos + len(title):]
    for j in JUNK_PHRASES + ["Respond", "Share Opportunity"]:
        text = text.replace(j, "")
    text = re.sub(r'\bRespond\b', '', text)
    text = re.sub(r'[\t]+', ' ', text)
    text = re.sub(r' {2,}', ' ', text)
    lines = [l.strip() for l in text.split('\n') if l.strip() and l.strip() not in ['ongoing', 'Respond', '']]
    return '\n'.join(lines).strip()


def extract_hours(raw):
    if not raw:
        return ""
    cleaned = deep_clean(raw)
    times = re.findall(
        r'\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)\s*[-\u2013to]+\s*\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)',
        cleaned
    )
    if times:
        return '; '.join(times)
    sched = re.findall(
        r'(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*[-\s]+)+.*?\d{1,2}(?::\d{2})?\s*(?:am|pm).*?\d{1,2}(?::\d{2})?\s*(?:am|pm)',
        cleaned, re.IGNORECASE
    )
    if sched:
        return '; '.join(s.strip() for s in sched)
    if 'ongoing' in cleaned.lower():
        return "Ongoing"
    return ""


def extract_event_date(raw_hours, raw_date):
    for source in [raw_hours, raw_date]:
        if not source:
            continue
        match = re.search(r'Happens On\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', source)
        if match:
            return match.group(1).strip()
        match = re.search(r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4})', source)
        if match:
            return match.group(1).strip()
    return ""


def extract_contact(raw):
    if not raw:
        return ""
    cleaned = deep_clean(raw)
    email = ""
    m = re.search(r'[\w.+-]+@[\w.-]+\.\w+', cleaned)
    if m:
        email = m.group(0)
    phone = ""
    m = re.search(r'[\d\(\)]{3,}[\s\-\.\d\(\)ext]+\d', cleaned)
    if m:
        phone = m.group(0).strip()
    name_parts = []
    for w in cleaned.split():
        w = w.strip()
        if '@' in w or re.match(r'^[\d\(\)\-\.ext]+$', w):
            continue
        if w and w[0].isupper() and len(w) > 1:
            name_parts.append(w)
        elif name_parts:
            break
    name = ' '.join(name_parts) if name_parts else ""
    parts = [p for p in [name, email, phone] if p]
    return ' | '.join(parts)


def extract_interests(raw):
    if not raw:
        return ""
    cleaned = deep_clean(raw) if isinstance(raw, str) else raw
    found = []
    seen = set()
    for cat in KNOWN_INTERESTS:
        if cat.lower() in cleaned.lower() and cat not in seen:
            seen.add(cat)
            found.append(cat)
    return ", ".join(found)


# ── Scraping ──────────────────────────────────────────────────────

async def scrape_opportunities(url, ssl_only=False):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        print(f"  Navigating to {url}...")
        await page.goto(url, wait_until="networkidle", timeout=30000)

        try:
            await page.wait_for_selector(
                ".need-card, .card-item, .need-list-item, [class*='need'], [class*='Card']",
                timeout=15000
            )
            print("  Page loaded, cards found.")
        except Exception:
            print("  Warning: Could not find cards with expected selectors.")

        for i in range(MAX_LOAD_MORE_CLICKS):
            try:
                btn = page.locator(
                    "button:has-text('Load More'), a:has-text('Load More'), "
                    ".load-more-btn, [class*='load-more'], button:has-text('Show More')"
                )
                if await btn.count() > 0 and await btn.first.is_visible():
                    await btn.first.click()
                    await page.wait_for_timeout(2000)
                    if (i + 1) % 5 == 0:
                        print(f"    Clicked 'Load More' {i + 1} times...")
                else:
                    print(f"    No more 'Load More' after {i} clicks.")
                    break
            except Exception:
                break

        opportunities = await page.evaluate("""
            () => {
                const links = document.querySelectorAll('a[href*="/need/detail/"], a[href*="need_id="]');
                const seen = new Set();
                const results = [];
                for (const a of links) {
                    const href = a.href;
                    if (seen.has(href)) continue;
                    seen.add(href);
                    let card = a.closest(
                        '.need-card, .card-item, .need-list-item, [class*="Card"], ' +
                        '[class*="card"], li, article, .col-sm-6, .col-md-4'
                    ) || a.parentElement;
                    const getText = (sels) => {
                        for (const sel of sels) {
                            const el = card.querySelector(sel);
                            if (el && el.textContent.trim()) return el.textContent.trim();
                        }
                        return '';
                    };
                    results.push({
                        url: href,
                        id: (href.match(/need_id=(\\d+)/) || href.match(/detail\\/(\\d+)/) || ['',''])[1],
                        title: getText(['h3', 'h4', 'h2', '.title', '[class*="title"]', 'strong']),
                        organization: getText(['.agency', '.org', '[class*="agency"]', '[class*="org"]', 'small']),
                    });
                }
                return results;
            }
        """)

        print(f"  Found {len(opportunities)} unique links.")

        detailed = []
        for i, opp in enumerate(opportunities):
            if not opp.get("url"):
                continue
            try:
                await page.goto(opp["url"], wait_until="networkidle", timeout=15000)
                await page.wait_for_timeout(500)

                detail = await page.evaluate("""
                    () => {
                        const getText = (sels) => {
                            for (const sel of sels) {
                                const el = document.querySelector(sel);
                                if (el && el.textContent.trim()) return el.textContent.trim();
                            }
                            return '';
                        };

                        // Get description after "Description" header
                        let descText = '';
                        const headers = document.querySelectorAll('h2, h3, h4, strong, b');
                        for (const h of headers) {
                            if (h.textContent.trim() === 'Description') {
                                let el = h.nextElementSibling || h.parentElement.nextElementSibling;
                                if (el) descText = el.textContent.trim();
                                if (!descText && h.parentElement) {
                                    const parent = h.closest('section, .col-sm-8, .col-md-8, [class*="detail"]') || h.parentElement.parentElement;
                                    if (parent) {
                                        const full = parent.textContent;
                                        const idx = full.indexOf('Description');
                                        if (idx !== -1) descText = full.substring(idx + 11).trim();
                                    }
                                }
                                break;
                            }
                        }
                        if (!descText) descText = getText(['.need-description', '.description']);

                        // Address block
                        let addressText = '';
                        const addrEl = document.querySelector('.address, .location-address, [class*="address"]');
                        if (addrEl) addressText = addrEl.textContent.trim();

                        // Date
                        let dateText = '';
                        const dateMatch = document.body.textContent.match(/Happens On\\s+(\\w+\\s+\\d{1,2},?\\s+\\d{4})/);
                        if (dateMatch) dateText = dateMatch[1];

                        // Hours
                        const hoursEl = document.querySelector('.hours, .time-desc, [class*="hours"]');
                        let hoursText = hoursEl ? hoursEl.textContent.trim() : '';

                        // Contact
                        const contactEl = document.querySelector('.contact, [class*="contact"]');
                        let contactText = contactEl ? contactEl.textContent.trim() : '';

                        // Interests
                        const interests = Array.from(
                            document.querySelectorAll('.interest-item, .category, [class*="interest"], [class*="category"]')
                        ).map(e => e.textContent.trim());

                        // SSL
                        const bodyLower = document.body.textContent.toLowerCase();
                        const isSSL = bodyLower.includes('mcps ssl') || bodyLower.includes('ssl approved') ||
                                      bodyLower.includes('ssl hours') || bodyLower.includes('student service learning');

                        // Volunteers needed
                        let capacity = 0;
                        const capMatch = document.body.textContent.match(/(\\d+)\\s*(?:volunteers?\\s*needed|spots?\\s*(?:left|available|remaining))/i);
                        if (capMatch) capacity = parseInt(capMatch[1]);

                        // Teams
                        const allowTeams = bodyLower.includes('group') || bodyLower.includes('respond as group');

                        return {
                            title: getText(['h1', 'h2']),
                            organization: getText(['.agency-name', '.org-name', '[class*="agency-name"]']),
                            descriptionText: descText,
                            address: addressText,
                            dateInfo: dateText,
                            hours: hoursText,
                            contact: contactText,
                            interests: interests,
                            isSSL: isSSL,
                            capacity: capacity,
                            allowTeams: allowTeams,
                        };
                    }
                """)

                merged = {**opp, **detail}
                detailed.append(merged)
                if (i + 1) % 10 == 0 or (i + 1) == len(opportunities):
                    print(f"    Scraped detail {i + 1}/{len(opportunities)}")
            except Exception as e:
                print(f"    Warning: Failed '{opp.get('title', '?')}': {e}")
                detailed.append(opp)

        await browser.close()
    return detailed


def normalize(raw):
    normalized = []
    seen_ids = set()

    for opp in raw:
        opp_id = opp.get("id", "")
        if not opp_id:
            url = opp.get("url", "")
            m = re.search(r'need_id=(\d+)', url) or re.search(r'detail/(\d+)', url)
            opp_id = m.group(1) if m else ""
        if not opp_id or opp_id in seen_ids:
            continue
        seen_ids.add(opp_id)

        title = (opp.get("title") or "").strip()
        if not title:
            continue

        raw_addr = opp.get("address", "")
        full_addr, city, zip_code = parse_address_block(raw_addr)

        raw_desc = opp.get("descriptionText", "")
        description = extract_description(raw_desc, title)

        raw_hours = opp.get("hours", "")
        hours = extract_hours(raw_hours)

        raw_date = opp.get("dateInfo", "")
        event_date = extract_event_date(raw_hours, raw_date)

        contact = extract_contact(opp.get("contact", ""))

        raw_interests = opp.get("interests", "")
        if isinstance(raw_interests, list):
            raw_interests = ", ".join(raw_interests)
        interests = extract_interests(raw_interests)

        org = deep_clean(opp.get("organization", ""))

        is_ssl = opp.get("isSSL", False)
        tags = opp.get("tags", [])
        cap = opp.get("capacity", 0) or 0

        normalized.append({
            "id": opp_id,
            "needtitle": title,
            "agencyname": org,
            "needdetails": description,
            "needlinkURL": opp.get("url", ""),
            "signupURL": opp.get("url", ""),
            "needaddress": full_addr,
            "needcity": city,
            "needstate": "MD",
            "needzip": zip_code,
            "needdatetype": event_date if event_date else "ongoing",
            "needdate": event_date,
            "registrationclosed": "",
            "needhoursdescription": hours,
            "needagerequirements": "",
            "needallowteams": opp.get("allowTeams", False),
            "needvolunteersneeded": cap,
            "needcontact": contact,
            "interests": interests,
            "qualifications": "",
            "initiativetitle": "MCPS SSL" if is_ssl else "",
            "isSSL": is_ssl,
            "tags": tags,
            "dateadded": "",
            "dateupdated": "",
            "agencyid": "",
        })

    return normalized


async def main():
    print("=" * 60)
    print("SSL Finder Scraper v2")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    print("\n📋 Scraping ALL opportunities...")
    all_opps = await scrape_opportunities(SITE_URL)

    print("\n🎓 Scraping SSL-filtered opportunities...")
    ssl_opps = await scrape_opportunities(SSL_URL, ssl_only=True)

    ssl_ids = set()
    for opp in ssl_opps:
        opp["isSSL"] = True
        if opp.get("id"):
            ssl_ids.add(opp["id"])

    combined = all_opps + ssl_opps
    for opp in combined:
        if opp.get("id") in ssl_ids:
            opp["isSSL"] = True

    normalized = normalize(combined)

    print(f"\n📊 Results:")
    print(f"   Total unique: {len(normalized)}")
    print(f"   SSL-approved: {sum(1 for o in normalized if o.get('isSSL'))}")

    for opp in normalized[:3]:
        print(f"\n   Sample: {opp['needtitle']}")
        print(f"     Org: {opp['agencyname']}")
        print(f"     Addr: {opp['needaddress']}")
        print(f"     City: {opp['needcity']}")

    output = {
        "metadata": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": "montgomerycountymd.galaxydigital.com",
            "total_count": len(normalized),
            "ssl_count": sum(1 for o in normalized if o.get("isSSL")),
        },
        "opportunities": normalized,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\n💾 Wrote {len(normalized)} opportunities to {OUTPUT_FILE}")

    (OUTPUT_DIR / "index.html").write_text(
        f'<!DOCTYPE html><html><head><title>SSL Finder API</title></head>'
        f'<body><h1>SSL Finder Data</h1>'
        f'<p>Last updated: {datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")}</p>'
        f'<p><a href="opportunities.json">opportunities.json</a> ({len(normalized)} opportunities)</p>'
        f'</body></html>'
    )
    print("✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
