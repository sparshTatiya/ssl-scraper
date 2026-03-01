"""
SSL Opportunity Scraper for Montgomery County Volunteer Center
Scrapes live volunteer opportunities from montgomerycountymd.galaxydigital.com
and outputs a JSON file for the iOS app to consume.
"""

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright

SITE_URL = "https://montgomerycountymd.galaxydigital.com/need/"
SSL_URL = "https://montgomerycountymd.galaxydigital.com/need/?s=1&need_init_id=2962"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "opportunities.json"
MAX_LOAD_MORE_CLICKS = 30
WAIT_TIMEOUT = 15000


async def scrape_opportunities(url: str, ssl_only: bool = False) -> list[dict]:
    """Scrape opportunities from the Galaxy Digital volunteer center."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        print(f"  Navigating to {url}...")
        await page.goto(url, wait_until="networkidle", timeout=30000)

        # Wait for cards to appear
        try:
            await page.wait_for_selector(
                ".need-card, .card-item, .need-list-item, [class*='need'], [class*='Card']",
                timeout=WAIT_TIMEOUT
            )
            print("  Page loaded, opportunity cards found.")
        except Exception:
            print("  Warning: Could not find cards with expected selectors. Trying fallback...")

        # Click "Load More" repeatedly
        for i in range(MAX_LOAD_MORE_CLICKS):
            try:
                load_more = page.locator(
                    "button:has-text('Load More'), a:has-text('Load More'), "
                    ".load-more-btn, [class*='load-more'], button:has-text('Show More')"
                )
                if await load_more.count() > 0 and await load_more.first.is_visible():
                    await load_more.first.click()
                    await page.wait_for_timeout(2000)
                    if (i + 1) % 5 == 0:
                        print(f"    Clicked 'Load More' {i + 1} times...")
                else:
                    print(f"    No more 'Load More' button after {i} clicks.")
                    break
            except Exception:
                break

        # Extract opportunity links and basic info from the listing page
        opportunities = await page.evaluate("""
            () => {
                // Find all links to detail pages
                const links = document.querySelectorAll('a[href*="/need/detail/"], a[href*="need_id="]');
                const seen = new Set();
                const results = [];

                for (const a of links) {
                    const href = a.href;
                    if (seen.has(href)) continue;
                    seen.add(href);

                    // Walk up to find the card container
                    let card = a.closest(
                        '.need-card, .card-item, .need-list-item, [class*="Card"], ' +
                        '[class*="card"], li, article, .col-sm-6, .col-md-4'
                    ) || a.parentElement;

                    // Extract text from various possible child elements
                    const getText = (selectors) => {
                        for (const sel of selectors) {
                            const el = card.querySelector(sel);
                            if (el && el.textContent.trim()) return el.textContent.trim();
                        }
                        return '';
                    };

                    results.push({
                        url: href,
                        id: (href.match(/need_id=(\d+)/) || href.match(/detail\/(\d+)/) || ['',''])[1],
                        title: getText(['h3', 'h4', 'h2', '.title', '[class*="title"]', 'strong']),
                        organization: getText(['.agency', '.org', '[class*="agency"]', '[class*="org"]', 'small']),
                        location: getText(['.location', '.address', '[class*="location"]', '[class*="address"]']),
                        date: getText(['.date', '[class*="date"]', 'time']),
                        tags: Array.from(
                            card.querySelectorAll('.tag, .badge, .label, .chip, [class*="tag"], [class*="badge"], [class*="init"]')
                        ).map(t => t.textContent.trim()),
                    });
                }
                return results;
            }
        """)

        print(f"  Found {len(opportunities)} unique opportunity links.")

        # Visit each detail page
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
                        const getHTML = (sels) => {
                            for (const sel of sels) {
                                const el = document.querySelector(sel);
                                if (el) return el.innerHTML;
                            }
                            return '';
                        };

                        const pageText = document.body.textContent.toLowerCase();

                        return {
                            title: getText(['h1', 'h2', '.need-title', '[class*="need-title"]']),
                            organization: getText(['.agency-name', '.org-name', '[class*="agency"]', '[class*="org-name"]']),
                            descriptionHTML: getHTML(['.need-details', '.need-description', '.description', '[class*="details"]', '[class*="description"]']),
                            descriptionText: getText(['.need-details', '.need-description', '.description', '[class*="details"]', '[class*="description"]']),
                            address: getText(['.address', '.location-address', '[class*="address"]']),
                            city: getText(['.city', '[class*="city"]']),
                            dateInfo: getText(['.date-type', '.schedule', '[class*="date"]', '[class*="schedule"]']),
                            hours: getText(['.hours', '.time-desc', '[class*="hours"]', '[class*="time"]']),
                            ageRequirements: getText(['.age', '.age-req', '[class*="age"]']),
                            contact: getText(['.contact', '.additional-contact', '[class*="contact"]']),
                            interests: Array.from(document.querySelectorAll('.interest-item, .category, [class*="interest"], [class*="category"]')).map(e => e.textContent.trim()),
                            qualifications: getText(['.qualifications', '[class*="qualification"]']),
                            capacity: getText(['.capacity', '.spots', '[class*="capacity"], [class*="spots"]']),
                            allowTeams: pageText.includes('ok for groups') || pageText.includes('teams welcome') || pageText.includes('group'),
                            isSSL: pageText.includes('mcps ssl') || pageText.includes('ssl approved') || pageText.includes('ssl hours') || pageText.includes('student service learning'),
                        };
                    }
                """)

                merged = {**opp, **detail}
                detailed.append(merged)

                if (i + 1) % 10 == 0 or (i + 1) == len(opportunities):
                    print(f"    Scraped detail {i + 1}/{len(opportunities)}")

            except Exception as e:
                print(f"    Warning: Failed to scrape '{opp.get('title', '?')}': {e}")
                detailed.append(opp)

        await browser.close()
    return detailed


def clean_html(html: str) -> str:
    """Strip HTML tags and clean up text."""
    text = re.sub(r'<img[^>]*>', '', html)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    for entity, char in [('&amp;','&'), ('&lt;','<'), ('&gt;','>'), ('&quot;','"'), ('&#x27;',"'"), ('&nbsp;',' ')]:
        text = text.replace(entity, char)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def normalize(raw: list[dict]) -> list[dict]:
    """Normalize scraped data into a consistent format for the app."""
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

        title = opp.get("title", "").strip()
        if not title:
            continue

        tags = opp.get("tags", [])
        is_ssl = opp.get("isSSL", False) or any("ssl" in t.lower() for t in tags)

        desc_text = opp.get("descriptionText", "") or clean_html(opp.get("descriptionHTML", ""))

        # Parse city from address
        addr = opp.get("address", "") or opp.get("location", "")
        city = opp.get("city", "")
        if not city and addr:
            parts = [p.strip() for p in addr.split(",")]
            if len(parts) >= 2:
                city = parts[-2] if len(parts) >= 3 else parts[0]

        # Parse capacity
        cap = 0
        cap_text = opp.get("capacity", "")
        cap_match = re.search(r'(\d+)', cap_text)
        if cap_match:
            cap = int(cap_match.group(1))

        normalized.append({
            "id": opp_id,
            "needtitle": title,
            "agencyname": opp.get("organization", ""),
            "needdetails": desc_text,
            "needlinkURL": opp.get("url", ""),
            "signupURL": opp.get("url", ""),
            "needaddress": addr,
            "needcity": city,
            "needstate": "MD",
            "needzip": "",
            "needdatetype": "ongoing",
            "needdate": opp.get("dateInfo", ""),
            "needhoursdescription": opp.get("hours", ""),
            "needagerequirements": opp.get("ageRequirements", ""),
            "needallowteams": opp.get("allowTeams", False),
            "needvolunteersneeded": cap,
            "needcontact": opp.get("contact", ""),
            "interests": ", ".join(opp.get("interests", [])) if isinstance(opp.get("interests"), list) else str(opp.get("interests", "")),
            "qualifications": opp.get("qualifications", ""),
            "initiativetitle": "MCPS SSL" if is_ssl else "",
            "isSSL": is_ssl,
            "tags": tags,
        })

    return normalized


async def main():
    print("=" * 60)
    print("SSL Finder Scraper")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    print("\n📋 Scraping ALL opportunities...")
    all_opps = await scrape_opportunities(SITE_URL)

    print("\n🎓 Scraping SSL-filtered opportunities...")
    ssl_opps = await scrape_opportunities(SSL_URL, ssl_only=True)

    # Mark SSL results
    ssl_ids = set()
    for opp in ssl_opps:
        opp["isSSL"] = True
        if opp.get("id"):
            ssl_ids.add(opp["id"])

    # Merge
    combined = all_opps + ssl_opps
    for opp in combined:
        if opp.get("id") in ssl_ids:
            opp["isSSL"] = True

    normalized = normalize(combined)

    print(f"\n📊 Results:")
    print(f"   Total unique opportunities: {len(normalized)}")
    print(f"   SSL-approved: {sum(1 for o in normalized if o.get('isSSL'))}")

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

    # Index page for GitHub Pages
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
