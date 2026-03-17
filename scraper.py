#!/usr/bin/env python3
"""
Vienna Apartment Hunter
-----------------------
Scrapes willhaben.at, immoscout24.at, and wohnnet.at every few minutes
via GitHub Actions and sends instant Telegram notifications for new listings.

"""

import requests
import json
import os
import re
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urlencode, quote

# ============================================================
# CONFIGURATION - Adjust these to your needs
# ============================================================

# Telegram (set via GitHub Secrets)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Search filters
MAX_PRICE = 1000      # EUR (warm/total)
MIN_SIZE = 30         # m2
TARGET_PLZ = {"1010", "1020", "1030", "1040", "1090", "1200"}

# District labels for nice messages
PLZ_LABELS = {
    "1010": "1. Innere Stadt",
    "1020": "2. Leopoldstadt",
    "1030": "3. Landstrasse",
    "1040": "4. Wieden",
    "1090": "9. Alsergrund",
    "1200": "20. Brigittenau",
}

# File to track already-seen listings (committed back to repo)
SEEN_FILE = Path("seen.json")

# Max age for seen entries before pruning (days)
SEEN_MAX_AGE_DAYS = 60

# Request headers (rotate if needed)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-AT,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ============================================================
# TELEGRAM NOTIFICATIONS
# ============================================================

def send_telegram(message: str) -> bool:
    """Send a message via Telegram bot. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[TELEGRAM OFF] {message[:80]}...")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")
        return False


def format_listing_message(listing: dict) -> str:
    """Format a listing into a nice Telegram message."""
    source_icons = {
        "willhaben": "\U0001f7e1",     # yellow circle
        "immoscout24": "\U0001f535",    # blue circle
        "wohnnet": "\U0001f7e2",        # green circle
    }
    icon = source_icons.get(listing["source"], "\u26aa")
    source_name = listing["source"].upper()

    lines = [
        f"{icon} <b>NEW: {source_name}</b>",
        "",
    ]

    if listing.get("title"):
        lines.append(f"\U0001f3e0 {listing['title'][:140]}")

    details = []
    if listing.get("price"):
        details.append(f"\U0001f4b0 \u20ac{listing['price']}")
    if listing.get("size"):
        details.append(f"\U0001f4d0 {listing['size']} m\u00b2")
    if listing.get("rooms"):
        details.append(f"\U0001f6aa {listing['rooms']} Zimmer")

    if details:
        lines.append(" | ".join(details))

    if listing.get("location"):
        lines.append(f"\U0001f4cd {listing['location']}")
    if listing.get("district"):
        label = PLZ_LABELS.get(listing["district"], listing["district"])
        lines.append(f"\U0001f3d8\ufe0f Bezirk: {label}")

    lines.append("")
    lines.append(f'\U0001f517 <a href="{listing["url"]}">Listing ansehen</a>')
    lines.append(f"\n\u23f0 {datetime.now().strftime('%H:%M  %d.%m.%Y')}")

    return "\n".join(lines)


# ============================================================
# SEEN LISTINGS TRACKER
# ============================================================

def load_seen() -> dict:
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text())
        except json.JSONDecodeError:
            return {}
    return {}


def save_seen(seen: dict):
    SEEN_FILE.write_text(json.dumps(seen, indent=2, ensure_ascii=False))


def prune_seen(seen: dict) -> dict:
    """Remove entries older than SEEN_MAX_AGE_DAYS."""
    cutoff = (datetime.utcnow() - timedelta(days=SEEN_MAX_AGE_DAYS)).isoformat()
    return {
        k: v for k, v in seen.items()
        if v.get("first_seen", "9999") > cutoff
    }


# ============================================================
# WILLHABEN SCRAPER
# ============================================================

def scrape_willhaben() -> list[dict]:
    """Scrape willhaben.at mietwohnungen in Wien."""
    listings = []
    url = "https://www.willhaben.at/iad/immobilien/mietwohnungen/wien/"
    params = {
        "PRICE_TO": MAX_PRICE,
        "ESTATE_SIZE/LIVING_AREA_FROM": MIN_SIZE,
        "rows": 50,
        "sort": 1,           # newest first
        "PROPERTY_TYPE": 1,  # Wohnung
    }

    try:
        print("[willhaben] Fetching...")
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Strategy 1: Extract __NEXT_DATA__ JSON (most reliable)
        match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if match:
            data = json.loads(match.group(1))
            listings = _parse_willhaben_nextdata(data)
        else:
            # Strategy 2: HTML parsing fallback
            listings = _parse_willhaben_html(html)

        # Post-filter by target districts
        if TARGET_PLZ:
            filtered = []
            for l in listings:
                plz = l.get("district", "")
                loc = l.get("location", "").lower()
                # Accept if PLZ matches or if location mentions a target area
                if plz in TARGET_PLZ:
                    filtered.append(l)
                elif any(p in loc for p in TARGET_PLZ):
                    filtered.append(l)
                elif not plz:
                    # If no PLZ extracted, include it (manual check better than miss)
                    filtered.append(l)
            listings = filtered

        print(f"[willhaben] {len(listings)} listings after district filter")

    except Exception as e:
        print(f"[willhaben] ERROR: {e}")

    return listings


def _parse_willhaben_nextdata(data: dict) -> list[dict]:
    """Parse listings from willhaben's __NEXT_DATA__ JSON."""
    listings = []
    try:
        search_result = (
            data.get("props", {})
            .get("pageProps", {})
            .get("searchResult", {})
        )
        ad_list = (
            search_result.get("advertSummaryList", {})
            .get("advertSummary", [])
        )

        for ad in ad_list:
            # Build attribute lookup
            attrs = {}
            for a in ad.get("attributes", {}).get("attribute", []):
                vals = a.get("values", [])
                if vals:
                    attrs[a["name"]] = vals[0]

            ad_id = str(ad.get("id", ""))
            listings.append({
                "id": f"wh_{ad_id}",
                "source": "willhaben",
                "title": ad.get("description", "N/A"),
                "price": attrs.get("PRICE/AMOUNT", ""),
                "size": attrs.get("ESTATE_SIZE/LIVING_AREA", ""),
                "rooms": attrs.get("NUMBER_OF_ROOMS", ""),
                "district": attrs.get("POSTCODE", ""),
                "location": attrs.get("LOCATION", ""),
                "url": f"https://www.willhaben.at/iad/immobilien/d/mietwohnungen/wien/{ad_id}/",
            })

    except (KeyError, TypeError, IndexError) as e:
        print(f"[willhaben] JSON parse error: {e}")

    return listings


def _parse_willhaben_html(html: str) -> list[dict]:
    """Fallback HTML parser for willhaben."""
    listings = []
    soup = BeautifulSoup(html, "html.parser")

    # Try various selectors (willhaben changes these periodically)
    selectors = [
        '[data-testid="search-result-entry"]',
        '[data-testid="ad-list-item"]',
        "article.search-result-entry",
        'a[href*="/iad/immobilien/d/mietwohnungen/"]',
    ]

    found_elements = []
    for sel in selectors:
        found_elements = soup.select(sel)
        if found_elements:
            break

    for el in found_elements:
        link = el.find("a", href=True) if el.name != "a" else el
        if not link or not link.get("href"):
            continue

        href = link["href"]
        if not href.startswith("http"):
            href = "https://www.willhaben.at" + href

        id_match = re.search(r"/(\d{6,})", href)
        if not id_match:
            continue

        listings.append({
            "id": f"wh_{id_match.group(1)}",
            "source": "willhaben",
            "title": link.get_text(strip=True)[:150],
            "price": "",
            "size": "",
            "rooms": "",
            "district": "",
            "location": "",
            "url": href,
        })

    return listings


# ============================================================
# IMMOSCOUT24.AT SCRAPER
# ============================================================

def scrape_immoscout() -> list[dict]:
    """Scrape immoscout24.at for mietwohnungen in Wien."""
    listings = []
    url = "https://www.immoscout24.at/suche/mietwohnungen/wien"
    params = {
        "price": f"-{MAX_PRICE}",
        "livingspace": f"{MIN_SIZE}-",
        "sorting": "2",  # newest first
    }

    try:
        print("[immoscout24] Fetching...")
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Strategy 1: Look for JSON data in script tags
        soup = BeautifulSoup(html, "html.parser")

        # Try __NEXT_DATA__
        nd_match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if nd_match:
            try:
                data = json.loads(nd_match.group(1))
                listings = _parse_immoscout_json(data)
            except json.JSONDecodeError:
                pass

        # Strategy 2: Look for other embedded JSON
        if not listings:
            for script in soup.find_all("script"):
                text = script.string or ""
                if "resultList" in text or "searchResult" in text:
                    try:
                        # Find JSON object in script
                        json_match = re.search(r'\{.*"resultList".*\}', text, re.DOTALL)
                        if json_match:
                            data = json.loads(json_match.group())
                            listings = _parse_immoscout_json(data)
                    except (json.JSONDecodeError, AttributeError):
                        continue

        # Strategy 3: HTML fallback
        if not listings:
            listings = _parse_immoscout_html(soup)

        # Post-filter by district (check address strings)
        if TARGET_PLZ:
            filtered = []
            for l in listings:
                loc = l.get("location", "").lower()
                plz = l.get("district", "")
                if plz in TARGET_PLZ:
                    filtered.append(l)
                elif any(p in loc for p in TARGET_PLZ):
                    filtered.append(l)
                elif not plz and not loc:
                    filtered.append(l)  # Keep unknowns
            listings = filtered

        print(f"[immoscout24] {len(listings)} listings after district filter")

    except Exception as e:
        print(f"[immoscout24] ERROR: {e}")

    return listings


def _parse_immoscout_json(data: dict) -> list[dict]:
    """Try to extract listings from immoscout24 JSON data."""
    listings = []

    # Navigate various possible JSON structures
    results = []
    if "props" in data:
        pp = data["props"].get("pageProps", {})
        results = pp.get("results", pp.get("searchResult", {}).get("results", []))
    elif "resultList" in data:
        results = data["resultList"].get("results", [])
    elif "searchResult" in data:
        results = data["searchResult"].get("results", [])

    for item in results:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", item.get("realEstateId", "")))
        if not item_id:
            continue

        listings.append({
            "id": f"is_{item_id}",
            "source": "immoscout24",
            "title": item.get("title", item.get("headline", "N/A")),
            "price": str(item.get("price", item.get("netRent", ""))),
            "size": str(item.get("livingSpace", item.get("livingArea", ""))),
            "rooms": str(item.get("numberOfRooms", item.get("rooms", ""))),
            "district": str(item.get("zipCode", item.get("postcode", ""))),
            "location": item.get("address", item.get("location", "")),
            "url": f"https://www.immoscout24.at/expose/{item_id}",
        })

    return listings


def _parse_immoscout_html(soup: BeautifulSoup) -> list[dict]:
    """Fallback HTML parser for immoscout24.at."""
    listings = []

    selectors = [
        '[data-testid="result-list-item"]',
        "article.result-item",
        ".result-list__listing",
        'a[href*="/expose/"]',
    ]

    found = []
    for sel in selectors:
        found = soup.select(sel)
        if found:
            break

    for el in found:
        link = el.find("a", href=True) if el.name != "a" else el
        if not link:
            continue

        href = link.get("href", "")
        if not href.startswith("http"):
            href = "https://www.immoscout24.at" + href

        id_match = re.search(r"/expose/(\d+)", href) or re.search(r"/(\d{5,})", href)
        if not id_match:
            continue

        listings.append({
            "id": f"is_{id_match.group(1)}",
            "source": "immoscout24",
            "title": el.get_text(strip=True)[:150],
            "price": "",
            "size": "",
            "rooms": "",
            "district": "",
            "location": "",
            "url": href,
        })

    return listings


# ============================================================
# WOHNNET.AT SCRAPER
# ============================================================

def scrape_wohnnet() -> list[dict]:
    """Scrape wohnnet.at for mietwohnungen in Wien."""
    listings = []
    url = "https://www.wohnnet.at/immobilien/mietwohnungen/wien"
    params = {
        "preis-bis": MAX_PRICE,
        "flaeche-von": MIN_SIZE,
        "sortierung": "datum",
    }

    try:
        print("[wohnnet] Fetching...")
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Try embedded JSON first
        nd_match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL
        )
        if nd_match:
            try:
                data = json.loads(nd_match.group(1))
                # Wohnnet structure varies, try common paths
                pp = data.get("props", {}).get("pageProps", {})
                results = pp.get("listings", pp.get("results", []))
                for item in results:
                    if not isinstance(item, dict):
                        continue
                    item_id = str(item.get("id", ""))
                    if not item_id:
                        continue
                    listings.append({
                        "id": f"wn_{item_id}",
                        "source": "wohnnet",
                        "title": item.get("title", "N/A"),
                        "price": str(item.get("price", "")),
                        "size": str(item.get("area", item.get("livingArea", ""))),
                        "rooms": str(item.get("rooms", "")),
                        "district": str(item.get("zipCode", "")),
                        "location": item.get("address", ""),
                        "url": item.get("url", f"https://www.wohnnet.at/immobilien/{item_id}"),
                    })
            except (json.JSONDecodeError, KeyError):
                pass

        # HTML fallback
        if not listings:
            for link in soup.select('a[href*="/immobilien/"]'):
                href = link.get("href", "")
                if "mietwohnung" not in href and "inserat" not in href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.wohnnet.at" + href
                id_match = re.search(r"/(\d{5,})", href)
                if id_match:
                    listings.append({
                        "id": f"wn_{id_match.group(1)}",
                        "source": "wohnnet",
                        "title": link.get_text(strip=True)[:150],
                        "price": "",
                        "size": "",
                        "rooms": "",
                        "district": "",
                        "location": "",
                        "url": href,
                    })

        # Deduplicate
        seen_ids = set()
        unique = []
        for l in listings:
            if l["id"] not in seen_ids:
                seen_ids.add(l["id"])
                unique.append(l)
        listings = unique

        print(f"[wohnnet] {len(listings)} listings found")

    except Exception as e:
        print(f"[wohnnet] ERROR: {e}")

    return listings


# ============================================================
# HEALTH CHECK
# ============================================================

def health_check(results: dict[str, int]):
    """
    Alert if a source returns 0 listings 3+ runs in a row.
    Means the parser is probably broken.
    """
    health_file = Path("health.json")
    health = {}
    if health_file.exists():
        try:
            health = json.loads(health_file.read_text())
        except json.JSONDecodeError:
            pass

    alerts = []
    for source, count in results.items():
        if count == 0:
            health.setdefault(source, 0)
            health[source] += 1
            if health[source] >= 3:
                alerts.append(source)
        else:
            health[source] = 0

    if alerts:
        msg = (
            "\u26a0\ufe0f <b>SCRAPER HEALTH WARNING</b>\n\n"
            f"These sources returned 0 listings for 3+ runs:\n"
            + "\n".join(f"\u2022 {s}" for s in alerts)
            + "\n\nThe parser may be broken. Check the Action logs."
        )
        send_telegram(msg)

    health_file.write_text(json.dumps(health))


# ============================================================
# MAIN
# ============================================================

def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*50}")
    print(f"Vienna Apartment Hunter  |  {now}")
    print(f"{'='*50}")
    print(f"Filters: max {MAX_PRICE}EUR, min {MIN_SIZE}m2, PLZ: {', '.join(sorted(TARGET_PLZ))}")
    print()

    seen = load_seen()
    seen = prune_seen(seen)  # Clean old entries
    new_count = 0
    source_counts = {}

    # --- Scrape all sources ---
    all_listings = []

    wh = scrape_willhaben()
    source_counts["willhaben"] = len(wh)
    all_listings.extend(wh)

    time.sleep(2)  # Be polite between sites

    ims = scrape_immoscout()
    source_counts["immoscout24"] = len(ims)
    all_listings.extend(ims)

    time.sleep(2)

    wn = scrape_wohnnet()
    source_counts["wohnnet"] = len(wn)
    all_listings.extend(wn)

    # --- Process new listings ---
    for listing in all_listings:
        lid = listing["id"]
        if lid not in seen:
            msg = format_listing_message(listing)
            send_telegram(msg)
            seen[lid] = {
                "first_seen": datetime.utcnow().isoformat(),
                "source": listing["source"],
                "title": listing.get("title", "")[:80],
                "price": listing.get("price", ""),
                "url": listing.get("url", ""),
            }
            new_count += 1
            time.sleep(0.5)  # Telegram rate limit

    save_seen(seen)
    health_check(source_counts)

    print(f"\n{'='*50}")
    print(f"DONE: {new_count} new | {len(all_listings)} total scanned | {len(seen)} tracked")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
