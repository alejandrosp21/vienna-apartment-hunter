#!/usr/bin/env python3
"""
Vienna Apartment Hunter
-----------------------
Scrapes willhaben.at, immobilienscout24.at, and wohnnet.at every few minutes
via GitHub Actions and sends instant Telegram notifications for new listings.
"""

import requests
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_PRICE = 1000
MIN_SIZE = 30

# Target PLZ for willhaben post-filtering
TARGET_PLZ = {"1010", "1020", "1030", "1040", "1050", "1090", "1200"}

# Immoscout region codes (from user's actual search URL)
IMMOSCOUT_REGIONS = "009001001,009001002,009001003,009001004,009001005,009001009,009001020"

# Wohnnet district codes (from user's actual search URL)
WOHNNET_DISTRICTS = "g90101--g90201--g90301--g90401--g90501--g90901--g92001"

PLZ_LABELS = {
    "1010": "1. Innere Stadt",
    "1020": "2. Leopoldstadt",
    "1030": "3. Landstrasse",
    "1040": "4. Wieden",
    "1050": "5. Margareten",
    "1090": "9. Alsergrund",
    "1200": "20. Brigittenau",
}

SEEN_FILE = Path("seen.json")
SEEN_MAX_AGE_DAYS = 60

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
# TELEGRAM
# ============================================================

def send_telegram(message: str) -> bool:
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
    source_icons = {
        "willhaben": "\U0001f7e1",
        "immoscout24": "\U0001f535",
        "wohnnet": "\U0001f7e2",
    }
    icon = source_icons.get(listing["source"], "\u26aa")
    lines = [f"{icon} <b>NEW: {listing['source'].upper()}</b>", ""]
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
# SEEN TRACKER
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
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_MAX_AGE_DAYS)).isoformat()
    return {k: v for k, v in seen.items() if v.get("first_seen", "9999") > cutoff}


# ============================================================
# WILLHABEN
#
# REST API with basic price/size filters only (areaId causes 400).
# Post-filter results by PLZ to target districts.
# Fallback: HTML page with rows=5.
# ============================================================

def scrape_willhaben() -> list[dict]:
    listings = []

    api_url = "https://api.willhaben.at/restapi/v2/search/atz/seo/immobilien/mietwohnungen/wien"
    params = [
        ("rows", 30),
        ("sort", 1),
        ("PRICE_TO", MAX_PRICE),
        ("ESTATE_SIZE/LIVING_AREA_FROM", MIN_SIZE),
    ]
    api_headers = {
        **HEADERS,
        "Accept": "application/json",
    }

    try:
        print("[willhaben] Fetching REST API...")
        resp = requests.get(api_url, params=params, headers=api_headers, timeout=30)
        print(f"[willhaben] Status: {resp.status_code}, Size: {len(resp.text)} chars")
        resp.raise_for_status()

        data = resp.json()
        ad_list = data.get("advertSummaryList", {}).get("advertSummary", [])
        if not ad_list:
            sr = data.get("searchResult", {})
            ad_list = sr.get("advertSummaryList", {}).get("advertSummary", [])

        print(f"[willhaben] API returned {len(ad_list)} ads")

        for ad in ad_list:
            if not isinstance(ad, dict):
                continue
            attrs = {}
            for a in ad.get("attributes", {}).get("attribute", []):
                vals = a.get("values", [])
                if vals:
                    attrs[a.get("name", "")] = vals[0]
            ad_id = str(ad.get("id", ""))
            if not ad_id:
                continue
            listings.append({
                "id": f"wh_{ad_id}",
                "source": "willhaben",
                "title": ad.get("description", "N/A"),
                "price": attrs.get("PRICE/AMOUNT", attrs.get("PRICE", "")),
                "size": attrs.get("ESTATE_SIZE/LIVING_AREA", ""),
                "rooms": attrs.get("NUMBER_OF_ROOMS", ""),
                "district": attrs.get("POSTCODE", ""),
                "location": attrs.get("LOCATION", ""),
                "url": f"https://www.willhaben.at/iad/object?adId={ad_id}",
            })

        print(f"[willhaben] Parsed: {len(listings)} listings")

        # Post-filter by target districts
        before = len(listings)
        listings = [l for l in listings if l.get("district", "") in TARGET_PLZ or not l.get("district")]
        print(f"[willhaben] District filter: {before} -> {len(listings)}")

        if not listings:
            print("[willhaben] No results after filter, trying HTML fallback...")
            listings = _willhaben_html_fallback()

    except Exception as e:
        print(f"[willhaben] ERROR: {e}")
        try:
            print("[willhaben] Trying HTML fallback after error...")
            listings = _willhaben_html_fallback()
        except Exception as e2:
            print(f"[willhaben] HTML fallback also failed: {e2}")

    return listings


def _willhaben_html_fallback() -> list[dict]:
    listings = []
    url = "https://www.willhaben.at/iad/immobilien/mietwohnungen/wien/"
    params = [
        ("rows", 5),
        ("sort", 1),
        ("PRICE_TO", MAX_PRICE),
        ("ESTATE_SIZE/LIVING_AREA_FROM", MIN_SIZE),
    ]
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
    if match:
        data = json.loads(match.group(1))
        pp = data.get("props", {}).get("pageProps", {})
        sr = pp.get("searchResult", {})
        ad_list = sr.get("advertSummaryList", {}).get("advertSummary", [])
        for ad in ad_list:
            if not isinstance(ad, dict):
                continue
            attrs = {}
            for a in ad.get("attributes", {}).get("attribute", []):
                vals = a.get("values", [])
                if vals:
                    attrs[a.get("name", "")] = vals[0]
            ad_id = str(ad.get("id", ""))
            if not ad_id:
                continue
            listings.append({
                "id": f"wh_{ad_id}",
                "source": "willhaben",
                "title": ad.get("description", "N/A"),
                "price": attrs.get("PRICE/AMOUNT", ""),
                "size": attrs.get("ESTATE_SIZE/LIVING_AREA", ""),
                "rooms": attrs.get("NUMBER_OF_ROOMS", ""),
                "district": attrs.get("POSTCODE", ""),
                "location": attrs.get("LOCATION", ""),
                "url": f"https://www.willhaben.at/iad/object?adId={ad_id}",
            })
        # Post-filter
        before = len(listings)
        listings = [l for l in listings if l.get("district", "") in TARGET_PLZ or not l.get("district")]
        print(f"[willhaben] HTML fallback found: {before}, after filter: {len(listings)}")
    return listings


# ============================================================
# IMMOSCOUT24.AT
# ============================================================

def scrape_immoscout() -> list[dict]:
    listings = []

    base_url = "https://www.immobilienscout24.at/regional/wohnung-mieten"
    params = {
        "countryCode": "AT",
        "numberOfRoomsFrom": 1,
        "primaryAreaFrom": MIN_SIZE,
        "primaryPriceTo": MAX_PRICE,
        "region": IMMOSCOUT_REGIONS,
    }

    try:
        print("[immoscout24] Fetching...")
        resp = requests.get(base_url, params=params, headers=HEADERS, timeout=30)
        print(f"[immoscout24] Status: {resp.status_code}, Size: {len(resp.text)} chars")
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld_data = json.loads(script.string)
                if isinstance(ld_data, dict) and ld_data.get("@type") == "CollectionPage":
                    items = ld_data.get("mainEntity", [])
                    if isinstance(items, list):
                        for item in items:
                            if not isinstance(item, dict):
                                continue
                            item_url = item.get("url", "")
                            id_match = re.search(r"/expose/([a-f0-9]+)", item_url)
                            if id_match:
                                addr = item.get("address", {})
                                location = ""
                                plz = ""
                                if isinstance(addr, dict):
                                    location = addr.get("streetAddress", "")
                                    plz = addr.get("postalCode", "")
                                listings.append({
                                    "id": f"is_{id_match.group(1)}",
                                    "source": "immoscout24",
                                    "title": item.get("name", "N/A"),
                                    "price": "",
                                    "size": "",
                                    "rooms": "",
                                    "district": plz,
                                    "location": location,
                                    "url": f"https://www.immobilienscout24.at{item_url}" if not item_url.startswith("http") else item_url,
                                })
                        if listings:
                            print(f"[immoscout24] JSON-LD found: {len(listings)}")
            except (json.JSONDecodeError, TypeError):
                continue

        # Strategy 2: HTML link extraction
        if not listings:
            print("[immoscout24] Using HTML link extraction...")
            seen_ids = set()
            for link in soup.select('a[href*="/expose/"]'):
                href = link.get("href", "")
                id_match = re.search(r"/expose/([a-f0-9]{10,})", href)
                if not id_match:
                    continue
                eid = id_match.group(1)
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)

                full_url = href if href.startswith("http") else f"https://www.immobilienscout24.at{href}"
                text = link.get_text(strip=True)

                price = ""
                price_match = re.search(r'(?:ab\s+)?([\d.]+(?:,\d+)?)\s*\u20ac(?!\s*/\s*m)', text)
                if price_match:
                    price = price_match.group(1).replace(".", "").replace(",", ".")

                size = ""
                size_match = re.search(r'([\d,]+)\s*m\u00b2', text)
                if size_match:
                    size = size_match.group(1).replace(",", ".")

                rooms = ""
                rooms_match = re.search(r'(\d+)\s*Zimmer', text)
                if rooms_match:
                    rooms = rooms_match.group(1)

                district = ""
                plz_match = re.search(r'(\d{4})\s*Wien', text)
                if plz_match:
                    district = plz_match.group(1)

                listings.append({
                    "id": f"is_{eid}",
                    "source": "immoscout24",
                    "title": text[:150] if text else "N/A",
                    "price": price,
                    "size": size,
                    "rooms": rooms,
                    "district": district,
                    "location": "",
                    "url": full_url,
                })

            print(f"[immoscout24] HTML links found: {len(listings)}")

    except Exception as e:
        print(f"[immoscout24] ERROR: {e}")

    return listings


# ============================================================
# WOHNNET.AT
# ============================================================

def scrape_wohnnet() -> list[dict]:
    listings = []
    url = "https://www.wohnnet.at/immobilien/mietwohnungen/wien"
    params = {
        "unterregionen": WOHNNET_DISTRICTS,
        "preis": f"-{MAX_PRICE}",
    }

    try:
        print("[wohnnet] Fetching...")
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        print(f"[wohnnet] Status: {resp.status_code}, Size: {len(resp.text)} chars")
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.select('a[href*="/immobilien/mietwohnung"]'):
            href = link.get("href", "")
            id_match = re.search(r'-(\d{6,})$', href)
            if not id_match:
                continue

            wn_id = id_match.group(1)
            full_url = href if href.startswith("http") else f"https://www.wohnnet.at{href}"

            plz = ""
            plz_match = re.search(r'mietwohnung-(\d{4})-wien', href)
            if plz_match:
                plz = plz_match.group(1)

            rooms = ""
            rooms_match = re.search(r'miete-([\d.]+)-zimmer', href)
            if rooms_match:
                rooms = rooms_match.group(1)

            text = link.get_text(strip=True)

            price = ""
            price_match = re.search(r'([\d.]+)\s*\u20ac', text)
            if price_match:
                price = price_match.group(1)

            size = ""
            size_match = re.search(r'(\d+)\s*m\u00b2', text)
            if size_match:
                size = size_match.group(1)

            listings.append({
                "id": f"wn_{wn_id}",
                "source": "wohnnet",
                "title": text[:150] if text else "N/A",
                "price": price,
                "size": size,
                "rooms": rooms,
                "district": plz,
                "location": "",
                "url": full_url,
            })

        seen_ids = set()
        unique = []
        for l in listings:
            if l["id"] not in seen_ids:
                seen_ids.add(l["id"])
                unique.append(l)
        listings = unique

        print(f"[wohnnet] Parsed: {len(listings)} listings")

    except Exception as e:
        print(f"[wohnnet] ERROR: {e}")

    return listings


# ============================================================
# HEALTH CHECK
# ============================================================

def health_check(results: dict[str, int]):
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
    print(f"Filters: max {MAX_PRICE}EUR, min {MIN_SIZE}m2")
    print(f"Districts: D1, D2, D3, D4, D5, D9, D20")
    print()

    seen = load_seen()
    seen = prune_seen(seen)
    new_count = 0
    source_counts = {}
    all_listings = []

    wh = scrape_willhaben()
    source_counts["willhaben"] = len(wh)
    all_listings.extend(wh)

    time.sleep(2)

    ims = scrape_immoscout()
    source_counts["immoscout24"] = len(ims)
    all_listings.extend(ims)

    time.sleep(2)

    wn = scrape_wohnnet()
    source_counts["wohnnet"] = len(wn)
    all_listings.extend(wn)

    for listing in all_listings:
        lid = listing["id"]
        if lid not in seen:
            msg = format_listing_message(listing)
            send_telegram(msg)
            seen[lid] = {
                "first_seen": datetime.now(timezone.utc).isoformat(),
                "source": listing["source"],
                "title": listing.get("title", "")[:80],
                "price": listing.get("price", ""),
                "url": listing.get("url", ""),
            }
            new_count += 1
            time.sleep(0.5)

    save_seen(seen)
    health_check(source_counts)

    print(f"\n{'='*50}")
    print(f"DONE: {new_count} new | {len(all_listings)} total scanned | {len(seen)} tracked")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
