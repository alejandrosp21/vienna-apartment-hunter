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

MAX_PRICE = 1000      # EUR (warm/total)
MIN_SIZE = 30         # m2
TARGET_PLZ = {"1010", "1020", "1030", "1040", "1090", "1200"}

PLZ_LABELS = {
    "1010": "1. Innere Stadt",
    "1020": "2. Leopoldstadt",
    "1030": "3. Landstrasse",
    "1040": "4. Wieden",
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
# ============================================================

def scrape_willhaben() -> list[dict]:
    listings = []
    url = "https://www.willhaben.at/iad/immobilien/mietwohnungen/wien/"
    params = {
        "PRICE_TO": MAX_PRICE,
        "ESTATE_SIZE/LIVING_AREA_FROM": MIN_SIZE,
        "rows": 50,
        "sort": 1,
        "PROPERTY_TYPE": 1,
    }

    try:
        print("[willhaben] Fetching...")
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        print(f"[willhaben] Status: {resp.status_code}, Size: {len(resp.text)} chars")
        print(f"[willhaben] Final URL: {resp.url}")
        resp.raise_for_status()
        html = resp.text

        if len(html) < 1000:
            print(f"[willhaben] WARNING: Very short response. Full content:\n{html}")
        elif "captcha" in html.lower() or "robot" in html.lower():
            print("[willhaben] WARNING: Possible captcha/bot detection page")

        # Strategy 1: __NEXT_DATA__ JSON
        match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if match:
            print("[willhaben] Found __NEXT_DATA__, parsing JSON...")
            data = json.loads(match.group(1))

            pp = data.get("props", {}).get("pageProps", {})
            print(f"[willhaben] pageProps keys: {list(pp.keys())[:15]}")

            listings = _parse_willhaben_nextdata(data)
            print(f"[willhaben] JSON parser found: {len(listings)} listings")
        else:
            print("[willhaben] No __NEXT_DATA__ found, trying HTML...")

            soup = BeautifulSoup(html, "html.parser")
            scripts = soup.find_all("script", id=True)
            print(f"[willhaben] Script tags with id: {[s.get('id') for s in scripts]}")

            json_ld = soup.find_all("script", type="application/ld+json")
            print(f"[willhaben] JSON-LD blocks: {len(json_ld)}")

            listings = _parse_willhaben_html(html)
            print(f"[willhaben] HTML parser found: {len(listings)} listings")

        # District filter
        if TARGET_PLZ:
            before = len(listings)
            filtered = []
            for l in listings:
                plz = l.get("district", "")
                loc = l.get("location", "").lower()
                if plz in TARGET_PLZ:
                    filtered.append(l)
                elif any(p in loc for p in TARGET_PLZ):
                    filtered.append(l)
                elif not plz:
                    filtered.append(l)
            listings = filtered
            print(f"[willhaben] District filter: {before} -> {len(listings)}")

    except Exception as e:
        print(f"[willhaben] ERROR: {e}")

    return listings


def _parse_willhaben_nextdata(data: dict) -> list[dict]:
    listings = []
    try:
        pp = data.get("props", {}).get("pageProps", {})

        search_result = pp.get("searchResult", {})
        ad_list = (
            search_result.get("advertSummaryList", {}).get("advertSummary", [])
        )

        if not ad_list:
            ad_list = pp.get("advertSummaryList", {}).get("advertSummary", [])
        if not ad_list:
            ad_list = pp.get("ads", [])
        if not ad_list:
            ad_list = search_result.get("ads", [])
        if not ad_list:
            for key in ["results", "items", "listings", "data"]:
                if key in pp and isinstance(pp[key], list):
                    ad_list = pp[key]
                    print(f"[willhaben] Found listings under pageProps.{key}")
                    break
                if key in search_result and isinstance(search_result[key], list):
                    ad_list = search_result[key]
                    print(f"[willhaben] Found listings under searchResult.{key}")
                    break

        print(f"[willhaben] ad_list length: {len(ad_list)}")
        if ad_list and isinstance(ad_list[0], dict):
            print(f"[willhaben] First item keys: {list(ad_list[0].keys())[:15]}")

        for ad in ad_list:
            if not isinstance(ad, dict):
                continue

            attrs = {}
            for a in ad.get("attributes", {}).get("attribute", []):
                vals = a.get("values", [])
                if vals:
                    attrs[a["name"]] = vals[0]

            ad_id = str(ad.get("id", ""))
            if not ad_id:
                continue

            listings.append({
                "id": f"wh_{ad_id}",
                "source": "willhaben",
                "title": ad.get("description", ad.get("title", "N/A")),
                "price": attrs.get("PRICE/AMOUNT", attrs.get("PRICE", "")),
                "size": attrs.get("ESTATE_SIZE/LIVING_AREA", attrs.get("LIVING_AREA", "")),
                "rooms": attrs.get("NUMBER_OF_ROOMS", attrs.get("ROOMS", "")),
                "district": attrs.get("POSTCODE", ""),
                "location": attrs.get("LOCATION", ""),
                "url": f"https://www.willhaben.at/iad/immobilien/d/mietwohnungen/wien/{ad_id}/",
            })

    except (KeyError, TypeError, IndexError) as e:
        print(f"[willhaben] JSON parse error: {e}")

    return listings


def _parse_willhaben_html(html: str) -> list[dict]:
    listings = []
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        '[data-testid="search-result-entry"]',
        '[data-testid="ad-list-item"]',
        "article.search-result-entry",
        'a[href*="/iad/immobilien/d/mietwohnungen/"]',
        'a[href*="/iad/immobilien/d/"]',
    ]

    for sel in selectors:
        found = soup.select(sel)
        print(f"[willhaben] Selector '{sel}': {len(found)} matches")
        if found:
            for el in found:
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
            break

    return listings


# ============================================================
# IMMOSCOUT24.AT (immobilienscout24.at)
# ============================================================

def scrape_immoscout() -> list[dict]:
    listings = []
    url = "https://www.immobilienscout24.at/suche/mietwohnungen/wien"
    params = {
        "price": f"-{MAX_PRICE}",
        "livingspace": f"{MIN_SIZE}-",
        "sorting": "2",
    }

    try:
        print("[immoscout24] Fetching...")
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        print(f"[immoscout24] Status: {resp.status_code}, Size: {len(resp.text)} chars")
        print(f"[immoscout24] Final URL: {resp.url}")
        resp.raise_for_status()
        html = resp.text

        if len(html) < 1000:
            print(f"[immoscout24] WARNING: Very short response. Full content:\n{html}")
        elif "captcha" in html.lower() or "robot" in html.lower():
            print("[immoscout24] WARNING: Possible captcha/bot detection page")

        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: __NEXT_DATA__
        nd_match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if nd_match:
            print("[immoscout24] Found __NEXT_DATA__, parsing...")
            try:
                data = json.loads(nd_match.group(1))
                pp = data.get("props", {}).get("pageProps", {})
                print(f"[immoscout24] pageProps keys: {list(pp.keys())[:15]}")
                listings = _parse_immoscout_json(data)
                print(f"[immoscout24] JSON parser found: {len(listings)}")
            except json.JSONDecodeError as e:
                print(f"[immoscout24] JSON decode error: {e}")
        else:
            print("[immoscout24] No __NEXT_DATA__")
            scripts = soup.find_all("script", id=True)
            print(f"[immoscout24] Script tags with id: {[s.get('id') for s in scripts]}")

        # Strategy 2: embedded JSON
        if not listings:
            for script in soup.find_all("script"):
                text = script.string or ""
                if any(kw in text for kw in ["resultList", "searchResult", "realEstateId", "expose"]):
                    print(f"[immoscout24] Found promising script tag ({len(text)} chars)")
                    try:
                        json_match = re.search(r'\{.*"resultList".*\}', text, re.DOTALL)
                        if json_match:
                            data = json.loads(json_match.group())
                            listings = _parse_immoscout_json(data)
                            print(f"[immoscout24] Embedded JSON found: {len(listings)}")
                    except (json.JSONDecodeError, AttributeError):
                        continue

        # Strategy 3: HTML fallback
        if not listings:
            print("[immoscout24] Trying HTML fallback...")
            listings = _parse_immoscout_html(soup)
            print(f"[immoscout24] HTML parser found: {len(listings)}")

        # District filter
        if TARGET_PLZ:
            before = len(listings)
            filtered = []
            for l in listings:
                loc = l.get("location", "").lower()
                plz = l.get("district", "")
                if plz in TARGET_PLZ:
                    filtered.append(l)
                elif any(p in loc for p in TARGET_PLZ):
                    filtered.append(l)
                elif not plz and not loc:
                    filtered.append(l)
            listings = filtered
            print(f"[immoscout24] District filter: {before} -> {len(listings)}")

    except Exception as e:
        print(f"[immoscout24] ERROR: {e}")

    return listings


def _parse_immoscout_json(data: dict) -> list[dict]:
    listings = []

    results = []
    if "props" in data:
        pp = data["props"].get("pageProps", {})
        for key in ["results", "searchResult", "resultList", "listings", "items", "data"]:
            candidate = pp.get(key, None)
            if isinstance(candidate, list) and len(candidate) > 0:
                results = candidate
                print(f"[immoscout24] Found list under pageProps.{key}: {len(results)} items")
                break
            elif isinstance(candidate, dict):
                for subkey in ["results", "items", "listings", "realEstates"]:
                    sub = candidate.get(subkey, [])
                    if isinstance(sub, list) and len(sub) > 0:
                        results = sub
                        print(f"[immoscout24] Found list under pageProps.{key}.{subkey}: {len(results)} items")
                        break
            if results:
                break

    if results and isinstance(results[0], dict):
        print(f"[immoscout24] First result keys: {list(results[0].keys())[:15]}")

    for item in results:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", item.get("realEstateId", item.get("objectId", ""))))
        if not item_id:
            continue

        listings.append({
            "id": f"is_{item_id}",
            "source": "immoscout24",
            "title": item.get("title", item.get("headline", "N/A")),
            "price": str(item.get("price", item.get("netRent", item.get("totalRent", "")))),
            "size": str(item.get("livingSpace", item.get("livingArea", item.get("area", "")))),
            "rooms": str(item.get("numberOfRooms", item.get("rooms", ""))),
            "district": str(item.get("zipCode", item.get("postcode", item.get("zip", "")))),
            "location": item.get("address", item.get("location", item.get("district", ""))),
            "url": f"https://www.immobilienscout24.at/expose/{item_id}",
        })

    return listings


def _parse_immoscout_html(soup: BeautifulSoup) -> list[dict]:
    listings = []

    selectors = [
        '[data-testid="result-list-item"]',
        "article.result-item",
        ".result-list__listing",
        'a[href*="/expose/"]',
        'a[href*="/immobilie/"]',
        'div[class*="result"]',
    ]

    for sel in selectors:
        found = soup.select(sel)
        print(f"[immoscout24] Selector '{sel}': {len(found)} matches")
        if found:
            for el in found:
                link = el.find("a", href=True) if el.name != "a" else el
                if not link:
                    continue
                href = link.get("href", "")
                if not href.startswith("http"):
                    href = "https://www.immobilienscout24.at" + href

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
            break

    return listings


# ============================================================
# WOHNNET.AT
# ============================================================

def scrape_wohnnet() -> list[dict]:
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
        print(f"[wohnnet] Status: {resp.status_code}, Size: {len(resp.text)} chars")
        print(f"[wohnnet] Final URL: {resp.url}")
        resp.raise_for_status()
        html = resp.text

        if len(html) < 1000:
            print(f"[wohnnet] WARNING: Very short response. Full content:\n{html}")
        elif "captcha" in html.lower() or "robot" in html.lower():
            print("[wohnnet] WARNING: Possible captcha/bot detection page")

        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: __NEXT_DATA__
        nd_match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if nd_match:
            print("[wohnnet] Found __NEXT_DATA__, parsing...")
            try:
                data = json.loads(nd_match.group(1))
                pp = data.get("props", {}).get("pageProps", {})
                print(f"[wohnnet] pageProps keys: {list(pp.keys())[:15]}")

                results = []
                for key in ["listings", "results", "items", "data", "ads"]:
                    candidate = pp.get(key, None)
                    if isinstance(candidate, list) and len(candidate) > 0:
                        results = candidate
                        print(f"[wohnnet] Found list under pageProps.{key}: {len(results)} items")
                        break
                    elif isinstance(candidate, dict):
                        for subkey in ["results", "items", "listings"]:
                            sub = candidate.get(subkey, [])
                            if isinstance(sub, list) and len(sub) > 0:
                                results = sub
                                print(f"[wohnnet] Found list under pageProps.{key}.{subkey}")
                                break
                    if results:
                        break

                if results and isinstance(results[0], dict):
                    print(f"[wohnnet] First result keys: {list(results[0].keys())[:15]}")

                for item in results:
                    if not isinstance(item, dict):
                        continue
                    item_id = str(item.get("id", ""))
                    if not item_id:
                        continue
                    item_url = item.get("url", item.get("link", ""))
                    if item_url and not item_url.startswith("http"):
                        item_url = "https://www.wohnnet.at" + item_url
                    elif not item_url:
                        item_url = f"https://www.wohnnet.at/immobilien/{item_id}"

                    listings.append({
                        "id": f"wn_{item_id}",
                        "source": "wohnnet",
                        "title": item.get("title", item.get("name", "N/A")),
                        "price": str(item.get("price", item.get("rent", ""))),
                        "size": str(item.get("area", item.get("livingArea", item.get("size", "")))),
                        "rooms": str(item.get("rooms", "")),
                        "district": str(item.get("zipCode", item.get("zip", ""))),
                        "location": item.get("address", item.get("location", "")),
                        "url": item_url,
                    })
            except (json.JSONDecodeError, KeyError) as e:
                print(f"[wohnnet] JSON parse error: {e}")
        else:
            print("[wohnnet] No __NEXT_DATA__")
            scripts = soup.find_all("script", id=True)
            print(f"[wohnnet] Script tags with id: {[s.get('id') for s in scripts]}")

        # Strategy 2: HTML fallback
        if not listings:
            print("[wohnnet] Trying HTML fallback...")
            selectors = [
                'a[href*="/immobilien/mietwohnung"]',
                'a[href*="/immobilien/inserat"]',
                'a[href*="/immobilien/"]',
                'div[class*="listing"]',
                'article',
            ]
            for sel in selectors:
                found = soup.select(sel)
                print(f"[wohnnet] Selector '{sel}': {len(found)} matches")
                if found and len(found) > 2:
                    for el in found:
                        link = el if el.name == "a" else el.find("a", href=True)
                        if not link or not link.get("href"):
                            continue
                        href = link["href"]
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
                    break

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
    print(f"PLZ: {', '.join(sorted(TARGET_PLZ))}")
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
