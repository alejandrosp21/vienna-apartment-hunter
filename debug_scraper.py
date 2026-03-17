#!/usr/bin/env python3
"""
DEBUG SCRIPT: Dumps the actual data structures from each site.
Run this once, then delete it. The output tells us exactly how to parse each site.
"""

import requests
import json
import re
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-AT,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def explore_dict(d, prefix="", max_depth=4, current_depth=0):
    """Recursively print dict structure with types and sizes."""
    if current_depth >= max_depth:
        return
    if not isinstance(d, dict):
        return
    for key in sorted(d.keys()):
        val = d[key]
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(val, dict):
            print(f"  {full_key}: dict({len(val)} keys)")
            explore_dict(val, full_key, max_depth, current_depth + 1)
        elif isinstance(val, list):
            print(f"  {full_key}: list({len(val)} items)")
            if val and isinstance(val[0], dict):
                print(f"    -> first item keys: {list(val[0].keys())[:20]}")
                # Show a sample of the first item
                for k, v in list(val[0].items())[:10]:
                    if isinstance(v, (str, int, float, bool)):
                        print(f"       {k}: {repr(v)[:100]}")
                    elif isinstance(v, dict):
                        print(f"       {k}: dict({list(v.keys())[:10]})")
                    elif isinstance(v, list):
                        print(f"       {k}: list({len(v)} items)")
        elif isinstance(val, str):
            print(f"  {full_key}: str({len(val)} chars) = {repr(val[:80])}")
        else:
            print(f"  {full_key}: {type(val).__name__} = {repr(val)[:80]}")


# ============================================================
# WILLHABEN
# ============================================================
print("\n" + "="*60)
print("WILLHABEN DEBUG")
print("="*60)

resp = requests.get(
    "https://www.willhaben.at/iad/immobilien/mietwohnungen/wien/",
    params={"PRICE_TO": 1000, "ESTATE_SIZE/LIVING_AREA_FROM": 30, "rows": 5, "sort": 1},
    headers=HEADERS, timeout=30
)
print(f"Status: {resp.status_code}, Size: {len(resp.text)}")

match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
if match:
    data = json.loads(match.group(1))
    print("\nFull __NEXT_DATA__ structure:")
    pp = data.get("props", {}).get("pageProps", {})
    explore_dict(pp, "pageProps", max_depth=5)
else:
    print("No __NEXT_DATA__ found!")
    soup = BeautifulSoup(resp.text, "html.parser")
    # Show all script tags
    for i, script in enumerate(soup.find_all("script")):
        src = script.get("src", "")
        sid = script.get("id", "")
        text = (script.string or "")[:200]
        if sid or "immobilien" in text.lower() or "advert" in text.lower() or "listing" in text.lower():
            print(f"\n  Script #{i}: id={sid}, src={src}")
            print(f"  Content preview: {text}")


# ============================================================
# IMMOSCOUT24
# ============================================================
print("\n" + "="*60)
print("IMMOSCOUT24 DEBUG")
print("="*60)

# Try the correct URL from the website
resp = requests.get(
    "https://www.immobilienscout24.at/regional/wien/wien/wohnung-mieten",
    headers=HEADERS, timeout=30
)
print(f"Status: {resp.status_code}, Size: {len(resp.text)}")
print(f"URL: {resp.url}")

match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
if match:
    data = json.loads(match.group(1))
    print("\nFull __NEXT_DATA__ structure:")
    pp = data.get("props", {}).get("pageProps", {})
    explore_dict(pp, "pageProps", max_depth=5)
else:
    print("No __NEXT_DATA__ found!")
    soup = BeautifulSoup(resp.text, "html.parser")
    # Show links that look like listings
    expose_links = soup.select('a[href*="/expose/"]')
    print(f"\nLinks with '/expose/': {len(expose_links)}")
    for link in expose_links[:5]:
        print(f"  href={link.get('href', '')[:100]}")
        print(f"  text={link.get_text(strip=True)[:100]}")

    # Show any promising script tags
    for i, script in enumerate(soup.find_all("script")):
        text = (script.string or "")
        if any(kw in text for kw in ["expose", "listing", "result", "property", "immo"]):
            print(f"\n  Promising script #{i}: {len(text)} chars")
            print(f"  Preview: {text[:300]}")


# ============================================================
# WOHNNET
# ============================================================
print("\n" + "="*60)
print("WOHNNET DEBUG")
print("="*60)

resp = requests.get(
    "https://www.wohnnet.at/immobilien/mietwohnungen/wien",
    params={"preis-bis": 1000, "flaeche-von": 30},
    headers=HEADERS, timeout=30
)
print(f"Status: {resp.status_code}, Size: {len(resp.text)}")

soup = BeautifulSoup(resp.text, "html.parser")

# Show the actual matched links
mw_links = soup.select('a[href*="/immobilien/mietwohnung"]')
print(f"\nLinks matching '/immobilien/mietwohnung': {len(mw_links)}")
for link in mw_links[:10]:
    href = link.get("href", "")
    text = link.get_text(strip=True)[:80]
    print(f"  href={href}")
    print(f"  text={text}")
    print()

# Also check for other patterns
for pattern in ['/immobilien/inserat', '/expose/', '/detail/', '/objekt/']:
    found = soup.select(f'a[href*="{pattern}"]')
    if found:
        print(f"Links matching '{pattern}': {len(found)}")
        for link in found[:3]:
            print(f"  href={link.get('href', '')}")

print("\n" + "="*60)
print("DEBUG COMPLETE")
print("="*60)
