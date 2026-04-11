#!/usr/bin/env python3
"""Alpha Scout v1 — Daily estate-sale arbitrage scanner."""

import asyncio
import base64
import io
import json
import logging
import os
import re
import statistics
import sys
import time
from datetime import datetime, date, timedelta
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

import httpx
from google import genai
import PIL.Image
from telegram import Bot
from telegram.constants import ParseMode

# ---------------------------------------------------------------------------
# Budget Constants
# ---------------------------------------------------------------------------
DAILY_GEMINI_BUDGET = 450    # Hard stop below 500 RPD free limit
DAILY_ETSY_BUDGET   = 500    # Defensive cap
ALPHA_MIN_PROFIT    = 100    # Minimum gross profit in dollars
ALPHA_MIN_ROI       = 1.5    # Minimum ROI multiplier (paired with profit threshold)
ALPHA_HIGH_ROI      = 3.0    # High ROI threshold — alerts regardless of dollar amount
CONFIDENCE_SKIP     = 0.50   # Below this: no alert, just log
HISTORY_PRUNE_DAYS  = 90     # Remove unseen/unalerted items older than this
HISTORY_PRUNE_ALERTED_DAYS = 365  # Remove alerted items older than 1 year
GEMINI_MODEL        = "gemini-3.1-flash-lite-preview"
MAX_SCRAPE_PAGES    = 50     # Safety guard against infinite pagination

# ---------------------------------------------------------------------------
# Environment / Secrets
# ---------------------------------------------------------------------------
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY", "")
ETSY_API_KEY        = os.environ.get("ETSY_API_KEY", "")
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "")

# Optional eBay (Phase 2)
EBAY_CLIENT_ID      = os.environ.get("EBAY_CLIENT_ID", "")
EBAY_CLIENT_SECRET  = os.environ.get("EBAY_CLIENT_SECRET", "")
EBAY_ENABLED        = bool(EBAY_CLIENT_ID and EBAY_CLIENT_SECRET)

# FCM (Firebase Cloud Messaging)
# NOTE: FCM Legacy HTTP API (fcm.googleapis.com/fcm/send) is deprecated by Google.
# Migrate to FCM v1 API (fcm.googleapis.com/v1/projects/{id}/messages:send with OAuth2)
# when Google announces a sunset date. Legacy API still works as of April 2026.
LAUNCH_DATE_STR     = os.environ.get("LAUNCH_DATE", "").strip() or "2026-04-05"
LAUNCH_DATE         = date.fromisoformat(LAUNCH_DATE_STR)
FCM_SERVER_KEY      = os.environ.get("FCM_SERVER_KEY", "")  # Legacy key, kept for fallback
FCM_PROJECT_ID      = os.environ.get("FCM_PROJECT_ID", "")
FCM_DEVICE_TOKENS   = [
    t for t in [
        os.environ.get(f"FCM_DEVICE_TOKEN_{i}", "")
        for i in range(1, 6)  # Supports up to 5 devices — just add secrets
    ] if t
]
# FCM activates 5 days after launch; requires credentials + at least one device token
FCM_ENABLED         = (date.today() >= LAUNCH_DATE + timedelta(days=5)
                       and (bool(FCM_SERVER_KEY) or bool(FCM_PROJECT_ID))
                       and len(FCM_DEVICE_TOKENS) > 0)
TELEGRAM_ENABLED    = not FCM_ENABLED

# Dry-run mode — scrape + Gemini run normally, skip Etsy/Telegram/FCM sends
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
GITHUB_EVENT_NAME   = os.environ.get("GITHUB_EVENT_NAME", "")  # "workflow_dispatch" for manual triggers

ESTATE_BASE_URL     = "https://mainstreetestatesales.com"
PRODUCTS_JSON_URL   = f"{ESTATE_BASE_URL}/collections/all/products.json"
HISTORY_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.json")
CURRENT_ALERT_PATH  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "current_alert.json")
ACTIONS_URL         = "https://github.com/Brad-Matthews/alpha-scout/actions"
ALERT_PAGE_URL      = "https://brad-matthews.github.io/alpha-scout/alert.html"

DENVER_TZ = ZoneInfo("America/Denver")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("alpha_scout")

# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

def retry(fn, *args, attempts: int = 3, delay: float = 2.0, label: str = "request", **kwargs):
    """Call fn(*args, **kwargs) up to `attempts` times with `delay` seconds between retries."""
    last_err = None
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            if i < attempts - 1:
                log.warning(f"{label} failed (attempt {i + 1}/{attempts}): {e} — retrying in {delay}s")
                time.sleep(delay)
            else:
                log.error(f"{label} failed after {attempts} attempts: {e}")
    raise last_err  # type: ignore[misc]

# ---------------------------------------------------------------------------
# History helpers
# ---------------------------------------------------------------------------

def load_history() -> dict:
    if not os.path.exists(HISTORY_PATH):
        log.warning("history.json missing — creating fresh file (cold start)")
        return {"items": {}, "last_run": None, "run_count": 0, "gemini_calls_today": 0}
    with open(HISTORY_PATH, "r") as f:
        return json.load(f)


def save_history(history: dict) -> None:
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)


def write_current_alert(item: dict, gemini_data: dict, etsy_data: dict,
                        market_est: float, gross_profit: float, tier: str,
                        is_price_drop: bool, alert_type: str = "profit") -> None:
    """Write the latest alert to current_alert.json for the GitHub Pages alert card."""
    now = datetime.now(DENVER_TZ)
    profit_pct = int((gross_profit / item["price"]) * 100) if item["price"] > 0 else 0
    clean_google_title = re.sub(r"\(.*?\)", "", item["title"]).strip()
    alert_data = {
        "title": item["title"],
        "image_url": item.get("image_url", ""),
        "estate_price": int(item["price"]),
        "market_estimate": int(market_est),
        "gross_profit": int(gross_profit),
        "profit_pct": profit_pct,
        "tier": tier,
        "confidence": int(gemini_data.get("confidence", 0) * 100),
        "key_signals": gemini_data.get("key_signals", ""),
        "estate_url": f"{ESTATE_BASE_URL}/products/{item['handle']}",
        "etsy_url": etsy_data.get("etsy_search_url", ""),
        "google_url": f"https://google.com/search?q={quote_plus(clean_google_title)}",
        "scouted_at": f"{now.strftime('%-I:%M %p')} · {now.strftime('%B %-d, %Y')}",
        "alert_type": alert_type,
    }
    try:
        with open(CURRENT_ALERT_PATH, "w") as f:
            json.dump(alert_data, f, indent=2)
        log.info(f"Wrote current_alert.json: {item['title'][:50]}")
    except Exception as e:
        log.error(f"Failed to write current_alert.json: {e}")


def reset_daily_gemini_counter(history: dict) -> dict:
    """Reset gemini_calls_today if last_run was a different calendar day."""
    last = history.get("last_run")
    if last:
        last_date = datetime.fromisoformat(last).date()
        today = datetime.now(DENVER_TZ).date()
        if last_date < today:
            history["gemini_calls_today"] = 0
    return history


def prune_old_history(history: dict) -> int:
    """Remove items from history that are stale: unalerted >90 days, or alerted >365 days."""
    today = date.today()
    to_remove = []
    for handle, entry in history["items"].items():
        last_seen = entry.get("last_seen")
        if not last_seen:
            continue
        try:
            last_seen_date = date.fromisoformat(last_seen)
        except (ValueError, TypeError):
            continue
        age_days = (today - last_seen_date).days
        if entry.get("alerted", False):
            if age_days > HISTORY_PRUNE_ALERTED_DAYS:
                to_remove.append(handle)
        else:
            if age_days > HISTORY_PRUNE_DAYS:
                to_remove.append(handle)
    for handle in to_remove:
        del history["items"][handle]
    return len(to_remove)

# ---------------------------------------------------------------------------
# Stage 0 — Scrape Shopify JSON API
# ---------------------------------------------------------------------------

def _scrape_page(client: httpx.Client, page: int) -> list[dict]:
    """Fetch a single page from the Shopify JSON API (retryable unit)."""
    resp = client.get(PRODUCTS_JSON_URL, params={"limit": 250, "page": page}, timeout=30)
    resp.raise_for_status()
    return resp.json().get("products", [])


def scrape_products(client: httpx.Client) -> list[dict]:
    """Paginate the Shopify JSON API and return all available products."""
    all_products = []
    page = 1
    while page <= MAX_SCRAPE_PAGES:
        log.info(f"Scraping page {page} ...")
        products = retry(_scrape_page, client, page, label=f"Shopify page {page}")
        if not products:
            break
        all_products.extend(products)
        page += 1
        time.sleep(0.5)  # Polite scraping delay between pages
    if page > MAX_SCRAPE_PAGES:
        log.warning(f"Hit max page limit ({MAX_SCRAPE_PAGES}). Possible pagination issue.")
    log.info(f"Scraped {len(all_products)} total products")
    return all_products


def parse_product(p: dict) -> dict | None:
    """Extract the fields we need; return None if item should be skipped."""
    variants = p.get("variants", [])
    if not variants:
        return None
    variant = variants[0]
    if not variant.get("available", True):
        return None  # sold out
    try:
        price = float(variant.get("price", 0))
    except (ValueError, TypeError):
        return None
    images = p.get("images", [])
    image_url = images[0]["src"] if images else None
    tags = [t.strip() for t in p.get("tags", "").split(",")] if isinstance(p.get("tags"), str) else p.get("tags", [])
    return {
        "handle": p["handle"],
        "title": p.get("title", ""),
        "price": price,
        "image_url": image_url,
        "tags": tags,
        "converted": "CONVERTED" in [t.upper() for t in tags],
    }

# ---------------------------------------------------------------------------
# Stage 1 — Gemini Appraisal
# ---------------------------------------------------------------------------

GEMINI_PROMPT_TEMPLATE = """You are an expert reseller who scouts estate sales for arbitrage
opportunities across ALL categories: jewelry, art, paintings,
furniture, collectibles, sports memorabilia, vintage items,
electronics, cameras, tools, clothing, coins, and anything else
with resale value on Etsy or eBay.

Item: {title}
Estate Asking Price: ${price}
{price_note}

Analyze this item's resale alpha potential. Consider: brand
markings, hallmarks, materials, era, condition signals visible
in the image, collectibility, rarity, and current market demand
on Etsy and eBay. Use your training knowledge of what comparable
items actually sell for on resale platforms.

Respond ONLY in valid JSON, no markdown, no preamble:
{{
  "alpha_signal": "YES" or "NO",
  "confidence": 0.0 to 1.0,
  "estimated_resale_low": <integer dollars>,
  "estimated_resale_high": <integer dollars>,
  "best_platform": "etsy" | "ebay" | "either",
  "category": "jewelry|art|furniture|collectibles|sports|electronics|clothing|tools|coins|other",
  "key_signals": "<one sentence: most important signals>",
  "skip_reason": "<if NO: brief reason. if YES: empty string>"
}}"""


def build_gemini_prompt(title: str, price: float, converted: bool) -> str:
    price_note = "NOTE: Price recently reduced from original asking price." if converted else ""
    return GEMINI_PROMPT_TEMPLATE.format(title=title, price=f"{price:.0f}", price_note=price_note)


def _gemini_generate(gemini_client: genai.Client, contents: list) -> str:
    """Single Gemini API call (retryable unit). Returns raw text."""
    response = gemini_client.models.generate_content(
        model=GEMINI_MODEL,
        contents=contents,
    )
    try:
        raw_text = response.text or ""
    except (ValueError, AttributeError):
        raw_text = ""
    return raw_text


def call_gemini(gemini_client: genai.Client, http_client: httpx.Client,
                prompt: str, image_url: str | None) -> dict | None:
    """Send multimodal prompt to Gemini; return parsed JSON or None."""
    try:
        contents: list = []
        if image_url:
            try:
                img_resp = http_client.get(image_url, timeout=15)
                img_resp.raise_for_status()
                img = PIL.Image.open(io.BytesIO(img_resp.content))
                contents = [prompt, img]
            except Exception as e:
                log.warning(f"Image download failed — falling back to text-only: {e}")
                contents = [prompt]
        else:
            contents = [prompt]

        raw_text = retry(_gemini_generate, gemini_client, contents,
                         attempts=3, delay=5.0, label="Gemini API")
        log.debug(f"Gemini raw response: {raw_text[:500]}")
        text = raw_text.strip()
        if not text:
            log.error("Gemini returned empty response")
            return None
        # Strip markdown code fences if Gemini wraps them
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.error(f"Gemini JSON parse failure: {e}")
        return None
    except Exception as e:
        log.error(f"Gemini API error: {e}")
        return None

# ---------------------------------------------------------------------------
# Stage 2 — Etsy Market Validation
# ---------------------------------------------------------------------------

def clean_title_for_etsy(title: str) -> str:
    """Remove dimensions, weights, and generic estate-sale filler words."""
    cleaned = re.sub(r"\([\d\.]+[gG]\)", "", title)          # (6.5g)
    cleaned = re.sub(r"\([\d]+x[\d]+\)", "", cleaned)        # (22x14)
    cleaned = re.sub(r"\(.*?\)", "", cleaned)                 # other parentheticals
    for word in ["antique", "vintage", "estate"]:
        cleaned = re.sub(rf"\b{word}\b", "", cleaned, flags=re.IGNORECASE)
    return " ".join(cleaned.split())[:100]


def _etsy_request(client: httpx.Client, cleaned: str) -> dict:
    """Single Etsy API call (retryable unit)."""
    headers = {"x-api-key": ETSY_API_KEY, "Accept": "application/json"}
    params = {"keywords": cleaned, "limit": 25, "sort_on": "price", "sort_order": "asc"}
    resp = client.get(
        "https://api.etsy.com/v3/application/listings/active",
        headers=headers, params=params, timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    prices = []
    for r in results:
        p = r.get("price", {})
        amount = p.get("amount")
        divisor = p.get("divisor", 100)
        if amount is not None:
            prices.append(amount / divisor)
    median_ask = statistics.median(prices) if prices else 0
    return {
        "etsy_median_ask": median_ask,
        "etsy_listing_count": len(results),
        "etsy_search_url": f"https://www.etsy.com/search?q={quote_plus(cleaned)}",
    }


def query_etsy(client: httpx.Client, title: str) -> dict:
    """Search Etsy active listings with retry; return median price + listing count."""
    cleaned = clean_title_for_etsy(title)
    try:
        return retry(_etsy_request, client, cleaned, label="Etsy API")
    except Exception as e:
        log.error(f"Etsy API error after retries: {e}")
        return {"etsy_median_ask": 0, "etsy_listing_count": 0, "etsy_search_url": "", "error": True}

# ---------------------------------------------------------------------------
# Stage 2b — eBay Browse API (optional, Phase 2)
# ---------------------------------------------------------------------------

def get_ebay_token(client: httpx.Client) -> str | None:
    """Client-credentials grant for eBay Browse API."""
    if not EBAY_ENABLED:
        return None
    creds = base64.b64encode(f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()).decode()
    try:
        resp = client.post(
            "https://api.ebay.com/identity/v1/oauth2/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {creds}",
            },
            data={"grant_type": "client_credentials", "scope": "https://api.ebay.com/oauth/api_scope"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("access_token")
    except Exception as e:
        log.error(f"eBay token error: {e}")
        return None


def query_ebay(client: httpx.Client, token: str, title: str) -> dict:
    """Search eBay Browse API for active listing comps."""
    cleaned = clean_title_for_etsy(title)
    try:
        resp = client.get(
            "https://api.ebay.com/buy/browse/v1/item_summary/search",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            params={"q": cleaned, "limit": 25, "sort": "price"},
            timeout=15,
        )
        resp.raise_for_status()
        items = resp.json().get("itemSummaries", [])
        prices = []
        for item in items:
            p = item.get("price", {})
            val = p.get("value")
            if val:
                prices.append(float(val))
        return {
            "ebay_median_ask": statistics.median(prices) if prices else 0,
            "ebay_listing_count": len(items),
        }
    except Exception as e:
        log.error(f"eBay API error: {e}")
        return {"ebay_median_ask": 0, "ebay_listing_count": 0}

# ---------------------------------------------------------------------------
# Profit / ROI calculation
# ---------------------------------------------------------------------------

def compute_market_estimate(gemini_data: dict, etsy_data: dict, ebay_data: dict | None) -> float:
    low = gemini_data.get("estimated_resale_low", 0)
    high = gemini_data.get("estimated_resale_high", 0)
    gemini_midpoint = (low + high) / 2

    etsy_count = etsy_data.get("etsy_listing_count", 0)
    etsy_median = etsy_data.get("etsy_median_ask", 0)

    if EBAY_ENABLED and ebay_data and ebay_data.get("ebay_listing_count", 0) >= 3:
        ebay_median = ebay_data.get("ebay_median_ask", 0)
        if etsy_count >= 3:
            return (gemini_midpoint * 0.60) + (etsy_median * 0.20) + (ebay_median * 0.20)
        else:
            return (gemini_midpoint * 0.60) + (ebay_median * 0.40)

    if etsy_count >= 3:
        return (gemini_midpoint * 0.60) + (etsy_median * 0.40)
    else:
        return gemini_midpoint

# ---------------------------------------------------------------------------
# Confidence tier
# ---------------------------------------------------------------------------

def confidence_tier(confidence: float, etsy_count: int) -> str:
    if confidence >= 0.80 and etsy_count >= 1:
        return "HIGH"
    elif confidence >= 0.65:
        return "MEDIUM"
    elif confidence >= 0.50:
        return "SPECULATIVE"
    return "SKIP"

# ---------------------------------------------------------------------------
# Telegram helpers — alerts use HTML (not MarkdownV2) to avoid escaping issues
# ---------------------------------------------------------------------------

def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram HTML parse mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_alert_message(item: dict, gemini_data: dict, etsy_data: dict, market_est: float,
                        gross_profit: float, roi: float, tier: str,
                        is_price_drop: bool = False, alert_type: str = "profit") -> str:
    """Build alert message as HTML for Telegram."""
    conf_pct = int(gemini_data["confidence"] * 100)
    low = gemini_data["estimated_resale_low"]
    high = gemini_data["estimated_resale_high"]
    key_signals = escape_html(gemini_data.get("key_signals", ""))
    category = escape_html(gemini_data.get("category", "other").capitalize())
    best_platform = escape_html(gemini_data.get("best_platform", "either").capitalize())

    etsy_count = etsy_data.get("etsy_listing_count", 0)
    etsy_median = etsy_data.get("etsy_median_ask", 0)
    etsy_url = etsy_data.get("etsy_search_url", "")

    estate_url = f"{ESTATE_BASE_URL}/products/{item['handle']}"
    google_query = re.sub(r"\(.*?\)", "", item["title"]).strip()
    google_url = f"https://google.com/search?q={quote_plus(google_query)}"

    title_esc = escape_html(item["title"])
    price_str = f"{item['price']:.0f}"

    drop_suffix = " - Price Drop" if is_price_drop else ""
    if alert_type == "roi":
        if tier == "HIGH":
            header = f"\U0001f48e HIGH ROI FIND{drop_suffix}"
        elif tier == "MEDIUM":
            header = f"\U0001f48e MEDIUM CONFIDENCE - High ROI{drop_suffix}"
        else:
            header = f"\U0001f48e SPECULATIVE - High ROI{drop_suffix}"
    else:
        if tier == "HIGH":
            header = f"\U0001f7e2 HIGH CONFIDENCE ALPHA{drop_suffix}"
        elif tier == "MEDIUM":
            header = f"\U0001f7e1 MEDIUM CONFIDENCE ALPHA{drop_suffix}"
        else:
            header = f"\U0001f534 SPECULATIVE - Verify before buying{drop_suffix}"

    # Etsy market line
    if etsy_count >= 3:
        etsy_line = f"\U0001f6cd Etsy Active Market: <b>${etsy_median:.0f} median</b> ({etsy_count} listings)"
    elif etsy_count > 0:
        etsy_line = f"\U0001f6cd Etsy Active Market: <b>thin</b> ({etsy_count} listings found)"
    else:
        etsy_line = "\U0001f6cd Etsy Active Market: <b>thin</b> (0 listings found)"

    # For HIGH/MEDIUM include market value + profit lines
    if tier in ("HIGH", "MEDIUM"):
        profit_pct = (gross_profit / item["price"] * 100) if item["price"] > 0 else 0
        value_lines = (
            f"\u2705 Est. Market Value: <b>${market_est:.0f}</b>\n"
            f"\U0001f4c8 Potential Profit: <b>~${gross_profit:.0f}</b> (+{profit_pct:.0f}% return)"
        )
        if alert_type == "roi":
            value_lines += f"\n\U0001f48e High ROI alert - strong multiplier on low-cost item"
    else:
        value_lines = ""

    price_drop_line = "\U0001f53d Price Drop - previously listed at a higher price\n" if is_price_drop else ""

    msg = (
        f"{header}\n\n"
        f"<b>{title_esc}</b>\n\n"
        f"\U0001f4b0 Estate Price: <b>${price_str}</b>\n"
        f"{price_drop_line}"
        f"\U0001f4ca Gemini Estimate: <b>${low} - ${high}</b> | Confidence: {conf_pct}%\n"
        f"{etsy_line}\n"
    )
    if value_lines:
        msg += f"{value_lines}\n"
    msg += (
        f"\n\U0001f916 <i>{key_signals}</i>\n\n"
        f"\U0001f4e6 {category} | Best platform: {best_platform} | Confidence: {conf_pct}%\n\n"
        f'<a href="{estate_url}">\U0001f517 View Estate Listing</a>  '
        f'<a href="{etsy_url}">\U0001f6cd Search Etsy</a>  '
        f'<a href="{google_url}">\U0001f50d Google This</a>'
    )
    return msg


def build_heartbeat(stats: dict) -> str:
    """Build heartbeat message as plain HTML (no MarkdownV2)."""
    now = datetime.now(DENVER_TZ)
    s = stats

    dry_prefix = "[DRY RUN] " if DRY_RUN else ""

    msg = (
        f"{dry_prefix}\U0001f916 <b>Alpha Scout - Run #{s['run_count']}</b>\n\n"
        f"\U0001f4c5 {escape_html(now.strftime('%B %d, %Y'))} - {escape_html(now.strftime('%I:%M %p'))} Denver\n"
        f"\U0001f50d Scraped: <b>{s['total_scraped']}</b> items total\n"
        f"\U0001f195 New items: <b>{s['new_items']}</b>\n"
        f"\u23ed Skipped (seen before): <b>{s['skipped']}</b>\n"
        f"\U0001f504 Re-evaluated (price drop): <b>{s['price_drops']}</b>\n"
        f"\u2705 Alerts sent: <b>{s['alerts_sent']}</b> ({s.get('high_profit_alerts', 0)} profit, {s.get('high_roi_alerts', 0)} high ROI)\n"
        f"\u274c Below threshold: <b>{s['below_threshold']}</b>\n"
        f"\U0001f916 Gemini calls used: <b>{s['gemini_calls']} / {DAILY_GEMINI_BUDGET}</b> daily budget\n"
        f"\U0001f9e0 Model: <b>{escape_html(GEMINI_MODEL)}</b>\n\n"
    )
    if s.get("cold_start_remaining", 0) > 0:
        remaining = s['cold_start_remaining']
        est_days = s.get('cold_start_days', '?')
        msg += f"Status: Cold start in progress: {s['gemini_calls']} processed, ~{remaining} remaining. Est. {est_days} days to full baseline."
    else:
        msg += "Status: All clear"
    return msg

# ---------------------------------------------------------------------------
# FCM (Firebase Cloud Messaging) helpers
# ---------------------------------------------------------------------------

def _send_fcm_v1(token: str, title: str, body: str, url: str, image_url: str) -> None:
    """Send via FCM HTTP v1 API (requires FCM_PROJECT_ID + service account or server key)."""
    notification = {"title": title, "body": body}
    if image_url:
        notification["image"] = image_url
    message = {
        "message": {
            "token": token,
            "notification": notification,
            "webpush": {
                "fcm_options": {"link": url},
            },
            "data": {"url": url},
        }
    }
    resp = httpx.post(
        f"https://fcm.googleapis.com/v1/projects/{FCM_PROJECT_ID}/messages:send",
        headers={
            "Authorization": f"Bearer {_get_fcm_access_token()}",
            "Content-Type": "application/json",
        },
        json=message,
        timeout=15,
    )
    resp.raise_for_status()


def _send_fcm_legacy(token: str, title: str, body: str, url: str, image_url: str) -> None:
    """Send via FCM Legacy HTTP API (key= header). Deprecated but still functional."""
    notification = {"title": title, "body": body, "click_action": url}
    if image_url:
        notification["image"] = image_url
    resp = httpx.post(
        "https://fcm.googleapis.com/fcm/send",
        headers={
            "Authorization": f"key={FCM_SERVER_KEY}",
            "Content-Type": "application/json",
        },
        json={"to": token, "notification": notification, "data": {"url": url}},
        timeout=15,
    )
    resp.raise_for_status()


_fcm_access_token_cache: dict = {"token": "", "expires": 0}


def _get_fcm_access_token() -> str:
    """Get OAuth2 access token for FCM v1 API using google-auth default credentials."""
    import google.auth
    import google.auth.transport.requests

    now = time.time()
    if _fcm_access_token_cache["token"] and now < _fcm_access_token_cache["expires"]:
        return _fcm_access_token_cache["token"]

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/firebase.messaging"]
    )
    credentials.refresh(google.auth.transport.requests.Request())
    _fcm_access_token_cache["token"] = credentials.token
    _fcm_access_token_cache["expires"] = now + 3500  # tokens last ~3600s
    return credentials.token


def send_fcm(title: str, body: str, url: str, image_url: str = "") -> None:
    """Send FCM push notification to all registered device tokens.
    Uses v1 API if FCM_PROJECT_ID is set, otherwise falls back to legacy API."""
    if not FCM_DEVICE_TOKENS:
        log.warning("FCM enabled but no device tokens configured")
        return
    if not FCM_PROJECT_ID and not FCM_SERVER_KEY:
        log.warning("FCM enabled but neither FCM_PROJECT_ID nor FCM_SERVER_KEY set")
        return

    use_v1 = bool(FCM_PROJECT_ID)

    for token in FCM_DEVICE_TOKENS:
        try:
            if use_v1:
                _send_fcm_v1(token, title, body, url, image_url)
            else:
                _send_fcm_legacy(token, title, body, url, image_url)
            log.info(f"FCM notification sent ({'v1' if use_v1 else 'legacy'}) to token ...{token[-6:]}")
        except Exception as e:
            log.error(f"FCM send failed ({'v1' if use_v1 else 'legacy'}): {e}")
            # If v1 fails, try legacy as fallback
            if use_v1 and FCM_SERVER_KEY:
                try:
                    _send_fcm_legacy(token, title, body, url, image_url)
                    log.info(f"FCM notification sent (legacy fallback) to token ...{token[-6:]}")
                except Exception as e2:
                    log.error(f"FCM legacy fallback also failed: {e2}")


def build_fcm_alert(item: dict, gemini_data: dict, market_est: float,
                    gross_profit: float, roi: float, tier: str,
                    is_price_drop: bool = False) -> tuple[str, str, str, str]:
    """Returns (title, body, url, image_url) for FCM notification."""
    drop_tag = " - Price Drop" if is_price_drop else ""
    title = f"{tier} ALPHA{drop_tag}: {item['title'][:50]}"
    body = (
        f"Estate: ${item['price']:.0f} -> Est. Value: ${market_est:.0f} "
        f"(~${gross_profit:.0f} profit, {roi:.1f}x ROI)"
    )
    url = ALERT_PAGE_URL
    image_url = item.get("image_url", "")
    return title, body, url, image_url

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def send_telegram(bot: Bot, text: str) -> None:
    """Send a message via Telegram using HTML parse mode."""
    if DRY_RUN or not TELEGRAM_ENABLED:
        if DRY_RUN:
            log.info(f"[DRY RUN] Would send Telegram message ({len(text)} chars)")
        return
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.HTML,
                               disable_web_page_preview=True)
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


async def main() -> None:
    log.info("=== Alpha Scout starting ===")
    log.info(f"Notification mode: {'FCM' if FCM_ENABLED else 'Telegram'} | Day {(date.today() - LAUNCH_DATE).days + 1} since launch")
    log.info(f"FCM debug: LAUNCH_DATE={LAUNCH_DATE}, FCM_ENABLED={FCM_ENABLED}, "
             f"FCM_PROJECT_ID={'set' if FCM_PROJECT_ID else 'unset'}, "
             f"FCM_SERVER_KEY={'set' if FCM_SERVER_KEY else 'unset'}, "
             f"FCM_DEVICE_TOKENS={len(FCM_DEVICE_TOKENS)} device(s), "
             f"API={'v1' if FCM_PROJECT_ID else 'legacy'}")

    if DRY_RUN:
        log.info("[DRY RUN] Dry-run mode enabled — skipping Etsy calls, Telegram sends, and FCM sends")

    # -----------------------------------------------------------------------
    # Startup validation
    # -----------------------------------------------------------------------
    if DRY_RUN:
        required = [("GEMINI_API_KEY", GEMINI_API_KEY)]
    elif FCM_ENABLED:
        required = [
            ("GEMINI_API_KEY", GEMINI_API_KEY),
            ("ETSY_API_KEY", ETSY_API_KEY),
            ("FCM_SERVER_KEY", FCM_SERVER_KEY),
        ]
    else:
        required = [
            ("GEMINI_API_KEY", GEMINI_API_KEY),
            ("ETSY_API_KEY", ETSY_API_KEY),
            ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
            ("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID),
        ]
    missing = [name for name, val in required if not val]
    if missing:
        log.error(f"Missing required secrets: {', '.join(missing)}")
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Load history
    # -----------------------------------------------------------------------
    history = load_history()
    history = reset_daily_gemini_counter(history)

    # Reset budget on manual triggers if budget was exhausted
    if (GITHUB_EVENT_NAME == "workflow_dispatch"
            and history.get("gemini_calls_today", 0) >= DAILY_GEMINI_BUDGET):
        log.warning(f"Manual trigger detected with exhausted budget ({history['gemini_calls_today']}). Resetting counter.")
        history["gemini_calls_today"] = 0

    # -----------------------------------------------------------------------
    # Prune old unseen items
    # -----------------------------------------------------------------------
    pruned = prune_old_history(history)
    if pruned:
        log.info(f"Pruned {pruned} old items from history")

    # -----------------------------------------------------------------------
    # Configure Gemini
    # -----------------------------------------------------------------------
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)

    # -----------------------------------------------------------------------
    # Configure Telegram
    # -----------------------------------------------------------------------
    bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None

    # -----------------------------------------------------------------------
    # Optional eBay token
    # -----------------------------------------------------------------------
    ebay_token = None

    # -----------------------------------------------------------------------
    # Scrape
    # -----------------------------------------------------------------------
    with httpx.Client() as client:
        try:
            products_raw = scrape_products(client)
        except Exception as e:
            log.error(f"Estate site unreachable: {e}")
            if TELEGRAM_ENABLED and bot and not DRY_RUN:
                await send_telegram(bot, f"\u274c Alpha Scout FAILED - Estate site unreachable: {escape_html(str(e))}")
            if FCM_ENABLED and not DRY_RUN:
                send_fcm("Alpha Scout FAILED", f"Estate site unreachable: {e}", ACTIONS_URL)
            return

        products = []
        for p in products_raw:
            parsed = parse_product(p)
            if parsed:
                products.append(parsed)

        log.info(f"Parsed {len(products)} available products")

        # -------------------------------------------------------------------
        # Determine which items need processing
        # -------------------------------------------------------------------
        today_str = datetime.now(DENVER_TZ).strftime("%Y-%m-%d")
        current_handles = {p["handle"] for p in products}

        # Remove items from history that are no longer on site
        # Guard: skip stale removal if scrape returned suspiciously few items
        if len(products) >= 100:
            stale = [h for h in history["items"] if h not in current_handles]
            for h in stale:
                del history["items"][h]
            if stale:
                log.info(f"Removed {len(stale)} stale items from history")
        elif len(products) > 0:
            log.warning(f"Only {len(products)} products scraped (expected 4000+). Skipping stale item removal to protect history.")

        stats = {
            "total_scraped": len(products),
            "new_items": 0,
            "skipped": 0,
            "price_drops": 0,
            "alerts_sent": 0,
            "below_threshold": 0,
            "gemini_calls": history["gemini_calls_today"],
            "gemini_calls_this_run": 0,
            "gemini_failures": 0,
            "cold_start_remaining": 0,
        }

        items_to_process: list[dict] = []
        etsy_calls_today = 0

        for item in products:
            handle = item["handle"]
            if handle in history["items"]:
                hist_entry = history["items"][handle]
                old_price = hist_entry.get("last_seen_price", 0)
                new_price = item["price"]
                if old_price == new_price:
                    # Same price — skip
                    hist_entry["last_seen"] = today_str
                    stats["skipped"] += 1
                    continue
                elif new_price < old_price:
                    # Price DECREASED — re-evaluate
                    stats["price_drops"] += 1
                    item["is_price_drop"] = True
                    items_to_process.append(item)
                else:
                    # Price INCREASED — update silently, no re-evaluation
                    hist_entry["last_seen"] = today_str
                    hist_entry["last_seen_price"] = new_price
                    stats["skipped"] += 1
                    continue
            else:
                stats["new_items"] += 1
                items_to_process.append(item)

        log.info(f"Items to process: {len(items_to_process)} (new: {stats['new_items']}, price drops: {stats['price_drops']})")

        # -------------------------------------------------------------------
        # Process items through pipeline
        # -------------------------------------------------------------------
        budget_hit = False

        if EBAY_ENABLED:
            ebay_token = get_ebay_token(client)

        try:
            for item in items_to_process:
                # Check Gemini budget
                if stats["gemini_calls"] >= DAILY_GEMINI_BUDGET:
                    remaining = len(items_to_process) - items_to_process.index(item)
                    stats["cold_start_remaining"] = remaining
                    est_days = (remaining // DAILY_GEMINI_BUDGET) + 1
                    stats["cold_start_days"] = est_days
                    log.warning(f"Gemini daily budget hit. {remaining} items queued for tomorrow.")
                    budget_hit = True
                    break

                handle = item["handle"]
                is_price_drop = item.get("is_price_drop", False)

                # ---------------------------------------------------------------
                # Stage 1 — Gemini
                # ---------------------------------------------------------------
                prompt = build_gemini_prompt(item["title"], item["price"], item["converted"])
                gemini_data = call_gemini(gemini_client, client, prompt, item["image_url"])
                stats["gemini_calls"] += 1
                stats["gemini_calls_this_run"] += 1
                history["gemini_calls_today"] = stats["gemini_calls"]
                time.sleep(4.0)  # Respect Gemini 15 RPM free tier limit

                if gemini_data is None:
                    # Parse / API failure — write to history so it's skipped on future runs
                    history["items"][handle] = {
                        "title": item["title"],
                        "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                        "last_seen": today_str,
                        "last_seen_price": item["price"],
                        "alerted": False,
                        "gemini_category": "error",
                    }
                    stats["gemini_failures"] += 1
                    continue

                confidence = gemini_data.get("confidence", 0)
                alpha_signal = gemini_data.get("alpha_signal", "NO").upper()
                category = gemini_data.get("category", "other")

                # Below skip threshold — add to history, no alert
                if confidence < CONFIDENCE_SKIP or alpha_signal != "YES":
                    history["items"][handle] = {
                        "title": item["title"],
                        "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                        "last_seen": today_str,
                        "last_seen_price": item["price"],
                        "alerted": False,
                        "gemini_category": category,
                    }
                    stats["below_threshold"] += 1
                    continue

                # Gate: must be YES + confidence >= 0.65 to proceed to Stage 2
                if confidence < 0.65:
                    # Between 0.50 and 0.65 — SPECULATIVE tier, skip Etsy
                    etsy_data = {"etsy_median_ask": 0, "etsy_listing_count": 0, "etsy_search_url": f"https://www.etsy.com/search?q={quote_plus(clean_title_for_etsy(item['title']))}"}
                    ebay_data = None
                    market_est = compute_market_estimate(gemini_data, etsy_data, ebay_data)
                    gross_profit = market_est - item["price"]
                    roi = market_est / item["price"] if item["price"] > 0 else 0

                    tier = confidence_tier(confidence, etsy_data["etsy_listing_count"])

                    high_profit = gross_profit >= ALPHA_MIN_PROFIT and roi >= ALPHA_MIN_ROI
                    high_roi = roi >= ALPHA_HIGH_ROI
                    if high_profit or high_roi:
                        alert_type = "roi" if high_roi and not high_profit else "profit"
                        if DRY_RUN:
                            log.info(f"[DRY RUN] Would send alert: {tier} ({alert_type}) — {item['title']}")
                            if FCM_ENABLED:
                                log.info(f"[DRY RUN] Would send FCM notification: {tier} ALPHA: {item['title'][:50]}")
                        else:
                            write_current_alert(item, gemini_data, etsy_data, market_est, gross_profit, tier, is_price_drop, alert_type)
                            if TELEGRAM_ENABLED and bot:
                                alert_msg = build_alert_message(item, gemini_data, etsy_data, market_est, gross_profit, roi, tier,
                                                                is_price_drop=is_price_drop, alert_type=alert_type)
                                await send_telegram(bot, alert_msg)
                            if FCM_ENABLED:
                                fcm_title, fcm_body, fcm_url, fcm_img = build_fcm_alert(item, gemini_data, market_est, gross_profit, roi, tier, is_price_drop)
                                send_fcm(fcm_title, fcm_body, fcm_url, fcm_img)
                            time.sleep(1)
                        stats["alerts_sent"] += 1
                        if alert_type == "roi":
                            stats["high_roi_alerts"] = stats.get("high_roi_alerts", 0) + 1
                        else:
                            stats["high_profit_alerts"] = stats.get("high_profit_alerts", 0) + 1
                        history["items"][handle] = {
                            "title": item["title"],
                            "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                            "last_seen": today_str,
                            "last_seen_price": item["price"],
                            "alerted": True,
                            "gemini_category": category,
                        }
                    else:
                        stats["below_threshold"] += 1
                        history["items"][handle] = {
                            "title": item["title"],
                            "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                            "last_seen": today_str,
                            "last_seen_price": item["price"],
                            "alerted": False,
                            "gemini_category": category,
                        }
                    continue

                # ---------------------------------------------------------------
                # Stage 2 — Etsy (and optionally eBay)
                # ---------------------------------------------------------------
                if DRY_RUN:
                    etsy_data = {"etsy_median_ask": 0, "etsy_listing_count": 0,
                                 "etsy_search_url": f"https://www.etsy.com/search?q={quote_plus(clean_title_for_etsy(item['title']))}"}
                elif etsy_calls_today < DAILY_ETSY_BUDGET:
                    etsy_data = query_etsy(client, item["title"])
                    etsy_calls_today += 1
                else:
                    etsy_data = {"etsy_median_ask": 0, "etsy_listing_count": 0,
                                 "etsy_search_url": f"https://www.etsy.com/search?q={quote_plus(clean_title_for_etsy(item['title']))}",
                                 "error": True}
                    log.warning("Etsy daily budget hit")

                ebay_data = None
                if EBAY_ENABLED and ebay_token:
                    ebay_data = query_ebay(client, ebay_token, item["title"])

                # ---------------------------------------------------------------
                # Profit calculation
                # ---------------------------------------------------------------
                market_est = compute_market_estimate(gemini_data, etsy_data, ebay_data)
                gross_profit = market_est - item["price"]
                roi = market_est / item["price"] if item["price"] > 0 else 0

                etsy_count = etsy_data.get("etsy_listing_count", 0)

                # Thin market override: still alert if Gemini confidence >= 0.80
                if etsy_count == 0 and confidence < 0.80:
                    stats["below_threshold"] += 1
                    history["items"][handle] = {
                        "title": item["title"],
                        "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                        "last_seen": today_str,
                        "last_seen_price": item["price"],
                        "alerted": False,
                        "gemini_category": category,
                    }
                    continue

                # ---------------------------------------------------------------
                # Stage 3 — Alert or skip
                # ---------------------------------------------------------------
                tier = confidence_tier(confidence, etsy_count)

                high_profit = gross_profit >= ALPHA_MIN_PROFIT and roi >= ALPHA_MIN_ROI
                high_roi = roi >= ALPHA_HIGH_ROI
                if high_profit or high_roi:
                    alert_type = "roi" if high_roi and not high_profit else "profit"
                    if DRY_RUN:
                        log.info(f"[DRY RUN] Would send alert: {tier} ({alert_type}) — {item['title']}")
                        if FCM_ENABLED:
                            log.info(f"[DRY RUN] Would send FCM notification: {tier} ALPHA: {item['title'][:50]}")
                    else:
                        write_current_alert(item, gemini_data, etsy_data, market_est, gross_profit, tier, is_price_drop, alert_type)
                        if TELEGRAM_ENABLED and bot:
                            alert_msg = build_alert_message(item, gemini_data, etsy_data, market_est, gross_profit, roi, tier,
                                                            is_price_drop=is_price_drop, alert_type=alert_type)
                            await send_telegram(bot, alert_msg)
                        if FCM_ENABLED:
                            fcm_title, fcm_body, fcm_url, fcm_img = build_fcm_alert(item, gemini_data, market_est, gross_profit, roi, tier, is_price_drop)
                            send_fcm(fcm_title, fcm_body, fcm_url, fcm_img)
                        time.sleep(1)  # 1-second delay between messages
                    stats["alerts_sent"] += 1
                    if alert_type == "roi":
                        stats["high_roi_alerts"] = stats.get("high_roi_alerts", 0) + 1
                    else:
                        stats["high_profit_alerts"] = stats.get("high_profit_alerts", 0) + 1
                    history["items"][handle] = {
                        "title": item["title"],
                        "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                        "last_seen": today_str,
                        "last_seen_price": item["price"],
                        "alerted": True,
                        "gemini_category": category,
                    }
                else:
                    stats["below_threshold"] += 1
                    history["items"][handle] = {
                        "title": item["title"],
                        "first_seen": history["items"].get(handle, {}).get("first_seen", today_str),
                        "last_seen": today_str,
                        "last_seen_price": item["price"],
                        "alerted": False,
                        "gemini_category": category,
                    }
        finally:
            # Always save history even if processing loop crashes
            save_history(history)
            log.info("History saved (in-loop or post-loop)")

    # -----------------------------------------------------------------------
    # Gemini batch summary (per-run stats, not cumulative)
    # -----------------------------------------------------------------------
    calls_this_run = stats["gemini_calls_this_run"]
    failures_this_run = stats["gemini_failures"]
    successes_this_run = calls_this_run - failures_this_run
    log.info(f"Gemini batch: {successes_this_run} parsed OK, {failures_this_run} failed out of {calls_this_run} calls this run")

    # -----------------------------------------------------------------------
    # Heartbeat
    # -----------------------------------------------------------------------
    history["run_count"] = history.get("run_count", 0) + 1
    history["last_run"] = datetime.now(DENVER_TZ).isoformat()
    stats["run_count"] = history["run_count"]

    heartbeat = build_heartbeat(stats)
    if DRY_RUN:
        log.info(f"[DRY RUN] Heartbeat:\n{heartbeat}")
        if FCM_ENABLED:
            log.info("[DRY RUN] Would send FCM heartbeat")
    else:
        if TELEGRAM_ENABLED and bot:
            try:
                await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=heartbeat,
                                       parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                log.error(f"Telegram heartbeat send failed: {e}")
        if FCM_ENABLED:
            days_since = (date.today() - LAUNCH_DATE).days + 1
            send_fcm(
                f"Alpha Scout - Run #{stats['run_count']}",
                f"Scraped {stats['total_scraped']} | New: {stats['new_items']} | Alerts: {stats['alerts_sent']} | Day {days_since}",
                ACTIONS_URL,
            )

    # -----------------------------------------------------------------------
    # Final save (heartbeat updated run_count/last_run)
    # -----------------------------------------------------------------------
    save_history(history)
    log.info(f"=== Alpha Scout complete. Alerts: {stats['alerts_sent']}, Gemini calls this run: {calls_this_run} ===")


if __name__ == "__main__":
    asyncio.run(main())
