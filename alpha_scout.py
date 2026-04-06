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
from google.genai import types
import PIL.Image
from telegram import Bot
from telegram.constants import ParseMode

# ---------------------------------------------------------------------------
# Budget Constants
# ---------------------------------------------------------------------------
DAILY_GEMINI_BUDGET = 1400   # Hard stop below 1,500 free limit
DAILY_ETSY_BUDGET   = 500    # Defensive cap
ALPHA_MIN_PROFIT    = 100    # Minimum gross profit in dollars
ALPHA_MIN_ROI       = 1.5    # Minimum ROI multiplier
CONFIDENCE_SKIP     = 0.50   # Below this: no alert, just log
HISTORY_PRUNE_DAYS  = 90     # Remove unseen/unalerted items older than this
GEMINI_MODEL        = "gemini-1.5-flash"

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
LAUNCH_DATE_STR     = os.environ.get("LAUNCH_DATE", "2026-04-05")
LAUNCH_DATE         = date.fromisoformat(LAUNCH_DATE_STR)
FCM_SERVER_KEY      = os.environ.get("FCM_SERVER_KEY", "")
FCM_DEVICE_TOKENS   = [
    t for t in [
        os.environ.get("FCM_DEVICE_TOKEN_1", ""),
        os.environ.get("FCM_DEVICE_TOKEN_2", ""),
    ] if t
]
FCM_ENABLED         = date.today() >= LAUNCH_DATE + timedelta(days=5)
TELEGRAM_ENABLED    = not FCM_ENABLED

# Dry-run mode — scrape + Gemini run normally, skip Etsy/Telegram/FCM sends
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

ESTATE_BASE_URL     = "https://mainstreetestatesales.com"
PRODUCTS_JSON_URL   = f"{ESTATE_BASE_URL}/collections/all/products.json"
HISTORY_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.json")

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
    """Remove items from history that are unalerted and unseen for >HISTORY_PRUNE_DAYS days."""
    today = date.today()
    to_remove = []
    for handle, entry in history["items"].items():
        if entry.get("alerted", False):
            continue
        last_seen = entry.get("last_seen")
        if not last_seen:
            continue
        try:
            last_seen_date = date.fromisoformat(last_seen)
        except (ValueError, TypeError):
            continue
        if (today - last_seen_date).days > HISTORY_PRUNE_DAYS:
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
    while True:
        log.info(f"Scraping page {page} ...")
        products = retry(_scrape_page, client, page, label=f"Shopify page {page}")
        if not products:
            break
        all_products.extend(products)
        page += 1
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


def call_gemini(client: genai.Client, prompt: str, image_url: str | None) -> dict | None:
    """Send multimodal prompt to Gemini; return parsed JSON or None."""
    try:
        contents: list = []
        if image_url:
            try:
                with httpx.Client(timeout=15) as img_client:
                    img_resp = img_client.get(image_url)
                    img_resp.raise_for_status()
                    img_data = img_resp.content
                img = PIL.Image.open(io.BytesIO(img_data))
                contents = [prompt, img]
            except Exception as e:
                log.warning(f"Image download failed — falling back to text-only: {e}")
                contents = [prompt]
        else:
            contents = [prompt]

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=contents,
        )

        raw_text = response.text if hasattr(response, 'text') and response.text else ""
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
    return " ".join(cleaned.split())


def _etsy_request(client: httpx.Client, cleaned: str) -> dict:
    """Single Etsy API call (retryable unit)."""
    headers = {"x-api-key": ETSY_API_KEY}
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
            # 60% Gemini + 20% Etsy + 20% eBay
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
# Telegram helpers
# ---------------------------------------------------------------------------

def escape_md2(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    special = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))


def build_alert_message(item: dict, gemini_data: dict, etsy_data: dict, market_est: float,
                        gross_profit: float, roi: float, tier: str,
                        is_price_drop: bool = False) -> str:
    conf_pct = int(gemini_data["confidence"] * 100)
    low = gemini_data["estimated_resale_low"]
    high = gemini_data["estimated_resale_high"]
    key_signals = gemini_data.get("key_signals", "")
    category = gemini_data.get("category", "other")
    best_platform = gemini_data.get("best_platform", "either")

    etsy_count = etsy_data.get("etsy_listing_count", 0)
    etsy_median = etsy_data.get("etsy_median_ask", 0)
    etsy_url = etsy_data.get("etsy_search_url", "")

    estate_url = f"{ESTATE_BASE_URL}/products/{item['handle']}"
    google_url = f"https://google.com/search?q={quote_plus(item['title'] + ' sold price resale')}"

    # Pre-compute all formatted strings to avoid nested f-string quote issues (BUG 1)
    title_esc = escape_md2(item["title"])
    signals_esc = escape_md2(key_signals)
    price_str = escape_md2(f"{item['price']:.0f}")
    low_str = escape_md2(str(low))
    high_str = escape_md2(str(high))
    conf_str = escape_md2(str(conf_pct))
    category_str = escape_md2(category.capitalize())
    platform_str = escape_md2(best_platform.capitalize())
    market_est_str = escape_md2(f"{market_est:.0f}")
    profit_str = escape_md2(f"{gross_profit:.0f}")
    roi_str = escape_md2(f"{roi:.1f}")
    etsy_median_str = escape_md2(f"{etsy_median:.0f}")
    etsy_count_str = escape_md2(str(etsy_count))

    # Escape link label text to prevent MarkdownV2 breakage on titles with ) etc. (BUG 4)
    estate_label = escape_md2("\U0001f517 View Estate Listing")
    etsy_label = escape_md2("\U0001f6cd Search Etsy")
    google_label = escape_md2("\U0001f50d Google This")

    drop_suffix = " \u2014 Price Drop" if is_price_drop else ""
    if tier == "HIGH":
        emoji = "\U0001f7e2"  # green circle
        header = f"{emoji} HIGH CONFIDENCE ALPHA{drop_suffix}"
    elif tier == "MEDIUM":
        emoji = "\U0001f7e1"  # yellow circle
        header = f"{emoji} MEDIUM CONFIDENCE ALPHA{drop_suffix}"
    else:
        emoji = "\U0001f534"  # red circle
        header = f"{emoji} SPECULATIVE \u2014 Verify before buying{drop_suffix}"

    # Etsy market line
    if etsy_count >= 3:
        etsy_line = f"\U0001f6cd Etsy Active Market: *${etsy_median_str} median* \\({etsy_count_str} listings\\)"
    elif etsy_count > 0:
        etsy_line = f"\U0001f6cd Etsy Active Market: *thin* \\({etsy_count_str} listings found\\)"
    else:
        etsy_line = "\U0001f6cd Etsy Active Market: *thin* \\(0 listings found\\)"

    # For HIGH/MEDIUM include market value + profit lines
    if tier in ("HIGH", "MEDIUM"):
        value_lines = (
            f"\u2705 Est\\. Market Value: *${market_est_str}*\n"
            f"\U0001f4c8 Potential Profit: *~${profit_str}* \\({roi_str}x ROI\\)"
        )
    else:
        value_lines = ""

    price_drop_line = "\U0001f53d Price Drop \u2014 previously listed at a higher price\n" if is_price_drop else ""

    msg = (
        f"{header}\n\n"
        f"*{title_esc}*\n\n"
        f"\U0001f4b0 Estate Price: *${price_str}*\n"
        f"{price_drop_line}"
        f"\U0001f4ca Gemini Estimate: *${low_str} \u2013 ${high_str}* \\| Confidence: {conf_str}%\n"
        f"{etsy_line}\n"
    )
    if value_lines:
        msg += f"{value_lines}\n"
    msg += (
        f"\n\U0001f916 _{signals_esc}_\n\n"
        f"\U0001f4e6 {category_str} \\| Best platform: {platform_str} \\| Confidence: {conf_str}%\n\n"
        f"[{estate_label}]({estate_url})  "
        f"[{etsy_label}]({etsy_url})  "
        f"[{google_label}]({google_url})"
    )
    return msg


def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram HTML parse mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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
        f"\u2705 Alerts sent: <b>{s['alerts_sent']}</b>\n"
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

def send_fcm(title: str, body: str, url: str) -> None:
    """Send FCM push notification to all registered device tokens."""
    if not FCM_SERVER_KEY or not FCM_DEVICE_TOKENS:
        log.warning("FCM enabled but no server key or device tokens configured")
        return
    for token in FCM_DEVICE_TOKENS:
        try:
            resp = httpx.post(
                "https://fcm.googleapis.com/fcm/send",
                headers={
                    "Authorization": f"key={FCM_SERVER_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "to": token,
                    "notification": {
                        "title": title,
                        "body": body,
                        "click_action": url,
                    },
                    "data": {"url": url},
                },
                timeout=15,
            )
            resp.raise_for_status()
            log.info(f"FCM notification sent to token ...{token[-6:]}")
        except Exception as e:
            log.error(f"FCM send failed: {e}")


def build_fcm_alert(item: dict, gemini_data: dict, market_est: float,
                    gross_profit: float, roi: float, tier: str,
                    is_price_drop: bool = False) -> tuple[str, str, str]:
    """Returns (title, body, url) for FCM notification."""
    drop_tag = " - Price Drop" if is_price_drop else ""
    title = f"{tier} ALPHA{drop_tag}: {item['title'][:50]}"
    body = (
        f"Estate: ${item['price']:.0f} -> Est. Value: ${market_est:.0f} "
        f"(~${gross_profit:.0f} profit, {roi:.1f}x ROI)"
    )
    url = f"{ESTATE_BASE_URL}/products/{item['handle']}"
    return title, body, url

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def send_telegram(bot: Bot, text: str) -> None:
    if DRY_RUN or not TELEGRAM_ENABLED:
        if DRY_RUN:
            log.info(f"[DRY RUN] Would send Telegram message ({len(text)} chars)")
        return
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN_V2,
                               disable_web_page_preview=True)
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


async def main() -> None:
    log.info("=== Alpha Scout starting ===")
    log.info(f"Notification mode: {'FCM' if FCM_ENABLED else 'Telegram'} | Day {(date.today() - LAUNCH_DATE).days + 1} since launch")

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

    # -----------------------------------------------------------------------
    # Prune old unseen items (ENHANCEMENT 4)
    # -----------------------------------------------------------------------
    pruned = prune_old_history(history)
    if pruned:
        log.info(f"Pruned {pruned} old unseen items from history")

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
                await send_telegram(bot, escape_md2(f"❌ Alpha Scout FAILED — Estate site unreachable: {e}"))
            if FCM_ENABLED and not DRY_RUN:
                send_fcm("Alpha Scout FAILED", f"Estate site unreachable: {e}", "")
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
        stale = [h for h in history["items"] if h not in current_handles]
        for h in stale:
            del history["items"][h]
        if stale:
            log.info(f"Removed {len(stale)} stale items from history")

        stats = {
            "total_scraped": len(products),
            "new_items": 0,
            "skipped": 0,
            "price_drops": 0,
            "alerts_sent": 0,
            "below_threshold": 0,
            "gemini_calls": history["gemini_calls_today"],
            "cold_start_remaining": 0,
        }

        items_to_process: list[dict] = []
        etsy_calls_today = 0

        for item in products:
            handle = item["handle"]
            if handle in history["items"]:
                hist_entry = history["items"][handle]
                if hist_entry.get("last_seen_price") == item["price"]:
                    # Same price — skip
                    hist_entry["last_seen"] = today_str
                    stats["skipped"] += 1
                    continue
                else:
                    # Price dropped — re-evaluate
                    stats["price_drops"] += 1
                    item["is_price_drop"] = True
                    items_to_process.append(item)
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

            # ---------------------------------------------------------------
            # Stage 1 — Gemini
            # ---------------------------------------------------------------
            prompt = build_gemini_prompt(item["title"], item["price"], item["converted"])
            gemini_data = call_gemini(gemini_client, prompt, item["image_url"])
            stats["gemini_calls"] += 1
            history["gemini_calls_today"] = stats["gemini_calls"]
            time.sleep(0.5)  # Respect Gemini 15 RPM rate limit

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
                stats["gemini_failures"] = stats.get("gemini_failures", 0) + 1
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
                # Between 0.50 and 0.65 — still below the Etsy gate but above skip
                # These are SPECULATIVE tier
                etsy_data = {"etsy_median_ask": 0, "etsy_listing_count": 0, "etsy_search_url": f"https://www.etsy.com/search?q={quote_plus(clean_title_for_etsy(item['title']))}"}
                ebay_data = None
                market_est = compute_market_estimate(gemini_data, etsy_data, ebay_data)
                gross_profit = market_est - item["price"]
                roi = market_est / item["price"] if item["price"] > 0 else 0

                tier = confidence_tier(confidence, etsy_data["etsy_listing_count"])

                if gross_profit >= ALPHA_MIN_PROFIT and roi >= ALPHA_MIN_ROI:
                    if DRY_RUN:
                        log.info(f"[DRY RUN] Would send alert: {tier} — {item['title']}")
                        if FCM_ENABLED:
                            log.info(f"[DRY RUN] Would send FCM notification: {tier} ALPHA: {item['title'][:50]}")
                    else:
                        if TELEGRAM_ENABLED and bot:
                            alert_msg = build_alert_message(item, gemini_data, etsy_data, market_est, gross_profit, roi, tier,
                                                            is_price_drop=is_price_drop)
                            await send_telegram(bot, alert_msg)
                        if FCM_ENABLED:
                            fcm_title, fcm_body, fcm_url = build_fcm_alert(item, gemini_data, market_est, gross_profit, roi, tier, is_price_drop)
                            send_fcm(fcm_title, fcm_body, fcm_url)
                        time.sleep(1)
                    stats["alerts_sent"] += 1
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

            if gross_profit >= ALPHA_MIN_PROFIT and roi >= ALPHA_MIN_ROI:
                if DRY_RUN:
                    log.info(f"[DRY RUN] Would send alert: {tier} — {item['title']}")
                    if FCM_ENABLED:
                        log.info(f"[DRY RUN] Would send FCM notification: {tier} ALPHA: {item['title'][:50]}")
                else:
                    if TELEGRAM_ENABLED and bot:
                        alert_msg = build_alert_message(item, gemini_data, etsy_data, market_est, gross_profit, roi, tier,
                                                        is_price_drop=is_price_drop)
                        await send_telegram(bot, alert_msg)
                    if FCM_ENABLED:
                        fcm_title, fcm_body, fcm_url = build_fcm_alert(item, gemini_data, market_est, gross_profit, roi, tier, is_price_drop)
                        send_fcm(fcm_title, fcm_body, fcm_url)
                    time.sleep(1)  # 1-second delay between messages
                stats["alerts_sent"] += 1
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

    # -----------------------------------------------------------------------
    # Gemini batch summary
    # -----------------------------------------------------------------------
    gemini_failures = stats.get("gemini_failures", 0)
    gemini_successes = stats["gemini_calls"] - gemini_failures
    log.info(f"Gemini batch: {gemini_successes} parsed OK, {gemini_failures} failed out of {stats['gemini_calls']} calls")

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
                "",
            )

    # -----------------------------------------------------------------------
    # Save history
    # -----------------------------------------------------------------------
    save_history(history)
    log.info(f"=== Alpha Scout complete. Alerts: {stats['alerts_sent']}, Gemini calls: {stats['gemini_calls']} ===")


if __name__ == "__main__":
    asyncio.run(main())
