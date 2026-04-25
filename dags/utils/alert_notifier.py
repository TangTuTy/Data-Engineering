"""
Alert Notifier — ส่งแจ้งเตือนผ่าน Discord Webhook
มี cooldown เพื่อป้องกัน spam (หุ้นตัวเดียวกัน + alert type เดียวกัน
จะไม่ถูกส่งซ้ำใน N วินาที)
"""
import os
import json
import logging
import time
from datetime import datetime, timezone
from urllib import request, error

logger = logging.getLogger(__name__)

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
ALERT_COOLDOWN_SECONDS = int(os.environ.get("ALERT_COOLDOWN_SECONDS", 300))  # default 5 นาที

# in-memory cooldown tracker: {(symbol, alert_type): last_sent_timestamp}
_cooldown_tracker = {}

# Discord embed colors (decimal RGB)
COLOR_MAP = {
    "alert_extreme_up":   0x16A34A,  # dark green
    "alert_spike":        0x22C55E,  # green
    "alert_extreme_down": 0x991B1B,  # dark red
    "alert_crash":        0xEF4444,  # red
    "alert_war_sensitive":0xF97316,  # orange
    "alert_volume_spike": 0xA855F7,  # purple
}

ALERT_DISPLAY = {
    "alert_extreme_up":   ("🔥", "Extreme Spike",    "Price up >10%"),
    "alert_spike":        ("⬆️",  "Price Spike",     "Price up >5%"),
    "alert_extreme_down": ("💥", "Extreme Crash",    "Price down >10%"),
    "alert_crash":        ("⬇️",  "Price Drop",      "Price down >5%"),
    "alert_war_sensitive":("⚔️",  "War-Sensitive",   "Negative sector + drop"),
    "alert_volume_spike": ("📊", "Volume Spike",     "Volume >2x baseline"),
}


def _is_in_cooldown(symbol, alert_type):
    """Return True ถ้าเพิ่งส่ง alert นี้ไปยังไม่ครบ cooldown"""
    key = (symbol, alert_type)
    last_sent = _cooldown_tracker.get(key)
    if last_sent is None:
        return False
    elapsed = time.time() - last_sent
    return elapsed < ALERT_COOLDOWN_SECONDS


def _mark_sent(symbol, alert_type):
    _cooldown_tracker[(symbol, alert_type)] = time.time()


def send_discord_alert(symbol, alert_type, price, impact_pct, sector=None,
                       volume=None, baseline_volume=None, war_impact=None):
    """
    ส่ง alert ไปที่ Discord (ถ้าไม่อยู่ใน cooldown)

    Returns:
        True  = ส่งสำเร็จ
        False = ถูก cooldown หรือไม่ได้ตั้งค่า webhook หรือ error
    """
    if not DISCORD_WEBHOOK_URL:
        return False

    if _is_in_cooldown(symbol, alert_type):
        logger.debug(f"⏸️  Cooldown: {symbol} {alert_type}")
        return False

    icon, title, desc = ALERT_DISPLAY.get(alert_type, ("❓", alert_type, ""))
    color = COLOR_MAP.get(alert_type, 0x6B7280)

    # สร้าง Discord embed
    fields = [
        {"name": "Symbol",  "value": f"**{symbol}**",          "inline": True},
        {"name": "Price",   "value": f"${price:.2f}",          "inline": True},
        {"name": "Impact",  "value": f"{impact_pct:+.2f}%",    "inline": True},
    ]
    if sector:
        fields.append({"name": "Sector", "value": sector, "inline": True})
    if war_impact:
        fields.append({"name": "War Impact", "value": war_impact, "inline": True})
    if volume is not None and baseline_volume:
        ratio = volume / baseline_volume
        fields.append({
            "name": "Volume",
            "value": f"{int(volume):,} ({ratio:.1f}× baseline)",
            "inline": True,
        })

    embed = {
        "title": f"{icon} {title}",
        "description": desc,
        "color": color,
        "fields": fields,
        "footer": {"text": f"S&P 500 War Impact Monitor"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    payload = {"embeds": [embed]}
    data = json.dumps(payload).encode("utf-8")

    try:
        req = request.Request(
            DISCORD_WEBHOOK_URL,
            data=data,
            headers={
            "Content-Type": "application/json",
            "User-Agent": "DiscordBot (https://github.com, 1.0)"  
            },
            method="POST",
        )
        with request.urlopen(req, timeout=5) as resp:
            if 200 <= resp.status < 300:
                _mark_sent(symbol, alert_type)
                logger.info(f"📨 Discord alert sent: {symbol} {alert_type}")
                return True
            else:
                logger.warning(f"Discord webhook returned status {resp.status}")
                return False
    except error.HTTPError as e:
        logger.error(f"Discord HTTP error: {e.code} {e.reason}")
        return False
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")
        return False


def send_critical_pipeline_alert(message, stage="pipeline"):
    """ส่ง alert เมื่อ pipeline เกิด CRITICAL error (สำหรับใช้ใน Airflow)"""
    if not DISCORD_WEBHOOK_URL:
        return False

    embed = {
        "title": "🚨 Pipeline Critical Error",
        "description": f"**Stage:** {stage}\n\n{message[:1500]}",
        "color": 0x991B1B,
        "footer": {"text": "Airflow Pipeline Alert"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    payload = {"embeds": [embed]}
    data = json.dumps(payload).encode("utf-8")

    try:
        req = request.Request(DISCORD_WEBHOOK_URL, data=data,
                             headers={
            "Content-Type": "application/json",
            "User-Agent": "DiscordBot (https://github.com, 1.0)"  
            }, method="POST")
        with request.urlopen(req, timeout=5):
            return True
    except Exception as e:
        logger.error(f"Failed to send pipeline alert: {e}")
        return False