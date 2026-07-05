"""
Smart timezone inference and optimal send time calculation (Phase 6.1)
Infers timezone from city + country and calculates best send time.
"""

import logging
from datetime import datetime, time, timedelta
from typing import Optional
import pytz

logger = logging.getLogger(__name__)


# City → Timezone mapping (major business cities)
CITY_TIMEZONE_MAP = {
    # North America
    "new york": "America/New_York",
    "los angeles": "America/Los_Angeles",
    "chicago": "America/Chicago",
    "san francisco": "America/Los_Angeles",
    "boston": "America/New_York",
    "seattle": "America/Los_Angeles",
    "denver": "America/Denver",
    "atlanta": "America/New_York",
    "dallas": "America/Chicago",
    "houston": "America/Chicago",
    "miami": "America/New_York",
    "phoenix": "America/Phoenix",
    "toronto": "America/Toronto",
    "vancouver": "America/Vancouver",
    "montreal": "America/Toronto",

    # Europe
    "london": "Europe/London",
    "paris": "Europe/Paris",
    "berlin": "Europe/Berlin",
    "madrid": "Europe/Madrid",
    "rome": "Europe/Rome",
    "amsterdam": "Europe/Amsterdam",
    "brussels": "Europe/Brussels",
    "zurich": "Europe/Zurich",
    "dublin": "Europe/Dublin",
    "stockholm": "Europe/Stockholm",
    "copenhagen": "Europe/Copenhagen",
    "oslo": "Europe/Oslo",
    "helsinki": "Europe/Helsinki",
    "vienna": "Europe/Vienna",
    "prague": "Europe/Prague",
    "warsaw": "Europe/Warsaw",
    "budapest": "Europe/Budapest",

    # Asia Pacific
    "tokyo": "Asia/Tokyo",
    "singapore": "Asia/Singapore",
    "hong kong": "Asia/Hong_Kong",
    "shanghai": "Asia/Shanghai",
    "beijing": "Asia/Shanghai",
    "sydney": "Australia/Sydney",
    "melbourne": "Australia/Melbourne",
    "mumbai": "Asia/Kolkata",
    "bangalore": "Asia/Kolkata",
    "delhi": "Asia/Kolkata",
    "seoul": "Asia/Seoul",
    "bangkok": "Asia/Bangkok",
    "jakarta": "Asia/Jakarta",
    "manila": "Asia/Manila",
    "dubai": "Asia/Dubai",

    # South America
    "sao paulo": "America/Sao_Paulo",
    "rio de janeiro": "America/Sao_Paulo",
    "buenos aires": "America/Argentina/Buenos_Aires",
    "mexico city": "America/Mexico_City",
    "bogota": "America/Bogota",
    "santiago": "America/Santiago",
}

# Country → Default Timezone (fallback)
COUNTRY_TIMEZONE_MAP = {
    "united states": "America/New_York",
    "usa": "America/New_York",
    "us": "America/New_York",
    "canada": "America/Toronto",
    "united kingdom": "Europe/London",
    "uk": "Europe/London",
    "germany": "Europe/Berlin",
    "france": "Europe/Paris",
    "spain": "Europe/Madrid",
    "italy": "Europe/Rome",
    "netherlands": "Europe/Amsterdam",
    "belgium": "Europe/Brussels",
    "switzerland": "Europe/Zurich",
    "ireland": "Europe/Dublin",
    "sweden": "Europe/Stockholm",
    "denmark": "Europe/Copenhagen",
    "norway": "Europe/Oslo",
    "finland": "Europe/Helsinki",
    "austria": "Europe/Vienna",
    "poland": "Europe/Warsaw",
    "japan": "Asia/Tokyo",
    "singapore": "Asia/Singapore",
    "china": "Asia/Shanghai",
    "australia": "Australia/Sydney",
    "india": "Asia/Kolkata",
    "south korea": "Asia/Seoul",
    "thailand": "Asia/Bangkok",
    "indonesia": "Asia/Jakarta",
    "philippines": "Asia/Manila",
    "uae": "Asia/Dubai",
    "brazil": "America/Sao_Paulo",
    "argentina": "America/Argentina/Buenos_Aires",
    "mexico": "America/Mexico_City",
    "colombia": "America/Bogota",
    "chile": "America/Santiago",
}


def infer_timezone(city: Optional[str], country: Optional[str]) -> Optional[str]:
    """
    Infer timezone from city and country.

    Priority:
    1. City match (more specific)
    2. Country match (fallback)
    3. None (if no match)

    Args:
        city: City name (e.g., "New York", "London")
        country: Country name (e.g., "United States", "UK")

    Returns:
        IANA timezone string (e.g., "America/New_York") or None
    """
    if not city and not country:
        return None

    # Normalize to lowercase for matching
    city_lower = city.lower().strip() if city else ""
    country_lower = country.lower().strip() if country else ""

    # Try city match first (more specific)
    if city_lower in CITY_TIMEZONE_MAP:
        tz = CITY_TIMEZONE_MAP[city_lower]
        logger.debug(f"Matched city '{city}' → timezone '{tz}'")
        return tz

    # Fallback to country
    if country_lower in COUNTRY_TIMEZONE_MAP:
        tz = COUNTRY_TIMEZONE_MAP[country_lower]
        logger.debug(f"Matched country '{country}' → timezone '{tz}'")
        return tz

    # No match
    logger.warning(f"Could not infer timezone for city='{city}', country='{country}'")
    return None


def calculate_optimal_send_time(
    timezone_str: str,
    preferred_day: str = "Tuesday",
    preferred_hour: int = 10,
) -> Optional[datetime]:
    """
    Calculate optimal send time in prospect's local timezone.

    Best practices:
    - Tuesday-Thursday (avoid Monday/Friday)
    - 10 AM - 2 PM local time
    - Default: Tuesday 10 AM local time

    Args:
        timezone_str: IANA timezone (e.g., "America/New_York")
        preferred_day: Day of week (default: "Tuesday")
        preferred_hour: Hour of day in 24h format (default: 10)

    Returns:
        Next occurrence of optimal send time in UTC
    """
    try:
        tz = pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        logger.error(f"Unknown timezone: {timezone_str}")
        return None

    # Get current time in prospect's timezone
    now_local = datetime.now(tz)

    # Find next occurrence of preferred day
    target_day = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
    }.get(preferred_day, 1)  # Default to Tuesday

    current_day = now_local.weekday()
    days_ahead = target_day - current_day

    # If today is past the preferred time, go to next week
    if days_ahead <= 0 and now_local.hour >= preferred_hour:
        days_ahead += 7
    elif days_ahead < 0:
        days_ahead += 7

    # Calculate target time
    target_date = now_local.date() + timedelta(days=days_ahead)
    target_time = time(hour=preferred_hour, minute=0)
    target_datetime_local = datetime.combine(target_date, target_time)
    target_datetime_local = tz.localize(target_datetime_local)

    # Convert to UTC for storage
    target_datetime_utc = target_datetime_local.astimezone(pytz.UTC)

    logger.debug(
        f"Optimal send time: {target_datetime_local.isoformat()} "
        f"({timezone_str}) → {target_datetime_utc.isoformat()} (UTC)"
    )

    return target_datetime_utc.replace(tzinfo=None)  # Store as naive UTC


def get_next_optimal_send_time(
    city: Optional[str],
    country: Optional[str],
    preferred_day: str = "Tuesday",
    preferred_hour: int = 10,
) -> Optional[datetime]:
    """
    One-stop function: Infer timezone and calculate optimal send time.

    Args:
        city: City name
        country: Country name
        preferred_day: Day of week (default: "Tuesday")
        preferred_hour: Hour in 24h format (default: 10)

    Returns:
        Optimal send time in UTC or None if timezone cannot be inferred
    """
    timezone_str = infer_timezone(city, country)
    if not timezone_str:
        return None

    return calculate_optimal_send_time(timezone_str, preferred_day, preferred_hour)


# ── Historical Timing Analysis ──


async def get_optimal_timing_from_history() -> dict:
    """
    Aggregate outreach_history open events by hour and day-of-week
    to learn the best performing time slots from historical data.

    Returns:
        Dict with best_hour, best_day, and breakdown stats.
        Can be used as a fallback when prospect timezone is unknown.
    """
    from database import prospects_collection

    pipeline = [
        {"$unwind": "$outreach_history"},
        {"$match": {"outreach_history.event_type": "open"}},
        {"$project": {
            "hour": {"$hour": "$outreach_history.timestamp"},
            "day_of_week": {"$dayOfWeek": "$outreach_history.timestamp"},
        }},
        {"$group": {
            "_id": {"hour": "$hour", "day_of_week": "$day_of_week"},
            "count": {"$sum": 1},
        }},
        {"$sort": {"count": -1}},
    ]

    try:
        results = await prospects_collection.aggregate(pipeline).to_list(168)  # 24h * 7d
    except Exception as e:
        logger.error(f"Historical timing analysis failed: {e}")
        return {
            "best_hour": 10,
            "best_day": "Tuesday",
            "source": "default",
        }

    if not results:
        return {
            "best_hour": 10,
            "best_day": "Tuesday",
            "source": "default",
        }

    # Find overall best hour and day
    best = results[0]
    best_hour = best["_id"]["hour"]
    # MongoDB dayOfWeek: 1=Sunday, 2=Monday, ..., 7=Saturday
    day_map = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
               5: "Thursday", 6: "Friday", 7: "Saturday"}
    best_day = day_map.get(best["_id"]["day_of_week"], "Tuesday")

    # Build breakdown
    by_hour = {}
    by_day = {}
    for r in results:
        h = r["_id"]["hour"]
        d = day_map.get(r["_id"]["day_of_week"], "Unknown")
        by_hour[h] = by_hour.get(h, 0) + r["count"]
        by_day[d] = by_day.get(d, 0) + r["count"]

    return {
        "best_hour": best_hour,
        "best_day": best_day,
        "total_opens_analyzed": sum(r["count"] for r in results),
        "by_hour": dict(sorted(by_hour.items(), key=lambda x: x[1], reverse=True)[:5]),
        "by_day": dict(sorted(by_day.items(), key=lambda x: x[1], reverse=True)),
        "source": "historical",
    }


def calculate_optimal_send_time_with_history(
    timezone_str: Optional[str],
    historical_best: Optional[dict] = None,
) -> Optional[datetime]:
    """
    Calculate optimal send time using timezone + historical data as override.

    If historical data shows a strong pattern, use that hour instead of default.
    If no timezone available but historical data exists, use UTC with best hour.

    Args:
        timezone_str: Prospect timezone (may be None)
        historical_best: Result from get_optimal_timing_from_history()

    Returns:
        Optimal send time in UTC
    """
    preferred_day = "Tuesday"
    preferred_hour = 10

    # Use historical data as override if available
    if historical_best and historical_best.get("source") == "historical":
        if historical_best.get("total_opens_analyzed", 0) >= 20:
            preferred_hour = historical_best.get("best_hour", 10)
            preferred_day = historical_best.get("best_day", "Tuesday")

    if timezone_str:
        return calculate_optimal_send_time(timezone_str, preferred_day, preferred_hour)

    # No timezone: use UTC with best hour
    if historical_best:
        now = datetime.utcnow()
        target_day_map = {
            "Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3,
            "Friday": 4, "Saturday": 5, "Sunday": 6,
        }
        target_day = target_day_map.get(preferred_day, 1)
        days_ahead = target_day - now.weekday()
        if days_ahead <= 0 and now.hour >= preferred_hour:
            days_ahead += 7
        elif days_ahead < 0:
            days_ahead += 7

        target_date = now.date() + timedelta(days=days_ahead)
        return datetime.combine(target_date, time(hour=preferred_hour, minute=0))

    return None


# ── Bulk Processing ──

def enrich_prospects_with_timezone(prospects: list[dict]) -> list[dict]:
    """
    Enrich a list of prospects with timezone and optimal_send_time.
    Modifies prospects in-place and returns them.

    Args:
        prospects: List of prospect dicts (must have 'city' and 'country' fields)

    Returns:
        Same list with 'timezone' and 'optimal_send_time' added
    """
    for prospect in prospects:
        city = prospect.get("city")
        country = prospect.get("country")

        timezone_str = infer_timezone(city, country)
        if timezone_str:
            prospect["timezone"] = timezone_str
            optimal_time = calculate_optimal_send_time(timezone_str)
            if optimal_time:
                prospect["optimal_send_time"] = optimal_time
        else:
            prospect["timezone"] = None
            prospect["optimal_send_time"] = None

    return prospects
