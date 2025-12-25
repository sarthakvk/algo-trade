from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import logging

logger = logging.getLogger(__name__)


def get_historical_data(
    kite: KiteConnect, ins_token: int, interval, start_date: datetime, end_date: datetime
) -> list[dict]:
    """Fetches historical data for given instrument token and interval.
    Args:
        kite: KiteConnect instance
        ins_token: instrument token
        interval: e.g., "minute", "5minute", "day"
        start_date: start datetime
        end_date: end datetime
    Returns:
        List of historical data dicts
    """
    interval_limit = {
        "minute": 60,
        "3minute": 100,
        "5minute": 100,
        "10minute": 100,
        "15minute": 200,
        "30minute": 200,
        "60minute": 400,
        "day": 2000,
    }

    days_limit = interval_limit[interval]
    total_days = (end_date - start_date).days + 1
    chunks = (total_days + days_limit - 1) // days_limit

    output = []
    for i in range(chunks):
        start = start_date + timedelta(days=i * days_limit)
        end = min(start_date + timedelta(days=(i +   1) * days_limit - 1), end_date)

        start = datetime.combine(start.date(), datetime.min.time())
        end = datetime.combine(end.date(), datetime.max.time())

        logger.info(f"Fetching data from {start.date()} to {end.date()}...")
        chunk_data = kite.historical_data(
            ins_token,
            start,
            end,
            interval,
        )
        output.extend(chunk_data)
    return output
