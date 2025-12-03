import random
from datetime import datetime, timedelta, timezone
from typing import Iterable
from kiteconnect import KiteConnect, KiteTicker
import pyotp
from .kite_utils import KiteSecrets, get_request_token
from .parquet_utils import StreamingParquetWriter, get_tick_schema
from itertools import batched
from functools import partial, lru_cache
import time
import logging
import threading
import queue
import requests

logger = logging.getLogger(__name__)

def is_trading_day() -> bool:
    today = (
        datetime.now(timezone.utc)
        .astimezone(timezone(timedelta(hours=5, minutes=30))).date()
    )

    if today.weekday() >= 5:  # Saturday=5, Sunday=6
        return False

    headers = {"User-Agent": "Mozilla/5.0"}
    url = "https://www.nseindia.com/api/holiday-master?type=trading"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # date format: '26-Jan-2025'
        holidays = [datetime.strptime(i["tradingDate"], "%d-%b-%Y").date()
                    for i in response.json()["CM"]]

        return not (today in holidays)
    return False

class Ticker:
    def __new__(cls):
        # Singleton pattern
        if not hasattr(cls, "instance"):
            cls.instance = super(Ticker, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # Initialise writer once; do not recreate on token refresh
        self._connected = [False] * 3
        self.refresh_token()

    def refresh_token(self):
        # Refresh session and rebuild tickers with new access token
        self.kite = KiteConnect(api_key=KiteSecrets.ApiKey.value)
        self.totp = pyotp.TOTP(KiteSecrets.TOTP_SECRET.value)
        self.request_token = get_request_token(
            KiteSecrets.UserId.value, KiteSecrets.Password.value, self.kite, self.totp
        )
        self.session_info = self.kite.generate_session(
            self.request_token, api_secret=KiteSecrets.ApiSecret.value
        )
        self.kite.set_access_token(self.session_info["access_token"])
        # Close any existing sockets before recreating
        try:
            for t in getattr(self, "tickers", []):
                try:
                    t.close(code=1000, reason="refresh_token")
                except Exception:
                    pass
        finally:
            self.tickers = [
                KiteTicker(KiteSecrets.ApiKey.value, self.session_info["access_token"])
                for _ in range(3)
            ]
            self.writers = [
                StreamingParquetWriter(
                    base_path="ticks/",
                    schema=get_tick_schema(),
                )
                for _ in range(3)
            ]

    @lru_cache(maxsize=1)
    def get_batched_instrument_tokens(
        self, exchange: str = "NSE", batch_size: int = 3000
    ) -> list[list[int]]:
        instrument_tokens = [
            instrument["instrument_token"]
            for instrument in self.kite.instruments(exchange=exchange)
        ]

        # random shuffle to distribute load
        random.shuffle(instrument_tokens)

        logger.info(f"Total instrument tokens fetched: {len(instrument_tokens)}")
        return list(batched(instrument_tokens, batch_size))

    def start(self):
        # Connect once per ticker; avoid spawning new threads in a loop
        for idx, (ticker, ins_tokens) in enumerate(
            zip(self.tickers, self.get_batched_instrument_tokens())
        ):
            logger.info(
                f"Starting ticker {idx} with {len(ins_tokens)} instrument tokens..."
            )
            ticker.on_ticks = partial(self.on_ticks, idx)
            ticker.on_reconnect = partial(self.on_reconnect, idx)
            ticker.on_connect = partial(self.on_connect, ins_tokens, idx)
            ticker.on_close = partial(self.on_close, idx)
            ticker.on_error = partial(self.on_error, idx)
            ticker.connect(threaded=True)

    def stop(self):
        try:
            for idx, ticker in enumerate(self.tickers):
                logger.info(f"Stopping ticker {idx}...")
                ticker.close(code=1000, reason="stop")
                logger.info(f"Ticker {idx} stopped.")
        finally:
            for writer in self.writers:
                writer.close()

    # Callback methods
    def on_ticks(self, idx, ws, ticks):
        logger.info(f"Ticker {idx} received {len(ticks)} ticks.")
        self.writers[idx].write_rows(ticks)

    def on_connect(self, ins_tokens, idx, ws, response):
        ws.subscribe(ins_tokens)
        ws.set_mode(ws.MODE_FULL, ins_tokens)
        self._connected[idx] = True
        logger.info(f"Ticker {idx} connected")

    def on_close(self, idx, ws, code, reason):
        # Callback when connection is closed
        self._connected[idx] = False
        logger.info(f"Ticker {idx} connection closed: {code} - {reason}")

    def on_error(self, idx, ws, code, reason):
        # Callback when connection is closed or error occurs
        self._connected[idx] = False
        logger.error(
            f"Ticker {idx} connection closed or error occurred: {code} - {reason}"
        )

    def on_reconnect(self, idx, ws, attempts_count):
        # Callback on reconnect attempts; can be used for logging or metrics
        logger.info(f"Ticker {idx} attempting to reconnect, attempt #{attempts_count}")
