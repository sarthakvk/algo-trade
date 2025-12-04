from urllib.parse import parse_qs, urlparse
from kiteconnect import KiteConnect, KiteTicker
import pyotp
import requests
from enum import Enum
from os import environ as env

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())  # Load .env variables


class KiteSecrets(Enum):
    # Secrets
    UserId: str = env["USER_ID"]
    Password: str = env["PASSWORD"]
    ApiKey: str = env["API_KEY"]
    ApiSecret: str = env["API_SECRET"]
    TOTP_SECRET: str = env["TOTP_SECRET"]


class KiteUrls(Enum):
    LOGIN = "https://kite.zerodha.com/api/login"
    TWOFA = "https://kite.zerodha.com/api/twofa"


def get_request_token(
    user_id: str, password: str, kite: KiteConnect, totp: pyotp.TOTP
) -> str:
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    with requests.Session() as session:
        # 1) post credentials
        login_payload = {"user_id": user_id, "password": password, "type": "user_id"}
        login_resp = session.post(
            KiteUrls.LOGIN.value, data=login_payload, headers=headers
        ).json()

        # 2) post TOTP
        totp_payload = {
            "user_id": user_id,
            "request_id": login_resp["data"]["request_id"],
            "twofa_type": "totp",
            "skip_session": True,
            "twofa_value": f"{totp.now()}",
        }
        session.post(KiteUrls.TWOFA.value, data=totp_payload, headers=headers)

        # 3) complete connect/login flow and extract request_token
        connect_resp = session.get(kite.login_url(), allow_redirects=False)
        finish_resp = session.get(
            connect_resp.headers["location"], allow_redirects=False
        )

        return parse_qs(urlparse(finish_resp.headers["location"]).query)[
            "request_token"
        ][0]
