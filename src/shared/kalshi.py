import os
from dotenv import load_dotenv
import datetime as dt
from urllib.parse import urlencode
import requests
from rich import print
import polars as pl

load_dotenv(override=True)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/"


class KalshiClient:

    def __init__(self, api_key_id: str, private_key_pem: str) -> None:
        self.api_key_id = api_key_id
        self.private_key_pem = private_key_pem

    def get_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        max_close_ts: dt.datetime | None = None,
        min_close_ts: dt.datetime | None = None,
    ) -> pl.DataFrame:
        if min_close_ts is not None:
            min_close_ts = int(min_close_ts.timestamp())
        
        if max_close_ts is not None:
            max_close_ts = int(max_close_ts.timestamp())

        endpoint = "markets/"
        
        params = {}
        params['limit'] = 1000
        if series_ticker is not None:
            params['series_ticker'] = series_ticker
        if event_ticker is not None:
            params['event_ticker'] = event_ticker
        if min_close_ts is not None:
            params['min_close_ts'] = min_close_ts
        if max_close_ts is not None:
            params['max_close_ts'] = max_close_ts
        
        url = BASE_URL + endpoint
        if params:
            url += "?" + urlencode(params)

        response = requests.get(url, params)

        columns = [
            'ticker',
            'event_ticker',
            # 'market_type',
            'title',
            # 'subtitle',
            # 'yes_sub_title',
            # 'no_sub_title',
            # 'open_time',
            # 'close_time',
            'expected_expiration_time',
            # 'expiration_time',
            # 'latest_expiration_time',
            # 'settlement_timer_seconds',
            'status',
            # 'response_price_units',
            # 'notional_value',
            # 'notional_value_dollars',
            'yes_bid',
            # 'yes_bid_dollars',
            'yes_ask',
            # 'yes_ask_dollars',
            'no_bid',
            # 'no_bid_dollars',
            'no_ask',
            # 'no_ask_dollars',
            # 'last_price',
            # 'last_price_dollars',
            # 'previous_yes_bid',
            # 'previous_yes_bid_dollars',
            # 'previous_yes_ask',
            # 'previous_yes_ask_dollars',
            # 'previous_price',
            # 'previous_price_dollars',
            'volume',
            # 'volume_24h',
            # 'liquidity',
            # 'liquidity_dollars',
            # 'open_interest',
            'result',
            # 'settlement_value',
            # 'can_close_early',
            # 'expiration_value',
            # 'category',
            # 'risk_limit_cents',
            # 'yes_topbook_liquidity_dollars',
            # 'no_topbook_liquidity_dollars',
            # 'strike_type',
            # 'custom_strike',
            # 'rules_primary',
            # 'rules_secondary',
            # 'early_close_condition',
            # 'tick_size'
        ]

        return pl.DataFrame(response.json()['markets']).select(columns)


def _create_kalshi_client():
    kalshi_api_key = os.getenv("KALSHI_API_KEY")

    with open("private-key.pem", "r") as f:
        private_key = f.read()

    return KalshiClient(kalshi_api_key, private_key)


kalshi_client = _create_kalshi_client()
