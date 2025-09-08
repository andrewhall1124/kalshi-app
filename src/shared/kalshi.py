from kalshi_python import Configuration, KalshiClient
import os
from dotenv import load_dotenv

load_dotenv(override=True)

def _create_kalshi_client():
    kalshi_api_key = os.getenv("KALSHI_API_KEY")
    
    config = Configuration(
        host="https://api.elections.kalshi.com/trade-api/v2"
    )
    
    with open("private-key.pem", "r") as f:
        private_key = f.read()
    
    config.api_key_id = kalshi_api_key
    config.private_key_pem = private_key
    
    return KalshiClient(config)

kalshi_client = _create_kalshi_client()

