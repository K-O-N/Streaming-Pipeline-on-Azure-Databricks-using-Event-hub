from datetime import datetime
import requests
import json 
 


base_url = "https://api.coinbase.com/api/v3/brokerage"
product_ids = ['BTC-USD', 'BTC-USDC', 'ETH-USD']



# Standardize event data to a consistent format
def enrich_event(event_type, id, payload):
  return {
    "event_type": event_type,
    "product_id": id,
    "ingested_at": datetime.now().isoformat(),
    "payload": payload
  }



# Stream event type 1 - Ticker 
def get_ticker_data(base_url, product_ids):

  for id in product_ids:
    url = f"{base_url}/market/products/{id}/ticker"

    try:
      response = requests.get(url, timeout=5)
      if response.status_code==200:
        yield enrich_event("ticker", id, response.json())

    except requests.RequestException:
      continue



# Stream event type 2 - Product data 
def get_product_data(base_url, product_ids):
  for id in product_ids:
    url = f"{base_url}/market/products/{id}/"

    try:
      response = requests.get(url, timeout=5)
      if response.status_code ==200:
        yield enrich_event("product", id, response.json())

    except requests.RequestException:
      continue



# Stream event type 3 - Candle data 
def get_candle_data(base_url, product_ids):
  for id in product_ids:
    url = f"{base_url}/market/products/{id}/candles?granularity=ONE_MINUTE"
    
    try:
      response = requests.get(url, timeout=5)

      if response.status_code == 200:
        data = response.json() 

        for candle in data.get("candles", []):
          yield enrich_event("candle", id, 
                             {
                               "start": candle['start'],
                                "low": candle['low'],
                                "high": candle['high'],
                                "open": candle['open'],
                                "close": candle['close'],
                                "volume": candle['volume']
                             })
          
    except requests.RequestException:
        continue




