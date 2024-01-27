import asyncio
import time
import cProfile
import aiohttp
from aiohttp import ClientSession, TCPConnector
from api_keys import paper_key, paper_secret
import pandas as pd
import os
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from tenacity import retry, wait_fixed, retry_if_exception_type
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm
import aiofiles

class RateLimitedSession:
    def __init__(self):
        self.session = ClientSession(connector=TCPConnector(limit_per_host=10))
        self.rate_limiter = AsyncLimiter(200, 70)  # Adjust the rate limits as needed

    async def close(self):
        await self.session.close()

    async def get(self, url, **kwargs):
        async with self.rate_limiter:
            return await self.session.get(url, **kwargs)

async def save_to_csv(data, ticker):
    if not data:
        return

    directory = f"data/bars/{ticker}"
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, "2y_5m.csv")
    df = pd.DataFrame(data)

    async with aiofiles.open(file_path, mode='w') as f:
        await f.write(df.to_csv(index=False))

@retry(retry=retry_if_exception_type(aiohttp.ClientError), wait=wait_fixed(5))
async def process_ticker(rate_limited_session, ticker, formatted_start_time, paper_key, paper_secret):
    all_bars = []
    params = {
        'timeframe': '5Min',
        'start': formatted_start_time,
        'adjustment': 'all'
    }
    headers = {
        'APCA-API-KEY-ID': paper_key,
        'APCA-API-SECRET-KEY': paper_secret
    }
    ALPACA_BARS_URL = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"

    while True:
        try:
            response = await rate_limited_session.get(ALPACA_BARS_URL, headers=headers, params=params)
            response.raise_for_status()

            data = await response.json()
            if 'bars' in data and data['bars']:
                all_bars.extend(data['bars'])
                next_page_token = data.get('next_page_token')
                if next_page_token and (next_page_token != params.get('page_token')):
                    params['page_token'] = next_page_token
                else:
                    break
            else:
                break

            # Handle rate limiting
            rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
            rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', time.time()))
            current_time = int(time.time())
            if rate_limit_remaining <= 10 and rate_limit_remaining > 0:
                wait_time = max((rate_limit_reset - current_time) / rate_limit_remaining, 1)
                await asyncio.sleep(wait_time)
            else:
                await asyncio.sleep(5)

        except aiohttp.ClientError as http_err:
            print(f"HTTP error for {ticker}: {http_err}")
            raise
        except Exception as err:
            print(f"General error for {ticker}: {err}")
            continue

    if all_bars:
        await save_to_csv(all_bars, ticker)

async def main():
    all_tickers = pd.read_csv("../../data/shortable_assets.csv")['symbol'].tolist()
    start_time = (datetime.now() - relativedelta(years=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_start_time = start_time.astimezone(timezone.utc).isoformat()

    rate_limited_session = RateLimitedSession()
    batch_size = 10  # Adjust this number based on your requirements

    try:
        async with rate_limited_session.session:
            for i in tqdm(range(0, len(all_tickers), batch_size), desc="Processing batches"):
                batch = all_tickers[i:i + batch_size]
                tasks = [asyncio.ensure_future(process_ticker(rate_limited_session, ticker, formatted_start_time, paper_key, paper_secret)) for ticker in batch]
                await asyncio.gather(*tasks)
    finally:
        await rate_limited_session.close()

if __name__ == "__main__":
    asyncio.run(main())