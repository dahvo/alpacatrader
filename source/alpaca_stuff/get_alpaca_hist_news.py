import asyncio
import time
import aiohttp
from aiohttp import ClientSession, TCPConnector
from api_keys import paper_key, paper_secret
import pandas as pd
import os
from tenacity import retry, wait_fixed, retry_if_exception_type
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm
import aiofiles



class RateLimitedSession:
    def __init__(self):
        self.session = ClientSession(connector=TCPConnector(limit_per_host=1))
        self.rate_limiter = AsyncLimiter(200, 120)  # Adjust the rate limits as needed

    async def close(self):
        await self.session.close()

    async def get(self, url, **kwargs):
        async with self.rate_limiter:
            return await self.session.get(url, **kwargs)

async def save_to_csv(data, ticker):
    try:
        if not data:
            return

        directory = f"data/news/{ticker}"
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, "TickerNewsSummary.csv")
        df = pd.DataFrame(data)

        async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
            await f.write(df.to_csv(index=False))
    except Exception as e:
        print(f"Error saving data for {ticker}: {e}")

@retry(retry=retry_if_exception_type(aiohttp.ClientError), wait=wait_fixed(5))
async def process_ticker(rate_limited_session, ticker, start_time, end_time, paper_key, paper_secret):
    all_bars = []
    params = {
        'start': start_time,
        'end': end_time,
        'symbols': ticker,
    }
    headers = {
        'APCA-API-KEY-ID': paper_key,
        'APCA-API-SECRET-KEY': paper_secret
    }
    ALPACA_BARS_URL = "https://data.alpaca.markets/v1beta1/news"

    while True:
        try:
            response = await rate_limited_session.get(ALPACA_BARS_URL, headers=headers, params=params)
            response.raise_for_status()

            data = await response.json()
            if 'news' in data and data['news']:
                all_bars.extend(data['news'])
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
    #all_tickers = pd.read_csv("data/shortable_assets.csv")['symbol'].tolist()[0:1]
    directory_path = "../../data/bars"
    folder_names = os.listdir(directory_path)
    all_tickers = [folder_name for folder_name in folder_names if os.path.isdir(os.path.join(directory_path, folder_name))]
    start_time = {}
    end_time = {}
    for ticker in all_tickers:
        file_path = os.path.join(directory_path, ticker, "2y_5m.csv")
        df = pd.read_csv(file_path)
        start_time[ticker] = df['t'].min()
        end_time[ticker] = df['t'].max()

    rate_limited_session = RateLimitedSession()
    batch_size = 2

    try:
        for i in tqdm(range(0, len(all_tickers), batch_size), desc="Processing batches"):
            batch = all_tickers[i:i + batch_size]
            tasks = [
                asyncio.create_task(
                    process_ticker(
                        rate_limited_session,
                        ticker,
                        start_time[ticker],  # pass a single timestamp
                        end_time[ticker],  # pass a single timestamp
                        paper_key,
                        paper_secret
                    )
                ) for ticker in batch
            ]

            await asyncio.gather(*tasks)
    finally:
        await rate_limited_session.close()

if __name__ == "__main__":
    asyncio.run(main())