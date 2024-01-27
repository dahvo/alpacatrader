from api_keys import paper_key, paper_secret
from datetime import datetime, timezone
import pandas as pd
from requests.exceptions import HTTPError
import os
from dateutil.relativedelta import relativedelta
import requests_cache
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

class CachedLimiterSession(CacheMixin,Session):
    pass



def remove_headers(response):
    """Remove headers from the response before caching it."""
    response.headers.clear()
    return response

requests_cache.install_cache('cache/alpaca_bars', backend='sqlite')
#requests_cache.delete(expired=True)
session = CachedLimiterSession(
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache("cache/alpaca_bars"),
        expire_after=Duration.DAY,  # Cache expiration time
        filter_fn = remove_headers,
        allowable_codes=(200,),
    )


# Function to save data to CSV
def save_to_csv(data, ticker):
    directory = f"data/bars/{ticker}"
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, "2y_5m.csv")
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)

def file_exists(ticker):
    file_path = f"data/bars/{ticker}/2y_5m.csv"
    return os.path.exists(file_path)
@retry(retry=retry_if_exception_type(HTTPError), wait=wait_fixed(30))
def process_ticker(ticker, timeframe):

    # # Check if the file already exists
    # if file_exists(ticker):
    #     print(f"Data for {ticker} already exists. Skipping.")
    #     return
    # Iterate through all tickers with a progress bar
    all_bars = []
    params = {
        'timeframe': timeframe,
        'start': formatted_start_time,
        'adjustment': 'all'
    }
    # Clearing page_token at the start of each ticker
    params.pop('page_token', None)

    headers = {
        'APCA-API-KEY-ID': paper_key,
        'APCA-API-SECRET-KEY': paper_secret
    }
    ALPACA_BARS_URL = f"https://data.alpaca.markets/v2/stocks/{ticker}/bars"

    while True:
        try:
            #print(f"Request Params: {params}")
            response = session.get(ALPACA_BARS_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if 'bars' in data:
                all_bars.extend(data['bars'])

                next_page_token = data.get('next_page_token')
                if next_page_token and next_page_token != params.get('page_token'):
                    params['page_token'] = next_page_token
                else:
                    break
            else:
                print(f"No data found for {ticker}")
                break


        except HTTPError as http_err:
                print(f"HTTP error for {ticker}: {http_err}")
                break
        except Exception as err:
            print(f"General error for {ticker}: {err}")
            break

    save_to_csv(all_bars, ticker)


# Main execution block
if __name__ == "__main__":
    all_tickers = pd.read_csv("../../data/shortable_assets.csv")['symbol'].tolist()
    timeframe = '5Min'
    start_time = (datetime.now() - relativedelta(years=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_start_time = start_time.astimezone(timezone.utc).isoformat()

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks and store the future objects
        futures = {executor.submit(process_ticker, ticker, timeframe): ticker for ticker in all_tickers}

        # Initialize tqdm progress bar with the total number of tasks
        with tqdm(total=len(futures), desc="Processing tickers") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()  # Wait for each future to complete
                except Exception as e:
                    print(f"An error occurred: {e}")
                    traceback.print_exc()  # Print the stack trace of the error
                finally:
                    # Update the progress bar after each task completes
                    pbar.update(1)
    session.close()