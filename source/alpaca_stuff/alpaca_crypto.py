from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from dateutil.relativedelta import relativedelta
from api_keys import paper_key, paper_secret
from alpaca.data.live import CryptoDataStream

# # no keys required for crypto data
# client = CryptoHistoricalDataClient()

# # Get the date three months ago
# three_months_ago = datetime.now() - relativedelta(months=3)

# request_params = CryptoBarsRequest(
#                symbol_or_symbols=["BTC/USD"],
#                timeframe=TimeFrame.Minute,
#                start=three_months_ago
#            )

# bars = client.get_crypto_bars(request_params)
# print(bars.df.head())

# keys are required for live data


wss_client = CryptoDataStream(paper_key, paper_secret)

# async handler
async def quote_data_handler(data):
    # quote data will arrive here
    print(data)

wss_client.subscribe_quotes(quote_data_handler, "BTC/USD")

wss_client.run()