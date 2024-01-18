from alpaca.data import StockHistoricalDataClient
import backtrader as bt
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from api_keys import paper_key, paper_secret
from bt_stuff import AlpacaStockData, MyStrategy, FractionalSizer, BuyDipsStrategy
from helper_functions import time_it

starting_cash = 1000.0
start_time = datetime.now() - relativedelta(months=6)


def get_stock_bars_for_bt(symbols):
    """
    Fetches stock bars from Alpaca's API.
    """
    try:
        # Initialize Alpaca client
        client = StockHistoricalDataClient(api_key=paper_key, secret_key=paper_secret)


        # Define request parameters
        request_params = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Hour,
            start=start_time
        )


        # Fetch data
        bars = client.get_stock_bars(request_params)
        data_df = bars.df

        # Reset index and convert column names to lowercase
        data_df.reset_index(inplace=True)
        data_df.columns = [col.lower() for col in data_df.columns]
        return data_df
        # data_df.to_csv("data.csv")
        # data = AlpacaStockData(dataname=data_df)
        # return data
    except Exception as e:
        return pd.DataFrame()
        print(f"Error with {ticker}\n{e}")




def analyze_bt_results(results):
    for run in results:
        for strat in run:

            print(f"The Starting Portfolio Value: {strat.broker.startingcash}")
            print(f"The Final Portfolio Value: {strat.broker.getvalue()}")
            print(f"The Final PnL: {strat.broker.getvalue() - strat.broker.startingcash}")


            trade_analysis = strat.analyzers.getbyname('ta').get_analysis()

            print("\nTrade Analysis:")
            print(f"Total Trades: {trade_analysis.total.total}")
            print(f"Total Closed Trades: {trade_analysis.total.closed}")
            print(f"Total Won Trades: {trade_analysis.won.total}")
            print(f"Total Lost Trades: {trade_analysis.lost.total}")
            print(f"Winning Streak: {trade_analysis.streak.won.longest}")
            print(f"Losing Streak: {trade_analysis.streak.lost.longest}")
            print(f"Profit/Loss Total: {trade_analysis.pnl.net.total:.2f}")
            print(f"Average P/L per Trade: {trade_analysis.pnl.net.average:.2f}")

            sharpe_ratio = strat.analyzers.sharpe.get_analysis()

            print("\nSharpe Ratio:")
            print(f"Sharpe Ratio: {sharpe_ratio['sharperatio']:.2f}")

            drawdown = strat.analyzers.drawdown.get_analysis()

            print("\nDrawdown:")
            print(f"Max Drawdown Length: {drawdown.max.len}")
            print(f"Max Drawdown: {drawdown.max.drawdown:.2f}%")
            print(f"Max Money Down: {drawdown.max.moneydown:.2f}")
            print(f"Current Drawdown: {drawdown.drawdown:.2f}%")
            print(f"Current Money Down: {drawdown.moneydown:.2f}")

            returns = strat.analyzers.returns.get_analysis()

            print("\nReturns:")
            print(f"Total Returns (rtot): {returns['rtot']:.4f}")
            print(f"Average Daily Return (ravg): {returns['ravg']:.6f}")
            print(f"Normalized Return (rnorm): {returns['rnorm']:.2f}")
            print(f"Normalized Return Annualized (rnorm100): {returns['rnorm100']:.2f}%")


def opt_bt_results(results):
    optimized_results = []

    for opt_run in results:  # Each opt_run is a list of OptReturn objects for a parameter combination
        for opt_return in opt_run:
            try:
                # Accessing analyzer results
                trade_analysis = opt_return.analyzers.ta.get_analysis()
                sharpe_ratio = opt_return.analyzers.sharpe.get_analysis()

                # Extracting parameter values
                params = {param: getattr(opt_return.params, param) for param in opt_return.params._getkeys()}

                # Handle None values in Sharpe Ratio
                sharpe_ratio_value = sharpe_ratio['sharperatio']
                if sharpe_ratio_value is None:
                    sharpe_ratio_value = float(-1)  # Assign a very low value for sorting

                # Store parameter combination and corresponding results
                result = {
                    'params': params,
                    'total_trades': trade_analysis.total.total,
                    'sharpe_ratio': sharpe_ratio_value,
                }
                optimized_results.append(result)

            except Exception as e:
                print(e)

    # Sort results based on a chosen metric, e.g., Sharpe Ratio
    optimized_results.sort(key=lambda x: x['sharpe_ratio'], reverse=True)

    # Print the best result after sorting
    if optimized_results and optimized_results[0]['sharpe_ratio'] != float('-inf'):
        best_result = optimized_results[0]
        print("Best Parameter Set:")
        for key, value in best_result['params'].items():
            print(f"{key}: {value}")
        print(f"Sharpe Ratio: {best_result['sharpe_ratio']}")
        print(f"Total Trades: {best_result['total_trades']}")
    else:
        print("No valid Sharpe Ratio results found.")



def analyze_simple_results(results):
    strat = results[0]
    print(f"The Starting Portfolio Value: {strat.broker.startingcash}")
    print(f"The Final Portfolio Value: {strat.broker.getvalue()}")
    print(f"The Final PnL: {strat.broker.getvalue() - strat.broker.startingcash}")


def build_analysis_cerebro(strategy):
    """
    Builds the Cerebro engine.
    """
    # Add strategy and analyzers to cerebro
    cerebro = bt.Cerebro()
    cerebro.addstrategy(strategy)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")

    # Add the strategy
    cerebro.addstrategy(strategy)


    return cerebro


def build_simple_cerebro(strategy):
    cerebro = bt.Cerebro()
    # Add the strategy
    cerebro.addstrategy(strategy)

    # Add the data feed to Cerebro
    # cerebro.adddata(data)

    return cerebro

#symbols = ["O", "MSFT", "DIS", "NVDA", 'MAR', 'BLDR', 'META']

# Load tickers from a CSV file
tickers = pd.read_csv("data/shortable_assets.csv")['symbol'].tolist()

# Create a tqdm progress bar
progress_bar = tqdm(total=len(tickers), desc="Fetching Bars")

# Loop through tickers
for ticker in tickers:
    data = get_stock_bars_for_bt(ticker)
    data.to_csv(f"data/bars/{ticker}_6mo_hr.csv", index=False)

    # Update the progress bar
    progress_bar.update(1)

# Close the progress bar when finished
progress_bar.close()

# data_feeds = []
# for symbol in symbols:
#     data = df[df['symbol'] == symbol]
#     data.to_csv(f"data/bars/{symbol}.csv",)
#     #data.set_index('timestamp', inplace=True)
#     data_feed = AlpacaStockData(dataname=data)
#     data_feeds.append(data_feed)
#
# # # Initialize Cerebro engine
# cerebro = build_analysis_cerebro(BuyDipsStrategy)
# cerebro.broker.setcash(starting_cash)
# # Add data feeds to Cerebro
# for data_feed in data_feeds:
#     cerebro.adddata(data_feed)
#
#
# cerebro.optstrategy(
#     BuyDipsStrategy,
#     dip_percentage=[0.05, 5.0, 7.0, 10.0],
#     buy_amount=[0.05, 0.03, 0.06]
# )
# # Define ranges for each parameter
# stop_loss_range = [0.01, 0.025, 0.05]
# entry_multiplier_range = [1.00, 1.01, 1.02]
# exit_multiplier_range = [0.98, 0.99, 1.00]
# macd_fast_range = [10, 12, 15]
# macd_slow_range = [24, 26, 30]
# macd_signal_range = [7, 9, 11]
# # Setup the strategy with the parameter ranges for optimization
# cerebro.optstrategy(
#     MyStrategy,
#     stop_loss=stop_loss_range,
#     entry_multiplier=entry_multiplier_range,
#     exit_multiplier=exit_multiplier_range,
#     macd_fast=macd_fast_range,
#     macd_slow=macd_slow_range,
#     macd_signal=macd_signal_range
# )
#
# cerebro.addsizer(FractionalSizer)
#
# results = cerebro.run(maxcpus=1)
# opt_bt_results(results)