import alpaca.trading as tradeapi
from source.alpaca_stuff.api_keys import live_key, live_secret
import pandas as pd
import yfinance as yf
import requests_cache
from tqdm import tqdm


def get_alpaca_shortable_assets():
    client = tradeapi.TradingClient(live_key, live_secret, paper=False)

    # Retrieve all assets
    assets = client.get_all_assets()

    # Filter assets by the 'tradable' attribute
    tradable_assets = [asset for asset in assets if (
                asset.tradable and asset.status == 'active' and asset.shortable and asset.fractionable and asset.asset_class == 'us_equity')]

    # Create a list to hold asset details
    assets_data = []

    for asset in tradable_assets:
        asset_details = {
            'id': str(asset.id),  # Assuming 'id' is a UUID object
            'asset_class': asset.asset_class,
            'exchange': asset.exchange,
            'symbol': asset.symbol,
            'name': asset.name,
            'status': asset.status,
            'tradable': asset.tradable,
            'marginable': asset.marginable,
            'shortable': asset.shortable,
            'easy_to_borrow': asset.easy_to_borrow,
            'fractionable': asset.fractionable,
            'maintenance_margin_requirement': asset.maintenance_margin_requirement,
        }
        # Add the asset details to the list
        assets_data.append(asset_details)
    # Convert the list of dictionaries to a DataFrame
    assets_df = pd.DataFrame(assets_data)

    # Display the DataFrame
    assets_df.to_csv("data/shortable_assets.csv")
    return assets_df


def get_yfinance_financials():
    # Configure yfinance settings outside the loop
    yf.set_tz_cache_location("data/cache/")
    # Set up a session with requests_cache
    # yf.enable_debug_mode()


    requests_cache.install_cache('yfinance.cache')
    session = requests_cache.CachedSession()
    session.headers['User-agent'] = 'my-program/1.0'

    tickers = pd.read_csv("../../data/shortable_assets.csv")['symbol'].tolist()

    # Create a tqdm progress bar
    progress_bar = tqdm(tickers, desc="Fetching Financial Data")

    for ticker_symbol in progress_bar:
        ticker = yf.Ticker(ticker_symbol, session=session)
        fin = ticker.get_financials(freq="quarterly")
        fin.to_csv(f"data/financials/{ticker_symbol}_financials.csv")

    # Close the progress bar when finished
    progress_bar.close()



def get_factor(df, factor_name):
    # This function can be expanded to include more sophisticated analysis per factor
    try:
        return df.loc[factor_name]
    except Exception as e:
        print(f"Error with {factor_name}"
              f"\n{e}")


def get_factor_data(tickers):
    # Get the list of tickers

    for ticker in tickers:
        factors = {
            "Date": [],
            "TaxEffectOfUnusualItems": [],
            "TaxRateForCalcs": [],
            "NormalizedEBITDA": [],
            "TotalUnusualItems": [],
            "TotalUnusualItemsExcludingGoodwill": [],
            "NetIncomeFromContinuingOperationNetMinorityInterest": [],
            "ReconciledDepreciation": [],
            "ReconciledCostOfRevenue": [],
            "EBITDA": [],
            "EBIT": [],
            "NetInterestIncome": [],
            "InterestExpense": [],
            "InterestIncome": [],
            "NormalizedIncome": [],
            "NetIncomeFromContinuingAndDiscontinuedOperation": [],
            "TotalExpenses": [],
            "TotalOperatingIncomeAsReported": [],
            "DilutedAverageShares": [],
            "BasicAverageShares": [],
            "DilutedEPS": [],
            "BasicEPS": [],
            "DilutedNIAvailtoComStockholders": [],
            "NetIncomeCommonStockholders": [],
            "NetIncome": [],
            "NetIncomeIncludingNoncontrollingInterests": [],
            "NetIncomeContinuousOperations": [],
            "TaxProvision": [],
            "PretaxIncome": [],
            "OtherIncomeExpense": [],
            "OtherNonOperatingIncomeExpenses": [],
            "SpecialIncomeCharges": [],
            "OtherSpecialCharges": [],
            "ImpairmentOfCapitalAssets": [],
            "NetNonOperatingInterestIncomeExpense": [],
            "InterestExpenseNonOperating": [],
            "InterestIncomeNonOperating": [],
            "OperatingIncome": [],
            "OperatingExpense": [],
            "DepreciationAmortizationDepletionIncomeStatement": [],
            "DepreciationAndAmortizationInIncomeStatement": [],
            "ResearchAndDevelopment": [],
            "SellingGeneralAndAdministration": [],
            "GrossProfit": [],
            "CostOfRevenue": [],
            "TotalRevenue": [],
            "OperatingRevenue": [],
        }
        try:
            df = pd.read_csv(f"data/financials/{ticker}_financials.csv", index_col=0)
            df.transpose()
            for factor in factors:
                factors[factor].append(get_factor(df, factor))



        except Exception as e:
            print(f"Error with {ticker}"
                  f"\n{e}")

tickers = pd.read_csv("../../data/shortable_assets.csv")['symbol'].tolist()[0:10]
# get_yfinance_financials(tickers)
get_factor_data()
