import pandas as pd
import numpy as np

# ----- Bars Processing Functions -----


def process_bars_data(ticker):
    """Processes bars data for a given ticker and combines it by day."""
    bars_df = pd.read_csv(f'../../data/bars/{ticker}/2y_5m.csv')

    # Convert to datetime, set timezone to UTC and then convert to US/Eastern
    bars_df['t'] = pd.to_datetime(bars_df['t'], utc=True)
    bars_df['t'] = bars_df['t'].dt.tz_convert('US/Eastern')

    # Sorting values
    bars_df = bars_df.sort_values('t').set_index('t')
    # Fill in missing values
    bars_df = bars_df.resample('5T').asfreq().interpolate(method='linear').reset_index()

    # Resample to daily frequency
    daily_df = bars_df.resample('D', on='t').agg({
        'o': 'first',    # Open price (first of the day)
        'h': 'max',      # High price (max of the day)
        'l': 'min',      # Low price (min of the day)
        'c': 'last',     # Close price (last of the day)
        'v': 'sum',      # Volume (sum of the day)
        'vw': 'mean',    # Volume Weighted Average Price (average of the day)
    })

    # Calculating percent change for the close price
    daily_df['percent_change'] = daily_df['c'].pct_change().astype(float) * 100

    # Create target column based on percent change
    conditions = [
        (daily_df['percent_change'] > 1),
        (daily_df['percent_change'] < -1)
    ]
    choices = [1, -1]
    daily_df['target'] = np.select(conditions, choices, default=0)
    # Remove the first day where percent change is NaN
    daily_df = daily_df.dropna(subset=['percent_change'])

    return daily_df


if __name__ == "__main__":
    output_path = "../../data/training_data/blah.csv"
    df = process_bars_data('AAT')
    df.to_csv(output_path, index=True)