import pandas as pd
from pandas.tseries.offsets import BDay
from preprocess_bars import process_bars_data  # Make sure process_bars.py is in the same directory or in the Python path
import os
# ----- News Processing Functions -----
# Aggregate news articles by day, combine weekends
def aggregate_news_by_day(ticker):
    df = pd.read_csv(f'../../data/news/{ticker}/TickerNewsSummary.csv')

    # Ensure 'created_at' is in datetime format and convert to US/Eastern timezone
    df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_convert('US/Eastern')

    # Adjust weekends: If the day is Saturday or Sunday, shift it to the following Monday
    df['adjusted_date'] = df['created_at'].apply(lambda x: x + BDay(1) if x.dayofweek in [5, 6] else x)

    # Extract just the date part for grouping
    df['adjusted_date'] = df['adjusted_date'].dt.date

    # Ensure that 'headline' columns are of type string
    df['headline'] = df['headline'].astype(str)

    # Group by the adjusted date and concatenate all headlines into a single string for each date
    aggregated_df = df.groupby(['adjusted_date']).agg({'headline': lambda x: ' '.join(x)})

    # Reset index to turn 'adjusted_date' back into a column
    aggregated_df = aggregated_df.reset_index()

    return aggregated_df

def ensure_timezone(df, timezone='US/Eastern'):
    """Ensure the index of the DataFrame is in the specified timezone."""
    if df.index.tz is None:
        df.index = df.index.tz_localize(timezone)
    else:
        df.index = df.index.tz_convert(timezone)
    return df
# Add target column to news data
def merge_news_and_bars_data(ticker):
    # Process news data
    news_df = aggregate_news_by_day(ticker)

    # Convert 'adjusted_date' in news_df to datetime and set to US/Eastern timezone
    news_df['adjusted_date'] = pd.to_datetime(news_df['adjusted_date']).dt.tz_localize('US/Eastern')

    # Process bars data
    bars_df = process_bars_data(ticker)

    # Ensure bars_df index is in US/Eastern timezone using the ensure_timezone function
    bars_df = ensure_timezone(bars_df, timezone='US/Eastern')

    # Reset index of bars_df to merge on 't'
    bars_df_reset = bars_df.reset_index()

    # Merge news_df with bars_df_reset to include 'target', 'percent_change', and 't' (timestamp)
    merged_df = pd.merge(news_df, bars_df_reset[['t', 'target', 'percent_change']], left_on='adjusted_date',
                         right_on='t', how='left')

    # Select relevant columns and rename for clarity
    merged_df = merged_df[['adjusted_date', 'headline', 'target', 'percent_change', 't']]
    merged_df.rename(columns={'t': 'time_of_bars'}, inplace=True)

    return merged_df

#Find the top 20 tickers with the most news articles
def find_top_20_csv_files():
    root_directory = '../../data/news'

    def count_csv_lines(file_path):
        """Counts the number of lines in a CSV file using pandas."""
        return len(pd.read_csv(file_path))

    def extract_ticker_symbol(file_path):
        """Extracts the ticker symbol from a CSV file path."""
        parts = file_path.split(os.path.sep)
        return parts[-2] if len(parts) >= 2 else "Unknown"

    # Find and process all CSV files
    csv_files = []
    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    lines_in_csv = count_csv_lines(file_path)
                    ticker = extract_ticker_symbol(file_path)
                    csv_files.append((ticker, file_path, lines_in_csv))
                except pd.errors.EmptyDataError:
                    print(f"Skipping empty file: {file_path}")

    # Sort the CSV files by line count in descending order
    csv_files.sort(key=lambda x: x[2], reverse=True)

    # Collect the top 20 tickers
    top_20_tickers = [ticker for ticker, _, _ in csv_files[:20]]

    return top_20_tickers

# ----- Example Usage -----

if __name__ == "__main__":
    ticker = 'A'  # Example ticker
    merged_df = merge_news_and_bars_data(ticker)

    # Specify the output path for the merged data
    merged_output_path = "../../data/training_data/merged_news_bars_{}.csv".format(ticker)
    merged_df.to_csv(merged_output_path, index=False)

