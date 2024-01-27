import pandas as pd
import re
import csv
import os
# ----- News Processing Functions -----
def clean_text(df, column):
    """Cleans text in a DataFrame column by removing special characters and whitespace."""
    pattern = r'[^\w\s&\'",.!?]|[\r\n]| '
    try:
        df[column] = df[column].apply(lambda x: re.sub(pattern, ' ', str(x)) if pd.notna(x) else x)
    except Exception as e:
        print(f"Error: {e}")
    return df

def aggregate_news_by_day(ticker):
    df = pd.read_csv(f'../../data/news/{ticker}/TickerNewsSummary.csv')
    """Aggregates news data by day."""
    # Ensure 'created_at' is in datetime format and convert to US/Eastern timezone
    df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_convert('US/Eastern')

    # Extract the date from 'created_at' for aggregation
    df['date'] = df['created_at'].dt.date

    # Ensure that 'headline' and 'summary' columns are of type string
    df['headline'] = df['headline'].astype(str)


    # Group by the date and aggregate headlines and summaries
    aggregated_df = df.groupby(['date']).agg({'headline': ' '.join})

    # Reset index to turn 'date' back into a column
    aggregated_df = aggregated_df.reset_index()

    # Optional: Convert 'date' back to a datetime object, localized to UTC
    #aggregated_df['date'] = pd.to_datetime(aggregated_df['date']).dt.tz_localize('US/Eastern').dt.tz_convert('UTC')

    return aggregated_df

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
    ticker = 'BAC'
    output_path = "../../data/training_data/blah_news.csv"
    df = aggregate_news_by_day(ticker)
    df.to_csv(output_path, index=False)
    #find_top_20_csv_files()

