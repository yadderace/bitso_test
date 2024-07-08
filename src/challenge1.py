import os
import json
import sys
import pandas as pd
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import src.utils as utils
import src.config as config


async def process_books(books, output_dir = config.SANDBOX_DIR):
    """
    Processes a list of books, fetches their order books, and stores the results in JSON files.

    Args:
        books (list): List of book strings to process.
        output_dir (str): The directory where the JSON files will be stored.

    Returns:
        None
    """

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Getting the timestamp when the process starts
    timestamp = datetime.now()        
    
    for book in books:
        print(f"Processing book: {book}")
        try:
            data = utils.list_order_book(book)
            payload = data['payload']
            
            # Extract the best bid and ask prices
            best_bid = max(payload['bids'], key=lambda x: float(x['price']))
            best_ask = min(payload['asks'], key=lambda x: float(x['price']))
            
            # Calculate the spread
            spread = (float(best_ask['price']) - float(best_bid['price'])) * 100 / float(best_ask['price'])
            
            # Prepare the JSON data
            orderbook_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            json_data = {
                'orderbook_timestamp': orderbook_timestamp,
                'book': book,
                'bid': float(best_bid['price']),
                'ask': float(best_ask['price']),
                'spread': spread
            }
            
            # Create the JSON file
            filename = f"{book}-{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
            file_path = os.path.join(output_dir, filename)
            
            with open(file_path, 'w') as json_file:
                json.dump(json_data, json_file)
            
            print(f"Successfully processed and saved data for book: {book}")
        except Exception as e:
            print(f"Error processing book {book}: {e}")


def json_to_partitioned_file(books, input_dir = config.SANDBOX_DIR, output_dir = config.PARTITIONED_DIR, format_type = 'csv'):
    """
    Reads JSON files from the input directory, converts them to a DataFrame, and writes them to a specified format 
    (CSV or Parquet) in a partitioned directory structure based on the book and date.
    
    Parameters:
    - books (list): List of books (strings).
    - input_dir (str): Directory where the JSON files are located.
    - output_dir (str): Directory where the output files should be saved.
    - format_type (str): Format type to store the file, either 'csv' or 'parquet'.
    """
    if format_type not in ["csv", "parquet"]:
        raise ValueError("format_type must be either 'csv' or 'parquet'")
    
    for book in books:
        print(f"Processing book: {book}")
        data = []

        # Collect all JSON files for the current book
        json_files = [f for f in os.listdir(input_dir) if f.startswith(book) and f.endswith('.json')]

        for file_name in json_files:
            file_path = os.path.join(input_dir, file_name)
            print(f"Reading file: {file_path}")
            with open(file_path, 'r') as f:
                record = json.load(f)
                data.append(record)

        if data:
            # Create DataFrame
            df = pd.DataFrame(data)

            # Extract date from the orderbook_timestamp for partitioning
            df['date'] = pd.to_datetime(df['orderbook_timestamp']).dt.date

            # Define the output path
            for date in df['date'].unique():
                date_str = date.strftime("%Y-%m-%d")
                partitioned_output_dir = os.path.join(output_dir, f"book={book}", f"date={date_str}")
                os.makedirs(partitioned_output_dir, exist_ok=True)
                
                partitioned_file_path = os.path.join(partitioned_output_dir, f"{book}.{format_type}")

                print(f"Writing to: {partitioned_file_path}")

                if format_type == "parquet":
                    df[df['date'] == date].drop(columns=['date']).to_parquet(partitioned_file_path, index=False)
                elif format_type == "csv":
                    df[df['date'] == date].drop(columns=['date']).to_csv(partitioned_file_path, index=False)

            # Remove processed JSON files
            for file_name in json_files:
                file_path = os.path.join(input_dir, file_name)
                print(f"Deleting file: {file_path}")
                os.remove(file_path)

        print(f"Finished processing book: {book}")


# Usage Example

# async def main():
#     for i in range(10):
#         print(f"Execution {i + 1} of {10}")
#         await process_books(['btc_mxn', 'usd_mxn'], 'data/sandbox')
#         if (i < 10 - 1):
#             await asyncio.sleep(1)

# # Run the async main function
# asyncio.run(main())

# json_to_partitioned_file(['btc_mxn', 'usd_mxn'], input_dir='data/sandbox', output_dir='data/s3-partitioned', format_type='parquet')