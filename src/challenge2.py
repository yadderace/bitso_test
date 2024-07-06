import os
import json
import utils
import config
import time
import asyncio

from datetime import datetime

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


async def main():
    for i in range(10):
        print(f"Execution {i + 1} of {10}")
        await process_books(['btc_mxn', 'usd_mxn'], 'data/sandbox')
        if (i < 10 - 1):
            await asyncio.sleep(1)

# Run the async main function
asyncio.run(main())