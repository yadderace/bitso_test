import config
import utils
import pandas as pd
import os
import hashlib
from datetime import datetime, timedelta



def download_data():
    """
    Downloads multiple files from predefined URLs and saves them to a specified directory.

    Modify the `CSV_URLs` and `ZIP_URLs` lists in config.py to add or remove URLs as needed.

    This function calls `download_file` and `download_and_unzip_file` for each URL in the `urls` list.
    """
    # Define URLs and output directory for CSV files
    csv_urls = config.CSV_URLs
    output_dir = config.INPUT_DIR  # Adjust the path as per your project structure

    # Download each file
    for url in csv_urls:
        utils.download_file(url, output_dir)

    # Define URLs and output directory for zipped files
    zip_urls = config.ZIP_URLs
    
    # Download each file
    for url in zip_urls:
        utils.download_and_unzip_file(url, output_dir)

def create_fct_active_users(pivot_date, offset_days, deposit_file, withdrawals_file, output_file):
    """
    Create a fact table CSV file with active users transactions by concatenating deposit and withdrawals data.

    Parameters:
    pivot_date (str): The pivot date in 'YYYY-MM-DD' format.
    offset_days (int): The number of days to include before the pivot date.
    deposit_file (str): Path to the deposit CSV file.
    withdrawals_file (str): Path to the withdrawals CSV file.
    output_file (str): Path to the output CSV file.
    """
    # Convert pivot_date to datetime
    pivot_date = datetime.strptime(pivot_date, '%Y-%m-%d')
    start_date = pivot_date - timedelta(days=offset_days)
    
    # Read the CSV files
    deposits = pd.read_csv(deposit_file)
    withdrawals = pd.read_csv(withdrawals_file)

    # Rename event_timestamp to event_tstamp
    deposits.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)
    withdrawals.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)
    

    # Check if the output file exists
    fct_file_exist = os.path.exists(output_file)

    # If file doesn't exist, it will use all the data.
    if(fct_file_exist):
        # Filter data based on pivot_date and offset_days
        deposits = deposits[(deposits['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                                    (deposits['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]
        
        withdrawals = withdrawals[(withdrawals['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                                        (withdrawals['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]
    
    # Add event_type column
    deposits['event_type'] = 'deposit'
    withdrawals['event_type'] = 'withdrawal'
    
    # Concatenate the data
    combined = pd.concat([deposits, withdrawals], ignore_index=True)
    
    # Generate rec_code using MD5 hash of tx_id
    combined['rec_code'] = combined['id'].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())
    
    # Add created_at column with the current timestamp
    combined['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Rename columns to match the required output
    combined = combined.rename(columns={
        'id': 'tx_id',
        'tx_status': 'event_status'
    })
    
    # Select and reorder columns
    final_df = combined[['rec_code', 'user_id', 'tx_id', 'event_tstamp', 'event_type', 
                         'event_status', 'currency', 'amount', 'created_at']]
    
    # Check if the output file exists
    if fct_file_exist:
        existing_df = pd.read_csv(output_file)
        # Merge existing data with the new data
        merged_df = pd.merge(existing_df, final_df, on='rec_code', how='outer', suffixes=('_old', ''))
        
        # Drop duplicate columns created by merge (keep the new ones)
        for col in ['user_id', 'tx_id', 'event_tstamp', 'event_type', 'event_status', 'currency', 'amount', 'created_at']:
            merged_df[col] = merged_df[col].combine_first(merged_df[col + '_old'])
            merged_df.drop(columns=[col + '_old'], inplace=True)
    else:
        merged_df = final_df

    # Write to the output file
    merged_df.to_csv(output_file, index=False)
    print(f"Data successfully written to {output_file}")


download_data()

# Example usage
pivot_date = "2024-07-01"
offset_days = 30
deposit_file = "data/input/deposit_sample_data.csv"
withdrawals_file = "data/input/withdrawals_sample_data.csv"
output_file = "data/output/fct_active_users.csv"

create_fct_active_users(pivot_date, offset_days, deposit_file, withdrawals_file, output_file)