import pandas as pd
import os
import hashlib
import sys
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import src.utils as utils
import src.config as config

def download_data():
    """
    Downloads multiple files from predefined URLs and saves them to a specified directory.

    Modify the `CSV_URLs` and `ZIP_URLs` lists in config.py to add or remove URLs as needed.

    This function calls `download_file` and `download_and_unzip_file` for each URL in the `urls` list.
    """
    print("Starting data download...")

    # Define URLs and output directory for CSV files
    csv_urls = config.CSV_URLs
    output_dir = config.INPUT_DIR  # Adjust the path as per your project structure

    # Download each file
    for url in csv_urls:
        print(f"Downloading CSV file from {url}...")
        utils.download_file(url, output_dir)
        print(f"Downloaded CSV file from {url}.")

    # Define URLs and output directory for zipped files
    zip_urls = config.ZIP_URLs
    
    # Download each file
    for url in zip_urls:
        print(f"Downloading and unzipping file from {url}...")
        utils.download_and_unzip_file(url, output_dir)
        print(f"Downloaded and unzipped file from {url}.")

    print("Data download completed.")

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
    print("Starting creation of active users fact table...")

    # Convert pivot_date to datetime
    pivot_date = datetime.strptime(pivot_date, '%Y-%m-%d')
    start_date = pivot_date - timedelta(days=offset_days)
    
    print(f"Reading deposits from {deposit_file}...")
    deposits = pd.read_csv(deposit_file)
    print(f"Reading withdrawals from {withdrawals_file}...")
    withdrawals = pd.read_csv(withdrawals_file)

    # Rename event_timestamp to event_tstamp
    deposits.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)
    withdrawals.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)
    
    # Check if the output file exists
    fct_file_exist = os.path.exists(output_file)

    # If file doesn't exist, it will use all the data.
    if fct_file_exist:
        print("Filtering data based on pivot_date and offset_days...")
        deposits = deposits[(deposits['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                            (deposits['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]
        
        withdrawals = withdrawals[(withdrawals['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                                  (withdrawals['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]
    
    # Add event_type column
    deposits['event_type'] = 'DEPOSIT'
    withdrawals['event_type'] = 'WITHDRAWAL'

    print("Concatenating deposit and withdrawal data...")
    # Concatenate the data
    combined = pd.concat([deposits, withdrawals], ignore_index=True)

    # Trim and uppercase the values in tx_status
    combined['tx_status'] = combined['tx_status'].str.strip().str.upper()
    
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
    
    print("Checking if the output file exists...")
    # Check if the output file exists
    if fct_file_exist:
        existing_df = pd.read_csv(output_file)
        print("Merging existing data with the new data...")
        
        existing_filtered_df = existing_df[(existing_df['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                                               (existing_df['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]
            
        out_filtered_df = existing_df[(existing_df['event_tstamp'] < start_date.strftime('%Y-%m-%d')) |
                                               (existing_df['event_tstamp'] > pivot_date.strftime('%Y-%m-%d'))]
        
        # Merge existing data with the new data
        merged_df = pd.merge(existing_filtered_df, final_df, on='rec_code', how='outer', suffixes=('_old', ''))
        
        # Drop duplicate columns created by merge (keep the new ones)
        for col in ['user_id', 'tx_id', 'event_tstamp', 'event_type', 'event_status', 'currency', 'amount', 'created_at']:
            merged_df[col] = merged_df[col].combine_first(merged_df[col + '_old'])
            merged_df.drop(columns=[col + '_old'], inplace=True)
        
        merged_df = pd.concat([out_filtered_df, merged_df], axis=1)
    else:
        merged_df = final_df

    print(f"Writing merged data to {output_file}...")
    # Write to the output file
    merged_df.to_csv(output_file, index=False)
    print(f"Data successfully written to {output_file}")

def create_fct_system_activity(pivot_date, offset_days, event_file, deposit_file, withdrawals_file, output_file):
    """
    Create a fact table CSV file with system activity by concatenating events, deposits, and withdrawals data.

    Parameters:
    pivot_date (str): The pivot date in 'YYYY-MM-DD' format.
    offset_days (int): The number of days to include before the pivot date.
    event_file (str): Path to the event CSV file.
    deposit_file (str): Path to the deposit CSV file.
    withdrawals_file (str): Path to the withdrawals CSV file.
    output_file (str): Path to the output CSV file.
    """
    print("Starting creation of system activity fact table...")

    # Convert pivot_date to datetime
    pivot_date = datetime.strptime(pivot_date, '%Y-%m-%d')
    start_date = pivot_date - timedelta(days=offset_days)
    
    print(f"Reading events from {event_file}...")
    events = pd.read_csv(event_file)
    print(f"Reading deposits from {deposit_file}...")
    deposits = pd.read_csv(deposit_file)
    print(f"Reading withdrawals from {withdrawals_file}...")
    withdrawals = pd.read_csv(withdrawals_file)
    
    # Rename event_timestamp to event_tstamp
    events.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)
    deposits.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)
    withdrawals.rename(columns={'event_timestamp': 'event_tstamp'}, inplace=True)

    # Just filtering login events
    events = events[events['event_name'] == 'login']
    
    # Add event_type and event_status columns
    events['event_type'] = events['event_name']
    events['event_status'] = 'COMPLETED'
    deposits['event_type'] = 'DEPOSIT'
    deposits['event_status'] = deposits['tx_status']
    withdrawals['event_type'] = 'WITHDRAWAL'
    withdrawals['event_status'] = withdrawals['tx_status']
    
    print("Concatenating events, deposits, and withdrawals data...")
    # Concatenate the data
    combined = pd.concat([events[['id', 'user_id', 'event_tstamp', 'event_type', 'event_status']],
                          deposits[['id', 'user_id', 'event_tstamp', 'event_type', 'event_status']],
                          withdrawals[['id', 'user_id', 'event_tstamp', 'event_type', 'event_status']]],
                         ignore_index=True)
    
    # Trim and uppercase the values in event_status and event_type
    combined['event_status'] = combined['event_status'].str.strip().str.upper()
    combined['event_type'] = combined['event_type'].str.strip().str.upper()
    
    # Generate rec_code using MD5 hash of concatenated id and event_type
    combined['rec_code'] = combined.apply(lambda row: hashlib.md5((str(row['id']) + row['event_type']).encode()).hexdigest(), axis=1)
    
    # Add created_at column with the current timestamp
    combined['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Rename columns to match the required output
    combined = combined.rename(columns={'id': 'event_id'})
    
    # Select and reorder columns
    final_df = combined[['rec_code', 'user_id', 'event_id', 'event_tstamp', 'event_type', 'event_status', 'created_at']]
    
    print("Checking if the output file exists...")
    # Check if the output file exists
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        
        # If the file exists, filter based on pivot_date and offset_days
        if not existing_df.empty:
            existing_filtered_df = existing_df[(existing_df['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                                               (existing_df['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]
            
            out_filtered_df = existing_df[(existing_df['event_tstamp'] < start_date.strftime('%Y-%m-%d')) |
                                               (existing_df['event_tstamp'] > pivot_date.strftime('%Y-%m-%d'))]
        
        print("Merging existing data with the new data...")
        # Merge existing data with the new data
        merged_df = pd.merge(existing_filtered_df, final_df, on='rec_code', how='outer', suffixes=('_old', ''))
        
        # Drop duplicate columns created by merge (keep the new ones)
        for col in ['user_id', 'event_id', 'event_tstamp', 'event_type', 'event_status', 'created_at']:
            merged_df[col] = merged_df[col].combine_first(merged_df[col + '_old'])
            merged_df.drop(columns=[col + '_old'], inplace=True)

        merged_df = pd.concat([out_filtered_df, merged_df], axis=1)
    else:
        merged_df = final_df

    print(f"Writing merged data to {output_file}...")
    # Write to the output file
    merged_df.to_csv(output_file, index=False)
    print(f"Data successfully written to {output_file}")

def create_dim_users(pivot_date, offset_days, activity_file, user_id_file, output_file):
    """
    Create a dimension table CSV file for users by extracting and summarizing data from system activity and user ID files.

    Parameters:
    pivot_date (str): The pivot date in 'YYYY-MM-DD' format.
    offset_days (int): The number of days to include before the pivot date.
    activity_file (str): Path to the system activity CSV file.
    user_id_file (str): Path to the user ID CSV file.
    output_file (str): Path to the output CSV file.
    """
    print("Starting creation of users dimension table...")

    # Convert pivot_date to datetime
    pivot_date = datetime.strptime(pivot_date, '%Y-%m-%d')
    start_date = pivot_date - timedelta(days=offset_days)
    
    print(f"Reading activity data from {activity_file}...")
    activity_df = pd.read_csv(activity_file)
    print(f"Reading user ID data from {user_id_file}...")
    user_id_df = pd.read_csv(user_id_file)
    
    print("Filtering activity data based on pivot_date and offset_days if the final file exists...")
    # Filter activity data based on pivot_date and offset_days if the final file exists
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        if not existing_df.empty:
            activity_df = activity_df[(activity_df['event_tstamp'] >= start_date.strftime('%Y-%m-%d')) &
                                      (activity_df['event_tstamp'] <= pivot_date.strftime('%Y-%m-%d'))]

    # Get all unique user_ids from both files
    all_user_ids = pd.concat([activity_df['user_id'], user_id_df['user_id']]).unique()
    
    # Initialize the dim_users DataFrame
    dim_users = pd.DataFrame(all_user_ids, columns=['user_id'])
    
    # Generate rec_code using MD5 hash of user_id
    dim_users['rec_code'] = dim_users['user_id'].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest())

    print("Calculating first and last event timestamps for logins, deposits, and withdrawals...")
    # Group by user_id and calculate min and max event_tstamp for logins
    result_df = activity_df[activity_df['event_type'] == 'LOGIN'].groupby('user_id')['event_tstamp'].agg(['min', 'max']).reset_index()
    # Rename columns for clarity
    result_df.columns = ['user_id', 'first_login_tstamp', 'last_login_tstamp']
    # Full outer join info with dim_users
    dim_users = pd.merge(dim_users, result_df, on='user_id', how='outer')

    # Group by user_id and calculate min and max event_tstamp for deposits
    result_df = activity_df[activity_df['event_type'] == 'DEPOSIT'].groupby('user_id')['event_tstamp'].agg(['min', 'max']).reset_index()
    # Rename columns for clarity
    result_df.columns = ['user_id', 'first_deposit_tstamp', 'last_deposit_tstamp']
    # Full outer join info with dim_users
    dim_users = pd.merge(dim_users, result_df, on='user_id', how='outer')
    
    # Group by user_id and calculate min and max event_tstamp for withdrawals
    result_df = activity_df[activity_df['event_type'] == 'WITHDRAWAL'].groupby('user_id')['event_tstamp'].agg(['min', 'max']).reset_index()
    # Rename columns for clarity
    result_df.columns = ['user_id', 'first_withdrawal_tstamp', 'last_withdrawal_tstamp']
    # Full outer join info with dim_users
    dim_users = pd.merge(dim_users, result_df, on='user_id', how='outer')

    # Add created_at column with the current timestamp
    dim_users['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # List of columns to cast to datetime
    timestamp_columns = ['first_login_tstamp', 'last_login_tstamp', 'first_deposit_tstamp', 'last_deposit_tstamp', 'first_withdrawal_tstamp', 'last_withdrawal_tstamp',]

    # Cast specific columns to datetime
    dim_users[timestamp_columns] = dim_users[timestamp_columns].apply(pd.to_datetime, errors='coerce')

    print("Checking if the output file exists...")
    # If the output file exists, merge/update the stored records
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)

        # Cast specific columns to datetime
        existing_df[timestamp_columns] = existing_df[timestamp_columns].apply(pd.to_datetime, errors='coerce')
        
        print("Merging existing data with the new data...")
        # Merge existing data with the new data
        merged_df = pd.merge(existing_df, dim_users, on='rec_code', how='outer', suffixes=('_old', ''))
        
        # Combine first/last timestamps ensuring min/max values are kept
        for col in ['first_login_tstamp', 'first_deposit_tstamp', 'first_withdrawal_tstamp']:
            merged_df[col] = merged_df[[col, col + '_old']].min(axis=1)
        
        for col in ['last_login_tstamp', 'last_deposit_tstamp', 'last_withdrawal_tstamp']:
            merged_df[col] = merged_df[[col, col + '_old']].max(axis=1)
        
        # Drop duplicate columns created by merge (keep the new ones)
        for col in ['user_id', 'created_at']:
            merged_df[col] = merged_df[col].combine_first(merged_df[col + '_old'])
            merged_df.drop(columns=[col + '_old'], inplace=True)

        merged_df.drop(columns=['first_login_tstamp_old', 'first_deposit_tstamp_old', 'first_withdrawal_tstamp_old', 'last_login_tstamp_old', 'last_deposit_tstamp_old', 'last_withdrawal_tstamp_old'], inplace=True)
        
        dim_users = merged_df

    print(f"Writing merged data to {output_file}...")
    # Write to the output file
    dim_users.to_csv(output_file, index=False)
    print(f"Data successfully written to {output_file}")

def zip_directory(input_directory, output_file):
    print('Creating output.zip')
    utils.zip_directory_with_limit(input_directory, output_file, size_limit=(100 * 1024 * 1024))  # 100 MB size limit per zip file


# Usage example

# download_data()

# pivot_date = "2024-07-01"
# offset_days = 30
# deposit_file = "data/input/deposit_sample_data.csv"
# withdrawals_file = "data/input/withdrawals_sample_data.csv"
# output_file = "data/output/fct_active_users.csv"

# create_fct_active_users(pivot_date, offset_days, deposit_file, withdrawals_file, output_file)


# pivot_date = "2024-07-01"
# offset_days = 30
# event_file = "data/input/event_sample_data.csv"
# deposit_file = "data/input/deposit_sample_data.csv"
# withdrawals_file = "data/input/withdrawals_sample_data.csv"
# output_file = "data/output/fct_system_activity.csv"

# create_fct_system_activity(pivot_date, offset_days, event_file, deposit_file, withdrawals_file, output_file)

# pivot_date = "2024-07-01"
# offset_days = 30
# activity_file = "data/output/fct_system_activity.csv"
# user_id_file = "data/input/user_id_sample_data.csv"
# output_file = "data/output/dim_users.csv"

# create_dim_users(pivot_date, offset_days, activity_file, user_id_file, output_file)


# zip_directory('data/output', 'data/zip/output.zip')