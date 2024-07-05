import requests
import os
import zipfile as zf
from io import BytesIO

def download_file(url, output_dir):
    """
    Downloads a file from the given URL and saves it to the specified directory.

    Args:
    - url (str): The URL from which to download the file.
    - output_dir (str): The directory path where the downloaded file will be saved.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Extract filename from URL
    filename = url.split('/')[-1]

    # Download file
    response = requests.get(url)
    if response.status_code == 200:
        # Save file to output directory
        with open(os.path.join(output_dir, filename), 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {filename} successfully.")
    else:
        print(f"Failed to download {url}. Status code: {response.status_code}")

def download_and_unzip_file(url, output_dir):
    """
    Download a zip file from a URL, unzip it, and save the CSV file in the specified output directory.
    
    Args:
    url (str): URL pointing to the zip file.
    output_dir (str): Directory where the unzipped CSV file will be saved.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Download the zip file
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        
        # Unzip the file
        with zf.ZipFile(BytesIO(response.content)) as zip_file:
            for file_info in zip_file.infolist():
                if file_info.filename.endswith('.csv'):
                    # Extract the CSV file to the output directory
                    zip_file.extract(file_info, output_dir)
                    print(f"Downloaded and extracted {file_info.filename} to {output_dir}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")
    except zf.BadZipFile as e:
        print(f"Failed to unzip {url}: {e}")

def get_first_last_timestamps(df, event_type):
    """
    Get the first and last timestamps for a specified event type from a DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the event data.
    event_type (str): The event type to filter by.

    Returns:
    tuple: A tuple containing the first and last timestamps for the specified event type.
           Returns (None, None) if no timestamps are found for the specified event type.
    """
    timestamps = df[df['event_type'] == event_type]['event_tstamp']
    if not timestamps.empty:
        return timestamps.min(), timestamps.max()
    return None, None

def zip_directory_with_limit(directory_path, output_zip_base, size_limit):
    """
    Zips all the contents of a directory into multiple zip files if necessary to respect the size limit.

    Parameters:
    directory_path (str): Path to the directory to be zipped.
    output_zip_base (str): Base name for the output zip files.
    size_limit (int): Size limit for each zip file in bytes.
    """
    def get_zip_file_name(base_name, index):
        return f"{base_name}_{index}.zip"
    
    current_zip_index = 1
    current_zip_file = get_zip_file_name(output_zip_base, current_zip_index)
    current_zip_size = 0

    with zf.ZipFile(current_zip_file, 'w', zf.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, directory_path)
                
                file_size = os.path.getsize(file_path)
                
                # Check if adding this file would exceed the size limit
                if current_zip_size + file_size > size_limit:
                    # Close the current zip file and start a new one
                    zipf.close()
                    current_zip_index += 1
                    current_zip_file = get_zip_file_name(output_zip_base, current_zip_index)
                    zipf = zf.ZipFile(current_zip_file, 'w', zf.ZIP_DEFLATED)
                    current_zip_size = 0
                
                # Add the file to the current zip file
                zipf.write(file_path, arcname)
                current_zip_size += file_size