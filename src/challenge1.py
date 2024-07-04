import requests
import os
import zipfile as zf
from io import BytesIO
import config

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

def download_multiple_files():
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
        download_file(url, output_dir)

    # Define URLs and output directory for zipped files
    zip_urls = config.ZIP_URLs
    
    # Download each file
    for url in zip_urls:
        download_and_unzip_file(url, output_dir)

download_multiple_files()