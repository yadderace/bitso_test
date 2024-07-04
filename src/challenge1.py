import requests
import os
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

def download_multiple_files():
    """
    Downloads multiple files from predefined URLs and saves them to a specified directory.

    Modify the `CSV_URLs` list in config.py to add or remove URLs as needed.

    This function calls `download_file` for each URL in the `urls` list.
    """
    # Define URLs and output directory
    csv_urls = config.CSV_URLs
    output_dir = config.INPUT_DIR  # Adjust the path as per your project structure

    # Download each file
    for url in csv_urls:
        download_file(url, output_dir)

download_multiple_files()