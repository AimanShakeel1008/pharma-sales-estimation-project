from estimation.estimation import sales_estimation
import os

from utils.db_utils import save_input_data_to_db
from utils.s3_utils import get_latest_zip_file, download_file
from utils.zip_utils import extract_zip, load_data

DATA_FOLDER = "data"

DOWNLOAD_FOLDER = "downloads"


def prepare_directories():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


def main():
    print("🚀 Starting Pharma Sales Estimation...")

    prepare_directories()

    # Step 1: Fetch latest zip key from S3
    zip_key = get_latest_zip_file()
    print("zip_key:",zip_key)

    # Step 2: Download zip to local downloads folder
    zip_path = download_file(zip_key, DOWNLOAD_FOLDER)
    print("zip_path:", zip_path)

    # Step 3: Extract to data folder
    extract_zip(zip_path, DATA_FOLDER)

    # Step 4: load the data from CSVs
    data = load_data(DATA_FOLDER)  # Load CSVs

    # Step 5: Save input data in database
    save_input_data_to_db(data)

    # Step 6: run the estimation
    sales_estimation(data)


if __name__ == "__main__":
    main()
