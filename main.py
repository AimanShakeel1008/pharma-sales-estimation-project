import sys

from estimation.estimation import sales_estimation
import os
import re

from utils.db_utils import save_input_data_to_db, update_status
from utils.s3_utils import get_latest_zip_file, download_file
from utils.zip_utils import extract_zip, load_data

DATA_FOLDER = "data"

DOWNLOAD_FOLDER = "downloads"


def prepare_directories():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


def main():
    global year_quarter

    try:
        # Step 1: Fetch latest zip key from S3
        zip_key = get_latest_zip_file()

        match = re.search(r"\d{4}Q\d", zip_key)

        year_quarter = match.group()

        print(f"🚀 Starting Pharma Sales Estimation for {year_quarter}...")

        update_status(year_quarter, "In Progress")

        prepare_directories()

        # Step 2: Download zip to local downloads folder
        zip_path = download_file(zip_key, DOWNLOAD_FOLDER)
        zip_extracted_folder = zip_key.replace(".zip", "")

        # Step 3: Extract to data folder
        extract_zip(zip_path, DATA_FOLDER)

        # Step 4: load the data from CSVs
        data = load_data(zip_extracted_folder, DATA_FOLDER)  # Load CSVs

        # Step 5: Save input data in database
        save_input_data_to_db(data)

        # Step 6: run the estimation
        sales_estimation(data)

        update_status(year_quarter, "Success", message="Estimation completed successfully.", completed=True)

        print(f"✅ Pharma Sales Estimation completed for {year_quarter}.")

    except Exception as e:
        update_status(year_quarter, "Failed", message=str(e), completed=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
