import zipfile
import pandas as pd


def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


def load_data(data_folder):
    """Loads extracted CSVs into Pandas DataFrames"""
    return {
        "regions": pd.read_csv(f"{data_folder}/regions.csv"),
        "countries": pd.read_csv(f"{data_folder}/countries.csv"),
        "categories": pd.read_csv(f"{data_folder}/categories.csv"),
        "companies": pd.read_csv(f"{data_folder}/companies.csv"),
        "drugs": pd.read_csv(f"{data_folder}/drugs.csv"),
        "category_sales": pd.read_csv(f"{data_folder}/category_sales.csv"),
        "drug_rankings": pd.read_csv(f"{data_folder}/drug_rankings.csv"),
        "sales_our_company": pd.read_csv(f"{data_folder}/sales_our_company.csv"),
    }
