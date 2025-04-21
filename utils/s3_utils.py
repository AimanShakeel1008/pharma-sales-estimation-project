import boto3
import os
import configparser

# Load bucket from settings.ini
config = configparser.ConfigParser()
config.read("config/settings.ini")

bucket_name = config['aws']['bucket_name']
region = config['aws']['region']

s3 = boto3.client("s3", region_name=region)


def get_latest_zip_file():
    response = s3.list_objects_v2(Bucket=bucket_name)
    zip_files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".zip")]
    latest = max(zip_files, key=lambda x: x["LastModified"])
    return latest["Key"]


def download_file(key, destination_folder):
    local_path = os.path.join(destination_folder, os.path.basename(key))
    s3.download_file(bucket_name, key, local_path)
    return local_path
