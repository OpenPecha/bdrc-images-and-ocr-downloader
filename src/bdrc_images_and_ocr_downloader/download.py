from pathlib import Path
from bdrc_images_and_ocr_downloader.config import s3_client, s3_resource, bdrc_archive_bucket, ocr_output_bucket


def download_images(s3_keys):
    work_id = s3_keys[0].split('/')[0]
    download_path = Path(f"../data/{work_id}/images")
    download_path.mkdir(parents=True, exist_ok=True)
    for key in s3_keys:
        file_name = key.split('/')[-1]
        local_file_path = f"{download_path}/{file_name}"
        try:
            s3_client.download_file(bdrc_archive_bucket, key, local_file_path)
            print(f"Downloaded {file_name} successfully.")
        except Exception as e:
            print(f"Error due to {e}")



def download_ocr(s3_keys):
    work_id = s3_keys[0].split('/')[0]
    download_path = Path(f"../../data/{work_id}/ocr")
    download_path.mkdir(parents=True, exist_ok=True)
    for key in s3_keys:
        file_name = key.split('/')[-1]
        local_file_path = f"{download_path}/{file_name}"
        try:
            s3_client.download_file(bdrc_archive_bucket, key, local_file_path)
            print(f"Downloaded {file_name} successfully.")
        except Exception as e:
            print(f"Error due to {e}")
