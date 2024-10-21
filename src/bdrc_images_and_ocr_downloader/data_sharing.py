from pathlib import Path
import csv
import os
import shutil
import subprocess
from work_info import get_s3_prefix
from download import get_s3_keys, download_images, filter_norbuketaka_images_keys


def download_images_of_work(work_id):
    s3_prefix = get_s3_prefix(work_id)
    s3_keys = get_s3_keys(s3_prefix)
    keys = filter_norbuketaka_images_keys(s3_keys)
    for key in keys:
        work_id = key.split("/")[2]
        image_group_id = key.split("/")[6]
        download_images(work_id, image_group_id, key)


def upload_to_s3(file_path):
    try:
        subprocess.run(["aws", "s3", "cp", file_path, "s3://monlam.ai.ocr/BDRC/"], check=True)
        print(f"File '{file_path}' has been uploaded successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while uploading the file: {e}")


def zip_folder(folder_path, output_path=None):
    if output_path is None:
        output_path = folder_path + ".zip"
    
    if not os.path.isdir(folder_path):
        raise ValueError(f"The folder '{folder_path}' does not exist.")
    
    try:
        subprocess.run(["zip", "-r", output_path, folder_path], check=True)
        print(f"Folder '{folder_path}' has been zipped successfully as '{output_path}'.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while zipping the folder: {e}")

    return output_path

def main(file_path):
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            work_id = row["work_id"]
            download_images_of_work(work_id)
            zip_path = zip_folder(f"./data/{work_id}")
            upload_to_s3(zip_path)


if __name__ == "__main__":
    file_path = Path(f"./data/Norbuketaka.csv")
    main(file_path)

