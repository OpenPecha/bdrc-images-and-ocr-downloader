import csv
from pathlib import Path
from work_info import get_s3_prefix
from download import get_s3_keys, download_google_books_images, filter_google_books_images_keys



def download_image_zip_keys(work_id):
    s3_prefix = get_s3_prefix(work_id)
    s3_keys = get_s3_keys(s3_prefix)
    keys = filter_google_books_images_keys(s3_keys)
    for key in keys:
        work_id = key.split("/")[2]
        image_group_id = key.split("/")[6]
        download_google_books_images(work_id, image_group_id, key)


def main():
    with open(input_file_path, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            work_id = row[0]
            download_image_zip_keys(work_id)
        


if __name__ == "__main__":
    input_file_path = Path(f"./data/batch_01_opfs.csv")
    main()