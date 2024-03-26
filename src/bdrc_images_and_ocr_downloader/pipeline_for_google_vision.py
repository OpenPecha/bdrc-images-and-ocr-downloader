import csv
import argparse
from bdrc_images_and_ocr_downloader.work_info import get_image_keys_and_s3_prefix
from bdrc_images_and_ocr_downloader.download import download_images, download_ocr


def main(input_file_path):
    with open(input_file_path, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            work_id = row[2]
            images_s3_key, s3_prefix = get_image_keys_and_s3_prefix(work_id)
            download_images(images_s3_key)
            download_ocr(work_id, s3_prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file_path", type=str, required=True)
    args = parser.parse_args()
    input_file_path = args.input_file_path
    main(input_file_path)