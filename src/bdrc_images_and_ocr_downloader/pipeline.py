from bdrc_images_and_ocr_downloader.work_info import get_s3_keys
from bdrc_images_and_ocr_downloader.download import download_images, download_ocr




def main():
    images_s3_key, ocr_s3_keys = get_s3_keys("W00EGS1016686")
    download_images(images_s3_key)
    download_ocr(ocr_s3_keys)


if __name__ == "__main__":
    main()