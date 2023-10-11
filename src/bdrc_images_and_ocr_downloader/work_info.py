from pathlib import Path
from openpecha.utils import dump_yaml, load_yaml
from openpecha.buda.api import get_buda_scan_info, get_image_list
from bdrc_images_and_ocr_downloader.utils import get_hash


def get_images_s3_key(work_id):
    s3_keys = []
    hash_two = get_hash(work_id)
    scan_info = get_buda_scan_info(work_id)
    for image_group_id, _ in scan_info["image_groups"].items():
        images_list = get_image_list(work_id, image_group_id)
        if type(image_group_id[1:]) == int:
            image_group_id = image_group_id[1:]
        for image in images_list:
            s3_key = f"Works/{hash_two}/{work_id}/images/{work_id}-{image_group_id}/{image['filename']}"
            s3_keys.append(s3_key)
        return s3_keys


def get_ocr_s3_key(image_s3_keys):
    s3_keys = []
    for key in image_s3_keys:
        ocr_s3_key = key.replace("images", "ocr")
        filename = ocr_s3_key.split("/")[-1].split(".")[0] + ".json.gz"
        s3_key = ocr_s3_key.replace(ocr_s3_key.split("/")[-1], filename)
        s3_keys.append(s3_key)
    return s3_keys


def get_s3_keys(work_id):
    image_s3_keys = get_images_s3_key(work_id)
    ocr_s3_keys = get_ocr_s3_key(image_s3_keys)
    return image_s3_keys, ocr_s3_keys

if __name__ == "__main__":
    get_s3_keys("W00EGS1016686")