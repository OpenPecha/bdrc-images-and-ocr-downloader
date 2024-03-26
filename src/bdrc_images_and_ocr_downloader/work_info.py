import gzip
import json
import io
import hashlib
import botocore
from openpecha.buda.api import get_buda_scan_info,get_s3_folder_prefix
from bdrc_images_and_ocr_downloader.config import s3_client


S3 = s3_client


def get_hash(work_id):
    md5 = hashlib.md5(str.encode(work_id))
    two = md5.hexdigest()[:2]
    return two


def gets3blob(s3Key):
    f = io.BytesIO()
    try:
        S3.download_fileobj('archive.tbrc.org', s3Key, f)
        return f
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        else:
            raise


def get_s3_folder_prefix(work_id, image_group_lname):
    hash = get_hash(work_id)
    pre, rest = image_group_lname[0], image_group_lname[1:]
    if pre == 'I' and rest.isdigit() and len(rest) == 4:
        suffix = rest
    else:
        suffix = image_group_lname
    folder_prefix = f'Works/{hash}/{work_id}/images/{work_id}-{suffix}/'
    return folder_prefix, suffix


def get_image_list(wlname, image_group_lname):
    s3key, image_group_suffix = get_s3_folder_prefix(wlname, image_group_lname)
    blob = gets3blob(s3key+"dimensions.json")
    if blob is None:
        return None
    blob.seek(0)
    b = blob.read()
    ub = gzip.decompress(b)
    s = ub.decode('utf8')
    data = json.loads(s)
    return data, image_group_suffix


def get_images_s3_key(work_id):
    s3_keys = []
    s3_prefix = get_s3_prefix(work_id)
    scan_info = get_buda_scan_info(work_id)
    for image_group_id, _ in scan_info["image_groups"].items():
        images_list, image_group_suffix = get_image_list(work_id, image_group_id)
        for image in images_list:
            s3_key = f"{s3_prefix}/images/{work_id}-{image_group_suffix}/{image['filename']}"
            s3_keys.append(s3_key)
    return s3_keys


def get_image_keys_and_s3_prefix(work_id):
    image_s3_keys = get_images_s3_key(work_id)
    s3_prefix = "/".join(image_s3_keys[0].split('/')[0:3])
    return image_s3_keys, s3_prefix

def get_s3_prefix(work_id):
    hash = get_hash(work_id)
    s3_prefix = f"Works/{hash}/{work_id}/"
    return s3_prefix

