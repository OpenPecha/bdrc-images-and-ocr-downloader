from pathlib import Path
from bdrc_images_and_ocr_downloader.config import s3_client, BDRC_ARCHIVE_BUCKET, OCR_OUTPUT_BUCKET


def get_s3_keys(prefix):
    obj_keys = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(Bucket=OCR_OUTPUT_BUCKET, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3_client.list_objects_v2(Bucket=OCR_OUTPUT_BUCKET, Prefix=prefix)
        if response['Contents']:
            for obj in response['Contents']:
                obj_key = obj['Key']
                obj_keys.append(obj_key)
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break
    return obj_keys


def download_images(s3_keys):
    work_id = s3_keys[0].split('/')[2]
    for key in s3_keys:
        image_group_id = key.split('/')[4]
        download_path = Path(f"./data/{work_id}/{image_group_id}/images/")
        download_path.mkdir(parents=True, exist_ok=True)
        file_name = key.split('/')[-1]
        local_file_path = f"{download_path}/{file_name}"
        if Path(local_file_path).exists():
            continue
        try:
            s3_client.download_file(BDRC_ARCHIVE_BUCKET, key, local_file_path)
            print(f"Downloaded {file_name} successfully.")
        except Exception as e:
            print(f"Error due to {e}")


def filter_ocr_s3_keys(s3_keys):
    s3_dict = {}
    for key in s3_keys:
        if key.split('/')[3] == "vision":
            if key.split('/')[5] == "output":
                batch = key.split('/')[4]
                image_group_id = key.split('/')[6]
                if batch not in s3_dict:
                    s3_dict[batch] = {
                        image_group_id: []
                    }
                elif image_group_id not in s3_dict[batch].keys():
                    s3_dict[batch][image_group_id] = []
                s3_dict[batch][image_group_id].append(key)
    return s3_dict


def download_images(work_id, image_group_id, key):
    download_path = Path(f"./data/{work_id}/{image_group_id}/")
    download_path.mkdir(parents=True, exist_ok=True)
    file_name = key.split('/')[-1]
    local_file_path = f"{download_path}/{file_name}"
    if Path(local_file_path).exists():
        return
    try:
        s3_client.download_file(OCR_OUTPUT_BUCKET, key, local_file_path)
        print(f"Downloaded {file_name} successfully.")
    except Exception as e:
        print(f"Error due to {e}")



def download_ocr(work_id, prefix):
    s3_keys = get_s3_keys(prefix)
    s3_dict = filter_ocr_s3_keys(s3_keys)
    for _, image_group_info in s3_dict.items():
        for image_group_id, keys in image_group_info.items():
            for key in keys:
                download_path = Path(f"./data/{work_id}/{image_group_id}/ocr")
                download_path.mkdir(parents=True, exist_ok=True)
                file_name = key.split('/')[-1]
                local_file_path = f"{download_path}/{file_name}"
                if Path(local_file_path).exists():
                    continue
                try:
                    s3_client.download_file(OCR_OUTPUT_BUCKET, key, local_file_path)
                    print(f"Downloaded {file_name} successfully.")
                except Exception as e:
                    print(f"Error due to {e}")
        break


def filter_google_books_images_keys(s3_keys):
    keys = []
    for s3_key in s3_keys:
        if s3_key.split("/")[3] == "google_books":
            if s3_key.split("/")[4] == "batch_2022":
                if s3_key.split("/")[5] == "output":
                    if s3_key.split("/")[-1] == "images.zip":
                        keys.append(s3_key)
    return keys

def filter_norbuketaka_images_keys(s3_keys):
    keys = []
    for s3_key in s3_keys:
        if s3_key.split("/")[3] == "vision":
            if s3_key.split("/")[5] == "images":
                keys.append(s3_key)
    return keys