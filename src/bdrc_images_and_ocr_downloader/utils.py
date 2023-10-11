import hashlib


def get_hash(work_id):
    md5 = hashlib.md5(str.encode(work_id))
    two = md5.hexdigest()[:2]
    return two