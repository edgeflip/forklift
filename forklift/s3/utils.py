from boto.s3.connection import S3Connection
from boto.s3.key import Key
from forklift.settings import AWS_ACCESS_KEY, AWS_SECRET_KEY


def get_conn_s3(key=AWS_ACCESS_KEY, sec=AWS_SECRET_KEY):
    return S3Connection(key, sec)


def move_file(bucket_name, key_name, new_directory):
    buck = get_conn_s3().get_bucket(bucket_name)
    buck.copy_key(os.path.join(new_directory, key_name), bucket_name, key_name)
    buck.delete_key(key_name)
