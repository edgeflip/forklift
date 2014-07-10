import logging
import os
import sys
sys.path.append(os.path.abspath(os.curdir))
from forklift.db.utils import checkout_raw_connection
from forklift.loaders.fbsync import load_and_dedupe, POSTS_TABLE, USER_POSTS_TABLE

logger = logging.getLogger(__name__)


###################################
# use this if we have changed how the CSVs are created and thus need to blow away
# the entire existing tableset

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replace the FBSync tables (posts, user_posts) with freshly populated ones')
    parser.add_argument('--bucket_name', type=str, help='S3 bucket in which the fbsync data resides')
    parser.add_argument('--posts_folder', type=str, help='S3 folder (excluding the bucket) in which the posts reside. Can be nested (e.g. rstransfer/posts)'
    parser.add_argument('--user_posts_folder', type=str, help='S3 folder (excluding the bucket) in which the user_posts reside. Can be nested (e.g. rstransfer/links)')
    args = parser.parse_args()

    bucket_name = args.bucket_name
    posts_folder = args.posts_folder
    user_posts_folder = args.user_posts_folder

    with checkout_raw_connection as connection:
        load_and_dedupe(bucket_name, posts_folder, POSTS_TABLE, connection, optimize=True)
        load_and_dedupe(bucket_name, user_posts_folder, USER_POSTS_TABLE, connection, optimize=True)
