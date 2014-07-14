import argparse
from contextlib import closing
from forklift.db.base import engine
from forklift.loaders.fbsync import add_new_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add new data to the FBSync tables (posts, user_posts). Data for each table must be in proper TSV format, in the same column order as its respective table, before running.')
    parser.add_argument(
        '--bucket_name',
        type=str,
        required=True,
        help='S3 bucket in which the new data resides'
    )
    parser.add_argument(
        '--posts_folder',
        type=str,
        required=True,
        help='Path to S3 folder or file (excluding the bucket) in which the posts reside. Can be nested (e.g. rstransfer/posts)'
    )
    parser.add_argument(
        '--user_posts_folder',
        type=str,
        required=True,
        help='Path to S3 folder or file (excluding the bucket) in which the user_posts reside. Can be nested (e.g. rstransfer/links)'
    )
    args = parser.parse_args()

    bucket_name = args.bucket_name
    posts_folder = args.posts_folder
    user_posts_folder = args.user_posts_folder

    with closing(engine.connect()) as connection:
        add_new_data(bucket_name, posts_folder, user_posts_folder, connection)
