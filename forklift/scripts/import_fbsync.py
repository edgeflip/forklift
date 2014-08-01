import argparse
from contextlib import closing
from forklift.db.base import engine
from forklift.loaders.fbsync import load_and_dedupe, POSTS_TABLE, USER_POSTS_TABLE, LIKES_TABLE, TOP_WORDS_TABLE


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replace the FBSync tables (posts, user_posts) with freshly populated ones. Data for each table must be in proper TSV format, in the same column order as its respective table, before running')
    parser.add_argument(
        '--bucket_name',
        type=str,
        required=True,
        help='S3 bucket in which the fbsync data resides'
    )
    parser.add_argument(
        '--posts_folder',
        type=str,
        required=True,
        help='S3 folder (excluding the bucket) in which the posts reside. Can be nested (e.g. rstransfer/posts)'
    )
    parser.add_argument(
        '--user_posts_folder',
        type=str,
        required=True,
        help='S3 folder (excluding the bucket) in which the user_posts reside. Can be nested (e.g. rstransfer/links)'
    )
    parser.add_argument(
        '--likes_folder',
        type=str,
        required=True,
        help='S3 folder (excluding the bucket) in which the page likes reside. Can be nested (e.g. rstransfer/likes)'
    )
    parser.add_argument(
        '--top_words_folder',
        type=str,
        required=True,
        help='S3 folder (excluding the bucket) in which the top words reside. Can be nested (e.g. rstransfer/top_words)'
    )
    args = parser.parse_args()

    bucket_name = args.bucket_name
    posts_folder = args.posts_folder
    user_posts_folder = args.user_posts_folder
    likes_folder = args.likes_folder
    top_words_folder = args.top_words_folder

def load_from_s3(connection, bucket_name, key_name, table_name, delim="\t", create_statement=None):
    with closing(engine.connect()) as connection:
        load_and_dedupe(bucket_name, posts_folder, POSTS_TABLE, connection, optimize=True)
        load_and_dedupe(bucket_name, user_posts_folder, USER_POSTS_TABLE, connection, optimize=True)
        load_and_dedupe(bucket_name, likes_folder, LIKES_TABLE, connection, optimize=True)
        load_from_s3(connection, bucket_name, top_words_folder, TOP_WORDS_TABLE)
