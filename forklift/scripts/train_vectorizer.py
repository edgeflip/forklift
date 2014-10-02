import argparse
import logging
import forklift.nlp.tfidf as tfidf
from forklift.s3.utils import get_conn_s3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Train a Tfidf vectorizer and save it to s3'
    )
    parser.add_argument(
        '--pretrained_vectorizer_bucket',
        type=str,
        help='s3 bucket housing the pre-trained vectorizer files',
        nargs='?',
        default=tfidf.VECTORIZER_DEFAULT_BUCKET
    )
    parser.add_argument(
        '--pretrained_vectorizer_prefix',
        type=str,
        help='s3 path prefix(sans bucket), housing pretrained vectorizer files',
        nargs='?',
        default=tfidf.VECTORIZER_DEFAULT_PREFIX
    )
    parser.add_argument(
        '--vectorizer_training_bucket',
        type=str,
        help='s3 bucket housing the raw feed files for training the vectorizer',
        nargs='?',
        default=tfidf.VECTORIZER_TRAINING_BUCKET
    )
    parser.add_argument(
        '--training_set_size',
        type=int,
        help='number of feeds to including in training set',
        default=tfidf.TRAINING_SET_SIZE
    )
    args = parser.parse_args()

    vocab_key, idf_key = tfidf.build_keys(
        get_conn_s3(),
        args.pretrained_vectorizer_bucket,
        args.pretrained_vectorizer_prefix
    )

    tfidf.train_and_save(
        vocab_key,
        idf_key,
        args.vectorizer_training_bucket,
        args.training_set_size
    )
