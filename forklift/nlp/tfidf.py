from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import spdiags
import pickle
from boto.s3.key import Key
import logging
import ssl
from forklift.s3.utils import S3ReservoirSampler
from forklift.loaders.fbsync import FeedFromS3
from forklift.nlp.utils import tokenizer

VOCAB_FILENAME = 'vocab'
IDF_FILENAME = 'idf_matrix'
TRAINING_SET_SIZE = 50000
VECTORIZER_TRAINING_BUCKET = "user_feeds_0"
VECTORIZER_DEFAULT_BUCKET = "warehouse-forklift"
VECTORIZER_DEFAULT_PREFIX = "vectorizer"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def create_vectorizer(**extra_kwargs):
    return TfidfVectorizer(
        input='content',
        min_df=0.001,
        max_df=0.4,
        tokenizer=tokenizer,
        **extra_kwargs
    )


def load_or_train_vectorizer_components(
    s3_conn,
    pretrained_bucket_name,
    prefix,
    data_bucket_name,
    training_set_size
):
    vocab_key, idf_key = build_keys(
        s3_conn,
        pretrained_bucket_name,
        prefix
    )

    if not vocab_key.exists() or not idf_key.exists():
        train_and_save(
            vocab_key,
            idf_key,
            data_bucket_name,
            training_set_size
        )

    vocab = pickle.loads(vocab_key.get_contents_as_string())
    idf_matrix = pickle.loads(idf_key.get_contents_as_string())

    return (vocab, idf_matrix)


def ensure_trained_default_vectorizer(
    s3_conn,
    data_bucket_name,
    training_set_size
):
    vocab_key, idf_key = build_keys(
        s3_conn,
        VECTORIZER_DEFAULT_BUCKET,
        VECTORIZER_DEFAULT_PREFIX
    )

    if not vocab_key.exists() or not idf_key.exists():
        logger.info("Vectorizer not found, training will now commence")
        train_and_save(
            vocab_key,
            idf_key,
            data_bucket_name,
            training_set_size
        )


# will fail if the vectorizer doesn't exist. you've been warned
def load_default_vectorizer(s3_conn):
    vocab_key, idf_key = build_keys(
        s3_conn,
        VECTORIZER_DEFAULT_BUCKET,
        VECTORIZER_DEFAULT_PREFIX
    )

    vocab = pickle.loads(vocab_key.get_contents_as_string())
    idf_matrix = pickle.loads(idf_key.get_contents_as_string())

    return bootstrap_trained_vectorizer(vocab, idf_matrix)


def build_keys(
    s3_conn,
    pretrained_bucket_name,
    prefix
):
    pretrained_bucket = s3_conn.get_bucket(pretrained_bucket_name)
    vocab_key = Key(
        bucket=pretrained_bucket,
        name="{}/{}".format(prefix, VOCAB_FILENAME)
    )
    idf_key = Key(
        bucket=pretrained_bucket,
        name="{}/{}".format(prefix, IDF_FILENAME)
    )
    return (vocab_key, idf_key)


def train_and_save(
    vocab_key,
    idf_key,
    data_bucket_name,
    training_set_size
):
    vocab, idf_matrix = train(data_bucket_name, training_set_size)
    vocab_key.set_contents_from_string(pickle.dumps(vocab))
    idf_key.set_contents_from_string(pickle.dumps(idf_matrix))


def bootstrap_trained_vectorizer(vocab, idf):
    vectorizer = create_vectorizer(vocabulary=vocab)
    n_features = idf.shape[0]
    vectorizer._tfidf._idf_diag = spdiags(
        idf,
        diags=0,
        m=n_features,
        n=n_features
    )
    return vectorizer


def training_feeds(in_bucket_name, training_set_size):
    sampler = S3ReservoirSampler(training_set_size)
    max_retries = 5
    for i, key in enumerate(sampler.sampling_bucket(in_bucket_name)):
        retries = 0
        while retries < max_retries:
            try:
                feed = FeedFromS3(key)
                yield feed.post_corpus
                break
            except KeyError:
                logger.info("Skipping data-less key %s", key)
                break
            except ssl.SSLError:
                retries += 1
                logger.info("Error on key %s, %d retries", key, retries)


def train(in_bucket_name, training_set_size):
    vectorizer = create_vectorizer()
    logger.debug("Fitting training set.")
    vectorizer.fit(training_feeds(in_bucket_name, training_set_size))
    logger.debug("Done fitting.")
    return (vectorizer.vocabulary_, vectorizer.idf_)
