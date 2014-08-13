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
TRAINING_SET_SIZE = 1000

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


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
    pretrained_bucket = s3_conn.get_bucket(pretrained_bucket_name)
    vocab_key = Key(
        bucket=pretrained_bucket,
        name="{}/{}".format(prefix, VOCAB_FILENAME)
    )
    idf_key = Key(
        bucket=pretrained_bucket,
        name="{}/{}".format(prefix, IDF_FILENAME)
    )
    if vocab_key.exists() and idf_key.exists():
        print "it exists!"
        vocab = pickle.loads(vocab_key.get_contents_as_string())
        idf_matrix = pickle.loads(idf_key.get_contents_as_string())
    else:
        print "uggh lets train it"
        vocab, idf_matrix = train(data_bucket_name, training_set_size)
        vocab_key.set_contents_from_string(pickle.dumps(vocab))
        idf_key.set_contents_from_string(pickle.dumps(idf_matrix))
    return (vocab, idf_matrix)


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
        if i % 1000 == 0:
            print i
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
