from forklift.db.utils import checkout_connection
import nltk
from forklift.tokenizer import Tokenizer as HappiestFunTokenizer
from nltk.corpus import stopwords
import string
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer, TfidfTransformer
from scipy.sparse import hstack
from os import listdir

stop = stopwords.words('english') + list(string.punctuation)

def retrieve_posts(connection, fbid):
    return connection.execute(
        """
        select fbid_post, message
        from posts_oldkey
        where fbid_user = {user} and post_from = {user} and message <> ''
        """.format(user=fbid)
    )


def rank_and_filter(fdist):
    return [w for w in fdist.items() if w[0] not in stop and len(w[0]) > 2][:20]


def builtin_tokenizer(message):
    return set(
        [nltk.word_tokenize(t) for t in nltk.sent_tokenize(message)][0]
    )


def marc_tokenizer(message):
    return nltk.regexp_tokenize(message, r'(?u)\b\w\w+\b')


def compute_stats(posts, vectorizer):
    cumulative_unigrams_dist = nltk.FreqDist()
    cumulative_bigrams_dist = nltk.FreqDist()
    unigram_appearances = nltk.FreqDist()
    bigram_appearances = nltk.FreqDist()
    for message in posts:
        message = message.lower()
        tfidf = vectorizer.transform([message])
        #print tfidf
        features = vectorizer.get_feature_names()
        #tokens = builtin_tokenizer(message)
        tokens = marc_tokenizer(message)
        #tokens = HappiestFunTokenizer().tokenize(message)
        bigrams = nltk.bigrams(tokens)
        unigram_frequency = nltk.FreqDist(tokens)
        bigram_frequency = nltk.FreqDist(bigrams)

        cumulative_unigrams_dist.update(unigram_frequency)
        cumulative_bigrams_dist.update(bigram_frequency)
        unigram_appearances.update(unigram_frequency.samples())
        bigram_appearances.update(bigram_frequency.samples())

    return (
        rank_and_filter(fdist) for fdist in (
            cumulative_unigrams_dist,
            cumulative_bigrams_dist,
            unigram_appearances,
            bigram_appearances
        )
    )


def train(directory, filenames):
    vectorizer = TfidfVectorizer(
        input='filename',
        min_df=0.001,
        max_df=0.4,
        tokenizer=marc_tokenizer,
        ngram_range=(1,1),
    )
    vectorizer.fit(directory + f for f in filenames)
    print "length = ", len(vectorizer.stop_words_)
    return vectorizer


def parse(filename, vectorizer):
    tfidf = vectorizer.transform([filename])
    top_n = tfidf[0].toarray().argsort()[0][::-1][:20]
    print top_n
    feature_names = vectorizer.get_feature_names()
    print len(feature_names)
    for y in top_n:
        print tfidf[0].toarray()[0][y], feature_names[y]


if __name__ == '__main__':
    directory = '/home/tristan/Documents/fb_argonaut/transformed/'
    files = listdir(directory)
    vectorizer = train(directory, files)
    print "trained"
    parse('messages.txt', vectorizer)
    #print cum_uni
    #print cum_bi
    #print uni_app
    #print bi_app

