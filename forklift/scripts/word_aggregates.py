from forklift.db.utils import checkout_connection
import nltk
import string
from sklearn.feature_extraction.text import TfidfVectorizer
from os import listdir
from scipy.sparse import spdiags


def marc_tokenizer(message):
    return nltk.regexp_tokenize(message, r'(?u)\b\w\w+\b')


def train(directory, filenames):
    vectorizer = TfidfVectorizer(
        input='filename',
        min_df=0.001,
        max_df=0.4,
        tokenizer=marc_tokenizer,
        ngram_range=(1,1),
    )
    vectorizer.fit(directory + f for f in filenames)
    return (vectorizer.vocabulary_, vectorizer.idf_)


def parse(filename, vocab, idf):
    vectorizer = TfidfVectorizer(
        input='filename',
        min_df=0.001,
        max_df=0.4,
        tokenizer=marc_tokenizer,
        vocabulary=vocab,
    )
    n_features = idf.shape[0]
    vectorizer._tfidf._idf_diag = spdiags(idf, diags=0, m=n_features, n=n_features)
    tfidf = vectorizer.transform([filename])[0].toarray()
    top_n = tfidf.argsort()[0][::-1][:20]
    feature_names = vectorizer.get_feature_names()
    print len(feature_names)
    for y in top_n:
        print tfidf[0][y], feature_names[y]


if __name__ == '__main__':
    directory = '/home/tristan/Documents/fb_argonaut/transformed/'
    files = listdir(directory)
    vocab, idf = train(directory, files)
    parse('messages.txt', vocab, idf)
