import unicodecsv
import logging
import json
import tempfile
import os
import sys
import StringIO
from boto.s3.key import Key
sys.path.append(os.path.abspath(os.curdir))
from forklift.s3.utils import get_conn_s3

FIELDS = (
    'fbid',
    'birthday',
    'fname',
    'lname',
    'email',
    'gender',
    'city',
    'state',
    'country',
    'activities',
    'affiliations',
    'books',
    'devices',
    'friend_request_count',
    'has_timeline',
    'interests',
    'languages',
    'likes_count',
    'movies',
    'music',
    'political',
    'profile_update_time',
    'quotes',
    'relationship_status',
    'religion',
    'sports',
    'tv',
    'wall_count',
    'updated',
)

S3_BUCKET_NAME = 'redshiftxfer'
S3_IN_FOLDER = 'dynamousers/2014-06-24_11.01'
S3_OUT_FOLDER = 'dynamousers/output'
DB_TEXT_LEN = 4096

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

def s3_key_iter(bucket):
    logger.debug("reading bucket (%s)" % (bucket.name,))
    feeds = bucket.list(prefix=S3_IN_FOLDER)
    for feed in feeds:
        yield feed

def format_field(field, obj):
    if field == 'birthday' or field == 'updated':
        if obj['n']:
            return datetime.datetime.fromtimestamp(obj['n'])
        elif field == 'updated':
            return datetime.datetime.now()
        else:
            return None
    if obj.has_key('n'):
        return obj['n']
    elif obj.has_key('s'):
        return obj['s']
    elif obj.has_key('sS'):
        return str(obj['sS'])
    else:
        return ''


def format_user(user):
    return [format_field(field, user[field]) if field in user else '' for field in FIELDS]


def extract_user(line):
    return json.loads('{"' + line.replace('\x02', ',"').replace('\x03', '":') + '}')


def extract_file(inputkey, outputkey):
    output_file = StringIO.StringIO()
    paperbackwriter = unicodecsv.writer(
        output_file,
        encoding='utf-8',
        delimiter=',',
        quotechar='"',
        quoting=unicodecsv.QUOTE_NONNUMERIC
    )
    with tempfile.TemporaryFile() as fp:
        inputkey.get_contents_to_file(fp)
        fp.seek(0)
        for line in fp:
            paperbackwriter.writerow(format_user(extract_user(line)))
    output_file.seek(0)
    outputkey.set_contents_from_file(output_file)

if __name__ == '__main__':
    hand_s = logging.StreamHandler()
    hand_s.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(hand_s)

    conn_s3 = get_conn_s3()
    bucket = conn_s3.get_bucket(S3_BUCKET_NAME)
    for inputkey in s3_key_iter(bucket):
        keyleaf = inputkey.name.replace(S3_IN_FOLDER, '')
        outputkeyname = S3_OUT_FOLDER + keyleaf
        outputkey = Key(bucket=bucket, name=outputkeyname)
        print 'extracting to', outputkey
        extract_file(inputkey, outputkey)
    conn_s3.close()
