import logging
from mock import patch
from forklift.loaders import fbsync as fbsync
from forklift.testing import ForkliftTransactionalTestCase
from sklearn.feature_extraction.text import TfidfVectorizer

logger = logging.getLogger(__name__)


class FBSyncTestCase(ForkliftTransactionalTestCase):
    primary = "12345"
    best_friend = "23456"
    well_wisher_one = "34567"
    well_wisher_two = "45678"
    post_sequence = "6"
    post_id = primary + "_" + post_sequence
    message = "Thank you to everyone for the amazing birthday wishes! They made an incredible day all the more special!!!"
    response_one = u"Happy Birthday to youoooooooooooooo!! \xc2\xa1I hope it was your best!!!!!!!!!!!"
    response_two = u"Thanks, Pegs! Aries rule!! \ud83d\ude04"
    response_three = u"Yes we do!"
    test_data = {"from": {"id": primary, "name": "Birthday girl"}, "privacy": {"deny": "", "description": "Friends", "value": "ALL_FRIENDS", "allow": "", "friends": "", "networks": ""}, "actions": [{"link": "https://www.facebook.com/{}/posts/{}".format(primary, post_sequence), "name": "Comment"}, {"link": "https://www.facebook.com/{}/posts/{}".format(primary, post_sequence), "name": "Like"}], "updated_time": "2014-04-20T14:48:57+0000", "application": {"namespace": "fbiphone", "name": "Facebook for iPhone", "id": "6628568379"}, "likes": {"paging": {"cursors": {"after": "MTM3ODY3Nzk4MQ==", "before": "MTAwMDAxNTM2ODMyMDU4"}}, "data": [{"id": well_wisher_one, "name": "Well-wisher"}, {"id": well_wisher_two, "name": "Well-wisher2"}]}, "created_time": "2014-04-19T13:22:16+0000", "message": message, "type": "status", "id": post_id, "status_type": "mobile_status_update", "comments": {"paging": {"cursors": {"after": "Mw==", "before": "MQ=="}}, "data": [{"from": {"id": best_friend, "name": "Vocal Well-wisher"}, "like_count": 0, "can_remove": True, "created_time": "2014-04-19T23:39:03+0000", "message": response_one, "id": "6_104288664", "user_likes": False}, {"from": {"id": primary, "name": "Birthday girl"}, "like_count": 0, "can_remove": True, "created_time": "2014-04-20T14:30:10+0000", "message": response_two, "id": "6_104290461", "user_likes": False}, {"from": {"id": best_friend, "name": "Vocal Well-wisher"}, "like_count": 0, "can_remove": True, "created_time": "2014-04-20T14:48:57+0000", "message": response_three, "id": "6_104290546", "user_likes": False}]}}

    def __init__(self, *args, **kwargs):
        super(FBSyncTestCase, self).__init__(*args, **kwargs)
        self.post = fbsync.FeedPostFromJson(self.test_data)
        self.posts = [self.post]
        with patch.object(fbsync.FeedFromS3, '__init__', return_value=None) as mock_method:
            self.feed = fbsync.FeedFromS3('test')
            self.feed.initialize()
            self.feed.posts = self.posts
            self.feed.user_id = self.primary

    def test_FeedPostFromJson(self):
        self.assertEquals(len(self.post.commenters), 2)
        self.assertEquals(len(self.post.comments), 3)
        self.assertEquals(len(self.post.like_ids), 2)
        self.assertFalse(hasattr(self.post, 'to_ids'))
        self.assertEquals(self.post.post_id, self.post_id)
        self.assertEquals(self.post.post_from, self.primary)
        self.assertEquals(self.post.post_message, self.message)

    def test_FeedFromS3_post_lines(self):
        lines = list(self.feed.post_lines(fbsync.DEFAULT_DELIMITER))
        self.assertEquals(len(lines), 1)
        fields = lines[0]
        self.assertEqual(fields,
            (
                self.primary,
                self.post_id,
                '2014-04-20 14:48:57',
                'status',
                '6628568379',
                self.primary,
                "", # link
                "", # link domain
                "", # story
                "", # description
                "", # caption
                self.message,
                '2', # num likes
                '3', # num comments
                '0', # num 'to's
                '2', # num commenters
            )
        )


    def test_FeedFromS3_link_lines(self):
        fbsync.DB_TEXT_LEN = 78 # Just enough to cut off the last comment
        lines = self.feed.link_lines(fbsync.DEFAULT_DELIMITER)
        for line in lines:
            (post_id, user_id, poster_id, has_to, has_like, has_comm, num_comm, comment_text) = line
            self.assertEquals(post_id, self.post_id)
            self.assertEquals(poster_id, self.primary)
            if user_id == self.primary:
                self.assertEquals(has_to, '')
                self.assertEquals(has_like, '')
                self.assertEquals(has_comm, '1')
                self.assertEquals(num_comm, '1')
                self.assertEquals(comment_text, self.response_two.encode('utf-8'))
            elif user_id == self.best_friend:
                self.assertEquals(has_to, '')
                self.assertEquals(has_like, '')
                self.assertEquals(has_comm, '1')
                self.assertEquals(num_comm, '2')
                self.assertEquals(comment_text, self.response_one.encode('utf-8'))
            else:
                self.assertEquals(has_to, '')
                self.assertEquals(has_like, '1')
                self.assertEquals(has_comm, '')
                self.assertEquals(num_comm, '0')

        self.assertEquals(len(lines), 4)

    def test_FeedFromS3_like_lines(self):
        lines = self.feed.like_lines(fbsync.DEFAULT_DELIMITER)
        self.assertEqual(lines, ())

        self.feed.page_likes = (1, 5)
        lines = list(self.feed.like_lines(fbsync.DEFAULT_DELIMITER))
        x = self.feed.like_lines(fbsync.DEFAULT_DELIMITER)
        print x
        lines = list(x)
        print lines
        self.assertEquals(
            lines,
            [
                (self.primary, "1"),
                (self.primary, "5"),
            ]
        )

    def test_FeedFromS3_post_corpus(self):
        corpus = self.feed.post_corpus

        self.assertEqual(corpus, self.message + " ")

    def test_FeedFromS3_top_word_lines(self):
        vectorizer = TfidfVectorizer(input='content')

        # The only word which shows up more than once in our sample message is 'the'.
        vectorizer.fit([self.message])
        lines = self.feed.top_word_lines(fbsync.DEFAULT_DELIMITER, vectorizer, k=1)
        user_id, word = lines[0]
        self.assertEquals(user_id, self.primary)
        self.assertEquals(word, "the")

        # To prove that the idf weighting works, train the vectorizer such that it
        # knows 'the' is a very popular word and force it to re-weight
        vectorizer.fit([self.message, "the", "the", "the", "the", "the"])
        lines = self.feed.top_word_lines(fbsync.DEFAULT_DELIMITER, vectorizer, k=1)
        _, word = lines[0]
        self.assertNotEquals(word, "the")

    def test_FeedChunk_merge_feed(self):
        vectorizer = TfidfVectorizer(input='content')
        vectorizer.fit([self.message])
        chunk = fbsync.FeedChunk(vectorizer, logger)
        chunk.merge_feed(self.feed)
        self.assertEquals(
            chunk.counts,
            {
                fbsync.POSTS: 1,
                fbsync.LINKS: 4,
                fbsync.LIKES: 0,
                fbsync.TOP_WORDS: 1,
            }
        )
        for entity in fbsync.ENTITIES:
            self.assertTrue(chunk.strings[entity])
