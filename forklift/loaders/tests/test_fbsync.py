import datetime
from mock import patch
from forklift.loaders.fbsync import FeedFromS3, FeedPostFromJson
from forklift.testing import ForkliftTestCase


class FBSyncTestCase(ForkliftTestCase):
    primary = "12345"
    best_friend = "23456"
    well_wisher_one = "34567"
    well_wisher_two = "45678"
    post_sequence = "6"
    post_id = primary + "_" + post_sequence
    message = "Thank you to everyone for the amazing birthday wishes! They made an incredible day all the more special!!!"
    test_data = {"from": {"id": primary, "name": "Birthday girl"}, "privacy": {"deny": "", "description": "Friends", "value": "ALL_FRIENDS", "allow": "", "friends": "", "networks": ""}, "actions": [{"link": "https://www.facebook.com/{}/posts/{}".format(primary, post_sequence), "name": "Comment"}, {"link": "https://www.facebook.com/{}/posts/{}".format(primary, post_sequence), "name": "Like"}], "updated_time": "2014-04-20T14:48:57+0000", "application": {"namespace": "fbiphone", "name": "Facebook for iPhone", "id": "6628568379"}, "likes": {"paging": {"cursors": {"after": "MTM3ODY3Nzk4MQ==", "before": "MTAwMDAxNTM2ODMyMDU4"}}, "data": [{"id": well_wisher_one, "name": "Well-wisher"}, {"id": well_wisher_two, "name": "Well-wisher2"}]}, "created_time": "2014-04-19T13:22:16+0000", "message": message, "type": "status", "id": post_id, "status_type": "mobile_status_update", "comments": {"paging": {"cursors": {"after": "Mw==", "before": "MQ=="}}, "data": [{"from": {"id": best_friend, "name": "Vocal Well-wisher"}, "like_count": 0, "can_remove": True, "created_time": "2014-04-19T23:39:03+0000", "message": "Happy Birthday to youoooooooooooooo!!  I hope it was your best!!!!!!!!!!!", "id": "6_104288664", "user_likes": False}, {"from": {"id": primary, "name": "Birthday girl"}, "like_count": 0, "can_remove": True, "created_time": "2014-04-20T14:30:10+0000", "message": "Thanks, Pegs! Aries rule!! \ud83d\ude04", "id": "6_104290461", "user_likes": False}, {"from": {"id": best_friend, "name": "Vocal Well-wisher"}, "like_count": 0, "can_remove": True, "created_time": "2014-04-20T14:48:57+0000", "message": "Yes we do!", "id": "6_104290546", "user_likes": False}]}}

    def __init__(self, *args, **kwargs):
        super(FBSyncTestCase, self).__init__(*args, **kwargs)
        self.post = FeedPostFromJson(self.test_data)
        self.posts = [self.post]

    def test_FeedPostFromJson(self):
        self.assertEquals(len(self.post.commenters), 2)
        self.assertEquals(len(self.post.comments), 3)
        self.assertEquals(len(self.post.like_ids), 2)
        self.assertFalse(hasattr(self.post, 'to_ids'))
        self.assertEquals(self.post.post_id, self.post_id)
        self.assertEquals(self.post.post_from, self.primary)
        self.assertEquals(self.post.post_message, self.message)

    @patch('forklift.loaders.fbsync.FeedFromS3.__init__')
    def test_FeedFromS3_post_lines(self, feed_constructor_patch):
        feed_constructor_patch.return_value = None
        feed = FeedFromS3('test')
        feed.posts = self.posts
        feed.user_id = self.primary
        lines = feed.get_post_lines()
        self.assertEquals(len(lines), 1)
        fields = lines[0].split("\t")
        self.assertEqual(fields,
            [
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
            ]
        )


    @patch('forklift.loaders.fbsync.FeedFromS3.__init__')
    def test_FeedFromS3_link_lines(self, feed_constructor_patch):
        feed_constructor_patch.return_value = None
        feed = FeedFromS3('test')
        feed.posts = self.posts
        feed.user_id = self.primary
        lines = feed.get_link_lines()
        for line in lines:
            (post_id, user_id, poster_id, has_to, has_like, has_comm, num_comm, _) = line.split("\t")
            self.assertEquals(post_id, self.post_id)
            self.assertEquals(poster_id, self.primary)
            if user_id == self.primary:
                self.assertEquals(has_to, '')
                self.assertEquals(has_like, '')
                self.assertEquals(has_comm, '1')
                self.assertEquals(num_comm, '1')
            elif user_id == self.best_friend:
                self.assertEquals(has_to, '')
                self.assertEquals(has_like, '')
                self.assertEquals(has_comm, '1')
                self.assertEquals(num_comm, '2')
            else:
                self.assertEquals(has_to, '')
                self.assertEquals(has_like, '1')
                self.assertEquals(has_comm, '')
                self.assertEquals(num_comm, '0')

        self.assertEquals(len(lines), 4)

    @patch('forklift.loaders.fbsync.FeedFromS3.__init__')
    def test_FeedFromS3_like_lines(self, feed_constructor_patch):
        feed_constructor_patch.return_value = None
        feed = FeedFromS3('test')
        feed.posts = self.posts
        feed.user_id = self.primary
        lines = feed.get_like_lines()
        self.assertEqual(lines, ())

        feed.page_likes = (1, 5)
        lines = list(feed.get_like_lines())
        self.assertEquals(
            lines,
            [
                "{}\t1".format(self.primary),
                "{}\t5".format(self.primary)
            ]
        )

