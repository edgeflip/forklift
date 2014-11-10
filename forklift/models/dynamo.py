from faraday import (
    Item,
    ItemField,
    ItemLinkField,
    NUMBER,
    DATETIME,
    STRING_SET,
    JSON,
    BOOL,
    types,
    HashKeyField,
    RangeKeyField
)


class IncomingEdge(Item):
    fbid_target = HashKeyField(data_type=NUMBER)
    fbid_source = RangeKeyField(data_type=NUMBER)
    post_likes = ItemField(data_type=NUMBER)
    post_comms = ItemField(data_type=NUMBER)
    stat_likes = ItemField(data_type=NUMBER)
    stat_comms = ItemField(data_type=NUMBER)
    wall_posts = ItemField(data_type=NUMBER)
    wall_comms = ItemField(data_type=NUMBER)
    tags = ItemField(data_type=NUMBER)
    photos_target = ItemField(data_type=NUMBER)
    photos_other = ItemField(data_type=NUMBER)
    mut_friends = ItemField(data_type=NUMBER)


class User(Item):
    fbid = HashKeyField(data_type=NUMBER)
    birthday = ItemField(data_type=DATETIME)
    fname = ItemField()
    lname = ItemField()
    email = ItemField()
    gender = ItemField()
    city = ItemField()
    state = ItemField()
    country = ItemField()
    activities = ItemField(data_type=STRING_SET)
    affiliations = ItemField(data_type=JSON)
    books = ItemField(data_type=STRING_SET)
    devices = ItemField(data_type=JSON)
    friend_request_count = ItemField(data_type=NUMBER)
    has_timeline = ItemField(data_type=BOOL)
    interests = ItemField(data_type=STRING_SET)
    languages = ItemField(data_type=JSON)
    likes_count = ItemField(data_type=NUMBER)
    movies = ItemField(data_type=STRING_SET)
    music = ItemField(data_type=STRING_SET)
    political = ItemField(data_type=STRING_SET)
    profile_update_time = ItemField(data_type=DATETIME)
    quotes = ItemField(data_type=STRING_SET(types.DOUBLE_NEWLINE))
    relationship_status = ItemField()
    religion = ItemField()
    sports = ItemField(data_type=JSON)
    tv = ItemField(data_type=STRING_SET)
    wall_count = ItemField(data_type=NUMBER)


class Token(Item):
    fbid = HashKeyField(data_type=NUMBER)
    appid = RangeKeyField(data_type=NUMBER)
    expires = ItemField(data_type=DATETIME)
    token = ItemField()

    user = ItemLinkField('User', db_key=fbid)
