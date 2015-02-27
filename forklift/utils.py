from forklift.models.magnus import FBAppUser, EFUser
from forklift.db.base import RDSCacheSession
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError
from datetime import datetime


def batcher(generator, batch_size):
    num_in_batch = 0
    batch = []
    for item in generator:
        batch.append(item)
        num_in_batch += 1
        if num_in_batch >= batch_size:
            num_in_batch = 0
            yield batch
            batch = []
    if num_in_batch > 0:
        yield batch



def get_or_create_efid(asid, appid, name=None):
    session = RDSCacheSession()
    now = datetime.today()
    try:
        efid = session.query(FBAppUser).filter_by(fbid=asid, fb_app_id=appid).one().efid
        session.close()
        return efid
    except NoResultFound:
        user = EFUser(name=name, created=now, updated=now)
        appuser = FBAppUser(fbid=asid, fb_app_id=appid, user=user, created=now, updated=now)
        try:
            session.add(user)
            session.add(appuser)
            session.commit()
            efid = user.efid
            session.close()
            return efid
        except IntegrityError:
            session.rollback()
            efid = session.query(FBAppUser).filter_by(fbid=asid, fb_app_id=appid).one().efid
            session.close()
            return efid
