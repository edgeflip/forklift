import argparse
import logging
import time
from datetime import datetime
from sqlalchemy import func

from forklift import tasks
from forklift.db.base import RDSCacheSession
from forklift.loaders import neo_fbsync
from forklift.models.fbsync import FBSyncRunList, FBSyncPageTask, FBSyncRun
from forklift.models.magnus import FBAppUser, FBUserToken

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def always_be_crawling(efid, fbid, appid, run_id, stop_datetime):
    logger.info("Kicking off crawl for efid %d, starting at %s", efid, stop_datetime)
    for crawl_type, endpoint in neo_fbsync.ENDPOINTS.iteritems():
        if endpoint.entity_type == neo_fbsync.USER_ENTITY_TYPE:
            tasks.extract_url.delay(
                neo_fbsync.get_user_endpoint(fbid, endpoint.endpoint),
                run_id,
                efid,
                fbid,
                appid,
                crawl_type,
                stop_datetime=stop_datetime
            )


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    db_session = RDSCacheSession()
    new_tokens = db_session.query(FBAppUser.efid, FBAppUser.fbid, FBAppUser.fb_app_id).\
        filter(FBUserToken.app_user_id==FBAppUser.app_user_id).\
        filter(FBUserToken.expiration > datetime.today()).\
        all()

    version = int(time.time())
    run = FBSyncRun(run_id=version, load_started=False)
    db_session.add(run)

    for efid, fbid, app_id in new_tokens:
        runlist = FBSyncRunList(efid=efid, run_id=version)
        db_session.add(runlist)
        maybe_stop_datetime = db_session.query(func.min(FBSyncPageTask.extracted)).filter_by(efid=efid).scalar()
        if maybe_stop_datetime:
            stop_datetime = maybe_stop_datetime
            db_session.query(FBSyncPageTask).filter_by(efid=efid).update({
                FBSyncPageTask.extracted: None,
                FBSyncPageTask.transformed: None,
                FBSyncPageTask.loaded: None,
            })
        else:
            stop_datetime = datetime.min

        db_session.commit()
        always_be_crawling(efid, fbid, app_id, version, stop_datetime)
