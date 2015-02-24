import argparse
import logging
import time
from datetime import datetime
from sqlalchemy import func

from forklift import tasks
from forklift.db.base import RDSCacheSession, RDSSourceSession
from forklift.loaders import neo_fbsync
from forklift.models.fbsync import FBSyncRunList, FBSyncPageTask, FBSyncRun
from forklift.models.raw import FBToken

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


def always_be_crawling(efid, appid, run_id, stop_datetime):
    logger.info("Kicking off crawl for efid %d, starting at %s", efid, stop_datetime)
    for crawl_type, endpoint in neo_fbsync.ENDPOINTS.iteritems():
        if endpoint.entity_type == neo_fbsync.USER_ENTITY_TYPE:
            tasks.extract_url.delay(
                neo_fbsync.get_user_endpoint(efid, endpoint.endpoint),
                run_id,
                efid,
                appid,
                crawl_type,
                stop_datetime=stop_datetime
            )


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    prod_session = RDSSourceSession()
    new_tokens = prod_session.query(FBToken).filter(
        FBToken.expiration > datetime.today()
    ).all()

    db_session = RDSCacheSession()
    version = int(time.time())
    run = FBSyncRun(run_id=version, load_started=False)
    db_session.add(run)

    for token in new_tokens:
        efid = token.efid
        appid = token.appid
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
        always_be_crawling(efid, appid, version, stop_datetime)
