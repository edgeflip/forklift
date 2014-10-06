from forklift.settings import REDSHIFT_URL, RDS_SOURCE_URL, RDS_CACHE_URL
from sqlalchemy import create_engine

redshift_engine = create_engine(REDSHIFT_URL)
rds_source_engine = create_engine(RDS_SOURCE_URL)
rds_cache_engine = create_engine(RDS_CACHE_URL)
