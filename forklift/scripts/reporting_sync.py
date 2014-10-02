from forklift.db.base import rds_source_engine, redshift_engine, cache_engine
from forklift.loaders.reporting import process

if __name__ == '__main__':
    process(rds_source_engine, redshift_engine, cache_engine)
