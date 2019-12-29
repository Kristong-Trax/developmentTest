from datetime import datetime

from Trax.Utils.Logging.Logger import Log
import pandas as pd
from Projects.INTEG21.Utils.Fetcher import MarsUsQueries


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


def strip_df(func):
    def wrapper(*args, **kwargs):
        data = func(*args, **kwargs)
        if data is None:
            return data
        data = data.fillna('')
        columns = data.columns
        for column in columns:
            if 'Unnamed: ' in column:
                del data[column]
            else:
                data[column] = data[column].apply(lambda x: x if not isinstance(x, (str, unicode)) else x.strip())
        return data
    return wrapper


def get_all_kpi_static_data(rds_conn):
    """
    This function extracts the static KPI data and saves it into one global data frame.
    The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
    """
    query = MarsUsQueries.get_all_kpi_data()
    kpi_static_data = pd.read_sql_query(query, rds_conn)
    return kpi_static_data
