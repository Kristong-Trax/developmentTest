import os
from datetime import datetime
import pandas as pd
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Files.FilesServices import create_folder

from Projects.RIPETCAREUK_PROD.Exceptions import AtomicKpiNotInStaticException, KpiNotInStaticException, \
    KpiSetNotInStaticException
from Projects.RIPETCAREUK_PROD.Utils.Fetcher import MarsUkQueries
from Projects.RIPETCAREUK_PROD.Utils.Utils import log_runtime, get_all_kpi_static_data

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class KpiResultsWriter(object):

    def __init__(self, session_uid, store_id, visit_date, rds_conn):
        self._kpi_static_data = get_all_kpi_static_data(rds_conn.db)
        self._kpi_results_queries = []
        self._session_uid = session_uid
        self._store_id = store_id
        self._visit_date = visit_date
        self._rds_conn = rds_conn

    def write_to_db_level_3_result(self, atomic_kpi_name, kpi_name, kpi_set_name, score, threshold, result):
        atomic_kpi_fk = self._get_atomic_fk(atomic_kpi_name=atomic_kpi_name, kpi_name=kpi_name,
                                            kpi_set_name=kpi_set_name)
        attributes = self._create_level_3_attributes_dict(atomic_kpi_fk, score, threshold, result)
        query = insert(attributes, KPI_RESULT)
        self._kpi_results_queries.append(query)

    def write_to_db_level_2_result(self, kpi_name, set_name, score):
        kpi_fk = self._get_kpi_fk(kpi_name, set_name)
        attributes = self._create_level_2_attributes_dict(fk=kpi_fk, score=score)
        query = insert(attributes, KPK_RESULT)
        self._kpi_results_queries.append(query)

    def write_to_db_level_1_result(self, set_name,score):
        set_fk = self._get_kpi_set_fk(set_name=set_name)
        attributes = self._create_level_1_attributes_dict(fk=set_fk, score=score)
        query = insert(attributes, KPS_RESULT)
        self._kpi_results_queries.append(query)

    def _create_level_1_attributes_dict(self, fk, score):
        kpi_set_name = self._kpi_static_data[self._kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
        attributes = pd.DataFrame([(kpi_set_name, self._session_uid, self._store_id, self._visit_date.isoformat(),
                                    format(score, '.1f'), fk)],
                                  columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                           'kpi_set_fk'])
        return attributes.to_dict()

    def _get_kpi_set_fk(self, set_name):
        cond = self._kpi_static_data['kpi_set_name'] == set_name
        kpi_set = self._kpi_static_data.loc[cond, 'kpi_set_fk']
        if kpi_set.empty:
            raise KpiSetNotInStaticException('set: {} does not exist in static'.format(set_name))
        return kpi_set.values[0]

    def _create_level_2_attributes_dict(self, fk, score):
        kpi_name = self._kpi_static_data[self._kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
        attributes = pd.DataFrame([(self._session_uid, self._store_id, self._visit_date.isoformat(),
                                    fk, kpi_name, score)],
                                  columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        return attributes.to_dict()

    def _get_kpi_fk(self, kpi_name, set_name):
        cond = (
            (self._kpi_static_data['kpi_name'] == kpi_name)
            &
            (self._kpi_static_data['kpi_set_name'] == set_name)
        )
        kpi = self._kpi_static_data.loc[cond, 'kpi_fk']
        if kpi.empty:
            raise KpiNotInStaticException('kpi: {} (set: {}) does not exist in static'.format(
                kpi_name, set_name
            ))
        return kpi.values[0]

    def _create_level_3_attributes_dict(self, fk,score, threshold=0, result=0):
        data = self._kpi_static_data[self._kpi_static_data['atomic_kpi_fk'] == fk]
        atomic_kpi_name = data['atomic_kpi_name'].values[0]
        kpi_fk = data['kpi_fk'].values[0]
        kpi_set_name = self._kpi_static_data[self._kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
        attributes = pd.DataFrame([(atomic_kpi_name, self._session_uid, kpi_set_name, self._store_id,
                                    self._visit_date.isoformat(), datetime.utcnow().isoformat(),
                                    score, kpi_fk, fk, threshold, result)],
                                  columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                           'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                           'result'])
        return attributes.to_dict()

    def _get_atomic_fk(self, atomic_kpi_name, kpi_name, kpi_set_name):
        """
        This function gets an Atomic KPI's FK out of the template data.
        """
        cond = (
            (self._kpi_static_data['kpi_set_name'] == kpi_set_name)
            &
            (self._kpi_static_data['kpi_name'] == kpi_name)
            &
            (self._kpi_static_data['atomic_kpi_name'] == atomic_kpi_name)
        )
        atomic_kpi = self._kpi_static_data.loc[cond, 'atomic_kpi_fk']
        if atomic_kpi.empty:
            raise AtomicKpiNotInStaticException('atomic_kpi: {} (kpi: {}, set: {}) does not exist in static'.format(
                atomic_kpi_name, kpi_name, kpi_set_name
            ))
        else:
            return atomic_kpi.values[0]

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self._rds_conn.db.cursor()
        delete_queries = MarsUkQueries.get_delete_session_results_query(self._session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self._kpi_results_queries:
            cur.execute(query)
        self._rds_conn.db.commit()


class KpiResultsWriterExcel(KpiResultsWriter):

    def __init__(self, session_uid, store_id, visit_date, rds_conn):
        super(KpiResultsWriterExcel, self).__init__(session_uid, store_id, visit_date, rds_conn)
        self._kpi_results = pd.DataFrame()
        self.path = '/home/{}/{}_results/'.format(os.getenv('USERNAME'), 'ripetcareuk')
        create_folder(self.path)

    def write_to_db_level_3_result(self, atomic_kpi_name, kpi_name, kpi_set_name, score, threshold, result):
        attributes = pd.DataFrame.from_records(
            [
                {
                    'kpi_set_name': kpi_set_name,
                    'atomic_kpi_name': atomic_kpi_name,
                    'kpi_name': kpi_name,
                    'score': score,
                    'threshold': threshold,
                    'result': result
                }
            ]
        )
        self._kpi_results = self._kpi_results.append(attributes)

    def write_to_db_level_2_result(self, kpi_name, set_name, score):
        attributes = pd.DataFrame.from_records(
            [
                {
                    'kpi_name': kpi_name,
                    'score': score,
                }
            ]
        )
        self._kpi_results = self._kpi_results.append(attributes)

    def write_to_db_level_1_result(self, set_name, score):
        attributes = pd.DataFrame.from_records(
            [
                {
                    'set_name': set_name,
                    'score': score,
                }
            ]
        )
        self._kpi_results = self._kpi_results.append(attributes)

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self._kpi_results.to_csv(self.path + '{}.csv'.format(self._session_uid))
