
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.SOLARBR_SAND.Utils.Fetcher import SOLARBRQueries
from Projects.SOLARBR_SAND.Utils.GeneralToolBox import SOLARBRGENERALToolBox
from KPIUtils.DB.Common import Common

__author__ = 'Ilan'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
EMPTY = 'Empty'


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


class SOLARBRToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = SOLARBRGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.common = Common(data_provider)

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = SOLARBRQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    # def main_calculation(self, kpi, scene_type, score):
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        total_scores = {}
        for index, scene in self.scif.iterrows():
            score_types = {
                'Total_Length_Empty': 0,
                'Total_Length_CC': 0,
                'Total_Length_Competitor': 0
            }
            scene = scene.fillna(0)
            scene_type = scene['template_name']
            if scene_type not in total_scores:
                total_scores[scene_type] = score_types
            if scene['product_type'] == EMPTY:
                total_scores[scene_type]['Total_Length_Empty'] += scene['gross_len_ign_stack']
            elif scene['manufacturer_fk'] == 1:
                total_scores[scene_type]['Total_Length_CC'] += scene['gross_len_ign_stack']
            else:
                total_scores[scene_type]['Total_Length_Competitor'] += scene['gross_len_ign_stack']
        set_kpi_scores = {
            'Total_Length_Empty': 0,
            'Total_Length_CC': 0,
            'Total_Length_Competitor': 0
        }
        for scene_type in total_scores:
            scores = total_scores[scene_type]
            for kpi in scores:
                atomic_kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi][
                    'atomic_kpi_fk'].values[0]

                self.write_to_db_result(atomic_kpi_fk,
                                        scores[kpi],
                                        self.common.LEVEL3,
                                        display_text='KPI_Type-' + kpi + '-Scene_Type-' + scene_type)
                set_kpi_scores[kpi] += scores[kpi]
        for set_kpi in set_kpi_scores:
            kpi_set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_kpi][
                'kpi_set_fk'].values[0]
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == set_kpi][
                'kpi_fk'].values[0]
            self.write_to_db_result(kpi_fk,
                                    set_kpi_scores[set_kpi],
                                    self.common.LEVEL2,
                                    display_text='')
            self.write_to_db_result(kpi_set_fk,
                                    set_kpi_scores[set_kpi],
                                    self.common.LEVEL1,
                                    display_text='')

    def write_to_db_result(self, fk, score, level, display_text):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level, display_text)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level, display_text):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            # atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(display_text, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = SOLARBRQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries
