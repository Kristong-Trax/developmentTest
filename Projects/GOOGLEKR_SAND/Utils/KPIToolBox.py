from datetime import datetime
import json
import os
import numpy as np
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
# from KPIUtils.GlobalProjects.HEINZ.Utils.Fetcher import HEINZQueries
# from KPIUtils.GlobalProjects.HEINZ.Utils.GeneralToolBox import HEINZGENERALToolBox

from Projects.GOOGLEKR_SAND.Utils.Fetcher import GOOGLEQueries
from Projects.GOOGLEKR_SAND.Utils.GeneralToolBox import GOOGLEGENERALToolBox

__author__ = 'Eli'


FIXTURE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data', 'KR - Google Fixture Targets test.csv')
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


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


class GOOGLEToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    LVL3_HEADERS = ['assortment_group_fk', 'assortment_fk', 'target', 'product_fk',
                    'in_store', 'kpi_fk_lvl1', 'kpi_fk_lvl2', 'kpi_fk_lvl3', 'group_target_date',
                    'assortment_super_group_fk']
    LVL2_HEADERS = ['assortment_group_fk', 'assortment_fk', 'target', 'passes', 'total',
                    'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date']
    LVL1_HEADERS = ['assortment_group_fk', 'target', 'passes', 'total', 'kpi_fk_lvl1']
    ASSORTMENT_FK = 'assortment_fk'
    ASSORTMENT_GROUP_FK = 'assortment_group_fk'
    ASSORTMENT_SUPER_GROUP_FK = 'assortment_super_group_fk'
    BRAND_VARIENT = 'brand_varient'
    NUMERATOR = 'numerator'
    DENOMINATOR = 'denominator'
    DISTRIBUTION_KPI = 'Distribution - SKU'
    OOS_SKU_KPI = 'OOS - SKU'
    OOS_KPI = 'OOS'

    FACINGS = 'facings'
    BRAND = 'brand_name'
    NOTABRAND = {'General', 'General.'}
    EXCLUDE_FILTERS = {
                        'product_type': ['Irelevant', 'Empty']
                        }
    SOS_KPIs = {
                'SOS BRAND out of SCENE': {'pk': 300000,
                                           'den': None},
                'SOS BRAND out of BRANDS in SCENE': {'pk': 300001,
                                                     'den': None}
                }
    FIXTURE_KPIs = {
                    'FIXTURE COMPLIANCE': {'pk': 300002}
                    }


    def __init__(self, data_provider, output, common_v2):
        self.common_v2 = common_v2
        # self.New_kpi_static_data = common.get_new_kpi_static_data()
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = GOOGLEGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.common_v2.get_kpi_static_data()
        self.kpi_results_queries = []
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.kpi_results_new_tables_queries = []
        self.all_products = self.ps_data_provider.get_sub_category(self.all_products)
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.fixture_template = pd.read_csv(FIXTURE_TEMPLATE_PATH)
        self.current_date = datetime.now()


    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = GOOGLEQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        return score

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
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

    def create_attributes_dict(self, fk, score, level):
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
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
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
        delete_queries = GOOGLEQueries.get_delete_session_results_query(self.session_uid)
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

    def filter_df(self, df, exclude=0):
        for filter in self.EXCLUDE_FILTERS:
            if exclude:
                df = df[df['product_type'].isin(self.EXCLUDE_FILTERS[filter])]
            else:
                df = df[~df['product_type'].isin(self.EXCLUDE_FILTERS[filter])]
        return df

    def division(self, num, den):
        if den:
            ratio = float(num) / den
        else:
            ratio = 0
        return ratio

    def google_global_SOS(self):
        scif = self.filter_df(self.scif.copy())
        brands_scif = scif[~scif[self.BRAND].isin(self.NOTABRAND)]
        if brands_scif.empty:
            return

        self.SOS_KPIs['SOS BRAND out of SCENE']['den'] = scif[self.FACINGS].sum()
        self.SOS_KPIs['SOS BRAND out of BRANDS in SCENE']['den'] = brands_scif[self.FACINGS].sum()
        brand_totals = brands_scif.set_index(['brand_fk', self.BRAND])\
                                  .groupby([self.BRAND, 'brand_fk'])[self.FACINGS]\
                                  .sum()

        for (brand_name, brand_fk), numerator in brand_totals.iteritems():
            for kpi in self.SOS_KPIs:
                ratio = self.division(numerator, self.SOS_KPIs[kpi]['den'])

                self.common_v2.write_to_db_result(fk=self.SOS_KPIs[kpi]['pk'],
                                                  numerator_id=brand_fk,
                                                  numerator_result=numerator,
                                                  denominator_id=self.common_v2.scene_id,
                                                  denominator_result=self.SOS_KPIs[kpi]['den'],
                                                  result=ratio*100,
                                                  by_scene=True)

    def google_global_fixture_compliance(self):
        store_num = self.store_info['store_number_1'][0]
        relevant_fixtures = self.fixture_template[self.fixture_template['Store Number'] == store_num]
        relevant_fixtures = relevant_fixtures.set_index('New Task Name (unique)')\
                                             .groupby('New Task Name (unique)')\
                                             ['Number of Fixtures(Task)'].sum()

        for fixture, denominator in relevant_fixtures.iteritems():
            fixture_pk = self.templates.set_index(['template_name']).loc[fixture, 'template_fk']
            numerator = self.scene_info[self.scene_info['template_fk'] == fixture_pk].shape[0]
            ratio = self.division(numerator, denominator)
            score = 0
            if ratio >= 1:
                ratio = 1
                score = 1

            self.common_v2.write_to_db_result(fk=self.FIXTURE_KPIs['FIXTURE COMPLIANCE']['pk'],
                                              numerator_id=fixture_pk,
                                              numerator_result=numerator,
                                              denominator_id=fixture_pk,
                                              denominator_result=denominator,
                                              score=score,
                                              result=ratio*100)

    def google_global_survey(self):
        'No Mock Survey Data Yet'
        pass

