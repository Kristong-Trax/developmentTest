
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.CCMY.Utils.Fetcher import CCMYQueries
from Projects.CCMY.Utils.GeneralToolBox import CCMYGENERALToolBox

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


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


class CCMYConsts(object):

    STORE_TYPE = 'Store Type'
    KPI_NAME = 'KPI Name'
    KPI_GROUP = 'KPI Group'
    KPI_TYPE = 'KPI Type'
    TEMPLATE_NAME = 'Template Name'
    MANUFACTURER = 'Manufacturer'
    CUSTOM_SHEET = 'Custom Sheet'
    TARGET_MIN = 'Target Min'
    TARGET_MAX = 'Target Max'
    SCORE = 'Score'
    ONLY_IF_PASS = 'Save only if passed'

    PRODUCT_EAN = 'Product EAN'
    BRAND = 'Brand Name'
    SIZE = 'Size'
    ALL = 'All'

    SEPARATOR = ','

    AVAILABILITY = 'Availability'
    FACINGS_SOS = 'Facing SOS'


class CCMYToolBox:

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
        self.scene_info = self.data_provider[Data.SCENES_INFO].merge(
            self.data_provider[Data.ALL_TEMPLATES][['template_fk', 'template_name']], on='template_fk', how='left')
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.segmentation = self.store_info['additional_attribute_2'].iloc[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = CCMYGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.template_data = pd.read_excel(TEMPLATE_PATH, 'KPIs').fillna('')
        self.kpi_results_queries = []

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCMYQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        total_score = 0
        for group in self.template_data[CCMYConsts.KPI_GROUP].unique():
            kpi_data = self.template_data[self.template_data[CCMYConsts.KPI_GROUP] == group]
            kpi_type = kpi_data.iloc[0][CCMYConsts.KPI_TYPE]

            if not self.validate_kpi(kpi_data.iloc[0]):
                continue

            if kpi_type == CCMYConsts.AVAILABILITY:
                score = self.calculate_availability(kpi_data)
            elif kpi_type == CCMYConsts.FACINGS_SOS:
                score = self.calculate_facings_sos(kpi_data)
            else:
                continue

            if score is not None:
                total_score += score
                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == group].iloc[0]['kpi_fk']
                self.write_to_db_result(kpi_fk, score, level=self.LEVEL2)

        set_fk = self.kpi_static_data.iloc[0]['kpi_set_fk']
        self.write_to_db_result(set_fk, total_score, level=self.LEVEL1)

    def validate_kpi(self, kpi_data):
        validation = True
        store_types = kpi_data[CCMYConsts.STORE_TYPE]
        if store_types and self.store_type not in store_types.split(CCMYConsts.SEPARATOR):
            validation = False
        return validation

    def calculate_availability(self, kpi_data):
        group_score = 0
        group_name = kpi_data.iloc[0][CCMYConsts.KPI_GROUP]
        scene_types = self.get_scene_types(kpi_data.iloc[0])
        custom_sheet = kpi_data.iloc[0][CCMYConsts.CUSTOM_SHEET]

        if custom_sheet:
            sheet_data = pd.read_excel(TEMPLATE_PATH, custom_sheet, skiprows=1).fillna('')
            sheet_data = sheet_data[sheet_data[CCMYConsts.STORE_TYPE] == self.store_type]
            if sheet_data.empty:
                return
            for x, params in sheet_data.iterrows():
                weight = params[CCMYConsts.ALL]
                if not weight and self.segmentation in sheet_data.columns:
                    weight = params[self.segmentation]
                if not weight:
                    continue

                product_ean = params[CCMYConsts.PRODUCT_EAN]
                size = params[CCMYConsts.SIZE]
                if product_ean:
                    filters = dict(product_ean_code=product_ean)
                elif size:
                    filters = dict(brand_name=params[CCMYConsts.BRAND], size=float(params[CCMYConsts.SIZE]))
                else:
                    filters = dict(brand_name=params[CCMYConsts.BRAND])
                availability = self.tools.calculate_availability(template_name=scene_types, **filters)
                score = 100 if availability > 0 else 0
                result = weight if score != 0 else 0
                group_score += result
                atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == params[CCMYConsts.KPI_NAME]) &
                                                 (self.kpi_static_data['kpi_name'] == group_name)].iloc[0]['atomic_kpi_fk']
                self.write_to_db_result(atomic_fk, (score, result, 1), level=self.LEVEL3)

        else:
            for x, params in kpi_data.iterrows():
                availability = self.tools.calculate_availability(manufacturer_name=params[CCMYConsts.MANUFACTURER],
                                                                 template_name=scene_types)
                target_min = float(params[CCMYConsts.TARGET_MIN])
                target_max = float(1000 if not params[CCMYConsts.TARGET_MAX] else params[CCMYConsts.TARGET_MAX])
                score = 100 if target_min <= availability < target_max else 0
                result = float(params[CCMYConsts.SCORE]) if score != 0 else 0
                if not result and params[CCMYConsts.ONLY_IF_PASS]:
                    continue
                group_score += result
                atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == params[CCMYConsts.KPI_NAME]) &
                                                 (self.kpi_static_data['kpi_name'] == group_name)].iloc[0]['atomic_kpi_fk']
                self.write_to_db_result(atomic_fk, (score, result, target_min), level=self.LEVEL3)

        return group_score

    def calculate_facings_sos(self, kpi_data):
        group_score = 0
        group_name = kpi_data.iloc[0][CCMYConsts.KPI_GROUP]
        scene_types = self.get_scene_types(kpi_data.iloc[0])

        for x, params in kpi_data.iterrows():
            target_min = float(params[CCMYConsts.TARGET_MIN])
            target_max = float(1000 if not params[CCMYConsts.TARGET_MAX] else params[CCMYConsts.TARGET_MAX])

            sos_filters = dict(manufacturer_name=params[CCMYConsts.MANUFACTURER])
            facings_sos = self.tools.calculate_share_of_shelf(sos_filters, template_name=scene_types)
            score = 100 if target_min <= facings_sos <= target_max else 0
            result = float(params[CCMYConsts.SCORE]) if score != 0 else 0
            if not score and params[CCMYConsts.ONLY_IF_PASS]:
                    continue
            group_score += result
            atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == params[CCMYConsts.KPI_NAME]) &
                                             (self.kpi_static_data['kpi_name'] == group_name)].iloc[0]['atomic_kpi_fk']
            self.write_to_db_result(atomic_fk, (score, result, target_min), level=self.LEVEL3)

        return group_score

    def get_scene_types(self, kpi_data):
        scene_types = kpi_data[CCMYConsts.TEMPLATE_NAME]
        if scene_types:
            scene_types = scene_types.split(CCMYConsts.SEPARATOR)
        else:
            scene_types = self.scene_info['template_name'].unique().tolist()
        return scene_types

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
            if isinstance(score, float) and int(score) == score:
                score = int(score)
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        score, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            if isinstance(score, tuple):
                score, result, threshold = score
                if isinstance(result, float) and int(result) == result:
                    result = int(result)
            else:
                result = threshold = None
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, result, threshold, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'threshold', 'kpi_fk',
                                               'atomic_kpi_fk'])
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
        delete_queries = CCMYQueries.get_delete_session_results_query(self.session_uid)
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
