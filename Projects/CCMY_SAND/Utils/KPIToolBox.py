import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.DB.CommonV2 import Common

from Projects.CCMY_SAND.Utils.Fetcher import CCMY_SANDQueries
from Projects.CCMY_SAND.Utils.GeneralToolBox import CCMY_SANDGENERALToolBox

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


class CCMY_SANDConsts(object):
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

    CCBM = 2  # CCBM
    GENERAL_MANUFACTURER = 0
    OTHER_MANUFACTURER = 12
    GENERAL_EMPTY_PRODUCT = 0
    IRRELEVANT = 184

    AVAILABILITY = 'Availability'
    FACINGS_SOS = 'Facing SOS'
    SHELF_PURITY = 'Shelf Purity'

    MANUFACTURER_FK = 'manufacturer_fk'
    SCENE_FK = 'scene_fk'
    BAY_NUMBER = 'bay_number'
    SHELF_NUMBER = 'shelf_number'
    PRODUCT_FK = 'product_fk'
    ATOMIC_KPI_NAME = 'atomic_kpi_name'
    ATOMIC_KPI_FK = 'atomic_kpi_fk'
    TEMPLATE_FK = 'template_fk'
    KPI_NUM_PURE_SHELVES = 'CCRM Cooler Number of Pure Shelves'
    KPI_TOTAL_NUM_OF_SHELVES = 'CCRM Cooler Total Number of Shelves'
    IS_PURE = 'is_pure'
    PURE = 1
    IMPURE = 0


class CCMY_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.common = Common(data_provider)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.manufacturer_fk = None if self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0] is None else \
            int(self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0])
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
        self.tools = CCMY_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_static_data_new = self.common.get_new_kpi_static_data()
        self.template_data = pd.read_excel(TEMPLATE_PATH, 'KPIs').fillna('')
        self.kpi_results_queries = []

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCMY_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        total_score = 0
        score, numerator_id = 0, None
        identifier_parent = self.common.get_dictionary(kpi_name='Red Score')

        for group in self.template_data[CCMY_SANDConsts.KPI_GROUP].unique():
            kpi_data = self.template_data[self.template_data[CCMY_SANDConsts.KPI_GROUP] == group]

            if kpi_data.empty:
                continue

            kpi_type = kpi_data.iloc[0][CCMY_SANDConsts.KPI_TYPE]

            if not self.validate_kpi(kpi_data.iloc[0]):
                continue

            if kpi_type == CCMY_SANDConsts.AVAILABILITY:
                score = self.calculate_availability(kpi_data)
                numerator = score
            elif kpi_type == CCMY_SANDConsts.FACINGS_SOS:
                score = self.calculate_facings_sos(kpi_data)
                numerator = score
            elif kpi_type == CCMY_SANDConsts.SHELF_PURITY:
                numerator, denominator, score, templates = self.calculate_self_purity(kpi_data)
                numerator_id = templates

            else:
                continue

            if score is not None:
                total_score += score
                kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == group].iloc[0]['kpi_fk']
                self.write_to_db_result(kpi_fk, score, level=self.LEVEL2)
                identifier_result = self.common.get_dictionary(kpi_name=group)
                # insert db results to new tables
                self.insert_db_new_results(kpi_data.iloc[0][CCMY_SANDConsts.KPI_GROUP], score, numerator, score, 1,
                                           identifier_result=identifier_result, identifier_parent=identifier_parent,
                                           numerator_id=numerator_id)

        if self.kpi_static_data.empty:
            return
        else:
            set_fk = self.kpi_static_data.iloc[0]['kpi_set_fk']
            self.write_to_db_result(set_fk, total_score, level=self.LEVEL1)
            self.insert_db_new_results('Red Score', total_score, total_score, total_score, 1,
                                       identifier_result=identifier_parent)

        self.common.commit_results_data()

    def validate_kpi(self, kpi_data):
        validation = True
        store_types = kpi_data[CCMY_SANDConsts.STORE_TYPE]
        if store_types and self.store_type not in store_types.split(CCMY_SANDConsts.SEPARATOR):
            # if list of stores in template  isnt empty and also this store type in it
            validation = False
        return validation

    def calculate_availability(self, kpi_data):
        group_score = 0
        group_name = kpi_data.iloc[0][CCMY_SANDConsts.KPI_GROUP]
        identifier_parent = self.common.get_dictionary(kpi_name=group_name)
        scene_types = self.get_scene_types(kpi_data.iloc[0])
        custom_sheet = kpi_data.iloc[0][CCMY_SANDConsts.CUSTOM_SHEET]

        if custom_sheet:
            sheet_data = pd.read_excel(TEMPLATE_PATH, custom_sheet, skiprows=1).fillna('')
            sheet_data = sheet_data[sheet_data[CCMY_SANDConsts.STORE_TYPE] == self.store_type]
            if sheet_data.empty:
                return
            for x, params in sheet_data.iterrows():
                weight = params[CCMY_SANDConsts.ALL]
                if not weight and self.segmentation in sheet_data.columns:
                    weight = params[self.segmentation]
                if not weight:
                    continue

                product_ean = params[CCMY_SANDConsts.PRODUCT_EAN]
                size = params[CCMY_SANDConsts.SIZE]
                if product_ean:
                    filters = dict(product_ean_code=product_ean)
                elif size:
                    filters = dict(brand_name=params[CCMY_SANDConsts.BRAND], size=float(params[CCMY_SANDConsts.SIZE]))
                else:
                    filters = dict(brand_name=params[CCMY_SANDConsts.BRAND])
                availability = self.tools.calculate_availability(template_name=scene_types, **filters)
                score = 100 if availability > 0 else 0
                result = weight if score != 0 else 0
                group_score += result
                atomic_fk = \
                self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == params[CCMY_SANDConsts.KPI_NAME]) &
                                     (self.kpi_static_data['kpi_name'] == group_name)].iloc[0]['atomic_kpi_fk']

                self.write_to_db_result(atomic_fk, (score, result, 1), level=self.LEVEL3)

                # writing results to new tables
                self.insert_db_new_results(params['KPI Name'], result, score, score, 1,
                                           identifier_parent=identifier_parent)

        else:
            for x, params in kpi_data.iterrows():
                availability = self.tools.calculate_availability(manufacturer_name=params[CCMY_SANDConsts.MANUFACTURER],
                                                                 template_name=scene_types)
                target_min = float(params[CCMY_SANDConsts.TARGET_MIN])
                target_max = float(
                    1000 if not params[CCMY_SANDConsts.TARGET_MAX] else params[CCMY_SANDConsts.TARGET_MAX])
                score = 100 if target_min <= availability < target_max else 0
                result = float(params[CCMY_SANDConsts.SCORE]) if score != 0 else 0
                if not result and params[CCMY_SANDConsts.ONLY_IF_PASS]:
                    continue
                group_score += result
                atomic_fk = \
                self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == params[CCMY_SANDConsts.KPI_NAME]) &
                                     (self.kpi_static_data['kpi_name'] == group_name)].iloc[0]['atomic_kpi_fk']
                self.write_to_db_result(atomic_fk, (score, result, target_min), level=self.LEVEL3)

                # writing results to new tables
                self.insert_db_new_results(params['KPI Name'], result, score, score, 1, target_min, identifier_parent)

        return group_score

    def get_kpi_fk_new_table(self, kpi_name):
        kpi_level_2_fk = \
            self.kpi_static_data_new[self.kpi_static_data_new['type'] == kpi_name]
        if not kpi_level_2_fk.empty:
            return kpi_level_2_fk['pk'].iloc[0]
        return None

    def insert_db_new_results(self, kpi_name, result, score, numerator_result, denominator_result,
                              identifier_parent=None, identifier_result=None, target=None, numerator_id=None):

        kpi_level_2_fk = self.get_kpi_fk_new_table(kpi_name)
        if kpi_level_2_fk is None:
            Log.warning("kpi {} from template, doesn't exist in DB".format(kpi_name))
            return
        numerator_id = self.manufacturer_fk if numerator_id is None else numerator_id
        self.common.write_to_db_result(fk=kpi_level_2_fk,
                                       numerator_id=numerator_id,
                                       denominator_id=self.store_id,
                                       numerator_result=numerator_result,
                                       denominator_result=denominator_result,
                                       identifier_parent=identifier_parent,
                                       identifier_result=identifier_result,
                                       result=result,
                                       score=score,
                                       target=target,
                                       should_enter=True)

    def calculate_self_purity(self, kpi_data):
        score = 0
        if kpi_data.empty:
            return

        group_name = kpi_data.iloc[0][CCMY_SANDConsts.KPI_GROUP]
        identifier_parent = self.common.get_dictionary(kpi_name=group_name)
        scene_types = self.get_scene_types(kpi_data.iloc[0])
        self_purity_scene_list = self.scene_info[self.scene_info['template_name'].isin(scene_types)][
            CCMY_SANDConsts.SCENE_FK].unique().tolist()
        template_fk = self.scene_info[self.scene_info['template_name'].isin(scene_types)][
            CCMY_SANDConsts.TEMPLATE_FK].unique().tolist()
        template_fk = template_fk if template_fk != [] else \
            self.data_provider.all_templates[self.data_provider.all_templates['template_name'].isin(
                scene_types)]['template_fk'].iloc[0]

        df_all_shelfs = self.match_product_in_scene

        if self.match_product_in_scene.empty:
            return 0, 0, 0, template_fk

        df_all_shelfs_products = df_all_shelfs.merge(self.products, how='inner', on=CCMY_SANDConsts.PRODUCT_FK)
        list_columns = [CCMY_SANDConsts.SCENE_FK, CCMY_SANDConsts.BAY_NUMBER, CCMY_SANDConsts.SHELF_NUMBER,
                        CCMY_SANDConsts.MANUFACTURER_FK, CCMY_SANDConsts.PRODUCT_FK]

        df_all_shelfs_products = pd.DataFrame(
            df_all_shelfs_products.groupby(list_columns).size().reset_index(name='count'))

        df_all_shelfs_products = df_all_shelfs_products[
            df_all_shelfs_products[CCMY_SANDConsts.SCENE_FK].isin(self_purity_scene_list)]
        df_shelf_pure = df_all_shelfs_products[
            [CCMY_SANDConsts.SCENE_FK, CCMY_SANDConsts.BAY_NUMBER, CCMY_SANDConsts.SHELF_NUMBER]]
        df_shelf_pure.drop_duplicates(subset=None, keep='first', inplace=True)
        df_shelf_pure[CCMY_SANDConsts.IS_PURE] = CCMY_SANDConsts.PURE

        for x, params in kpi_data.iterrows():
            for row_num_x, row_data_x in df_shelf_pure.iterrows():
                for row_num_y, row_data_y in df_all_shelfs_products.iterrows():
                    if ((row_data_x[CCMY_SANDConsts.SCENE_FK] == row_data_y[CCMY_SANDConsts.SCENE_FK]) &
                            (row_data_x[CCMY_SANDConsts.BAY_NUMBER] == row_data_y[CCMY_SANDConsts.BAY_NUMBER]) &
                            (row_data_x[CCMY_SANDConsts.SHELF_NUMBER] == row_data_y[CCMY_SANDConsts.SHELF_NUMBER]) &
                            (row_data_y[CCMY_SANDConsts.MANUFACTURER_FK] == CCMY_SANDConsts.GENERAL_MANUFACTURER) &
                            (row_data_y[CCMY_SANDConsts.PRODUCT_FK] == CCMY_SANDConsts.IRRELEVANT) &
                            (row_data_x[CCMY_SANDConsts.IS_PURE] == CCMY_SANDConsts.PURE)):

                        row_data_x[CCMY_SANDConsts.IS_PURE] = CCMY_SANDConsts.IMPURE
                        print "Impure Shelf={}".format(row_data_y[CCMY_SANDConsts.SHELF_NUMBER])
                        continue
                    elif ((row_data_x[CCMY_SANDConsts.SCENE_FK] == row_data_y[CCMY_SANDConsts.SCENE_FK]) &
                          (row_data_x[CCMY_SANDConsts.BAY_NUMBER] == row_data_y[CCMY_SANDConsts.BAY_NUMBER]) &
                          (row_data_x[CCMY_SANDConsts.SHELF_NUMBER] == row_data_y[CCMY_SANDConsts.SHELF_NUMBER]) &
                          (row_data_y[CCMY_SANDConsts.MANUFACTURER_FK] not in [CCMY_SANDConsts.CCBM,
                                                                               CCMY_SANDConsts.GENERAL_MANUFACTURER]) &
                          (row_data_y[CCMY_SANDConsts.PRODUCT_FK] != CCMY_SANDConsts.GENERAL_EMPTY_PRODUCT) &
                          (row_data_x[CCMY_SANDConsts.IS_PURE] == CCMY_SANDConsts.PURE)):

                        row_data_x[CCMY_SANDConsts.IS_PURE] = CCMY_SANDConsts.IMPURE
                        print "Impure Shelf={}".format(row_data_y[CCMY_SANDConsts.SHELF_NUMBER])
                        continue
                    elif ((row_data_x[CCMY_SANDConsts.SCENE_FK] == row_data_y[CCMY_SANDConsts.SCENE_FK]) &
                          (row_data_x[CCMY_SANDConsts.BAY_NUMBER] == row_data_y[CCMY_SANDConsts.BAY_NUMBER]) &
                          (row_data_x[CCMY_SANDConsts.SHELF_NUMBER] == row_data_y[CCMY_SANDConsts.SHELF_NUMBER]) &
                          (row_data_y[CCMY_SANDConsts.MANUFACTURER_FK] == CCMY_SANDConsts.GENERAL_MANUFACTURER) &
                          (row_data_y[CCMY_SANDConsts.PRODUCT_FK] != CCMY_SANDConsts.GENERAL_EMPTY_PRODUCT) &
                          (row_data_x[CCMY_SANDConsts.IS_PURE] == CCMY_SANDConsts.PURE)):
                        row_data_x[CCMY_SANDConsts.IS_PURE] = CCMY_SANDConsts.IMPURE
                        print "Impure Shelf={}".format(row_data_y[CCMY_SANDConsts.SHELF_NUMBER])
                        continue

            num_of_pure_shelves = 0 if df_shelf_pure.empty else df_shelf_pure[CCMY_SANDConsts.IS_PURE].sum()
            total_num_of_shelves = 0 if df_shelf_pure.empty else len(df_shelf_pure)
            score = score if total_num_of_shelves == 0 else (num_of_pure_shelves / float(total_num_of_shelves))
            df_atomic_kpi = self.kpi_static_data[
                (self.kpi_static_data[CCMY_SANDConsts.ATOMIC_KPI_NAME] == params['KPI Name']) & (
                        self.kpi_static_data['kpi_name'] == group_name)]
            if df_atomic_kpi.empty:
                Log.warning("kpi {} from template, doesn't exist in DB".format(params['KPI Name']))
                return 0
            atomic_kpi_fk = df_atomic_kpi.iloc[0][CCMY_SANDConsts.ATOMIC_KPI_FK]
            result = num_of_pure_shelves if df_atomic_kpi[CCMY_SANDConsts.ATOMIC_KPI_NAME].iloc[0] == \
                                            CCMY_SANDConsts.KPI_NUM_PURE_SHELVES else total_num_of_shelves
            self.write_to_db_result(atomic_kpi_fk, (result, result, 0),
                                    level=self.LEVEL3)

            self.insert_db_new_results(params['KPI Name'], score, score, result, result, target=0,
                                       identifier_parent=identifier_parent)

            return num_of_pure_shelves, total_num_of_shelves, score, template_fk

    def calculate_facings_sos(self, kpi_data):
        group_score = 0
        group_name = kpi_data.iloc[0][CCMY_SANDConsts.KPI_GROUP]
        identifier_parent = self.common.get_dictionary(kpi_name=group_name)
        scene_types = self.get_scene_types(kpi_data.iloc[0])

        for x, params in kpi_data.iterrows():
            target_min = float(params[CCMY_SANDConsts.TARGET_MIN])
            target_max = float(1000 if not params[CCMY_SANDConsts.TARGET_MAX] else params[CCMY_SANDConsts.TARGET_MAX])

            sos_filters = dict(manufacturer_name=params[CCMY_SANDConsts.MANUFACTURER])
            facings_sos = self.tools.calculate_share_of_shelf(sos_filters, template_name=scene_types)
            score = 100 if target_min <= facings_sos <= target_max else 0
            result = float(params[CCMY_SANDConsts.SCORE]) if score != 0 else 0
            if not score and params[CCMY_SANDConsts.ONLY_IF_PASS]:
                continue
            group_score += result

            atomic_fk = \
            self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == params[CCMY_SANDConsts.KPI_NAME]) &
                                 (self.kpi_static_data['kpi_name'] == group_name)].iloc[0]['atomic_kpi_fk']
            self.write_to_db_result(atomic_fk, (score, result, target_min), level=self.LEVEL3)

            # insert result to new table
            self.insert_db_new_results(params['KPI Name'], result, score, score, 1, identifier_parent=identifier_parent,
                                       target=target_min)

        return group_score

    def get_scene_types(self, kpi_data):
        scene_types = kpi_data[CCMY_SANDConsts.TEMPLATE_NAME]
        if scene_types:
            scene_types = scene_types.split(CCMY_SANDConsts.SEPARATOR)
        else:
            scene_types = self.scene_info['template_name'].unique().tolist()
        return scene_types

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        if fk == 0:
            return
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

            if data.empty:
                return {}
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
        delete_queries = CCMY_SANDQueries.get_delete_session_results_query(self.session_uid)
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
