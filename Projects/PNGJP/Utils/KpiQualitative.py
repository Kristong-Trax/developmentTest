# coding=utf-8

import os
import pandas as pd
from datetime import datetime
# from timeit import default_timer as timer

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime

from Projects.PNGJP.Utils.Fetcher import PNGJPQueries
from Projects.PNGJP.Utils.GeneralToolBox import PNGJPGENERALToolBox
from Projects.PNGJP.Utils.ParseTemplates import parse_template
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency

__author__ = 'Israels'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

IN_ASSORTMENT = 'in_assortment_osa'
IS_OOS = 'oos_osa'
PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'
PRODUCT_FK = 'product_fk'
SCENE_FK = 'scene_fk'


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


class PNGJPConsts(object):
    FACING_SOS = 'Facing SOS'
    FACING_SOS_BY_SCENE = 'Facing SOS by Scene'
    LINEAR_SOS = 'Linear SOS'
    SHELF_SPACE_LENGTH = 'Shelf Space Length'
    SHELF_SPACE_LENGTH_BY_SCENE = 'Shelf Space Length by Scene'
    FACING_COUNT = 'Facing Count'
    FACING_COUNT_BY_SCENE = 'Facing Count By Scene'
    DISTRIBUTION = 'Distribution'
    DISTRIBUTION_BY_SCENE = 'Distribution By Scene'
    SHARE_OF_DISPLAY = 'Share of Display'
    COUNT_OF_SCENES = 'Count of Scenes'
    COUNT_OF_SCENES_BY_SCENE_TYPE = 'Count of Scenes by scene type'
    COUNT_OF_POSM = 'Count of POSM'
    POSM_ASSORTMENT = 'POSM Assortment'
    SURVEY_QUESTION = 'Survey Question'

    SHELF_POSITION = 'Shelf Position'
    BRANDS = 'Brand'
    MANUFACTURERS = 'Manufacturer'
    AGGREGATED_SCORE = 'Aggregated Score'
    REFERENCE_KPI = 'Reference KPI'

    CATEGORY_PRIMARY_SHELF = 'Category Primary Shelf'
    DISPLAY = 'Display'
    PRIMARY_SHELF = 'Primary Shelf'

    KPI_TYPE = 'KPI Type'
    SCENE_TYPES = 'Scene Types to Include'
    KPI_NAME = 'KPI Name'
    CUSTOM_SHEET = 'Custom Sheet'
    PER_CATEGORY = 'Per Category'
    SUB_CALCULATION = 'Sub Calculation'
    VALUES_TO_INCLUDE = 'Values to Include'
    SHELF_LEVEL = 'Shelf Level'
    WEIGHT = 'Weight'
    SET_NAME = 'Set Name'
    UNICODE_DASH = u' \u2013 '

    CATEGORY_LOCAL_NAME = 'category_local_name'
    BRAND_LOCAL_NAME = 'brand_local_name'
    MANUFACTURER_NAME = 'manufacturer_name'
    CATEGORY = 'Category'
    POSM_NAME = 'POSM Name'
    POSM_TYPE = 'POSM Type'
    PRODUCT_NAME = 'Product Name'
    PRODUCT_EAN = 'Product EAN'
    PRODUCT_EAN_CODE_FIELD = 'product_ean_code'
    SURVEY_ID = 'Survey Question ID'
    SURVEY_TEXT = 'Survey Question Text'

    SEPARATOR = ','

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True
    EXCLUDE_IRRELEVANT = False
    INCLUDE_IRRELEVANT = True

    EMPTY = 'Empty'
    IRRELEVANT = 'Irrelevant'


class PNGJPKpiQualitative_ToolBox(PNGJPConsts):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    HIERARCHY = 'Hierarchy'
    GOLDEN_ZONE = 'Golden Zone'
    GOLDEN_ZONE_CRITERIA = 'Golden Zone Criteria'
    BLOCK = 'Block'
    ADJACENCY = 'Adjacency'
    ANCHOR = 'Anchor'
    PERFECT_EXECUTION = 'Perfect Execution'
    CATEGORY_LIST = 'Data List'
    PRODUCT_GROUP = 'Product Groups'
    VERTICAL = 'Vertical Block'
    GROUP_GOLDEN_ZONE_THRESHOLD = 'Threshold'
    PRODUCT_GROUP_ID = 'Product Group Id'
    ALLOWED_PRODUCT_GROUP_ID = 'ALLOWED;Product Group Id'
    KPI_FORMAT = 'Category: {category} - Brand: {brand} - Product group id: {group} - KPI Question: {question}'

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_display_in_scene = self.get_match_display()
        self.data_provider.probe_groups = self.get_probe_group(self.data_provider.session_uid)
        self.tools = PNGJPGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.template_name = 'TemplateQualitative.xlsx'
        self.TEMPLATE_PATH = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), '..', 'Data', self.template_name)
        self.template_data = parse_template(self.TEMPLATE_PATH, self.HIERARCHY)
        self.golden_zone_data = parse_template(self.TEMPLATE_PATH, self.GOLDEN_ZONE)
        self.golden_zone_data_criteria = parse_template(
            self.TEMPLATE_PATH, self.GOLDEN_ZONE_CRITERIA)
        self.block_data = parse_template(self.TEMPLATE_PATH, self.BLOCK)
        self.adjacency_data = parse_template(self.TEMPLATE_PATH, self.ADJACENCY)
        self.anchor_data = parse_template(self.TEMPLATE_PATH, self.ANCHOR)
        self.perfect_execution_data = parse_template(self.TEMPLATE_PATH, self.PERFECT_EXECUTION)
        self.category_list_data = parse_template(self.TEMPLATE_PATH, self.CATEGORY_LIST)
        self.product_groups_data = parse_template(self.TEMPLATE_PATH, self.PRODUCT_GROUP)
        self._custom_templates = {}
        self.scenes_types_for_categories = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_results = {}
        self.atomic_results = {}
        self.categories = self.all_products['category_fk'].unique().tolist()
        self.display_types = ['Aisle', 'Casher', 'End-shelf',
                              'Entrance', 'Island', 'Side-End', 'Side-net']
        self.custom_scif_queries = []
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.block = Block(data_provider=self.data_provider, rds_conn=self.rds_conn)
        self.adjacency = Adjancency(data_provider=self.data_provider, rds_conn=self.rds_conn)
        self.fix_utf_space_problem()
        self.kpi_scores = {}

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except:
            self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def _allowed_products(self):
        return {'product_type': ['Other', 'Empty']}

    def get_template(self, name):
        if name not in self._custom_templates.keys():
            self._custom_templates[name] = parse_template(self.TEMPLATE_PATH, name)
        return self._custom_templates[name]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGJPQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGJPQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display.merge(
            self.scene_info[['scene_fk', 'template_fk']], on='scene_fk', how='left')
        match_display = match_display.merge(
            self.all_templates, on='template_fk', how='left', suffixes=['', '_y'])
        return match_display

    def get_probe_group(self, session_uid):
        query = PNGJPQueries.get_probe_group(session_uid)
        probe_group = pd.read_sql_query(query, self.rds_conn.db)
        return probe_group

    def fix_utf_space_problem(self):
        self.template_data['fixed KPI name'] = self.template_data['KPI name'].str.replace(' ', '')
        self.golden_zone_data['fixed KPI name'] = self.golden_zone_data['KPI name'].str.replace(
            ' ', '')
        self.block_data['fixed KPI name'] = self.block_data['KPI name'].str.replace(' ', '')
        self.adjacency_data['fixed KPI name'] = self.adjacency_data['KPI name'].str.replace(' ', '')
        self.anchor_data['fixed KPI name'] = self.anchor_data['KPI name'].str.replace(' ', '')
        self.perfect_execution_data['fixed KPI name'] = self.perfect_execution_data['KPI name'].str.replace(
            ' ', '')
        self.template_data['fixed KPI name'] = self.template_data['KPI name'].str.replace(' ', '')
        self.kpi_static_data['fixed atomic_kpi_name'] = self.kpi_static_data['atomic_kpi_name'].str.replace(
            ' ', '')

    @log_runtime('Main Calculation')
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        for category in self.template_data['Category Name'].unique().tolist():
            category = \
                self.all_products[
                    self.all_products['category_local_name'].str.encode("utf8") == category.encode("utf8")][
                    self.CATEGORY_LOCAL_NAME].values[0]
            self.category_calculation(category)

        # for kpi_set in self.template_data[self.SET_NAME].unique().tolist():
        for kpi_set in ['Golden Zone', 'Block', 'Adjacency', 'Perfect Execution', 'Anchor']:
            self.write_to_db_result(score=None, level=self.LEVEL1, kpi_set_name=kpi_set)
            kpi_set_fk = self.kpi_static_data.loc[self.kpi_static_data[
                'kpi_set_name'] == kpi_set]['kpi_set_fk'].values[0]
            set_kpis = self.kpi_static_data.loc[self.kpi_static_data[
                'kpi_set_name'] == kpi_set]['kpi_name'].unique().tolist()
            for kpi in set_kpis:
                self.write_to_db_result(score=None, level=self.LEVEL2,
                                        kpi_set_fk=kpi_set_fk, kpi_name=kpi)

    def category_calculation(self, category):
        self.calculation_per_entity(category)
        self.category_aggregation_calculation(category)

    def calculation_per_entity(self, category):
        template_data = self.template_data[
            self.template_data['Category Name'].str.encode("utf8") == category.encode("utf8")]
        filters = {self.CATEGORY_LOCAL_NAME: category}

        for kpi in template_data['fixed KPI name'].unique().tolist():
            entity_kpis = template_data.loc[template_data['fixed KPI name'].str.encode("utf8") == kpi.encode("utf8")]
            entity_filters = filters

            for p in xrange(len(entity_kpis)):
                try:
                    score = threshold = result = None
                    params = entity_kpis.iloc[p]
                    set_name = params[self.SET_NAME]
                    kpi_type = params[self.KPI_TYPE]
                    scenes_filters = self.get_scenes_filters(params)
                    kpi_filters = dict(scenes_filters, **entity_filters)

                    if kpi_type == self.GOLDEN_ZONE:
                        kpi_params = self.golden_zone_data[
                            self.golden_zone_data['fixed KPI name'].str.encode("utf8") == kpi.encode("utf8")]
                        score, result, threshold= self.calculate_golden_zone(kpi, kpi_filters, kpi_params)

                    elif kpi_type == self.BLOCK:
                        kpi_params = self.block_data[
                            self.block_data['fixed KPI name'].str.encode("utf8") == kpi.encode("utf8")]
                        score, result, threshold = self.calculate_block(kpi, kpi_filters, kpi_params)

                    elif kpi_type == self.ANCHOR:
                        kpi_params = self.anchor_data[
                            self.anchor_data['fixed KPI name'].str.encode("utf8") == kpi.encode("utf8")]
                        score, result, threshold = self.calculate_anchor(kpi, kpi_filters, kpi_params)

                    elif kpi_type == self.ADJACENCY:
                        kpi_params = self.adjacency_data[
                            self.adjacency_data['fixed KPI name'].str.encode("utf8") == kpi.encode("utf8")]
                        score, result, threshold = self.calculate_adjacency(kpi, kpi_filters, kpi_params)

                    else:
                        Log.warning("KPI type '{}' is not supported".format(kpi_type))
                        continue

                    extra_data = self.get_extra_data_from_params(kpi_params)
                    self.kpi_scores.update({kpi: score})
                    self.write_result(score, result, threshold, kpi,
                                      category, set_name, template_data, extra_data=extra_data)
                except:
                    Log.warning("no score/result for '{}'".format(kpi_type))

    def category_aggregation_calculation(self, category):
        template_data = self.template_data[
            (self.template_data['Category Name'].str.encode("utf8") == category.encode("utf8")) & (
                    self.template_data['Set Name'] == 'Perfect Execution')]
        for kpi in template_data['fixed KPI name'].unique().tolist():
            entity_kpis = template_data.loc[template_data['fixed KPI name'].str.encode('utf-8') == kpi.encode('utf-8')]

            for p in xrange(len(entity_kpis)):
                score = threshold = result = None
                params = entity_kpis.iloc[p]
                set_name = params[self.SET_NAME]
                kpi_type = params[self.KPI_TYPE]

                if kpi_type == self.PERFECT_EXECUTION:
                    score, result, threshold = self.calculate_perfect_execution(kpi)

                    self.write_result(score, result, threshold, kpi,
                                      category, set_name, template_data)

    def _get_filtered_products(self):
        products = self.data_provider.products.copy()
        filtered_products_fk = set(products['product_fk'].tolist())
        return {'product_fk': list(filtered_products_fk)}

    def _get_ean_codes_by_product_group_id(self, column_name=PRODUCT_GROUP_ID, **params):
        return self.product_groups_data[self.product_groups_data['Group Id'] ==
                                        params[column_name].values[0].split('.')[0]]['Product EAN Code'].values[0].\
            split(self.SEPARATOR)

    def _get_allowed_products(self, allowed):
        allowed_products = set()
        # allowed.setdefault('product_type', []).extend(self._allowed_products['product_type'])
        for key, value in allowed.items():
            products = self.data_provider.products.copy()
            allowed_bulk = set(
                products[self.tools.get_filter_condition(products, **{key: value})]['product_fk'].tolist())
            allowed_products.update(allowed_bulk)

        return {'product_fk': list(allowed_products)}

    def check_bay(self, matches, probe_group, threshold, **filters):
        relevant_bays = matches[
            (matches['product_fk'].isin(filters['product_fk'])) & (matches['probe_group_id'] == probe_group)]
        relevant_bays['freq'] = relevant_bays.groupby('bay_number')['bay_number'].transform('count')
        relevant_bays = relevant_bays[relevant_bays['freq']
                                      >= threshold]['bay_number'].unique().tolist()

        if relevant_bays:
            relevant_bays.sort()
            return {'left': relevant_bays[0], 'right': relevant_bays[-1]}
        return {}

    def get_scenes_filters(self, params):
        filters = {}
        if params[self.SCENE_TYPES]:
            scene_types = params[self.SCENE_TYPES].split(self.SEPARATOR)
            template_names = []
            for scene_type in scene_types:
                template_names.append(scene_type)
            if template_names:
                filters['template_name'] = template_names
        return filters

    def write_to_db_result(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(score, level, threshold, level3_score, **kwargs)
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

    def create_attributes_dict(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            set_name = kwargs['kpi_set_name']
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name']
                                          == set_name]['kpi_set_fk'].values[0]
            if score is not None:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            format(score, '.2f'), set_fk)],
                                          columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                                   'kpi_set_fk'])
            else:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            None, set_fk)],
                                          columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                                   'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = kwargs['kpi_name']
            kpi_set_fk = kwargs['kpi_set_fk']
            kpi_fk = \
            self.kpi_static_data[(self.kpi_static_data['kpi_name'].str.encode('utf-8') == kpi_name.encode('utf-8')) &
                                 (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['kpi_fk'].values[0]

            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
            self.kpi_results[kpi_name] = score
        elif level == self.LEVEL3:
            kpi_name = kwargs['kpi_name']
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'].str.encode('utf-8')
                                          == kpi_name.encode('utf-8')]['kpi_fk'].values[0]
            atomic_kpi_name = kwargs['atomic_kpi_name']
            atomic_kpi_fk = kwargs['atomic_kpi_fk']
            kpi_set_name = kwargs['kpi_set_name']
            if level3_score is None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            atomic_kpi_fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk'])
            elif level3_score is not None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            level3_score, None, atomic_kpi_fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'score', 'threshold',
                                                   'atomic_kpi_fk'])
            elif level3_score is None and threshold is not None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, None, atomic_kpi_fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'threshold', 'score',
                                                   'atomic_kpi_fk'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, level3_score, atomic_kpi_fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'threshold', 'score',
                                                   'atomic_kpi_fk'])
            if kpi_set_name not in self.atomic_results.keys():
                self.atomic_results[kpi_set_name] = {}
            self.atomic_results[kpi_set_name][atomic_kpi_name] = score
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        # delete_queries = PNGJPQueries.get_delete_session_results_query(self.session_uid)
        # for query in delete_queries:
        #     cur.execute(query)
        queries = self.merge_insert_queries(self.kpi_results_queries)
        for query in queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def merge_insert_queries(self, insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))
        return merged_queries

    def get_extra_data_from_params(self, params):
        extra_df = {}
        if 'Brand' in params.columns and params['Brand'].values[0] != "":
            extra_df['brand'] = params['Brand'].values[0]
        else:
            extra_df['brand'] = 'XX'

        if 'Product Group Id' in params.columns and params['Product Group Id'].values[0] != "":
            extra_df['group'] = params['Product Group Id'].values[0]
        elif 'Product Group Id;A' in params.columns and 'Product Group Id;B' in params.columns:
            extra_df['group'] = "A-" + params['Product Group Id;A'].values[0] + ";B-" + \
                                                                    params['Product Group Id;B'].values[0]
        else:
            extra_df['group'] = 'XX'
        return extra_df

    @kpi_runtime(kpi_desc='calculate_golden_zone', project_name='pngjp')
    def calculate_golden_zone(self, kpi, kpi_filters, params):
        kpi_filter = kpi_filters.copy()
        assortment_entity = self.PRODUCT_EAN_CODE_FIELD
        if params[self.BRANDS].values[0]:
            kpi_filter['brand_local_name'] = params[self.BRANDS].values[0]
            total_group_skus = int(self.tools.calculate_availability(**kpi_filter))
        elif params[self.PRODUCT_GROUP_ID].values[0]:
            product_eans = self._get_ean_codes_by_product_group_id(**params)
            kpi_filter[assortment_entity] = product_eans
            total_group_skus = int(self.tools.calculate_availability(**kpi_filter))
        else:
            product_eans = params['Product EAN Code'].values[0].split(self.SEPARATOR)
            kpi_filter[assortment_entity] = product_eans
            total_group_skus = int(self.tools.calculate_availability(**kpi_filter))

        result = int(self.tools.calculate_linear_facings_on_golden_zone(self.golden_zone_data_criteria, **kpi_filter))
        score = 0
        threshold = float(params[self.GROUP_GOLDEN_ZONE_THRESHOLD].values[0])
        if total_group_skus:
            score = 100 if (result / float(total_group_skus)) >= \
                float(params[self.GROUP_GOLDEN_ZONE_THRESHOLD].values[0]) else 0
            result = (result / float(total_group_skus)) * 100
        return score, result, threshold

    @kpi_runtime(kpi_desc='calculate_block', project_name='pngjp')
    def calculate_block(self, kpi, kpi_filters, params):
        allowed_products_filters = {}
        threshold = 0
        kpi_filter = kpi_filters.copy()
        block_threshold = params['Threshold'].values[0]
        if params[self.PRODUCT_GROUP_ID].values[0] is not None:
            product_eans = self._get_ean_codes_by_product_group_id(**params)
            kpi_filter['product_ean_code'] = product_eans
        if (params[self.ALLOWED_PRODUCT_GROUP_ID].values[0] is not None) and (params[self.ALLOWED_PRODUCT_GROUP_ID].values[0] != ''):
            product_eans = self._get_ean_codes_by_product_group_id(
                column_name=self.ALLOWED_PRODUCT_GROUP_ID, **params)
            allowed_products_filters['product_ean_code'] = product_eans
        else:
            allowed_products_filters = None
        if params[self.VERTICAL].values[0] == 'Y':
            block_result, num_of_shelves = self.tools.calculate_block_together(vertical=True,
                                                                               allowed_products_filters=allowed_products_filters,
                                                                               minimum_block_ratio=float(
                                                                                   block_threshold),
                                                                               **kpi_filter)
            score = 100 if block_result and num_of_shelves >= 3 else 0
            result = 1 if block_result and num_of_shelves >= 3 else 0

        else:
            block_result = self.tools.calculate_block_together(minimum_block_ratio=float(block_threshold),
                                                               allowed_products_filters=allowed_products_filters,
                                                               **kpi_filter)
            score = 100 if block_result else 0
            result = 1 if block_result else 0
        return score, result, threshold

    @kpi_runtime(kpi_desc='calculate_anchor', project_name='pngjp')
    def calculate_anchor(self, kpi, kpi_filters, params):
        score = result = threshold = 0
        kpi_filter = kpi_filters.copy()
        minimum_products = int(params['Minimum Products'].values[0])
        params.pop('Minimum Products')
        block_threshold = params['Threshold'].values[0]
        params.pop('Threshold')

        product_eans = self._get_ean_codes_by_product_group_id(**params)
        kpi_filter[self.PRODUCT_EAN_CODE_FIELD] = product_eans

        allowed = {'product_type': ['Other', 'Empty', 'Irrelevant']}
        # allowed = params['allowed']
        allowed_products = self._get_allowed_products(allowed)
        filtered_products_all = self._get_filtered_products()
        filter_products_after_exclude = {
            'product_fk': list(
                set(filtered_products_all['product_fk']) - set(allowed_products['product_fk']))}

        filtered_products_sub_group = params.copy().to_dict()
        filtered_products_sub_group.update(kpi_filter)

        separate_filters, relevant_scenes = self.tools.separate_location_filters_from_product_filters(
            **filtered_products_sub_group)

        for scene in relevant_scenes:
            separate_filters.update({'scene_fk': scene})
            kpi_filter.update({'scene_fk': scene})
            block_result = self.tools.calculate_block_together(minimum_block_ratio=float(block_threshold),
                                                               **kpi_filter)

            if block_result:
                matches = self.tools.match_product_in_scene
                relevant_probe_group = matches[matches['scene_fk'] == scene]
                for probe_group in relevant_probe_group['probe_group_id'].unique().tolist():
                    relevant_bay = self.check_bay(relevant_probe_group, probe_group,
                                                  minimum_products, **filter_products_after_exclude)
                    if not relevant_bay:
                        continue
                    for direction in ['left', 'right']:
                        separate_filters.update({'bay_number': relevant_bay[direction]})
                        edge = self.tools.calculate_products_on_edge(position=direction,
                                                                     edge_population=filter_products_after_exclude,
                                                                     min_number_of_shelves=2,
                                                                     **separate_filters)
                        if edge[0] > 0:
                            score = 100
                            result = 1
                            break
        return score, result, threshold

    @kpi_runtime(kpi_desc='calculate_adjacency', project_name='pngjp')
    def calculate_adjacency(self, kpi, kpi_filters, params):
        score = result = threshold = 0
        kpi_filter = kpi_filters.copy()
        target = params['Threshold']
        target = float(target.values[0])
        a_target = params.get('Threshold A')
        if not a_target.empty:
            params.pop('Threshold A')
            a_target = float(a_target.values[0])
        b_target = params.get('Threshold B')
        if not b_target.empty:
            params.pop('Threshold B')
            b_target = float(b_target.values[0])

        group_a = {self.PRODUCT_EAN_CODE_FIELD: self._get_ean_codes_by_product_group_id(
            'Product Group Id;A', **params)}
        group_b = {self.PRODUCT_EAN_CODE_FIELD: self._get_ean_codes_by_product_group_id(
            'Product Group Id;B', **params)}

        # allowed_filter = self._get_allowed_products({'product_type': ([self.EMPTY, self.IRRELEVANT], self.EXCLUDE_FILTER)})
        allowed_filter = self._get_allowed_products(
            {'product_type': ['Irrelevant', 'Empty', 'Other']})
        allowed_filter_without_other = self._get_allowed_products(
            {'product_type': ['Irrelevant', 'Empty']})
        scene_filters = {'template_name': kpi_filter['template_name']}

        filters, relevant_scenes = self.tools.separate_location_filters_from_product_filters(
            **scene_filters)

        for scene in relevant_scenes:
            adjacency = self.tools.calculate_adjacency(group_a, group_b, {'scene_fk': scene}, allowed_filter,
                                                       allowed_filter_without_other, a_target, b_target, target)
            if adjacency:
                direction = params.get('Direction', 'All').values[0]
                if direction == 'All':
                    score = result = adjacency
                else:
                    # a = self.data_provider.products[self.tools.get_filter_condition(self.data_provider.products, **group_a)]['product_fk'].tolist()
                    # b = self.data_provider.products[self.tools.get_filter_condition(self.data_provider.products, **group_b)]['product_fk'].tolist()
                    # a = self.scif[self.scif['product_fk'].isin(a)]['product_name'].drop_duplicates()
                    # b = self.scif[self.scif['product_fk'].isin(b)]['product_name'].drop_duplicates()

                    edges_a = self.tools.calculate_block_edges(
                        minimum_block_ratio=a_target, **dict(group_a, allowed_products_filters=allowed_filter,
                                                             **{'scene_fk': scene}))
                    edges_b = self.tools.calculate_block_edges(
                        minimum_block_ratio=b_target, **dict(group_b, allowed_products_filters=allowed_filter,
                                                             **{'scene_fk': scene}))

                    if edges_a and edges_b:
                        if direction == 'Vertical':
                            if sorted(set(edges_a['shelfs'])) == sorted(set(edges_b['shelfs'])) and \
                                    len(set(edges_a['shelfs'])) == 1:
                                score = result = 0
                            elif max(edges_a['shelfs']) <= min(edges_b['shelfs']):
                                score = 100
                                result = 1
                            # elif max(edges_b['shelfs']) <= min(edges_a['shelfs']):
                            #     score = 100
                            #     result = 1
                        elif direction == 'Horizontal':
                            if set(edges_a['shelfs']).intersection(edges_b['shelfs']):
                                extra_margin_a = (
                                    edges_a['visual']['right'] - edges_a['visual']['left']) / 10
                                extra_margin_b = (
                                    edges_b['visual']['right'] - edges_b['visual']['left']) / 10
                                edges_a_right = edges_a['visual']['right'] - extra_margin_a
                                edges_b_left = edges_b['visual']['left'] + extra_margin_b
                                edges_b_right = edges_b['visual']['right'] - extra_margin_b
                                edges_a_left = edges_a['visual']['left'] + extra_margin_a
                                if edges_a_right <= edges_b_left:
                                    score = 100
                                    result = 1
                                elif edges_b_right <= edges_a_left:
                                    score = 100
                                    result = 1
        return score, result, threshold

    def calculate_perfect_execution(self, kpi):
        score = result = threshold = 0
        tested_kpis = self.perfect_execution_data[
            self.perfect_execution_data['fixed KPI name'].str.encode('utf-8') == kpi.encode('utf-8')]
        for i, tested_kpi in tested_kpis.iterrows():
            try:
                param_score = int(self.kpi_scores[tested_kpi['KPI test name'].replace(' ', '')])
            except:
                param_score = 0
            if param_score == 100:
                score = 100
                result = 1
            else:
                score = result = 0
                break
        return score, result, threshold

    def write_result(self, score, result, threshold, kpi, category, set_name, template_data, extra_data=None):
        kpi_name = template_data.loc[template_data['fixed KPI name'].str.encode('utf-8') == kpi.encode('utf-8')][
            'KPI name'].values[0]
        if extra_data is not None:
            brand = extra_data['brand']
            group = extra_data['group']
            kpi_name = self.KPI_FORMAT.format(
                category=category.encode('utf-8'),
                brand=brand.encode('utf-8'),
                group=str(group),
                question=kpi_name.encode('utf-8'))
        else:
            kpi_name = self.KPI_FORMAT.format(
                category=category.encode('utf-8'),
                brand='XX',
                group='XX',
                question=kpi_name.encode('utf-8'))
        while '  ' in kpi_name:
            kpi_name = kpi_name.replace('  ', ' ')
        atomic_kpi_fk = \
            self.kpi_static_data[
                self.kpi_static_data['fixed atomic_kpi_name'].str.encode('utf-8') == kpi.encode('utf-8')][
                'atomic_kpi_fk'].values[0]

        if result is not None or score is not None:
            if not kpi_name:
                kpi_name = self.KPI_FORMAT.format(category=category)
            if score is None and threshold is None:
                self.write_to_db_result(score=result, level=self.LEVEL3, kpi_set_name=set_name,
                                        kpi_name=category, atomic_kpi_name=kpi_name,
                                        atomic_kpi_fk=atomic_kpi_fk)

            elif score is not None and threshold is None:
                self.write_to_db_result(score=result, level=self.LEVEL3, level3_score=score,
                                        kpi_set_name=set_name, kpi_name=category,
                                        atomic_kpi_name=kpi_name, atomic_kpi_fk=atomic_kpi_fk)

            elif score is None and threshold is not None:
                self.write_to_db_result(score=result, level=self.LEVEL3, threshold=threshold,
                                        kpi_set_name=set_name,
                                        kpi_name=category, atomic_kpi_name=kpi_name,
                                        atomic_kpi_fk=atomic_kpi_fk)
            else:
                self.write_to_db_result(score=result, level=self.LEVEL3, level3_score=score,
                                        threshold=threshold, kpi_set_name=set_name,
                                        kpi_name=category, atomic_kpi_name=kpi_name,
                                        atomic_kpi_fk=atomic_kpi_fk)
