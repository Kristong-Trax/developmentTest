# coding=utf-8

from Trax.Data.ProfessionalServices.PsConsts.Consts import HelperConsts, ProductTypeConsts, BasicConsts, HelperConsts
from Trax.Data.ProfessionalServices.PsConsts.OldDB import KpiResults, KpkResults, KpsResults
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ProductsConsts, StoreInfoConsts, SceneInfoConsts, \
    TemplatesConsts
from datetime import datetime
import os
import pandas as pd
# from timeit import default_timer as timer

from Projects.PNGJP_SAND2.Data.LocalConsts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime

from Projects.PNGJP_SAND2.Utils.Fetcher import PNGJP_SAND2Queries
from Projects.PNGJP_SAND2.Utils.GeneralToolBox import PNGJP_SAND2GENERALToolBox
from Projects.PNGJP_SAND2.Utils.ParseTemplates import parse_template
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


class PNGJP_SAND2KpiQualitative_ToolBox(Consts):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True
    EXCLUDE_IRRELEVANT = False
    INCLUDE_IRRELEVANT = True

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
        self.store_type = self.data_provider[Data.STORE_INFO][StoreInfoConsts.STORE_TYPE].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_display_in_scene = self.get_match_display()
        self.data_provider.probe_groups = self.get_probe_group(self.data_provider.session_uid)
        self.tools = PNGJP_SAND2GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
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
        self.categories = self.all_products[ProductsConsts.CATEGORY_FK].unique().tolist()
        self.display_types = ['Aisle', 'Casher', 'End-shelf',
                              'Entrance', 'Island', 'Side-End', 'Side-net']
        self.custom_scif_queries = []
        self.session_fk = self.data_provider[Data.SESSION_INFO][BasicConsts.PK].iloc[0]
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
        return {ProductsConsts.PRODUCT_TYPE: [ProductTypeConsts.OTHER, ProductTypeConsts.EMPTY]}

    def get_template(self, name):
        if name not in self._custom_templates.keys():
            self._custom_templates[name] = parse_template(self.TEMPLATE_PATH, name)
        return self._custom_templates[name]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGJP_SAND2Queries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGJP_SAND2Queries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        match_display = match_display.merge(
            self.scene_info[[SceneInfoConsts.SCENE_FK, SceneInfoConsts.TEMPLATE_FK]], on=SceneInfoConsts.SCENE_FK,
            how='left')
        match_display = match_display.merge(
            self.all_templates, on=TemplatesConsts.TEMPLATE_FK, how='left', suffixes=['', '_y'])
        return match_display

    def get_probe_group(self, session_uid):
        query = PNGJP_SAND2Queries.get_probe_group(session_uid)
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
                    self.all_products[ProductsConsts.CATEGORY_LOCAL_NAME].str.encode(
                        HelperConsts.UTF8) == category.encode(HelperConsts.UTF8)][
                    ProductsConsts.CATEGORY_LOCAL_NAME].values[0]
            self.category_calculation(category)

        # for kpi_set in self.template_data[self.SET_NAME].unique().tolist():
        for kpi_set in ['Golden Zone', 'Block', 'Adjacency', 'Perfect Execution', 'Anchor']:
            self.write_to_db_result(score=None, level=self.LEVEL1, kpi_set_name=kpi_set)
            kpi_set_fk = self.kpi_static_data.loc[self.kpi_static_data[
                                                      'kpi_set_name'] == kpi_set][KpsResults.KPI_SET_FK].values[0]
            set_kpis = self.kpi_static_data.loc[self.kpi_static_data[
                                                    'kpi_set_name'] == kpi_set]['kpi_name'].unique().tolist()
            for kpi in set_kpis:
                self.write_to_db_result(score=None, level=self.LEVEL2,
                                        kpi_set_fk=kpi_set_fk, kpi_name=kpi)

    def category_calculation(self, category):
        self.calculation_per_entity(category)
        self.category_aggregation_calculation(category)

    def scene_type_not_exists(self, sfs):
        result = True
        session_scene_types = self.scif["template_name"].unique().tolist()

        for session_scene_type in session_scene_types:
            for sf in sfs:
                if session_scene_type == sf:
                    return False
        return result

    def calculation_per_entity(self, category):
        template_data = self.template_data[
            self.template_data['Category Name'].str.encode(HelperConsts.UTF8) == category.encode(HelperConsts.UTF8)]
        filters = {ProductsConsts.CATEGORY_LOCAL_NAME: category}

        for kpi in template_data['fixed KPI name'].unique().tolist():
            entity_kpis = template_data.loc[
                template_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(HelperConsts.UTF8)]
            entity_filters = filters

            for p in xrange(len(entity_kpis)):
                try:
                    score = threshold = result = None
                    params = entity_kpis.iloc[p]
                    set_name = params[self.SET_NAME]
                    kpi_type = params[self.KPI_TYPE]
                    scenes_filters = self.get_scenes_filters(params)
                    kpi_filters = dict(scenes_filters, **entity_filters)

                    if self.scene_type_not_exists(scenes_filters['template_name']):
                        continue

                    if kpi_type == self.GOLDEN_ZONE:
                        kpi_params = self.golden_zone_data[
                            self.golden_zone_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(
                                HelperConsts.UTF8)]
                        score, result, threshold = self.calculate_golden_zone(kpi, kpi_filters, kpi_params)

                    elif kpi_type == self.BLOCK:
                        kpi_params = self.block_data[
                            self.block_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(
                                HelperConsts.UTF8)]
                        score, result, threshold = self.calculate_block(kpi, kpi_filters, kpi_params)

                    elif kpi_type == self.ANCHOR:
                        kpi_params = self.anchor_data[
                            self.anchor_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(
                                HelperConsts.UTF8)]
                        score, result, threshold = self.calculate_anchor(kpi, kpi_filters, kpi_params)

                    elif kpi_type == self.ADJACENCY:
                        kpi_params = self.adjacency_data[
                            self.adjacency_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(
                                HelperConsts.UTF8)]
                        score, result, threshold = self.calculate_adjacency(kpi, kpi_filters, kpi_params)

                    else:
                        Log.debug("KPI type '{}' is not supported".format(kpi_type))
                        continue

                    extra_data = self.get_extra_data_from_params(kpi_params)

                    self.kpi_scores.update({kpi: score})
                    self.write_result(score, result, threshold, kpi,
                                      category, set_name, template_data, extra_data=extra_data)
                except Exception as ex:
                    Log.warning("Exception:{} no score/result for '{}'".format(ex.message, kpi_type))

    def category_aggregation_calculation(self, category):
        template_data = self.template_data[
            (self.template_data['Category Name'].str.encode(HelperConsts.UTF8) == category.encode(
                HelperConsts.UTF8)) & (
                    self.template_data['Set Name'] == 'Perfect Execution')]
        for kpi in template_data['fixed KPI name'].unique().tolist():
            entity_kpis = template_data.loc[
                template_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(HelperConsts.UTF8)]
            for p in xrange(len(entity_kpis)):
                score = threshold = result = None
                params = entity_kpis.iloc[p]
                set_name = params[self.SET_NAME]
                kpi_type = params[self.KPI_TYPE]

                st = [x.strip() for x in params['Scene Types to Include'].split(",")]
                if self.scene_type_not_exists(st):
                    continue

                if kpi_type == self.PERFECT_EXECUTION:
                    score, result, threshold = self.calculate_perfect_execution(kpi)

                    self.write_result(score, result, threshold, kpi,
                                      category, set_name, template_data)

    def _get_filtered_products(self):
        products = self.data_provider.products.copy()
        filtered_products_fk = set(products[ProductsConsts.PRODUCT_FK].tolist())
        return {ProductsConsts.PRODUCT_FK: list(filtered_products_fk)}

    def _get_ean_codes_by_product_group_id(self, column_name=Consts.PRODUCT_GROUP_ID, **params):
        return self.product_groups_data[self.product_groups_data['Group Id'] ==
                                        params[column_name].values[0].split('.')[0]]['Product EAN Code'].values[0]. \
            split(self.SEPARATOR)

    def _get_allowed_products(self, allowed):
        allowed_products = set()

        # allowed.setdefault(ProductsConsts.PRODUCT_TYPE, []).
        # extend(self._allowed_products[ProductsConsts.PRODUCT_TYPE])

        for key, value in allowed.items():
            products = self.data_provider.products.copy()
            allowed_bulk = set(
                products[self.tools.get_filter_condition(products, **{key: value})][ProductsConsts.PRODUCT_FK].tolist())
            allowed_products.update(allowed_bulk)

        return {ProductsConsts.PRODUCT_FK: list(allowed_products)}

    def check_bay(self, matches, probe_group, threshold, **filters):
        relevant_bays = matches[
            (matches[ProductsConsts.PRODUCT_FK].isin(filters[ProductsConsts.PRODUCT_FK]))
            & (matches['probe_group_id'] == probe_group)]
        relevant_bays['freq'] = relevant_bays.groupby(MatchesConsts.BAY_NUMBER)[MatchesConsts.BAY_NUMBER].transform(
            'count')

        relevant_bays = relevant_bays[relevant_bays['freq']
                                      >= threshold][MatchesConsts.BAY_NUMBER].unique().tolist()

        if relevant_bays:
            relevant_bays.sort()
            return {'left': relevant_bays[0], 'right': relevant_bays[-1]}
        return {}

    def get_scenes_filters(self, params):
        filters = {}
        if params[self.SCENE_TYPES_TO_INCLUDE]:
            scene_types = params[self.SCENE_TYPES_TO_INCLUDE].split(self.SEPARATOR)
            template_names = []
            for scene_type in scene_types:
                template_names.append(scene_type)
            if template_names:
                filters[TemplatesConsts.TEMPLATE_NAME] = template_names
        return filters

    def write_to_db_result(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(score, level, threshold, level3_score, **kwargs)
        if level == self.LEVEL1:
            table = Consts.KPS_RESULT
        elif level == self.LEVEL2:
            table = Consts.KPK_RESULT
        elif level == self.LEVEL3:
            table = Consts.KPI_RESULT
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
                                          == set_name][KpsResults.KPI_SET_FK].values[0]
            if score is not None:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            format(score, '.2f'), set_fk)],
                                          columns=[KpsResults.KPS_NAME,
                                                   KpiResults.SESSION_UID,
                                                   KpiResults.STORE_FK,
                                                   KpiResults.VISIT_DATE,
                                                   KpsResults.SCORE_1,
                                                   KpsResults.KPI_SET_FK])
            else:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            None, set_fk)],
                                          columns=[KpsResults.KPS_NAME,
                                                   KpiResults.SESSION_UID,
                                                   KpiResults.STORE_FK,
                                                   KpiResults.VISIT_DATE,
                                                   KpsResults.SCORE_1,
                                                   KpsResults.KPI_SET_FK])
        elif level == self.LEVEL2:
            kpi_name = kwargs['kpi_name']
            kpi_set_fk = kwargs[KpsResults.KPI_SET_FK]
            kpi_fk = \
                self.kpi_static_data[(self.kpi_static_data['kpi_name'].str.encode(HelperConsts.UTF8) == kpi_name.encode(
                    HelperConsts.UTF8)) &
                                     (self.kpi_static_data[KpsResults.KPI_SET_FK] == kpi_set_fk)][
                    KpiResults.KPI_FK].values[0]

            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, kpi_name, score)],
                                      columns=[KpkResults.SESSION_UID,
                                               KpkResults.STORE_FK,
                                               KpkResults.VISIT_DATE,
                                               KpiResults.KPI_FK,
                                               KpkResults.KPK_NAME,
                                               KpiResults.SCORE])
            self.kpi_results[kpi_name] = score
        elif level == self.LEVEL3:
            kpi_name = kwargs['kpi_name']
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'].str.encode(HelperConsts.UTF8)
                                          == kpi_name.encode(HelperConsts.UTF8)][KpiResults.KPI_FK].values[0]
            atomic_kpi_name = kwargs['atomic_kpi_name']
            atomic_kpi_fk = kwargs[KpiResults.ATOMIC_KPI_FK]
            kpi_set_name = kwargs['kpi_set_name']
            if level3_score is None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            atomic_kpi_fk)],
                                          columns=[KpiResults.DISPLAY_TEXT,
                                                   KpsResults.SESSION_UID,
                                                   KpsResults.KPS_NAME,
                                                   KpsResults.STORE_FK,
                                                   KpsResults.VISIT_DATE,
                                                   KpiResults.CALCULATION_TIME,
                                                   KpiResults.RESULT,
                                                   KpiResults.KPI_FK,
                                                   KpiResults.ATOMIC_KPI_FK])

            elif level3_score is not None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            level3_score, None, atomic_kpi_fk)],
                                          columns=[KpiResults.DISPLAY_TEXT,
                                                   KpsResults.SESSION_UID,
                                                   KpsResults.KPS_NAME,
                                                   KpsResults.STORE_FK,
                                                   KpsResults.VISIT_DATE,
                                                   KpiResults.CALCULATION_TIME,
                                                   KpiResults.RESULT,
                                                   KpiResults.KPI_FK,
                                                   KpiResults.SCORE,
                                                   KpiResults.THRESHOLD,
                                                   KpiResults.ATOMIC_KPI_FK])
            elif level3_score is None and threshold is not None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, None, atomic_kpi_fk)],
                                          columns=[KpiResults.DISPLAY_TEXT,
                                                   KpsResults.SESSION_UID,
                                                   KpsResults.KPS_NAME,
                                                   KpsResults.STORE_FK,
                                                   KpsResults.VISIT_DATE,
                                                   KpiResults.CALCULATION_TIME,
                                                   KpiResults.RESULT,
                                                   KpiResults.KPI_FK,
                                                   KpiResults.THRESHOLD,
                                                   KpiResults.SCORE,
                                                   KpiResults.ATOMIC_KPI_FK])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, level3_score, atomic_kpi_fk)],
                                          columns=[KpiResults.DISPLAY_TEXT,
                                                   KpsResults.SESSION_UID,
                                                   KpsResults.KPS_NAME,
                                                   KpsResults.STORE_FK,
                                                   KpsResults.VISIT_DATE,
                                                   KpiResults.CALCULATION_TIME,
                                                   KpiResults.RESULT,
                                                   KpiResults.KPI_FK,
                                                   KpiResults.THRESHOLD,
                                                   KpiResults.SCORE,
                                                   KpiResults.ATOMIC_KPI_FK])
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
        assortment_entity = ProductsConsts.PRODUCT_EAN_CODE
        if params[self.BRANDS].values[0]:
            kpi_filter[ProductsConsts.BRAND_LOCAL_NAME] = params[self.BRANDS].values[0]
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
            kpi_filter[ProductsConsts.PRODUCT_EAN_CODE] = product_eans
        if (params[self.ALLOWED_PRODUCT_GROUP_ID].values[0] is not None) and (
                params[self.ALLOWED_PRODUCT_GROUP_ID].values[0] != ''):
            product_eans = self._get_ean_codes_by_product_group_id(
                column_name=self.ALLOWED_PRODUCT_GROUP_ID, **params)
            allowed_products_filters[ProductsConsts.PRODUCT_EAN_CODE] = product_eans
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
        kpi_filter[ProductsConsts.PRODUCT_EAN_CODE] = product_eans

        allowed = {ProductsConsts.PRODUCT_TYPE: [ProductTypeConsts.OTHER, ProductTypeConsts.EMPTY,
                                                 ProductTypeConsts.IRRELEVANT]}
        # allowed = params['allowed']
        allowed_products = self._get_allowed_products(allowed)
        filtered_products_all = self._get_filtered_products()
        filter_products_after_exclude = {
            ProductsConsts.PRODUCT_FK: list(
                set(filtered_products_all[ProductsConsts.PRODUCT_FK]) - set(
                    allowed_products[ProductsConsts.PRODUCT_FK]))}

        filtered_products_sub_group = params.copy().to_dict()
        filtered_products_sub_group.update(kpi_filter)

        separate_filters, relevant_scenes = self.tools.separate_location_filters_from_product_filters(
            **filtered_products_sub_group)

        for scene in relevant_scenes:
            separate_filters.update({SceneInfoConsts.SCENE_FK: scene})
            kpi_filter.update({SceneInfoConsts.SCENE_FK: scene})
            block_result = self.tools.calculate_block_together(minimum_block_ratio=float(block_threshold),
                                                               **kpi_filter)

            if block_result:
                matches = self.tools.match_product_in_scene
                relevant_probe_group = matches[matches[MatchesConsts.SCENE_FK] == scene]
                for probe_group in relevant_probe_group['probe_group_id'].unique().tolist():
                    relevant_bay = self.check_bay(relevant_probe_group, probe_group,
                                                  minimum_products, **filter_products_after_exclude)
                    if not relevant_bay:
                        continue
                    for direction in ['left', 'right']:
                        separate_filters.update({MatchesConsts.BAY_NUMBER: relevant_bay[direction]})
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

        group_a = {ProductsConsts.PRODUCT_EAN_CODE: self._get_ean_codes_by_product_group_id(
            'Product Group Id;A', **params)}
        group_b = {ProductsConsts.PRODUCT_EAN_CODE: self._get_ean_codes_by_product_group_id(
            'Product Group Id;B', **params)}

        # allowed_filter = self._get_allowed_products({ProductsConsts.PRODUCT_TYPE:
        # ([self.EMPTY, self.IRRELEVANT], self.EXCLUDE_FILTER)})

        allowed_filter = self._get_allowed_products(
            {ProductsConsts.PRODUCT_TYPE: [ProductTypeConsts.IRRELEVANT,
                                           ProductTypeConsts.EMPTY,
                                           ProductTypeConsts.OTHER]})

        allowed_filter_without_other = self._get_allowed_products(
            {ProductsConsts.PRODUCT_TYPE: [ProductTypeConsts.IRRELEVANT, ProductTypeConsts.EMPTY]})
        scene_filters = {TemplatesConsts.TEMPLATE_NAME: kpi_filter[TemplatesConsts.TEMPLATE_NAME]}

        filters, relevant_scenes = self.tools.separate_location_filters_from_product_filters(
            **scene_filters)

        for scene in relevant_scenes:
            adjacency = self.tools.calculate_adjacency(group_a, group_b,
                                                       {SceneInfoConsts.SCENE_FK: scene},
                                                       allowed_filter,
                                                       allowed_filter_without_other,
                                                       a_target, b_target,
                                                       target)
            if adjacency:
                direction = params.get('Direction', 'All').values[0]
                if direction == 'All':
                    score = result = adjacency
                else:
                    # a = self.data_provider.products[
                    # self.tools.get_filter_condition(self.data_provider.products, **group_a)]
                    # [ProductsConsts.PRODUCT_FK].tolist()
                    #
                    # b = self.data_provider.products[
                    # self.tools.get_filter_condition(self.data_provider.products, **group_b)]
                    # [ProductsConsts.PRODUCT_FK].tolist()

                    # a = self.scif[self.scif[ProductsConsts.PRODUCT_FK].isin(a)]
                    # [ProductsConsts.PRODUCT_NAME].drop_duplicates()
                    # b = self.scif[self.scif[ProductsConsts.PRODUCT_FK].isin(b)]
                    # [ProductsConsts.PRODUCT_NAME].drop_duplicates()

                    edges_a = self.tools.calculate_block_edges(
                        minimum_block_ratio=a_target, **dict(group_a, allowed_products_filters=allowed_filter,
                                                             **{MatchesConsts.SCENE_FK: scene}))
                    edges_b = self.tools.calculate_block_edges(
                        minimum_block_ratio=b_target, **dict(group_b, allowed_products_filters=allowed_filter,
                                                             **{MatchesConsts.SCENE_FK: scene}))

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
            self.perfect_execution_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(
                HelperConsts.UTF8)]
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
        kpi_name = template_data.loc[
            template_data['fixed KPI name'].str.encode(HelperConsts.UTF8) == kpi.encode(HelperConsts.UTF8)][
            'KPI name'].values[0]
        if extra_data is not None:
            brand = extra_data['brand']
            group = extra_data['group']
            kpi_name = self.KPI_FORMAT.format(
                category=category.encode(HelperConsts.UTF8),
                brand=brand.encode(HelperConsts.UTF8),
                group=str(group),
                question=kpi_name.encode(HelperConsts.UTF8))
        else:
            kpi_name = self.KPI_FORMAT.format(
                category=category.encode(HelperConsts.UTF8),
                brand='XX',
                group='XX',
                question=kpi_name.encode(HelperConsts.UTF8))
        while '  ' in kpi_name:
            kpi_name = kpi_name.replace('  ', ' ')
        atomic_kpi_fk = \
            self.kpi_static_data[
                self.kpi_static_data['fixed atomic_kpi_name'].str.encode(HelperConsts.UTF8) == kpi.encode(
                    HelperConsts.UTF8)][
                KpiResults.ATOMIC_KPI_FK].values[0]

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
