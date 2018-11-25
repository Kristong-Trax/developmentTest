import pandas as pd
import numpy as np
import collections as collect
import copy
from datetime import datetime, timedelta

from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from KPIUtils.INBEV.UploadNewTemplate import NewTemplate
from KPIUtils.INBEV.Fetcher import Queries
from KPIUtils.INBEV.INBEVJSON import JsonGenerator
from KPIUtils.INBEV.ToolBox import ToolBox
import sys

sys.path.append('.')

__author__ = 'urid'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
CUSTOM_OSA = 'pservice.custom_osa'
OSA = 'OSA'
ALL_PALLETS = ['Full Pallet', 'Metal Bracket', 'Half Pallet']
PALLET = ['Full Pallet', 'Metal Bracket']
HALF_PALLET = 'Half Pallet'
PALLET_WEIGHT = 1
HALF_PALLET_WEIGHT = 0.5

PALLET_FACTOR = 6
HALF_PALLET_FACTOR = 4
PALLET_STACK_PARAM = 2
PALLET_SEQUENCE_PARAM = 3
HALF_PALLET_STACK_PARAM = 2
HALF_PALLET_SEQUENCE_PARAM = 2
NON_PALLET_MIN_STACK_PARAM = 2
NON_PALLET_MAX_STACK_PARAM = 4
NON_PALLET_MAX_SEQUENCE_PARAM = 4
MIN_STACK_PARAM = 2

COMMERCIAL_GROUP = 'ABI_SFA_External_ID__c'
EMPTY = 'Empty'
OTHER = 'Other'


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


class INBEVNLINBEVBEToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    CUSTOM_SCENE_ITEM_FACTS = 'pservice.custom_scene_item_facts'
    ABINBEV = 'AB INBEV'
    DELISTED_MESSAGE_TYPE = 'DVOID'
    PALLET_SIZE_MM = 1200
    HALF_PALLET_SIZE_MM = 600
    NOT_OOS = 'Not OOS'
    SHELF_IMPACT_SCORE = 'Shelf Impact Score'

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_project_products = self.data_provider[Data.ALL_PRODUCTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.all_products = self.get_all_products()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].values[0]
        self.business_unit = self.get_business_unit()
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_number = self.get_store_number_1()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_display_in_scene = self.get_match_display()
        self.set_templates_data = {}
        self.kpi_static_data = self.get_kpi_static_data()
        try:
            self.inbev_template = NewTemplate(self.project_name)
            for set_name in ['Linear Share of Shelf', 'OSA']:
                self.get_missing_products_to_api_set(set_name)
        except Exception as e:
            Log.info('Updating API sets failed')
        self.kpi_static_data = self.get_kpi_static_data()
        self.tools = ToolBox(self.data_provider, output,
                                                kpi_static_data=self.kpi_static_data,
                                                match_display_in_scene=self.match_display_in_scene)
        self.kpi_results_queries = []
        self.download_time = timedelta(0)
        self.osa_table = self.get_osa_table()
        self.oos_messages = self.get_oos_messages()
        self.shelf_impact_score_results = {}
        self.scene_item_length_mm_dict = {}
        self.osa_scene_item_results = {}
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.must_have_assortment_score_range = {(0, 69.999): 0, (70, 89.999): 15, (90, 101): 30}
        self.linear_share_of_shelf_score_range = {(0, 94.999): 0, (95, 99.999): 15, (100, 102.999): 25,
                                                  (103, 500): 30}
        self.osa_score_range = {(0, 94.999): 0, (95, 96.999): 10, (97, 101): 15}
        self.shelf_level_score_range = {(0, 59.999): 0, (60, 79.999): 10, (80, 101): 15}
        self.product_blocking_score_range = {(0, 59.999): 0, (60, 79.999): 5, (80, 101): 10}
        self.products_to_add = []
        self.mha_product_results = {}
        self.has_assortment_list = []
        self.delisted_products = []
        self.missing_bundle_leads = []
        self.shelf_impact_score_thresholds = {}
        self.osa_product_dist_dict = {}
        self.linear_sos_value = 0
        self.must_have_assortment_list = []
        # self.rect_values = self.get_rect_values()
        self.extra_bundle_leads = []
        self.current_date = datetime.date

    @staticmethod
    def inrange(x, min, max):
        return (min is None or min <= x) and (max is None or max >= x)

    @staticmethod
    def get_max_value_from_dict_of_dicts(values):
        optional_factors = []
        for value in values:
            optional_factors.append(value)
        return max(optional_factors)

    def get_points_by_score_range(self, score, score_dict=None, max_point=None):
        points = 0
        if score_dict:
            for score_range in score_dict.keys():
                if self.inrange(score, score_range[0], score_range[1]):
                    points = score_dict[score_range]
        elif max_point:
            point = (float(score) / float(max_point)) * 100
            points = round(point)
        else:
            return

        return points

    def get_business_unit(self):
        """
        This function returns the session's business unit (equal to store type for some KPIs)
        """
        query = Queries.get_business_unit_data(self.store_info['store_fk'].values[0])
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        business_unit = pd.read_sql_query(query, self.rds_conn.db)['name']
        if not business_unit.empty:
            return business_unit.values[0]
        else:
            return ''

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = Queries.get_all_kpi_data()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = Queries.get_match_display(self.session_uid)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_osa_table(self):
        query = Queries.get_osa_table(self.store_id, self.visit_date, datetime.utcnow().date(),
                                                    self.data_provider.session_info.status)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        osa_table = pd.read_sql_query(query, self.rds_conn.db)
        return osa_table

    def get_oos_messages(self):
        query = Queries.get_oos_messages(self.store_id, self.session_uid)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        oos_messages = pd.read_sql_query(query, self.rds_conn.db)
        return oos_messages

    def get_store_number_1(self):
        query = Queries.get_store_number_1(self.store_id)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        store_number = pd.read_sql_query(query, self.rds_conn.db)
        return store_number.values[0]

    def get_eye_level_shelves(self, shelves_num):
        jg = JsonGenerator('inbevbe')
        jg.create_targets_json('golden_shelves.xlsx', 'golden_shelves')
        targets = jg.project_kpi_dict['golden_shelves']
        final_shelves = []
        for row in targets:
            if row.get('num. of shelves min') <= shelves_num <= row.get('num. of shelves max'):
                start_shelf = row.get('num. ignored from top') + 1
                end_shelf = shelves_num - row.get('num. ignored from bottom')
                final_shelves = range(start_shelf, end_shelf + 1)
            else:
                continue
        return final_shelves

    @kpi_runtime()
    def check_on_shelf_availability(self, set_name):
        """
        This function is used for OSA set calculations
        :param set_name:
        :return:
        """
        try:
            store_assortment_df = self.osa_table.loc[self.osa_table['store_fk'] == self.store_id]
        except KeyError as e:
            store_assortment_df = pd.DataFrame([])
        if store_assortment_df.empty:
            json_data = self.tools.download_template(set_name)
            new_assortment_df = pd.DataFrame(json_data)

            initial_ass_prod_list = new_assortment_df.loc[new_assortment_df['store'] == self.store_number[0]][
                COMMERCIAL_GROUP].unique().tolist()
            ass_prod_list = []
            for group in initial_ass_prod_list:
                product_fk = self.get_bundle_lead_by_att1(group)
                if product_fk is not None:
                    ass_prod_list.append(product_fk)
            self.has_assortment_list = ass_prod_list
            object_type = 'product_fk'
            self.add_product_to_store_assortment(
                ass_prod_list, object_type)  # Insert all data from file
        else:
            ass_prod_list = store_assortment_df['product_fk'].unique().tolist()
            self.has_assortment_list = ass_prod_list
            object_type = 'product_fk'
        ass_prod_present_in_store = self.scif.loc[self.scif[object_type].isin(ass_prod_list)][
            'product_fk'].unique().tolist()
        new_products_df = self.scif.loc[
            (~self.scif[object_type].isin(ass_prod_list)) & (self.scif['manufacturer_name'] == self.ABINBEV) & (
                ~self.scif['product_type'].isin([EMPTY, OTHER]))]
        if not new_products_df.empty:
            for product in new_products_df['product_fk'].unique().tolist():
                bundle_product_fk = self.get_bundle_lead(product)
                if bundle_product_fk is not None and bundle_product_fk not in ass_prod_list:
                    self.products_to_add.append(bundle_product_fk)
            self.add_product_to_store_assortment(list(set(self.products_to_add)), 'product_fk')
        delisted_products = self.oos_messages.loc[self.oos_messages['description']
                                                  == self.DELISTED_MESSAGE_TYPE]
        if not delisted_products.empty:
            products_to_remove = delisted_products['product_fk'].unique().tolist()
            self.remove_product_from_store_assortment(products_to_remove)
        falsely_recognized_products = self.oos_messages.loc[
            self.oos_messages['description'] == self.NOT_OOS]
        if not falsely_recognized_products.empty:
            falsely_recognized_products_list = falsely_recognized_products['product_fk'].unique(
            ).tolist()
        else:
            falsely_recognized_products_list = []
        ass_prod_present_in_store.extend(self.products_to_add)
        ass_prod_list.extend(self.products_to_add)
        set_score = 0
        self.check_on_shelf_availability_on_scene_level(OSA)
        if set_name == OSA:
            products_list = list(set(ass_prod_list) - set(delisted_products['product_fk']))
            self.calculate_osa_assortment_and_oos(products_list, ass_prod_present_in_store, object_type,
                                                  falsely_recognized_prods=falsely_recognized_products_list)

            updated_ass_prod_list = self.get_relevant_assortment_product_list(ass_prod_list)
            updated_ass_prod_list.extend(set(self.extra_bundle_leads))
            for delisted_product in self.delisted_products:
                if delisted_product in updated_ass_prod_list:
                    updated_ass_prod_list.remove(delisted_product)
                if delisted_product in self.osa_product_dist_dict.keys():
                    del self.osa_product_dist_dict[delisted_product]
            if not updated_ass_prod_list:
                set_score = 0
                target = 0
            else:
                set_score = (sum(self.osa_product_dist_dict.values()) /
                             float(len(set(updated_ass_prod_list)))) * 100
                target = len(set(updated_ass_prod_list))
            self.shelf_impact_score_thresholds[OSA] = target
            self.shelf_impact_score_results[OSA] = round(sum(self.osa_product_dist_dict.values()), 2)

        return round(set_score, 2)

    def get_relevant_assortment_product_list(self, ass_prod_list):
        """
        This function was created to take care of cases that in the assortment list there are both product and it's
        lead.
        :param ass_prod_list: assortment product list from the DB without the delisted products
        :return: List of the products that need to be in the assortment
        """
        relevant_prod = self.all_products.loc[(self.all_products['product_fk'].isin(ass_prod_list)) & (
            (self.all_products['att3'] == 'YES'))]['product_fk'].unique().tolist()
        for prod in relevant_prod:
            lead = self.get_bundle_lead(prod)
            if lead in relevant_prod and lead != prod:
                relevant_prod.remove(prod)
        return relevant_prod


    def check_on_shelf_availability_on_scene_level(self, set_name):
        """
        This function is used for pservice.custom_scene_item_facts table calculations
        :param set_name:
        :return:
        """
        try:
            store_assortment_df = self.osa_table.loc[self.osa_table['store_fk'] == self.store_id]
        except KeyError as e:
            store_assortment_df = pd.DataFrame([])
        if store_assortment_df.empty:
            json_data = self.tools.download_template(set_name)
            new_assortment_df = pd.DataFrame(json_data)
            object_type = 'product_fk'
            initial_ass_prod_list = new_assortment_df.loc[new_assortment_df['store'] == self.store_number[0]][
                COMMERCIAL_GROUP].unique().tolist()
            ass_prod_list = []
            for product in initial_ass_prod_list:
                product_fk = self.get_bundle_lead_by_att1(product)
                if product_fk is not None:
                    ass_prod_list.append(product_fk)

        else:
            ass_prod_list = store_assortment_df['product_fk'].unique().tolist()
            object_type = 'product_fk'
        scenes_to_check = self.scif['scene_fk'].unique().tolist()
        ass_prod_list.extend(self.products_to_add)
        ass_prod_present_in_store_in_all_scenes = \
            self.scif.loc[
                (self.scif[object_type].isin(ass_prod_list)) & (self.scif['scene_fk'].isin(scenes_to_check))][
                'product_fk'].unique().tolist()
        scif_with_oos_messages = self.oos_messages.merge(self.scif, on='product_fk', how='left')
        falsely_recognized_products = scif_with_oos_messages.loc[
            (scif_with_oos_messages['description'] == self.NOT_OOS)]
        if not falsely_recognized_products.empty:
            falsely_recognized_products_list = falsely_recognized_products['product_fk'].unique(
            ).tolist()
        else:
            falsely_recognized_products_list = []
        products_without_oos_reasons = scif_with_oos_messages.loc[
            (~scif_with_oos_messages['product_fk'].isin(ass_prod_present_in_store_in_all_scenes)) & (
                ~scif_with_oos_messages['product_fk'].isin(ass_prod_list)) & (
                scif_with_oos_messages['product_fk'].isin(ass_prod_list))]
        if not products_without_oos_reasons.empty:
            products_without_oos_reasons_list = products_without_oos_reasons['product_fk'].unique(
            ).tolist()
        else:
            products_without_oos_reasons_list = []
        for scene in scenes_to_check:
            ass_prod_present_in_store_per_scene = \
                self.scif.loc[(self.scif[object_type].isin(ass_prod_list)) & (self.scif['scene_fk'] == scene)][
                    'product_fk'].unique().tolist()
            # Saving product results for custom_scene_item_facts table
            self.calculate_osa_assortment_and_oos(ass_prod_list, ass_prod_present_in_store_per_scene, object_type,
                                                  falsely_recognized_prods=falsely_recognized_products_list,
                                                  products_without_oos_reasons=products_without_oos_reasons_list,
                                                  scene=scene)

    def calculate_osa_assortment_and_oos(self, products_list, products_present_in_store, object_type,
                                         falsely_recognized_prods=[],
                                         products_without_oos_reasons=[], scene=None):
        for product in products_list:
            dist_score = 0
            product_bundle_name = self.get_bundle_lead(product)
            if product_bundle_name:
                bundle_products = self.get_bundle_products(product)
                if product != product_bundle_name and product in products_list and product_bundle_name in products_list:
                    continue
                if scene:
                    self.osa_scene_item_results[scene, product] = {'in_assortment': 0, 'oos': 1}
                if product_bundle_name not in products_list:
                    self.extra_bundle_leads.append(product_bundle_name)
                product = product_bundle_name
            else:
                if product in self.all_products['product_fk'].unique().tolist():
                    self.missing_bundle_leads.append(product)
                continue
            try:
                product_fk = self.all_products.loc[self.all_products[object_type]
                                                   == product]['product_fk'].values[0]
                product_ean_code = \
                    self.all_products.loc[self.all_products[object_type]
                                          == product]['product_ean_code'].values[0]
            except IndexError as e:
                Log.info('Product with the {} number {} is not defined in the DB'.format(
                    object_type, product))
                continue
            assortment_kpi_name = str(product_ean_code) + ' - In Assortment'
            oos_kpi_name = str(product_ean_code) + ' - OOS'
            if scene:
                bundle_number_of_skus = self.tools.calculate_assortment(
                    product_fk=bundle_products, scene_id=scene)
            else:
                bundle_number_of_skus = self.tools.calculate_assortment(product_fk=bundle_products,
                                                                        session_id=self.session_fk)
            if bundle_number_of_skus > 0:
                dist_score = 1
                assortment_result = 1
                oos_result = 0
            elif product in falsely_recognized_prods:
                dist_score = 1
                assortment_result = 1
                oos_result = 0
            elif (bundle_number_of_skus == 0) and not (product in falsely_recognized_prods):
                assortment_result = 1
                oos_result = 1
            else:
                assortment_result = 0
                oos_result = 0
            if not scene:
                self.save_level2_and_level3(OSA, assortment_kpi_name, assortment_result)
                self.save_level2_and_level3(OSA, oos_kpi_name, oos_result)
            else:
                if product in self.osa_product_dist_dict:
                    existing_dist = self.osa_product_dist_dict[product]
                    self.osa_product_dist_dict[product] = max(dist_score, existing_dist)
                else:
                    self.osa_product_dist_dict[product] = dist_score
                if (scene, product_bundle_name) in self.osa_scene_item_results:
                    existing_ass = self.osa_scene_item_results[scene, product].get('in_assortment')
                    existing_oos = self.osa_scene_item_results[scene, product].get('oos')
                    in_ass = max(existing_ass, assortment_result)
                    oos = min(existing_oos, oos_result)
                    self.osa_scene_item_results[scene, product_fk] = {
                        'in_assortment': in_ass, 'oos': oos}
                else:
                    self.osa_scene_item_results[scene, product_fk] = {'in_assortment': assortment_result,
                                                                      'oos': oos_result}

    def add_product_to_store_assortment(self, products_list, object_type):
        for product in products_list:
            try:
                product_fk = self.all_products.loc[self.all_products[object_type]
                                                   == product]['product_fk'].values[0]
                self.write_to_osa_table(product_fk)
            except Exception as e:
                Log.info('Product {} is not defined'.format(product))

        return

    def remove_product_from_store_assortment(self, products_list):
        for product in products_list:
            custom_osa_query = Queries.get_delete_osa_records_query(product, self.store_id,
                                                                                  datetime.utcnow(), self.visit_date,
                                                                                  datetime.utcnow().date())
            self.kpi_results_queries.append(custom_osa_query)
            self.delisted_products.append(product)
        return

    def main_calculation(self, set_name):
        """
        This function calculates the KPI results.
        """
        if set_name not in self.tools.KPI_SETS_WITHOUT_A_TEMPLATE and set_name not in self.set_templates_data.keys():
            calc_start_time = datetime.utcnow()
            self.set_templates_data[set_name] = self.tools.download_template(set_name)
            self.download_time += datetime.utcnow() - calc_start_time

        if set_name in ('OSA',):
            set_score = self.check_on_shelf_availability(set_name)
        elif set_name in ('Linear Share of Shelf vs. Target', 'Linear Share of Shelf'):
            set_score = self.custom_share_of_shelf(set_name)
        elif set_name in ('Shelf Level',):
            set_score = self.calculate_eye_level_availability(set_name)
        elif set_name in ('Product Blocking',):
            set_score = self.calculate_block_together_sets(set_name)
        elif set_name == 'Pallet Presence':
            set_score, pallet_score, half_pallet_score = self.calculate_pallet_presence()
            self.save_level2_and_level3(set_name, 'Full Pallet', pallet_score)
            self.save_level2_and_level3(set_name, HALF_PALLET, half_pallet_score)
        elif set_name == 'Share of Assortment':
            set_score = self.calculate_share_of_assortment()
            self.save_level2_and_level3(set_name, set_name, set_score)
        elif set_name == 'Product Stacking':
            set_score = self.product_stacking(set_name)
        elif set_name == 'Shelf Impact Score':
            set_score = self.shelf_impact_score()

        else:
            return
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name']
                                      == set_name]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, set_score, self.LEVEL1)
        return set_score

    def save_level2_and_level3(self, set_name, kpi_name, result, score=None, threshold=None, level_2_only=False,
                               level_3_only=False, level2_name_for_atomic=None):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        try:
            if level_2_only:
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                (self.kpi_static_data['kpi_name'] == kpi_name)]
                kpi_fk = kpi_data['kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, result, self.LEVEL2)
            elif level_3_only:
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                (self.kpi_static_data['kpi_name'] == level2_name_for_atomic) & (
                                                    self.kpi_static_data['atomic_kpi_name'] == kpi_name)]
                atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
                if score is None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
                elif score is not None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score)
                else:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3,
                                            score=score, threshold=threshold)
            else:
                kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                (self.kpi_static_data['kpi_name'] == kpi_name)]
                kpi_fk = kpi_data['kpi_fk'].values[0]
                atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, result, self.LEVEL2)
                if score is None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
                elif score is not None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score)
                else:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3,
                                            score=score, threshold=threshold)
        except IndexError as e:
            Log.info('KPI {} is not defined in the DB'.format(kpi_name))

    def save_level1(self, set_name, score):
        kpi_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]
        kpi_set_fk = kpi_data['kpi_set_fk'].values[0]
        self.write_to_db_result(kpi_set_fk, score, self.LEVEL1)

    @kpi_runtime()
    def calculate_eye_level_availability(self, set_name):
        results = []
        scene_recognition_flag = False
        oos_products = [product for product,
                        dist_score in self.osa_product_dist_dict.items() if dist_score == 0]
        # todo add handler for bays without pallets
        for params in self.set_templates_data[set_name]:
            try:
                if params.get(self.store_type) in self.tools.RELEVANT_FOR_STORE:
                    scenes_results_dict = {}
                    scenes_to_check = self.match_product_in_scene['scene_fk'].unique().tolist()
                    object_type = self.tools.ENTITY_TYPE_CONVERTER.get(params.get(self.tools.ENTITY_TYPE),
                                                                       'product_ean_code')
                    objects = str(params.get(self.tools.PRODUCT_EAN_CODE,
                                             params.get(self.tools.PRODUCT_EAN_CODE2, '')))
                    product_fks = self.all_products[self.all_products['product_ean_code'].isin(
                        [objects])]['product_fk'].unique().tolist()
                    if set(product_fks) & set(oos_products):
                        result = None
                    elif not set(product_fks) & \
                            set(self.scif.loc[self.scif['dist_sc'] == 1]['product_fk'].unique().tolist()):
                        result = None
                    else:
                        for scene in scenes_to_check:
                            if set(ALL_PALLETS) & set(
                                    [display for display in
                                     self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk'] == scene][
                                         'name'].values]):
                                scene_recognition_flag = True
                            bays = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene)][
                                'bay_number'].unique().tolist()
                            bays_results_dict = {}
                            for bay in bays:
                                shelves_result_dict = {}
                                bay_data = self.match_product_in_scene.loc[
                                    (self.match_product_in_scene['scene_fk'] == scene) & (
                                        self.match_product_in_scene['bay_number'] == bay)]
                                if scene_recognition_flag:
                                    bay_match_display_in_scene = self.match_display_in_scene.loc[
                                        (self.match_display_in_scene['scene_fk'] == scene) &
                                        (self.match_display_in_scene['bay_number'] == bay)]
                                    bay_displays = bay_match_display_in_scene['name'].unique(
                                    ).tolist()
                                    if bay_displays:
                                        factor = self.get_factor(bay_displays[0])
                                    else:
                                        factor = 1
                                    number_of_shelves = len(
                                        bay_data['shelf_number'].unique()) + (int(factor) - 1)
                                else:
                                    number_of_shelves = len(bay_data['shelf_number'].unique())
                                bay_eye_level_shelves = self.get_eye_level_shelves(
                                    number_of_shelves)
                                for shelf in bay_eye_level_shelves:
                                    filters = {object_type: objects, 'bay_number': bay,
                                               'shelf_number': shelf, 'scene_fk': scene}
                                    object_facings = self.tools.calculate_assortment(oos_products=oos_products,
                                                                                     **filters)  # todo: validate
                                    if object_facings > 0:
                                        shelves_result_dict[shelf] = 1
                                    else:
                                        shelves_result_dict[shelf] = 0
                                if 1 in shelves_result_dict.values():
                                    bays_results_dict[bay] = 1
                                else:
                                    bays_results_dict[bay] = 0
                            if 1 in bays_results_dict.values():
                                scenes_results_dict[scene] = 1
                            else:
                                scenes_results_dict[scene] = 0
                        if 1 in scenes_results_dict.values():  # If not all SKUs are at eye level
                            result = 1  # TRUE
                        else:
                            result = 0  # FALSE
                        results.append(result)
                    if set_name == 'Shelf Level':
                        self.save_level2_and_level3(set_name, params.get('SKU Name (Description)'), result,
                                                    score=result)
                    elif set_name == 'Shelf Impact Score':
                        self.save_level2_and_level3(
                            set_name, params.get('SKU Name (Description)'), result)
                    else:
                        pass
            except Exception as e:
                Log.info('Store type {} does not exist in the template'.format(self.store_type))
        if results:
            set_score = (sum(results) / float(len(results))) * 100
            target = len(results)
        else:
            set_score = 0
            target = 0
        self.shelf_impact_score_results[set_name] = round(sum(results), 2)
        self.shelf_impact_score_thresholds[set_name] = target

        return round(set_score, 2)

    @kpi_runtime()
    def custom_share_of_shelf(self, set_name):
        set_score = 0
        if set_name == 'Linear Share of Shelf vs. Target':
            self.shelf_impact_score_results[set_name] = round(set_score, 2)
            self.shelf_impact_score_thresholds[set_name] = 0
            targets_df = pd.DataFrame(self.set_templates_data[set_name])
            relevant_df = targets_df.loc[targets_df['Store Number'] == self.store_number[0]]
            if not relevant_df.empty:
                target = relevant_df['Target'].values[0]
                linear_sos_value = self.linear_sos_value
                final_result = (linear_sos_value / float(target)) * 100
                set_score = round(final_result, 2)
                self.save_level2_and_level3(set_name=set_name, kpi_name=set_name, result=linear_sos_value,
                                            score=round(final_result, 2), threshold=target, level_3_only=True,
                                            level2_name_for_atomic=set_name)
                self.save_level2_and_level3(set_name=set_name, kpi_name=set_name, result=final_result,
                                            level_2_only=True)
                self.shelf_impact_score_results[set_name] = round(linear_sos_value, 2)
                self.shelf_impact_score_thresholds[set_name] = target
        elif set_name == 'Linear Share of Shelf':
            set_score = self.calculate_custom_share_of_shelf(set_name)
        else:
            return

        return set_score

    def calculate_custom_share_of_shelf(self, set_name, target=None):
        pallet_size_dict = {'Full Pallet': self.PALLET_SIZE_MM, 'Half Pallet': self.HALF_PALLET_SIZE_MM,
                            '1_display': self.HALF_PALLET_SIZE_MM, '2_display': self.HALF_PALLET_SIZE_MM,
                            'Metal Bracket': self.PALLET_SIZE_MM}
        sos_dict = {}
        sos_dict['nominator'] = 0
        sos_dict['denominator'] = 0
        nominator = 0
        denominator = 0
        scenes_to_check = self.scif['scene_fk'].unique().tolist()
        for scene in scenes_to_check:
            matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
            bays = matches['bay_number'].unique().tolist()
            for bay in bays:
                factors = {}
                half_pallet_half_mixed = 0
                custom_bay_data = matches.loc[(matches['bay_number'] == bay) & (
                    matches['stacking_layer'] == 1)]
                shelves = custom_bay_data['shelf_number'].unique().tolist()
                filtered_products = pd.merge(custom_bay_data, self.all_products, on=['product_fk', 'product_fk'],
                                             suffixes=['', '_1'])
                bay_match_display_in_scene = self.match_display_in_scene.loc[
                    (self.match_display_in_scene['scene_fk'] == scene) &
                    (self.match_display_in_scene['bay_number'] == bay)]
                bay_displays = bay_match_display_in_scene[
                    bay_match_display_in_scene['name'].isin(ALL_PALLETS)]['name'].tolist()
                lowest_shelf = max(shelves)
                products_not_on_pallet = []
                lowest_shelf_data = matches.loc[
                    (matches['bay_number'] == bay) & (matches['shelf_number'] == lowest_shelf) &
                    (matches['stacking_layer'] <= 4)]
                if bay_displays:  # todo: to insert the rules in case we recoginzed displays
                    pallet_products = lowest_shelf_data['product_fk'].tolist()
                    pallet_products_facings_dict = collect.Counter(pallet_products)
                    pallet_products_above_two = dict(
                        (k, v) for k, v in pallet_products_facings_dict.items() if v > 2)
                    common_products = pallet_products_above_two.keys()
                    if len(bay_displays) > 1:
                        if len(pallet_products_above_two) <= 2:
                            factors = self.get_product_factor_by_lowest_shelf(lowest_shelf_data, common_products,
                                                                              display_unit=bay_displays,
                                                                              display_data=bay_match_display_in_scene)
                            if factors is not None:
                                if len(factors.keys()) == 1:
                                    half_pallet_half_mixed = 1
                                    remove_product = list(
                                        set(common_products) - set(factors[factors.keys()[0]].keys()))
                                    common_products.remove(remove_product[0])
                                for display, factor_dict in factors.items():
                                    denominator += self.calculate_denominator_for_pallet(display, factor_dict,
                                                                                         pallet_size_dict, scene)
                                    nominator += self.calculate_nominator_for_pallet(display, factor_dict,
                                                                                     pallet_size_dict,
                                                                                     scene)
                                    self.insert_product_to_csif_legth_dict(scene, product=factor_dict.keys()[0],
                                                                           custom_mm_length=pallet_size_dict[
                                        display] * int(factor_dict.values()[0]))

                    else:
                        if len(pallet_products_above_two) == 1:
                            if bay_displays[0] == HALF_PALLET:
                                if self.check_rule(lowest_shelf_data, common_products[0], HALF_PALLET_STACK_PARAM,
                                                   HALF_PALLET_SEQUENCE_PARAM):
                                    factors = self.get_product_factor_by_lowest_shelf(lowest_shelf_data,
                                                                                      common_products,
                                                                                      display_unit=bay_displays)
                            elif bay_displays[0] in PALLET:
                                if self.check_rule(lowest_shelf_data, common_products[0], PALLET_STACK_PARAM,
                                                   PALLET_SEQUENCE_PARAM):
                                    factors = self.get_product_factor_by_lowest_shelf(lowest_shelf_data,
                                                                                      common_products,
                                                                                      display_unit=bay_displays)
                            if factors is not None:
                                for display, factor_dict in factors.items():
                                    denominator += self.calculate_denominator_for_pallet(display, factor_dict,
                                                                                         pallet_size_dict,
                                                                                         scene)
                                    nominator += self.calculate_nominator_for_pallet(display, factor_dict,
                                                                                     pallet_size_dict,
                                                                                     scene)
                                    self.insert_product_to_csif_legth_dict(scene, product=factor_dict.keys()[0],
                                                                           custom_mm_length=pallet_size_dict[display]
                                                                                            * int(
                                                                               list(factor_dict.values())[0]))
                    if factors:
                        pallet_products_below_two = dict(
                            (k, v) for k, v in pallet_products_facings_dict.items() if v <= 2)
                        if len(pallet_products_below_two) > 0 or half_pallet_half_mixed:
                            products_not_on_pallet = \
                                lowest_shelf_data.loc[~(lowest_shelf_data['product_fk'].isin(common_products))][
                                    'product_fk'].unique().tolist()
                        else:
                            shelves.remove(lowest_shelf)
                for shelf in shelves:
                    if shelf == lowest_shelf:
                        if products_not_on_pallet:
                            products_in_shelf = copy.copy(products_not_on_pallet)
                        else:
                            products_in_shelf = \
                                lowest_shelf_data.loc[(lowest_shelf_data['shelf_number'] == shelf)][
                                    'product_fk'].unique().tolist()
                        lowest_shelf_data_with_manufacturer = pd.merge(lowest_shelf_data, self.all_products,
                                                                       on=['product_fk',
                                                                           'product_fk'],
                                                                       suffixes=['', '_1'])
                        for product in products_in_shelf:  # Saving results to custom scene item facts table
                            size = \
                                self.all_products.loc[(self.all_products['product_fk'] == product)][
                                    'width_mm'].values[0]
                            if size is not None and not np.isnan(size):
                                nominator += self.calculate_nominator(lowest_shelf_data_with_manufacturer, scene,
                                                                      shelf, product, size, width_mm_field=None)
                                denominator += self.calculate_denominator(lowest_shelf_data_with_manufacturer,
                                                                          scene, shelf, product,
                                                                          size, width_mm_field=None)
                                custom_mm_length = self.calculate_custom_mm_length(lowest_shelf_data_with_manufacturer,
                                                                                   scene, shelf,
                                                                                   product, size, width_mm_field=None)
                            else:
                                if any(filtered_products.loc[
                                    (filtered_products['manufacturer_name'] == self.ABINBEV) & (
                                        filtered_products['shelf_number'] == shelf) & (
                                        filtered_products['product_fk'] == product)][
                                        'width_mm_advance']):
                                    width_mm_field = 'width_mm_advance'
                                else:
                                    width_mm_field = 'width_mm'
                                nominator += self.calculate_nominator(lowest_shelf_data_with_manufacturer, scene, shelf,
                                                                      product, size,
                                                                      width_mm_field)
                                denominator += self.calculate_denominator(lowest_shelf_data_with_manufacturer, scene,
                                                                          shelf, product,
                                                                          size, width_mm_field)
                                custom_mm_length = self.calculate_custom_mm_length(lowest_shelf_data_with_manufacturer,
                                                                                   scene, shelf,
                                                                                   product, size, width_mm_field)
                            self.insert_product_to_csif_legth_dict(scene, product, custom_mm_length)

                    else:
                        products_in_shelf = \
                            filtered_products.loc[(filtered_products['shelf_number'] == shelf)][
                                'product_fk'].unique().tolist()
                        for product in products_in_shelf:  # Saving results to custom scene item facts table
                            size = \
                                self.all_products.loc[(
                                    self.all_products['product_fk'] == product)]['width_mm'].values[0]
                            if size is not None and not np.isnan(size):
                                nominator += self.calculate_nominator(filtered_products, scene, shelf, product, size,
                                                                      width_mm_field=None)
                                denominator += self.calculate_denominator(filtered_products, scene, shelf, product,
                                                                          size, width_mm_field=None)
                                custom_mm_length = self.calculate_custom_mm_length(filtered_products, scene, shelf,
                                                                                   product, size, width_mm_field=None)
                            else:
                                if any(filtered_products.loc[
                                    (filtered_products['manufacturer_name'] == self.ABINBEV) & (
                                        filtered_products['shelf_number'] == shelf) & (
                                        filtered_products['product_fk'] == product)][
                                        'width_mm_advance']):
                                    width_mm_field = 'width_mm_advance'
                                else:
                                    width_mm_field = 'width_mm'
                                nominator += self.calculate_nominator(filtered_products, scene, shelf, product, size,
                                                                      width_mm_field)
                                denominator += self.calculate_denominator(filtered_products, scene, shelf, product,
                                                                          size, width_mm_field)
                                custom_mm_length = self.calculate_custom_mm_length(filtered_products, scene, shelf,
                                                                                   product, size, width_mm_field)
                            self.insert_product_to_csif_legth_dict(scene, product, custom_mm_length)
        if denominator:
            linear_sos_value = (nominator / float(denominator)) * 100
        else:
            linear_sos_value = 0
        if set_name == 'Linear Share of Shelf':
            set_score = linear_sos_value
            self.linear_sos_value = linear_sos_value
            self.save_level2_and_level3(set_name=set_name, kpi_name=set_name, result=nominator,
                                        score=round(linear_sos_value, 2), threshold=denominator)
        else:
            set_score = 0

        return round(set_score, 2)

    def insert_product_to_csif_legth_dict(self, scene, product, custom_mm_length):
        product_bundle_lead = self.get_bundle_lead(product)
        if not product_bundle_lead:
            product_bundle_lead = product
        if (scene, product_bundle_lead) in self.scene_item_length_mm_dict:
            current_value = self.scene_item_length_mm_dict[scene, product_bundle_lead]
            updated_value = current_value + custom_mm_length
            self.scene_item_length_mm_dict[scene, product_bundle_lead] = updated_value
        else:
            self.scene_item_length_mm_dict[scene, product_bundle_lead] = custom_mm_length

    def calculate_nominator_for_pallet(self, display, factor_dict, pallet_size_dict, scene):
        nominator = 0
        if self.all_products.loc[self.all_products['product_fk'] ==
                                 factor_dict.keys()[0]]['manufacturer_name'].values[0] == self.ABINBEV:
            nominator = pallet_size_dict[display] * int(list(factor_dict.values())[0]) * \
                (self.scif.loc[(self.scif['scene_id'] == scene) &
                               (self.scif['product_fk'] == factor_dict.keys()[
                                   0])]
                 ['rlv_sos_sc'].values[0])
        return nominator

    def calculate_denominator_for_pallet(self, display, factor_dict, pallet_size_dict, scene):
        denominator = pallet_size_dict[display] * int(list(factor_dict.values())[0]) * \
            (self.scif.loc[(self.scif['scene_id'] == scene) &
                           (
                self.scif['product_fk'] == factor_dict.keys()[0])]
             ['rlv_sos_sc'].values[0])
        return denominator

    def calculate_nominator(self, shelf_data, scene, shelf, product, size, width_mm_field):
        nominator = 0
        if size is not None and not np.isnan(size):
            nominator = (shelf_data.loc[(shelf_data['manufacturer_name'] == self.ABINBEV) & (
                shelf_data['shelf_number'] == shelf) & (shelf_data['product_fk'] == product)][
                'product_fk'].count()) * size * (self.scif.loc[(self.scif['scene_id'] == scene) &
                                                               (self.scif['product_fk'] == product)][
                    'rlv_sos_sc'].values[0])
        else:
            nominator = (shelf_data.loc[(shelf_data['manufacturer_name'] == self.ABINBEV) & (
                shelf_data['shelf_number'] == shelf) & (shelf_data['product_fk'] == product)][
                width_mm_field].sum()) * (self.scif.loc[(self.scif['scene_id'] == scene) &
                                                        (self.scif['product_fk'] == product)][
                    'rlv_sos_sc'].values[0])
        return nominator

    def calculate_denominator(self, shelf_data, scene, shelf, product, size, width_mm_field):
        if size is not None and not np.isnan(size):
            denominator = (shelf_data.loc[(shelf_data['shelf_number'] == shelf) & (
                shelf_data['product_fk'] == product)]
                ['product_fk'].count()) * size * (self.scif.loc[(self.scif['scene_id'] == scene) &
                                                                (self.scif['product_fk'] == product)][
                    'rlv_sos_sc'].values[0])
        else:
            denominator = (shelf_data.loc[(shelf_data['shelf_number'] == shelf) & (
                shelf_data['product_fk'] == product)]
                [width_mm_field].sum()) * (self.scif.loc[(self.scif['scene_id'] == scene) &
                                                         (self.scif['product_fk'] == product)][
                    'rlv_sos_sc'].values[0])
        return denominator

    def calculate_custom_mm_length(self, shelf_data, scene, shelf, product, size, width_mm_field):
        if size is not None and not np.isnan(size):
            custom_mm_length = (shelf_data.loc[(shelf_data['shelf_number'] == shelf) & (
                shelf_data['product_fk'] == product)][
                'product_fk'].count()) * size * (self.scif.loc[(self.scif['scene_id'] == scene) &
                                                               (self.scif[
                                                                   'product_fk'] == product)][
                    'rlv_sos_sc'].values[0])
        else:
            custom_mm_length = (shelf_data.loc[(shelf_data['shelf_number'] == shelf) & (
                shelf_data['product_fk'] == product)][
                width_mm_field].sum()) * (self.scif.loc[(self.scif['scene_id'] == scene) &
                                                        (self.scif[
                                                            'product_fk'] == product)][
                    'rlv_sos_sc'].values[0])
        return custom_mm_length

    def get_product_factor_by_lowest_shelf(self, bay_data, products_to_check, display_unit=[], display_data=None):
        products_sos_dict = {}
        factors = {}
        displays = ['1_display', '2_display']
        if len(display_unit) > 1:  # todo: to add condition for 2 displays
            for display in displays:
                products_for_two_half_pallets = {}
                passed_rule = 0
                for product in products_to_check:
                    if self.check_rule(bay_data, product, HALF_PALLET_STACK_PARAM, HALF_PALLET_SEQUENCE_PARAM):
                        passed_rule += 1
                        products_for_two_half_pallets['{}_display'.format(passed_rule)] = product
                        if passed_rule >= 2:
                            break
                if passed_rule == 0:
                    return None
                if passed_rule == 1:
                    if len(products_to_check) == 1:
                        products_for_two_half_pallets['2_display'] = products_for_two_half_pallets['1_display']
                for display, product in products_for_two_half_pallets.items():
                    factor = self.get_factor(HALF_PALLET)
                    if factor == 'NO VALUE' or factor is None:
                        factor = 1
                    factors[display] = {product: factor}
                return factors

        factor = self.get_factor(display_unit[0])
        if factor == 'NO VALUE' or factor is None:
            factor = 1
        factors[display_unit[0]] = {products_to_check[0]: factor}
        return factors

    @staticmethod
    def most_common(lst):
        return max(set(lst), key=lst.count)

    @kpi_runtime()
    def product_stacking(self, set_name):
        kpi_df_include_stacking = self.match_product_in_scene.merge(
            self.products, on=['product_fk'])
        results = []
        for params in self.set_templates_data[set_name]:
            result = 0
            product_data = kpi_df_include_stacking.loc[
                kpi_df_include_stacking['product_ean_code'] == params.get('EAN Number')]
            if not product_data.empty:
                max_prod_stacking_layer = max(product_data['stacking_layer'].values)
                if max_prod_stacking_layer >= int(params.get('Target')):
                    result = 1
            self.save_level2_and_level3(set_name, params.get('SKU Name (Description)'), result)
            results.append(result)
        if not results:
            set_score = 0
        else:
            set_score = (sum(results) / float(len(results))) * 100
        return round(set_score, 2)

    @kpi_runtime()
    def shelf_impact_score(self):
        total_score = 0
        must_have_assortment_score = self.calculate_must_have_assortment()
        must_have_assortment_points = self.get_points_by_score_range(must_have_assortment_score,
                                                                     score_dict=self.must_have_assortment_score_range)
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Must Have Assortment',
                                    result=self.shelf_impact_score_results['must_have_assortment'],
                                    score=must_have_assortment_points, level_3_only=True,
                                    level2_name_for_atomic='Must Have Assortment',
                                    threshold=self.shelf_impact_score_thresholds['must_have_assortment'])
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Must Have Assortment', result=must_have_assortment_points,
                                    level_2_only=True)
        total_score += must_have_assortment_points

        share_of_shelf_result = self.shelf_impact_score_results['Linear Share of Shelf vs. Target']
        if self.shelf_impact_score_thresholds['Linear Share of Shelf vs. Target'] > 0:
            share_of_shelf_score = (share_of_shelf_result / float(
                self.shelf_impact_score_thresholds['Linear Share of Shelf vs. Target'])) * 100
        else:
            share_of_shelf_score = 0
        share_of_shelf_points = self.get_points_by_score_range(share_of_shelf_score,
                                                               score_dict=self.linear_share_of_shelf_score_range)
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Share of Shelf', result=share_of_shelf_result,
                                    score=share_of_shelf_points, level_3_only=True,
                                    level2_name_for_atomic='Share of Shelf',
                                    threshold=self.shelf_impact_score_thresholds['Linear Share of Shelf vs. Target'])
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Share of Shelf', result=share_of_shelf_points,
                                    level_2_only=True)
        total_score += share_of_shelf_points

        osa_result = self.shelf_impact_score_results['OSA']
        if self.shelf_impact_score_thresholds['OSA'] > 0:
            osa_score = (osa_result / float(self.shelf_impact_score_thresholds['OSA'])) * 100
        else:
            osa_score = 0
        osa_points = self.get_points_by_score_range(osa_score, score_dict=self.osa_score_range)
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'On Shelf Availability', result=osa_result,
                                    level_3_only=True, level2_name_for_atomic='On Shelf Availability',
                                    score=osa_points, threshold=self.shelf_impact_score_thresholds['OSA'])
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'On Shelf Availability', result=osa_points,
                                    level_2_only=True)
        total_score += osa_points

        shelf_level_result = self.shelf_impact_score_results['Shelf Level']
        if self.shelf_impact_score_thresholds['Shelf Level'] > 0:
            shelf_level_score = (shelf_level_result /
                                 float(self.shelf_impact_score_thresholds['Shelf Level'])) * 100
        else:
            shelf_level_score = 0
        shelf_level_points = self.get_points_by_score_range(
            shelf_level_score, score_dict=self.shelf_level_score_range)
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Shelf Level Availability', result=shelf_level_result,
                                    score=shelf_level_points, level_3_only=True,
                                    level2_name_for_atomic='Shelf Level Availability',
                                    threshold=self.shelf_impact_score_thresholds['Shelf Level'])
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Shelf Level Availability', result=shelf_level_points,
                                    level_2_only=True)
        total_score += shelf_level_points

        blocked_together_result = self.shelf_impact_score_results['Product Blocking']
        if self.shelf_impact_score_thresholds['Product Blocking'] > 0:
            blocked_together_score = (blocked_together_result / float(
                self.shelf_impact_score_thresholds['Product Blocking'])) * 100
        else:
            blocked_together_score = 0
        blocked_together_points = self.get_points_by_score_range(blocked_together_score,
                                                                 score_dict=self.product_blocking_score_range)
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Product Blocking', result=blocked_together_result,
                                    score=blocked_together_points, level_3_only=True,
                                    level2_name_for_atomic='Product Blocking',
                                    threshold=self.shelf_impact_score_thresholds['Product Blocking'])
        self.save_level2_and_level3(self.SHELF_IMPACT_SCORE, 'Product Blocking', result=blocked_together_points,
                                    level_2_only=True)
        total_score += blocked_together_points

        return total_score

    def calculate_must_have_assortment(self):
        product_dist_dict = {}
        mha_json = self.tools.download_template('must_have_assortment')
        mha_df = pd.DataFrame(mha_json)
        relevant_store_df = mha_df.loc[mha_df['store'] == self.store_number[0]]
        product_ean_lst = [x.strip("'") for x in relevant_store_df['EAN'].unique().tolist()]
        ass_prod_list = self.all_products.loc[
            self.all_products['product_ean_code'].isin(product_ean_lst)]['product_fk'].unique().tolist()
        falsely_unrecognized_products = self.oos_messages.loc[
            self.oos_messages['description'] == self.NOT_OOS]
        if not falsely_unrecognized_products.empty:
            falsely_unrecognized_products_list = falsely_unrecognized_products['product_fk'].unique(
            ).tolist()
        else:
            falsely_unrecognized_products_list = []
        missing_bundle_leads = []
        for scene in self.scif['scene_fk'].unique().tolist():
            scene_products = self.scif.loc[(self.scif['scene_fk'] == scene)
                                           ]['product_fk'].unique().tolist()
            scene_products.extend(ass_prod_list)
            for product in set(scene_products):
                dist_score = 0

                product_bundle_name = self.get_bundle_lead(product)
                if product_bundle_name:
                    if product_bundle_name != product and product in ass_prod_list \
                            and product_bundle_name in ass_prod_list:
                        ass_prod_list.remove(product)
                    bundle_products = self.get_bundle_products(product)
                    product = product_bundle_name
                else:
                    bundle_products = [product]
                    missing_bundle_leads.append(product)
                bundle_number_of_skus = self.tools.calculate_assortment(
                    product_fk=bundle_products, scene_id=scene)
                if (bundle_number_of_skus > 0) and (product in ass_prod_list):
                    dist_score = 1
                    mha_in_ass = 1
                    mha_oos = 0
                elif (product in falsely_unrecognized_products_list) and (product in ass_prod_list):
                    dist_score = 1
                    mha_in_ass = 1
                    mha_oos = 0
                elif (bundle_number_of_skus == 0) and (
                        product in ass_prod_list) and not (product in falsely_unrecognized_products_list):
                    mha_in_ass = 1
                    mha_oos = 1
                elif product in ass_prod_list:
                    mha_in_ass = 1
                    mha_oos = 1
                else:
                    mha_in_ass = 0
                    mha_oos = 0
                if product in product_dist_dict:
                    existing_dist = product_dist_dict[product]
                    product_dist_dict[product] = max(dist_score, existing_dist)
                else:
                    product_dist_dict[product] = dist_score
                if (scene, product) in self.mha_product_results:
                    existing_in_ass = self.mha_product_results[scene, product].get('in_assortment')
                    existing_oos = self.mha_product_results[scene, product].get('in_assortment')
                    in_ass = max(existing_in_ass, mha_in_ass)
                    oos = min(existing_oos, mha_oos)
                    self.mha_product_results[scene, product] = {'in_assortment': in_ass, 'oos': oos}
                else:
                    self.mha_product_results[scene, product] = {
                        'in_assortment': mha_in_ass, 'oos': mha_oos}
        updated_ass_prod_list = self.all_products.loc[(self.all_products['product_fk'].isin(ass_prod_list)) & (
            (self.all_products['att3'] == 'YES') | (self.all_products['product_fk'].isin(missing_bundle_leads)))][
            'product_fk'].unique().tolist()
        self.must_have_assortment_list = updated_ass_prod_list
        if updated_ass_prod_list:
            set_score = (sum(product_dist_dict.values()) / float(len(updated_ass_prod_list))) * 100
            target = len(updated_ass_prod_list)
        else:
            set_score = 0
            target = 0
        self.shelf_impact_score_results['must_have_assortment'] = sum(product_dist_dict.values())
        self.shelf_impact_score_thresholds['must_have_assortment'] = target

        return round(set_score, 2)

    def get_bundle_products(self, product_fk):
        try:
            product_bundle_name = self.all_products.loc[self.all_products['product_fk'] == product_fk]['att1'].values[
                0]
            bundle_products = self.all_products.loc[
                (self.all_products['att1'] == product_bundle_name)]['product_fk'].unique().tolist()
        except Exception as e:
            bundle_products = None

        return bundle_products

    def get_bundle_lead(self, product_fk):
        try:
            product_bundle_name = self.all_products.loc[self.all_products['product_fk']
                                                        == product_fk]['att1'].values[0]
            bundle_lead = self.all_products.loc[
                (self.all_products['att1'] == product_bundle_name) & (self.all_products['att3'] == 'YES')][
                'product_fk'].values[0]
        except Exception as e:
            bundle_lead = None

        return bundle_lead

    def get_bundle_lead_by_att1(self, commercial_group):
        try:
            bundle_lead = self.all_products.loc[
                (self.all_products['att1'] == commercial_group) & (self.all_products['att3'] == 'YES')][
                'product_fk'].values[0]
        except Exception as e:
            bundle_lead = None

        return bundle_lead

    def get_store_attribute_8(self, store_fk):
        query = Queries.get_store_attribute_8(store_fk)
        final_df = pd.read_sql_query(query, self.rds_conn.db)
        value = final_df.values[0][0]
        return value

    def get_all_products(self):
        query = Queries.get_att3_att4_for_products()
        attributes = pd.read_sql_query(query, self.rds_conn.db)
        final_df = self.all_project_products.merge(attributes, on='product_fk', suffixes=['', '_1'])
        return final_df

    def save_custom_scene_item_facts_results(self):
        scenes_to_check = self.scif['scene_fk'].unique().tolist()
        for scene in scenes_to_check:
            scene_products = self.scif.loc[(self.scif['scene_fk'] == scene)
                                           ]['product_fk'].unique().tolist()
            scene_products_to_check = self.all_products.loc[
                (self.all_products['product_fk'].isin(scene_products)) & (
                    (self.all_products['att3'] == 'YES') | (
                        self.all_products['product_type'].isin(['Empty', 'Other'])) | ((self.all_products[
                            'att1'].notnull()) & (
                            self.all_products[
                                'att3'].isnull())) | (
                        self.all_products['manufacturer_name'] != self.ABINBEV) |
                    ((self.all_products['att1'].notnull()) & (self.all_products['att3'].isin(['NO', 'NO VALUE']))) |
                    ((self.all_products['att1'].isnull()) & (self.all_products['att3'].isnull())))]['product_fk'].unique().tolist()
            missing_bundle_leads = []
            scene_products_to_check.extend(self.has_assortment_list)
            scene_products_to_check.extend(self.must_have_assortment_list)
            for product in scene_products:
                product_bundle_lead = self.get_bundle_lead(product)
                if product_bundle_lead not in scene_products_to_check and product_bundle_lead is not None \
                        and product_bundle_lead not in missing_bundle_leads:
                    missing_bundle_leads.append(product_bundle_lead)
            scene_products_to_check.extend(missing_bundle_leads)
            for product in set(scene_products_to_check):
                if (scene, product) in self.osa_scene_item_results and not (
                        product in self.delisted_products):
                    in_ass_res = self.osa_scene_item_results[scene, product].get('in_assortment')
                    oos_res = self.osa_scene_item_results[scene, product].get('oos')
                elif product in self.delisted_products:
                    in_ass_res = 0
                    oos_res = 0
                else:
                    in_ass_res = 0
                    oos_res = 0
                if (scene, product) in self.scene_item_length_mm_dict:
                    product_length = self.scene_item_length_mm_dict[scene, product]
                    if np.isnan(product_length):
                        product_length = 0
                else:
                    product_length = 0
                if (scene, product) in self.mha_product_results:
                    mha_in_ass_res = self.mha_product_results[scene, product].get('in_assortment')
                    mha_oos_res = self.mha_product_results[scene, product].get('oos')
                else:
                    mha_in_ass_res = 0
                    mha_oos_res = 0
                attributes = pd.DataFrame(
                    [(self.session_fk, scene, product, in_ass_res,
                      oos_res,
                      product_length, mha_in_ass_res, mha_oos_res)],
                    columns=['session_fk', 'scene_fk', 'product_fk', 'in_assortment_osa', 'oos_osa',
                             'length_mm_custom', 'mha_in_assortment', 'mha_oos'])
                query = insert(attributes.to_dict(), self.CUSTOM_SCENE_ITEM_FACTS)
                self.kpi_results_queries.append(query)

    def save_linear_length_results(self):
        product_list_to_write = []
        scenes_to_check = self.scif['scene_fk'].unique().tolist()
        session_products = self.scif['product_fk'].unique().tolist()
        products_to_check = self.all_products.loc[
            (self.all_products['product_fk'].isin(session_products)) & (
                (self.all_products['att3'] == 'YES') | (
                    self.all_products['product_type'].isin(['Empty', 'Other'])) |
                ((self.all_products['att1'].notnull()) & (self.all_products['att3'].notnull())) | (
                    self.all_products['manufacturer_name'] != self.ABINBEV) |
                ((self.all_products['att1'].notnull()) & (self.all_products['att3'] == 'NO')) |
                ((self.all_products['att1'].isnull()) & (self.all_products['att3'].isnull())))][
            'product_fk'].unique().tolist()
        for product in products_to_check:
            aggregated_linear_length = 0
            for scene in scenes_to_check:

                product_bundle_lead = self.get_bundle_lead(product)
                if not product_bundle_lead:
                    product_bundle_lead = product
                if (scene, product_bundle_lead) in self.scene_item_length_mm_dict:
                    scene_product_length = self.scene_item_length_mm_dict[scene,
                                                                          product_bundle_lead]
                else:
                    scene_product_length = 0

                aggregated_linear_length += scene_product_length
            try:
                product_ean_code = \
                    self.all_products.loc[self.all_products['product_fk']
                                          == product_bundle_lead]['product_ean_code'].values[0]
            except IndexError as e:
                Log.info('Product {} is not defined'.format(product_bundle_lead))
                continue
            if product_ean_code:
                if product_ean_code not in product_list_to_write:
                    self.save_level2_and_level3('Linear Share of Shelf',
                                                product_ean_code, aggregated_linear_length)
                    product_list_to_write.append(product_ean_code)

    @kpi_runtime()
    def calculate_block_together_sets(self, set_name):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        parameters_df = pd.DataFrame(self.set_templates_data[set_name])
        product_groups = parameters_df['Atomic Name'].unique().tolist()
        for group in product_groups:
            if self.store_type not in parameters_df['Store Type'].unique().tolist():
                relevant_df = parameters_df.loc[
                    (parameters_df['Atomic Name'] == group) & (parameters_df['Store Type'] == 'All')]
            else:
                relevant_df = parameters_df.loc[
                    (parameters_df['Atomic Name'] == group) & (parameters_df['Store Type'] == self.store_type)]
            if not relevant_df.empty:
                products_for_block_check = relevant_df['EAN Number'].unique().tolist()
                result = self.tools.calculate_block_together(product_ean_code=products_for_block_check,
                                                             template_name=relevant_df['Scene Type'].values[0])
                score = 1 if result else 0
                scores.append(score)

                self.save_level2_and_level3(set_name, group, score, level_2_only=True)
                product_atomic_names = relevant_df['SKU Name'].unique().tolist()
                for atomic in product_atomic_names:
                    self.save_level2_and_level3(set_name, atomic, score, score=score, level_3_only=True,
                                                level2_name_for_atomic=group)

        if not scores:
            set_score = 0
            target = 0
        else:
            set_score = (sum(scores) / float(len(scores))) * 100
            target = len(scores)
        self.shelf_impact_score_results[set_name] = round(sum(scores), 2)
        self.shelf_impact_score_thresholds[set_name] = target

        return round(set_score, 2)

    @kpi_runtime()
    def calculate_pallet_presence(self):
        """
        This function calculates every Pallet-Presence typed KPI from the relevant sets, and returns the set final score.
        """
        pallet_score = len(
            self.match_display_in_scene[self.match_display_in_scene['name'].isin(PALLET)])
        half_pallet_score = len(
            self.match_display_in_scene[self.match_display_in_scene['name'] == HALF_PALLET])
        set_score = (PALLET_WEIGHT * pallet_score) + (HALF_PALLET_WEIGHT * half_pallet_score)
        return set_score, pallet_score, half_pallet_score

    @kpi_runtime()
    def calculate_share_of_assortment(self):
        """
        This function calculates every Share-of-Assortment typed KPI from the relevant sets, and returns the set final score.
        """
        assortment_inbev = self.tools.calculate_assortment(product_type=['SKU', 'Other'],
                                                           manufacturer_name=self.ABINBEV)
        assortment_all = self.tools.calculate_assortment(product_type=['SKU', 'Other'])
        if not assortment_all:
            set_score = 0
        else:
            set_score = (assortment_inbev / float(assortment_all)) * 100
        return round(set_score, 2)

    def get_product_att4(self, product_fk):
        query = Queries.get_product_att4(product_fk)
        att4 = pd.read_sql_query(query, self.rds_conn.db)
        return att4.values[0][0]

    def get_factor(self, display):
        if display in PALLET:
            factor = PALLET_FACTOR
        else:
            factor = HALF_PALLET_FACTOR
        return factor

    def check_rule(self, lowest_shelf_data, common_product, min_stack_param, min_sequence_param):
        relevant_bay_data = lowest_shelf_data.loc[lowest_shelf_data['product_fk'] == common_product]
        relevant_bay_data_first_layer = lowest_shelf_data.loc[
            (lowest_shelf_data['product_fk'] == common_product) & (lowest_shelf_data['stacking_layer'] == 1)]
        if max(relevant_bay_data['stacking_layer'].values) >= min_stack_param:
            if relevant_bay_data_first_layer['product_fk'].size >= min_sequence_param:
                return True
        return False

    def check_rule_hidden_pallet(self, bay_data, common_product, min_stack_param, max_stack_param, max_sequence_param):
        relevant_bay_data = bay_data.loc[bay_data['product_fk'] == common_product]
        relevant_bay_data_first_layer = bay_data.loc[
            (bay_data['product_fk'] == common_product) & (bay_data['stacking_layer'] == 1)]
        if (max(relevant_bay_data['stacking_layer'].values) >= min_stack_param) and (
                max(relevant_bay_data['stacking_layer'].values) <= max_stack_param):
            if relevant_bay_data_first_layer['product_fk'].size <= max_sequence_param:
                return True
        return False

    def get_rect_values(self):
        query = Queries.get_rect_values_query(self.session_uid)
        rect_values = pd.read_sql_query(query, self.rds_conn.db)
        return rect_values

    def get_missing_products_to_api_set(self, set_name):
        existing_skus = self.all_products[~self.all_products['product_ean_code'].isin(['746', '747', '748'])
                                          ]['product_ean_code'].unique().tolist()
        set_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name']
                                        == set_name]['atomic_kpi_name'].unique().tolist()
        if set_name == 'OSA':
            missing_products_in_osa_format = [
                str(sku) + ' - OOS' for sku in existing_skus if sku is not None]
            missing_products = []
            for missing_product in missing_products_in_osa_format:
                if missing_product not in set_data:
                    missing_products.append(missing_product)
        else:
            missing_products = []
            for existing_sku in existing_skus:
                if existing_sku not in set_data and existing_sku is not None:
                    missing_products.append(existing_sku)
        if missing_products:
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name']
                                          == set_name]['kpi_set_fk'].values[0]
            if set_name == 'OSA':
                kpi_list = []
                for missing_product in missing_products:
                    sku = missing_product.strip(' - OOS')
                    in_ass_kpi_name = str(sku) + ' - In Assortment'
                    kpi_list.append(in_ass_kpi_name)
                    oos_kpi_name = str(sku) + ' - OOS'
                    kpi_list.append(oos_kpi_name)
                self.inbev_template.add_new_kpi_to_static_tables(set_fk, kpi_list)
            else:
                self.inbev_template.add_new_kpi_to_static_tables(set_fk, missing_products)

    def write_to_db_result(self, fk, result, level, score=None, threshold=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(
            fk, result, level, score=score, threshold=threshold)
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

    def write_to_osa_table(self, product_fk):
        attributes = pd.DataFrame([(product_fk, self.store_id, 1, datetime.utcnow())],
                                  columns=['product_fk', 'store_fk', 'is_current', 'start_date'])
        query = insert(attributes.to_dict(), CUSTOM_OSA)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, result, level, score2=None, score=None, threshold=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk']
                                                == fk]['kpi_set_name'].values[0]
            score_type = '%' if kpi_set_name in self.tools.KPI_SETS_WITH_PERCENT_AS_SCORE else ''
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, score_type, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, result)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk']
                                                == fk]['kpi_set_name'].values[0]
            if score is None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, None, None)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                                   'score'])
            elif score is not None and not threshold:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, None, score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                                   'score'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, threshold, score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                                   'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk',
                                                   'threshold',
                                                   'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn.disconnect_rds()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        delete_queries = Queries.get_delete_session_results_query(self.session_uid)
        pservice_tables_delete_query = Queries.get_delete_custom_scif_query(
            self.session_fk)
        for query in delete_queries:
            cur.execute(query)
        cur.execute(pservice_tables_delete_query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()