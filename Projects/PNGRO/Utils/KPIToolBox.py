import os
from datetime import datetime
import json

import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

# from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.PNGRO.Utils.Fetcher import PNGRO_PRODQueries
from Projects.PNGRO.Utils.GeneralToolBox import PNGRO_PRODGENERALToolBox
from Projects.PNGRO.Utils.ParseTemplates import parse_template
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from KPIUtils_v2.Calculations.BlockCalculations import Block


__author__ = 'Israel'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')
SBD_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SBD_Template.xlsx')


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


class PNGRO_PRODToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    KPI_NAME = 'KPI name'
    SBD_KPI_NAME = 'KPI Name'
    KPI_TYPE = 'KPI Type'
    BRAND = 'Brand'
    FORM = 'Form'
    MANUFACTURER = 'Manufacturer'
    CATEGORY = 'Product Category'
    DISPLAY_NAME = 'display name'
    WEIGHTS = 'weight'
    DISPLAYS = 'display name'
    DISPLAYS_COUNT = 'display count'
    DISPLAYS_COUNT_BY_TYPE = 'display count by display type'
    SOD_BY_BRAND = 'share of display by brand'
    SOD_BY_MANUFACTURER = 'share of display by manufacturer'
    WEIGHT = 'weight'
    KPI_FAMILY = 'KPI Family'
    BLOCKED_TOGETHER = 'Blocked Together'
    SOS = 'SOS'
    RELATIVE_POSITION = 'Relative Position'
    AVAILABILITY = 'Availability'
    SHELF_POSITION = 'Shelf Position'
    SURVEY = 'Survey'
    SHELF_NUMBERS = 'Shelf number for the eye level counting from the bottom'
    NUMBER_OF_SHELVES = 'Number of shelves'

    LOCATION_TYPE = 'location_type'
    PRIMARY_SHELF = 'Primary Shelf'
    ASSORTMENT_KPI = 'PSKUs Assortment'
    ASSORTMENT_SKU_KPI = 'PSKUs Assortment - SKU'

    SOS_FACING = 'SOS Facing'
    EYE_LEVEL = 'Eye Level'
    DISPLAY_PRESENCE = 'Display Presence'
    EXCLUDE_EMPTY = False
    EXCLUDE_FILTER = 0
    EMPTY = 'Empty'
    PRODUCT_PRESENCE = 'Product Presence'
    BLOCK_ADJACENCY = 'Block Adjacency'
    BLOCKED_TOGETHER_V = 'Blocked Together Vertical'
    BINARY = 'Binary'
    PERCENT = 'Precentages'
    BLOCKED_TOGETHER_V_BRAND = 'Blocked Together Vertical Brand'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.match_stores_by_retailer = self.get_match_stores_by_retailer()
        self.match_template_fk_by_category_fk = self.get_template_fk_by_category_fk()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.retailer = \
                self.match_stores_by_retailer[self.match_stores_by_retailer['pk'] == self.store_id]['name'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = PNGRO_PRODGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.display_data = parse_template(TEMPLATE_PATH, 'display weight')
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        self.sbd_kpis_data = self.get_relevant_sbd_kpis()
        self.common = Common(self.data_provider)
        # self.new_kpi_static_data = self.common.get_new_kpi_static_data()
        self.new_kpi_static_data = self.common.get_kpi_static_data()

        self.main_shelves = self.are_main_shelves()
        self.assortment = Assortment(self.data_provider, self.output, common=self.common)
        self.eye_level_args = self.get_eye_level_shelf_data()
        self.scene_display_bay = self.get_scene_display_bay()
        self.display_scene_count = self.get_display_agg()
        self.displays_per_scene = self.get_number_of_displays_in_scene()
        self.adjacency = Adjancency(self.data_provider)
        self.block_calc = Block(self.data_provider)

    @property
    def matches(self):
        if not hasattr(self, '_matches'):
            self._matches = self.match_product_in_scene
            self._matches = self._matches.merge(self.scif, on='product_fk')
        return self._matches

    @property
    def match_display(self):
        if not hasattr(self, '_match_display'):
            self._match_display = self.get_status_session_by_display(self.session_uid)
        return self._match_display

    def get_relevant_sbd_kpis(self):
        sbd_kpis = parse_template(TEMPLATE_PATH, 'SBD_kpis', lower_headers_row_index=2)
        retailer_targets = parse_template(TEMPLATE_PATH, 'retailer_targets')
        retailer_targets = retailer_targets[retailer_targets['Retailer'] == self.retailer]
        relevant_df = sbd_kpis.merge(retailer_targets, left_on='KPI Number', right_on='KPI No', how='left')
        relevant_df.loc[(relevant_df['By Retailer'] == 'Y'), 'Target Policy'] = relevant_df['Retailer Target']
        sbd_kpis_all_retailers = relevant_df[~(relevant_df['By Retailer']=='Y')]
        sbd_kpis_retailer_specific = relevant_df[((relevant_df['Retailer'] == self.retailer)
                                                 | (relevant_df['Retailer'] == ''))]
        relevant_sbd_kpis = sbd_kpis_all_retailers.append(sbd_kpis_retailer_specific, ignore_index=True)
        return relevant_sbd_kpis

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGRO_PRODQueries.get_all_kpi_data()
        self.connect_rds_if_disconnected()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGRO_PRODQueries.get_match_display(self.session_uid)
        self.connect_rds_if_disconnected()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_match_stores_by_retailer(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = PNGRO_PRODQueries.get_match_stores_by_retailer()
        self.connect_rds_if_disconnected()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_template_fk_by_category_fk(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = PNGRO_PRODQueries.get_template_fk_by_category_fk()
        self.connect_rds_if_disconnected()
        template_category = pd.read_sql_query(query, self.rds_conn.db)
        return template_category

    def get_status_session_by_display(self, session_uid):
        query = PNGRO_PRODQueries.get_status_session_by_display(session_uid)
        self.connect_rds_if_disconnected()
        status_session = pd.read_sql_query(query, self.rds_conn.db)
        return status_session

    def get_status_session_by_category(self, session_uid):
        query = PNGRO_PRODQueries.get_status_session_by_category(session_uid)
        self.connect_rds_if_disconnected()
        status_session_by_cat = pd.read_sql_query(query, self.rds_conn.db)
        return status_session_by_cat

    def connect_rds_if_disconnected(self):
        query = PNGRO_PRODQueries.get_test_query(self.session_uid)
        try:
            test_query = pd.read_sql_query(query, self.rds_conn.db)
        except Exception as e:
            Log.warning('Lost connection with mysql or error in query. Connecting again {}'.format(repr(e)))
            self.rds_conn.connect_rds()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # Assortment(self.data_provider, self.output, common=self.common).main_assortment_calculation()
        # if not self.match_display.empty:
        #     if self.match_display['exclude_status_fk'][0] in (1, 4):

        self.calculate_assortment_main_shelf()
        self.calculate_linear_share_of_shelf_per_product_display()
        self.calculate_sos_pallets_per_product_by_scene_type_secondary_shelves()
        category_status_ok = self.get_status_session_by_category(self.session_uid)['category_fk'].tolist()
        if self.main_shelves:
            self.calculate_sbd()

    def calculate_sbd(self):
        for x, params in self.sbd_kpis_data.iterrows():
            # if self.check_if_blade_ok(params, self.match_display, category_status_ok):
            if True:
                general_filters = self.get_general_filters(params)
                if general_filters:
                    score = 0
                    result = threshold = None
                    kpi_type = params[self.KPI_FAMILY]
                    if kpi_type == self.BLOCKED_TOGETHER:
                        score = self.block_together(params, **general_filters)
                    elif kpi_type == self.SOS:
                        score, result, threshold = self.calculate_sos_linear_ign_stack(params, **general_filters)
                    elif kpi_type == self.RELATIVE_POSITION:
                        score, result, threshold = self.calculate_relative_position(params, **general_filters)
                    elif kpi_type == self.AVAILABILITY:
                        # self.rds_conn.connect_rds() # for debugging
                        score, result, threshold = self.calculate_availability(params, **general_filters)
                    elif kpi_type == self.SHELF_POSITION:
                        score, result, threshold = self.calculate_shelf_position(params, **general_filters)
                    elif kpi_type == self.SURVEY:
                        score, result, threshold = self.calculate_survey(params)

                    elif kpi_type == self.SOS_FACING:
                        score, result, threshold = self.calculate_sos_facings(params, **general_filters)
                    elif kpi_type == self.EYE_LEVEL:
                        score, result, threshold = self.calculate_eye_level_new(params, **general_filters)
                    elif kpi_type == self.DISPLAY_PRESENCE:
                        score, result, threshold = self.calculate_display_presence(params, **general_filters)
                    elif kpi_type == self.PRODUCT_PRESENCE:
                        general_filters = self.update_scene_filter(params, general_filters, by_location=True)
                        score, result, threshold = self.calculate_product_presence(params, **general_filters)
                    elif kpi_type == self.BLOCKED_TOGETHER_V:
                        general_filters = self.update_scene_filter(params, general_filters, by_location=True)
                        score, result, threshold = self.block_together_vertical(params, **general_filters)
                    elif kpi_type == self.BLOCKED_TOGETHER_V_BRAND:
                        general_filters = self.update_scene_filter(params, general_filters, by_location=True)
                        score, result, threshold = self.block_together_vertical_brand(params, **general_filters)
                    elif kpi_type == self.BLOCK_ADJACENCY:
                        general_filters = self.update_scene_filter(params, general_filters, by_location=True)
                        score, result, threshold = self.calculate_block_adjacency(params, **general_filters)

                    atomic_kpi_fk = self.get_kpi_fk_by_kpi_name(params[self.SBD_KPI_NAME])
                    location = self.get_location_for_db(params)
                    if atomic_kpi_fk is not None:
                        if result is not None and threshold is not None:
                            self.write_to_db_result(score=int(round(score*100, 0)), result=float(result),
                                                    result_2=float(threshold), level=self.LEVEL3, fk=atomic_kpi_fk,
                                                    logical_operator=location)
                        else:
                            self.write_to_db_result(score=int(score)*100, level=self.LEVEL3, fk=atomic_kpi_fk,
                                                    logical_operator=location)

    def calculate_sos_pallets_per_product_by_scene_type_secondary_shelves(self):
        if not self.scene_display_bay.empty:
            kpi_fk = self.get_new_kpi_fk_by_kpi_name('Share of SKU on Secondary Shelf')
            scene_display_bay = self.scene_display_bay
            scene_bay_display_width = self.get_display_width_df()
            scene_display_bay = pd.merge(scene_display_bay, scene_bay_display_width, on=['scene_fk', 'bay_number'], how='left')
            scene_bay_product_width = self.get_product_width_df()
            scene_bay_display_product = pd.merge(scene_bay_product_width, scene_display_bay, on=['scene_fk', 'bay_number'], how='left')
            scene_display_product = scene_bay_display_product.groupby(['scene_fk', 'product_fk', 'display_name', 'pk'], as_index=False)\
                                                                .agg({'product_width_total':np.sum, 'display_width_total':np.sum})
            scene_display_product['pallets'] = scene_display_product.apply(self.get_number_of_pallets, axis=1)
            templates = self.scif[['scene_fk', 'template_fk']].drop_duplicates(keep='last')
            final_df = scene_display_product.merge(templates, on='scene_fk', how='left')
            final_df = final_df[['scene_fk', 'template_fk', 'product_fk', 'display_name', 'pk',
                                 'product_width_total', 'display_width_total', 'pallets']]
            final_df = final_df.groupby(['template_fk', 'product_fk', 'display_name', 'pk'], as_index=False).agg(
                {'product_width_total': np.sum, 'display_width_total': np.sum, 'pallets': np.sum})
            # according to kpi logic denominator_id should be display_pk (pk) and context_id - template_fk but in TD
            # context_id filed is not retrieved => we switch values in denominator_id and context_id
            for i, row in final_df.iterrows():
                self.common.write_to_db_result(fk=kpi_fk, score=row['pallets'], result=row['pallets'],
                                               numerator_result=row['product_width_total'],
                                               numerator_id=row['product_fk'],
                                               denominator_result=row['display_width_total'],
                                               denominator_id=row['template_fk'],
                                               context_id=row['pk'])

    def get_number_of_pallets(self, row):
        display_weight = self.get_display_weight_by_display_name(row['display_name'])
        display_count = self.display_scene_count[self.display_scene_count['display_name'] == row['display_name']]['count'].sum()
        pallets = row['product_width_total'] / float(row['display_width_total']) * display_weight * display_count
        return pallets

    def get_product_width_df(self):
        matches = self.match_product_in_scene
        matches_filtered = matches[(matches['stacking_layer'] == 1) & (~(matches['bay_number'] == -1))][['scene_fk',
                                                                      'bay_number', 'product_fk', 'width_mm_advance']]
        scene_bay_product = matches_filtered.groupby(['scene_fk', 'bay_number', 'product_fk'], as_index=False)\
                                                    .agg({'width_mm_advance': np.sum})
        scene_bay_product = scene_bay_product.rename(columns={'width_mm_advance': 'product_width_total'})
        return scene_bay_product

    def get_display_width_df(self):
        matches = self.match_product_in_scene
        matches_filtered = matches[(matches['stacking_layer'] == 1) & (~(matches['bay_number'] == -1))][['scene_fk',
                                                                                                       'bay_number',
                                                                                                       'width_mm_advance']]
        scene_bay_display = matches_filtered.groupby(['scene_fk', 'bay_number'], as_index=False) \
                                                     .agg({'width_mm_advance': np.sum})
        scene_bay_display = scene_bay_display.rename(columns={'width_mm_advance': 'display_width_total'})
        return scene_bay_display

    def check_if_blade_ok(self, params, match_display, category_status_ok):
        if not params['Scene Category'].strip():
            if not match_display.empty:
                if match_display['exclude_status_fk'][0] in (1, 4):
                    return True
            else:
                return False
        elif int(float(params['Scene Category'])) in category_status_ok:
            return True
        else:
            return False

    def get_location_for_db(self, params):
        location_type = params['Location Type'].split(' ')
        return location_type[0]

    def calculate_eye_level_shelves(self, row):
        res_table = \
        self.eye_level_args[(self.eye_level_args["Number of shelves max"] >= row['shelf_number_from_bottom']) & (
                self.eye_level_args["Number of shelves min"] <= row['shelf_number_from_bottom'])][["Ignore from top",
                                                                                                "Ignore from bottom"]]
        if res_table.empty:
            return []
        start_shelf = res_table['Ignore from bottom'].iloc[0] + 1
        end_shelf = row['shelf_number_from_bottom'] - res_table['Ignore from top'].iloc[0]
        final_shelves = json.dumps(range(start_shelf, end_shelf + 1))
        return final_shelves

    def get_facings_scene_bay_shelf(self, row):
        facings_at_eye_lvl = 0
        if row['shelf_range']:
            relevant_matches_products = self.matches_products[(self.matches_products['bay_number'] == row.bay_number)&
                                                              (self.matches_products['shelf_number_from_bottom'].isin(
                                                                                        json.loads(row.shelf_range)))&
                                                            (self.matches_products['scene_fk'] == row.scene_fk)]
            facings_at_eye_lvl = len(relevant_matches_products)
        return facings_at_eye_lvl

    def calculate_eye_level_new(self, params, **general_filters):
        skus_at_eye_lvl = 0
        target = 1.0
        if general_filters['scene_id']:
            filters, target = self.get_eye_lvl_or_display_filters_for_kpi(params)
            filters['scene_fk'] = general_filters['scene_id']
            filters.update(**general_filters)
            matches_products_all = self.match_product_in_scene.merge(self.all_products, on='product_fk', how='left')
            self.matches_products = matches_products_all[self.tools.get_filter_condition(matches_products_all,
                                                                                         **filters)]
            if not self.matches_products.empty:
                scene_bays_shelves = self.matches_products[['scene_fk', 'bay_number']].drop_duplicates()
                scene_bays_shelves['shelf_number_from_bottom'] = scene_bays_shelves.apply(self.add_max_shelves_number,
                                                                                          axis=1)
                scene_bays_shelves = scene_bays_shelves.reset_index(drop=True)
                scene_bays_shelves['shelf_range'] = scene_bays_shelves.apply(self.calculate_eye_level_shelves, axis=1)
                scene_bays_shelves['facings_eye_lvl'] = scene_bays_shelves.apply(self.get_facings_scene_bay_shelf, axis=1)
                skus_at_eye_lvl = scene_bays_shelves['facings_eye_lvl'].sum()
        score = min(skus_at_eye_lvl / target, 1)
        # score = 1 if skus_at_eye_lvl >= target else 0
        return score, skus_at_eye_lvl, target

    def add_eye_level_shelf_range(self, scene_bays_shelves):
        scene_bays_shelves = scene_bays_shelves.assign(shelf_range='')
        for i, row in scene_bays_shelves.iterrows():
            res_table = \
                self.eye_level_args[
                    (self.eye_level_args["Number of shelves max"] >= row['shelf_number_from_bottom']) & (
                            self.eye_level_args["Number of shelves min"] <= row['shelf_number_from_bottom'])][
                    ["Ignore from top",
                     "Ignore from bottom"]]
            if res_table.empty:
                final_shelves = []
            else:
                start_shelf = res_table['Ignore from bottom'].iloc[0] + 1
                end_shelf = row['shelf_number_from_bottom'] - res_table['Ignore from top'].iloc[0]
                final_shelves = range(start_shelf, end_shelf + 1)
            scene_bays_shelves.at[i,'shelf_range'] = final_shelves
        return scene_bays_shelves

    def add_max_shelves_number(self, row):
        total_shelves = self.match_product_in_scene[(self.match_product_in_scene['bay_number'] ==
                                                     row.bay_number) & (
                                                            self.match_product_in_scene['scene_fk'] == row.scene_fk)][
            'shelf_number_from_bottom'].max()
        return total_shelves

    def get_eye_lvl_or_display_filters_for_kpi(self, params):
        type1 = params['Param Type (1)/ Numerator']
        value1 = map(unicode.strip, params['Param (1) Values'].split(','))
        type2 = params['Param Type (2)/ Denominator']
        value2 = map(unicode.strip, params['Param (2) Values'].split(','))
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        target = float(params.get('Target Policy', 1))
        filters = {type1: value1}
        if type2:
            filters.update({type2: value2})
        if type3:
            filters.update({type3: value3})
        return filters, target

    def calculate_assortment_main_shelf(self):
        assortment_result_lvl3 = self.assortment.get_lvl3_relevant_ass()
        if not self.main_shelves and not assortment_result_lvl3.empty:
            assortment_result_lvl3.drop(assortment_result_lvl3.index[0:], inplace=True)
        if not assortment_result_lvl3.empty:
            filters = {self.LOCATION_TYPE: self.PRIMARY_SHELF}
            filtered_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
            products_in_session = filtered_scif.loc[filtered_scif['facings'] > 0]['product_fk'].values
            assortment_result_lvl3.loc[assortment_result_lvl3['product_fk'].isin(products_in_session), 'in_store'] = 1

            for result in assortment_result_lvl3.itertuples():
                score = result.in_store * 100
                # self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                #                                           numerator_result=result.in_store, result=score,
                #                                           denominator_id=result.assortment_group_fk, denominator_result=1,
                #                                           score=score)
                self.common.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                                          numerator_result=result.in_store, result=score,
                                                          denominator_id=result.assortment_group_fk,
                                                          denominator_result=1,
                                                          score=score)
            lvl2_result = self.assortment.calculate_lvl2_assortment(assortment_result_lvl3)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if result.target and not np.math.isnan(result.target):
                    if result.group_target_date <= self.visit_date:
                        denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res)) * 100
                if res >= 100:
                    score = 100
                else:
                    score = 0
                # self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl2, numerator_id=result.assortment_group_fk,
                #                                           numerator_result=result.passes, result=res,
                #                                           denominator_id=result.assortment_super_group_fk,
                #                                           denominator_result=denominator_res,
                #                                           score=score)
                self.common.write_to_db_result(fk=result.kpi_fk_lvl2,
                                                          numerator_id=result.assortment_group_fk,
                                                          numerator_result=result.passes, result=res,
                                                          denominator_id=result.assortment_super_group_fk,
                                                          denominator_result=denominator_res,
                                                          score=score)

    def are_main_shelves(self):
        """
        This function returns a list with the main shelves of this session
        """
        are_main_shelves = True if self.PRIMARY_SHELF in self.scif[self.LOCATION_TYPE].unique().tolist() else False
        return are_main_shelves

    def get_general_filters(self, params):
        # template_name = params['Template Name']
        # category = params['Scene Category']
        location_type = params['Location Type']
        # retailer = params['by Retailer']

        relative_scenes = self.scif[(self.scif['location_type'] == location_type)]

        # if template_name.strip():
        #     relative_scenes = self.scif[
        #         (self.scif['template_name'] == template_name) & (self.scif['location_type'] == location_type)]
        # elif category.strip():
        #     template_fk = self.match_template_fk_by_category_fk['pk'][
        #         self.match_template_fk_by_category_fk['product_category_fk'] == int(float(category))].unique().tolist()
        #     relative_scenes = self.scif[
        #         (self.scif['template_fk'].isin(template_fk)) & (self.scif['location_type'] == location_type)]
        # else:
        #     relative_scenes = self.scif[(self.scif['location_type'] == location_type)]
        #
        # if retailer.strip():
        #     stores = self.match_stores_by_retailer['pk'][
        #         self.match_stores_by_retailer['name'] == retailer].unique().tolist()
        #     relative_scenes = relative_scenes[(relative_scenes['store_id'].isin(stores))]

        general_filters = {}
        if not relative_scenes.empty:
            general_filters['scene_id'] = relative_scenes['scene_id'].unique().tolist()

        return general_filters

    def block_together_vertical_brand(self, params, **general_filters):
        score = 0
        if general_filters['scene_id']:
            filters, facings, min_shelf_num = self.get_block_filters(params)
            general_filters = self.update_scene_filter(params, general_filters, product_filters=filters)
            filters.update(general_filters)

            item_blocks_passed = 0
            brand_list = self.scif[self.tools.get_filter_condition(self.scif, **filters)][
               'brand_name'].unique().tolist()
            for brand in brand_list:
                filters.update({'brand_name': brand})
                is_blocked, n_shelves = self.block_calc.calculate_block_together(minimum_block_ratio=0.75,
                                                                                 result_by_scene=False, vertical=True,
                                                                                 min_facings_in_block=facings, **filters)
                if is_blocked and n_shelves >= min_shelf_num:
                    item_blocks_passed += 1
            if item_blocks_passed == len(brand_list):
                score = 1
        return score, score, 1

    def get_block_filters(self, params):
        type1 = params['Param Type (1)/ Numerator']
        value1 = self.split_and_strip(params['Param (1) Values'])
        type2 = params['Param Type (2)/ Denominator']
        value2 = self.split_and_strip(params['Param (2) Values'])
        type3 = params['Param (3)']
        value3 = self.split_and_strip(params['Param (3) Values'])
        min_shelf_num = float(params['Min Shelf Number'])
        facings = float(params['Facings'])
        manufacturer = {'manufacturer_name': params['Manufacturer']}

        filters = {type1: value1}
        if type2:
            filters.update({type2: value2})
        if type3:
            filters.update({type3: value3})
        filters.update(manufacturer)
        return filters, facings, min_shelf_num

    def block_together_vertical(self, params, **general_filters):
        score = 0
        if general_filters['scene_id']:
            filters, facings, min_shelf_num = self.get_block_filters(params)
            general_filters = self.update_scene_filter(params, general_filters, product_filters=filters)
            filters.update(general_filters)
            is_blocked, n_shelves = self.block_calc.calculate_block_together(minimum_block_ratio=0.75, result_by_scene=False,
                                                                             vertical=True, min_facings_in_block=facings,
                                                                             **filters)
            if is_blocked and n_shelves >= min_shelf_num:
                score = 1
        return score, score, 1

    def block_together(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        score_pass = True

        for value in map(unicode.strip, params['Param (1) Values'].split(',')):
            if type3.strip():
                filters = {type1: value, type2: value2, type3: value3}
            else:
                filters = {type1: value, type2: value2}

            if score_pass:
                for scene in general_filters['scene_id']:
                    if score_pass:
                        score_pass = self.tools.calculate_block_together(include_empty=False, minimum_block_ratio=0.9,
                                                                         allowed_products_filters={
                                                                             'product_type': 'Other'},
                                                                         **dict(filters, **{'scene_id': scene}))
                    else:
                        return False
            else:
                return False
        return score_pass

    def calculate_survey(self, params):
        """
        This function calculates Survey-Question typed Atomics, and writes the result to the DB.
        """
        survey_name = params['Param (1) Values']
        target_answers = params['Param (2) Values'].split(',')
        survey_answer = self.tools.get_survey_answer(('question_text', survey_name))
        score = 1 if survey_answer in target_answers else 0
        return score, score, 1

    def calculate_sos_facings(self, params, **general_filters):
        numerator_filters, denominator_filters, target = self.get_sos_filters(params)
        numerator_result = self.calculate_facings_share_of_display(numerator_filters, include_empty=True,
                                                                   **general_filters)
        denominator_result = self.calculate_facings_share_of_display(denominator_filters, include_empty=True,
                                                                     **general_filters)
        ratio = 0 if denominator_result == 0 else numerator_result / float(denominator_result) * 100
        # score = 1 if ratio >= target else 0
        score = min(ratio / target, 1) if target else 0
        return score, str(ratio), str(target)

    def get_sos_filters(self, params):
        type1_n = params['Param Type (1)/ Numerator']
        value1_n = map(unicode.strip, params['Param (1) Values'].split(','))
        type1_1_n = params['Param Type (1-1)']
        value1_1_n = map(unicode.strip, params['Param (1-1) Values'].split(','))

        type2_d = params['Param Type (2)/ Denominator']
        value2_d = map(unicode.strip, params['Param (2) Values'].split(','))
        type2_1_d = params['Param Type (2-1)']
        value2_1_d = map(unicode.strip, params['Param (2-1) Values'].split(','))

        target = params['Target Policy']
        try:
            target = float(target)
        except:
            Log.info('The target: {} cannot parse to float'.format(str(target)))

        numerator_filters = {type1_n: value1_n, type2_d: value2_d}
        denominator_filters = {type2_d: value2_d}

        if type1_1_n:
            numerator_filters.update({type1_1_n: value1_1_n})

        if type2_1_d:
            numerator_filters.update({type2_1_d: value2_1_d})
            denominator_filters.update({type2_1_d: value2_1_d})

        return numerator_filters, denominator_filters, target

    def calculate_sos_linear_ign_stack(self, params, **general_filters):
        numerator_filters, denominator_filters, target = self.get_sos_filters(params)
        numerator_res = self.tools.calculate_linear_share_of_display(numerator_filters, include_empty=True,
                                                                     **general_filters)
        denominator_res = self.tools.calculate_linear_share_of_display(denominator_filters, include_empty=True,
                                                                       **general_filters)
        ratio = 0 if denominator_res == 0 else numerator_res / float(denominator_res) * 100
        # score = 1 if ratio >= target else 0
        score = min(ratio/target, 1) if target else 0
        return score, str(ratio), str(target)

    def calculate_relative_position(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = params['Param (1) Values']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']

        block_products1 = {type1: value1, type3: value3}
        block_products2 = {type2: value2, type3: value3}
        if type1 == type2:
            filters = {type1: [value1, value2], type3: value3}
        else:
            filters = {type1: value1, type2: value2, type3: value3}

        try:
            filters.pop('')
        except:
            pass
        try:
            block_products1.pop('')
        except:
            pass
        try:
            block_products2.pop('')
        except:
            pass
        score = self.tools.calculate_block_together(include_empty=False, minimum_block_ratio=0.9,
                                                    allowed_products_filters={'product_type': 'Other'},
                                                    block_of_blocks=True, block_products1=block_products1,
                                                    block_products2=block_products2,
                                                    **dict(filters, **general_filters))
        return score, score, 1

    def calculate_availability(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']

        for value in map(unicode.strip, params['Param (1) Values'].split(',')):
            filters = {type1: value, type2: value2, type3: value3}
            if self.tools.calculate_availability(**dict(filters, **general_filters)) > 0:
                return True, True, True
        return False, False, True

    def calculate_shelf_position(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = params['Param (1) Values']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        if type3.strip():
            filters = {type1: value1, type2: value2, type3: value3}
        else:
            filters = {type1: value1, type2: value2}
        target = params['Target Policy']
        target = map(int, target.split(','))
        product_fk_codes = self.scif[self.tools.get_filter_condition(self.scif,
                                                                     **dict(filters, **general_filters))][
            'product_fk'].unique().tolist()
        shelf_list = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene,
                                                                                 **dict({'product_fk': product_fk_codes,
                                                                                         'scene_fk': general_filters[
                                                                                             'scene_id']}))][
            'shelf_number'].unique()
        score = len(set(shelf_list) - set(target))
        if score > 0:
            return False, False, True
        else:
            return True, True, True

    @staticmethod
    def split_and_strip(string):
        return map(lambda x: x.strip(' '), str(string).split(',')) if string else []

    def calculate_linear_share_of_shelf_per_product_display(self):
        # display_agg, display_filter_from_scif = self.get_display_agg()
        # display_agg = self.get_display_agg()
        display_agg = self.display_scene_count
        for display in display_agg['display_name'].unique().tolist():
            display_pd = display_agg[display_agg['display_name'] == display]
            display_weight = self.get_display_weight_by_display_name(display)
            display_count = sum(display_pd['count'].tolist())
            general_filters = {'scene_id': display_pd['scene_fk'].tolist()}
            display_width = self.tools.calculate_share_space_length(**general_filters)
            for product in self.matches['item_id'].unique().tolist():
                general_filters.update({'item_id': product})
                product_width = self.tools.calculate_share_space_length(**general_filters)
                if product_width and display_width:
                    score = (product_width / float(display_width)) * display_weight * display_count
                    level_fk = self.get_new_kpi_fk_by_kpi_name('display count')
                    self.common.write_to_db_result(fk=level_fk, score=score, result=score,
                                                              numerator_result=product_width,
                                                              denominator_result=display_width,
                                                              numerator_id=product,
                                                              denominator_id=display_pd['pk'].values[0],
                                                              target=display_weight * display_count)
                    # self.common.write_to_db_result_new_tables(fk=level_fk, score=score, result=score,
                    #                                           numerator_result=product_width,
                    #                                           denominator_result=display_width,
                    #                                           numerator_id=product,
                    #                                           denominator_id=display_pd['pk'].values[0],
                    #                                           target=display_weight * display_count)

    def get_display_weight_by_display_name(self, display_name):
        assert isinstance(display_name, unicode), "name is not a string: %r" % display_name
        return float(
            self.display_data[self.display_data[self.DISPLAYS] == display_name][self.WEIGHT].values[0])

    def get_kpi_fk_by_kpi_name(self, kpi_name):
        assert isinstance(kpi_name, unicode), "name is not a string: %r" % kpi_name
        try:
            return self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].values[0]
        except IndexError:
            Log.info('Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def calculate_sod_by_filters(self, displays, numerator_filters, denominator_filters):
        """
        :return: The Linear SOS ratio.
        """
        numerator_score = denominator_score = 0

        for y, display in displays.iterrows():
            display_weight = self.get_display_weight_by_display_name(display['display_name'])
            display_count = display['count']
            general_filters = {'scene_id': display['scene_fk']}

            numerator_width = self.tools.calculate_linear_share_of_display(numerator_filters, **general_filters)
            numerator_width *= (display_weight * display_count)
            numerator_score += numerator_width

            denominator_width = self.tools.calculate_linear_share_of_display(denominator_filters, **general_filters)
            denominator_width *= (display_weight * display_count)
            denominator_score += denominator_width

        if denominator_score == 0:
            ratio = 0
        else:
            ratio = numerator_score / float(denominator_score)
        return ratio

    def get_new_kpi_fk_by_kpi_name(self, kpi_name):
        """
        convert kpi name to kpi_fk
        :param kpi_name: string
        :return: fk
        """
        assert isinstance(kpi_name, (unicode, basestring)), "name is not a string: %r" % kpi_name
        column_key = 'pk'
        column_value = 'client_name'
        try:
            return self.new_kpi_static_data[
                self.new_kpi_static_data[column_value] == kpi_name][column_key].values[0]
        except IndexError:
            Log.info('Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def write_to_db_result(self, fk, level, score=None, result=None, result_2=None, logical_operator=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        # assert isinstance(fk, int), "fk is not a int: %r" % fk
        # assert isinstance(score, float), "score is not a float: %r" % score
        attributes = self.create_attributes_dict(fk, score, result, result_2, level, logical_operator)
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

    def create_attributes_dict(self, fk, score=None, result=None, result_2=None, level=None, logical_operator=None):
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
                                        score, result, result_2, kpi_fk, fk, logical_operator)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'threshold', 'kpi_fk',
                                               'atomic_kpi_fk', 'kpi_logical_operator'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        # self.connect_rds_if_disconnected()
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGRO_PRODQueries.get_delete_session_results_query(self.session_uid)
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

    def get_scene_display_bay(self):
        secondary_shelfs = self.scif.loc[self.scif['template_group'] == 'Secondary Shelf'][
            'scene_id'].unique().tolist()
        display_filter_from_scif = self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk']
            .isin(secondary_shelfs)]
        return display_filter_from_scif

    def get_display_agg(self):
        scene_display_bay = self.scene_display_bay
        scene_display_bay['count'] = 0
        return \
            scene_display_bay.groupby(['scene_fk', 'display_name', 'pk'], as_index=False).agg({'count': np.size})

    def get_eye_level_shelf_data(self):
        eye_level_targets = parse_template(TEMPLATE_PATH, 'eye_level_parameters')
        eye_level_targets = eye_level_targets.astype('int64')
        return eye_level_targets

    def calculate_facings_share_of_display(self, sos_filters, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The facings SOS ratio.
        """
        if include_empty == self.EXCLUDE_EMPTY:
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)

        numerator_facings = self.calculate_facings(**dict(sos_filters, **general_filters))
        denominator_facings = self.calculate_facings(**general_filters)

        if denominator_facings == 0:
            ratio = 0
        else:
            ratio = numerator_facings / float(denominator_facings)
        return ratio

    def calculate_facings(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of facings.
        """
        filtered_scif = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        facings = filtered_scif['facings_ign_stack'].sum()
        return facings

    def calculate_display_presence(self, params, **general_filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of facings.
        """
        filters, target = self.get_eye_lvl_or_display_filters_for_kpi(params)
        filters.update(general_filters)
        number_of_displays = self.scif[self.tools.get_filter_condition(self.scif, **filters)]['facings'].sum()
        # number_of_displays = self.display_scene_count[self.tools.get_filter_condition(self.display_scene_count,
        #                                                                               **filters)]['count'].sum()
        score = min(number_of_displays/target, 1)
        # score = 1 if number_of_displays >= target else 0
        return score, number_of_displays, target

    def get_number_of_displays_in_scene(self):
        display_scene_count = self.display_scene_count
        displays_per_scene = display_scene_count.groupby(['scene_fk'], as_index=False).agg({'count': np.sum})
        return displays_per_scene

    def update_scene_filter(self, params, gen_filters, by_location=False, product_filters=None, by_product_majority=False):
        if by_location:
            if params['Location Type'] == 'Secondary Shelf':
                scenes_with_one_display = self.displays_per_scene[self.displays_per_scene['count'] == 1]['scene_fk'].unique().tolist()
                gen_filters.update({'scene_id': scenes_with_one_display})
        if product_filters:
            if gen_filters['scene_id']:
                if not by_product_majority:
                    product_filters.update(gen_filters)
                    filtered_scif = self.scif[self.tools.get_filter_condition(self.scif, **product_filters)]
                    scenes = filtered_scif['scene_id'].unique().tolist()
                    gen_filters.update({'scene_id': scenes})
                    return gen_filters

                passed_scenes = []
                for scene in gen_filters['scene_id']:
                    scene_filter = {'scene_id': scene}
                    denom_filters = gen_filters.copy()
                    denom_filters.update(scene_filter)
                    product_filters.update(denom_filters)
                    total_facings = self.scif[self.tools.get_filter_condition(self.scif, **denom_filters)]['facings'].sum()
                    filtered_scif = self.scif[self.tools.get_filter_condition(self.scif, **product_filters)]
                    relevant_categories_df = filtered_scif[['category', 'facings']].groupby(['category']).sum()
                    relevant_categories_df['cat_share'] = relevant_categories_df['facings']/total_facings
                    if len(relevant_categories_df[relevant_categories_df['cat_share'] > 0.5]) >= 1:
                        passed_scenes.append(scene)
                gen_filters.update({'scene_id': passed_scenes})
        return gen_filters

    def calculate_product_presence(self, params, **general_filters):
        group_1_filters, group_2_filters, facings = self.get_adjacency_and_product_presence_filters(params)

        type3_policy = self.split_and_strip(params['Param (3)'])
        value3_policy = self.split_and_strip(params['Param (3) Values'])
        manufacturer = {'manufacturer_name': params['Manufacturer']}
        score_type = params['KPI Calc. Type']
        target_kpi = float(params['Target Policy'])

        # filtering out scenes in case we need select scenes that follow certain product policy
        if type3_policy:
            is_prod_majority = True if len(type3_policy) > 1 else False
            product_policy = {type3_policy[0]: value3_policy}
            product_policy.update(manufacturer)
            general_filters = self.update_scene_filter(params, general_filters, product_filters=product_policy,
                                                       by_product_majority=is_prod_majority)

        target_scenes = float(len(general_filters['scene_id']))
        number_of_scenes_pass = 0
        if general_filters['scene_id']:
            group_1_filters.update(general_filters)
            group_2_filters.update(general_filters)

            group_1_facings = self.scif[self.tools.get_filter_condition(self.scif, **group_1_filters)]
            group_1_facings_scene = group_1_facings[['scene_id', 'facings']].groupby(['scene_id'], as_index=False).agg(
                {'facings': np.sum})

            group_2_facings = self.scif[self.tools.get_filter_condition(self.scif, **group_2_filters)]
            group_2_facings_scene = group_2_facings[['scene_id', 'facings']].groupby(['scene_id'], as_index=False).agg(
                {'facings': np.sum})

            merged = group_2_facings_scene.merge(group_1_facings_scene, left_on='scene_id', right_on='scene_id',
                                                 how='outer')
            scenes_pass = merged[(merged['facings_x'] >= facings) & (merged['facings_y'] >= facings)]
            number_of_scenes_pass = len(scenes_pass)

        if score_type == self.BINARY:
            result = number_of_scenes_pass
            score = 1 if result > 0 else 0
        else:
            result = number_of_scenes_pass/target_scenes * 100 if target_scenes else 0
            # score = 1 if result >= target_kpi else 0
            score = min(result/target_kpi, 1) if target_kpi else 0
        return score, result, target_kpi

    def get_adjacency_and_product_presence_filters(self, params):
        type1 = params['Param Type (1)/ Numerator']
        value1 = self.split_and_strip(params['Param (1) Values'])
        type1_1 = params['Param Type (1-1)']
        value1_1 = self.split_and_strip(params['Param (1-1) Values'])
        type1_2 = params['Param Type (1-2)']
        value1_2 = self.split_and_strip(params['Param (1-2) Values'])

        type2 = params['Param Type (2)/ Denominator']
        value2 = self.split_and_strip(params['Param (2) Values'])
        type2_1 = params['Param Type (2-1)']
        value2_1 = self.split_and_strip(params['Param (2-1) Values'])
        type2_2 = params['Param Type (2-2)']
        value2_2 = self.split_and_strip(params['Param (2-2) Values'])

        facings = float(params['Facings']) if params['Facings'] else None
        manufacturer = {'manufacturer_name': params['Manufacturer']}

        group_1_filters = dict({type1: value1}, **dict(manufacturer))
        if type1_1:
            group_1_filters.update({type1_1: value1_1})
        if type1_2:
            group_1_filters.update({type1_2: value1_2})

        group_2_filters = dict({type2: value2}, **dict(manufacturer))
        if type2_1:
            group_2_filters.update({type2_1: value2_1})
        if type2_2:
            group_2_filters.update({type2_2: value2_2})

        return group_1_filters, group_2_filters, facings

    def calculate_block_adjacency(self, params, **general_filters):
        score = 0
        if general_filters['scene_id']:
            group_1_filters, group_2_filters, facings = self.get_adjacency_and_product_presence_filters(params)
            group_1_filters.update(general_filters)
            group_2_filters.update(general_filters)

            scenes_filter = {'scene_fk': general_filters['scene_id']}
            is_adjacent = self.adjacency.calculate_adjacency(filter_group_a=group_1_filters, filter_group_b=group_2_filters,
                                                             scene_type_filter=scenes_filter, allowed_filter=[],
                                                             allowed_filter_without_other=[], a_target=None,
                                                             b_target=None, target=0.75)
            score = 1 if is_adjacent else 0
        return score, score, 1

# --------------------unused functions------------------------------#

    # def calculate_product_presence(self, params, total_displays, **general_filters):
    #     target = float(params['Target Policy'])
    #     number_of_displays_that_pass = 0
    #     # code to get the number of passing displays
    #
    #     # for display in scene_display_bay['display_name'].unique().tolist():
    #     #     display_pd = display_agg[display_agg['display_name'] == display]
    #     #     display_relevant_scenes = display_pd['scene_fk'].unique().tolist()
    #     #
    #     #     display_weight = self.get_display_weight_by_display_name(display)
    #     #     display_count = sum(display_pd['count'].tolist())
    #     #
    #     #     display_width = 0
    #     #     products_in_display_width = {}
    #     #
    #     #     for scene in display_relevant_scenes:
    #     #         scene_relevant_bays = display_pd[display_pd['scene_fk'] == scene]['bay_number'].unique().tolist()
    #     #         general_filters = {'scene_fk': scene, 'bay_number': scene_relevant_bays}
    #     #         filtered_matches = self.matches[self.get_filter_condition(self.matches, **general_filters)]
    #     #         display_width += filtered_matches['width_mm_advance'].sum()
    #     #         display_scene_products = filtered_matches['product_fk'].unique().tolist()
    #     #
    #     #         for product in display_scene_products:
    #     #             if not products_in_display_width.get(product):
    #     #                 products_in_display_width[product] = \
    #     #                 filtered_matches[filtered_matches['product_fk'] == product]['width_mm_advance'].sum()
    #     #             else:
    #     #                 products_in_display_width[product] = products_in_display_width[product] + filtered_matches[
    #     #                     filtered_matches['product_fk'] == product]['width_mm_advance'].sum()
    #     #
    #     #     for product, width in products_in_display.items():
    #     #         score = (products_in_display_width[product] / float(display_width)) * display_weight * display_count
    #     #         write_score_to_db()
    #
    #     result = number_of_displays_that_pass/total_displays * 100
    #     score = min(result/target, 100)
    #     return score, result, target

    # def get_display_agg(self):
        #     secondary_shelfs = self.scif.loc[self.scif['template_group'] == 'Secondary Shelf'][
        #         'scene_id'].unique().tolist()
        #     display_filter_from_scif = self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk']
        #         .isin(secondary_shelfs)]
        #     display_filter_from_scif['count'] = 0
        #     return \
        #         display_filter_from_scif.groupby(['scene_fk', 'display_name', 'pk'], as_index=False).agg({'count': np.size})

    # def calculate_shelf_position(self, params, **general_filters):
    #     type1 = params['Param Type (1)/ Numerator']
    #     value1 = params['Param (1) Values']
    #     type2 = params['Param Type (2)/ Denominator']
    #     value2 = params['Param (2) Values']
    #     type3 = params['Param (3)']
    #     value3 = params['Param (3) Values']
    #
    #     if type3.strip():
    #         filters = {type1: value1, type2: value2, type3: value3}
    #     else:
    #         filters = {type1: value1, type2: value2}
    #
    #     product_fk_codes = self.scif[self.tools.get_filter_condition(self.scif,
    #                                                                  **dict(filters, **general_filters))][
    #                                                                 'product_fk'].unique().tolist()
    #     bay_shelf_list = self.match_product_in_scene[self.tools.get_filter_condition(
    #         self.match_product_in_scene,
    #         **dict({'product_fk': product_fk_codes, 'scene_fk': general_filters['scene_id']}))] \
    #         [['shelf_number', 'bay_number', 'scene_fk']]
    #
    #     bay_shelf_count = self.match_product_in_scene[['shelf_number', 'bay_number', 'scene_fk']].drop_duplicates()
    #     bay_shelf_count['count'] = 0
    #     bay_shelf_count = bay_shelf_count.groupby(['bay_number', 'scene_fk'], as_index=False).agg({'count': np.size})
    #
    #     for scene in general_filters['scene_id']:
    #         for bay in bay_shelf_list['bay_number'].unique().tolist():
    #             shelf_list = bay_shelf_list[(bay_shelf_list['bay_number'] == bay) & (bay_shelf_list['scene_fk'] == scene)]['shelf_number']
    #             target = self.eye_level_target.copy()
    #             number_of_shelves = bay_shelf_count[(bay_shelf_count['bay_number'] == bay) &
    #                                                 (bay_shelf_count['scene_fk'] == scene)]['count'].values[0]
    #             try:
    #                 target = target[target['Number of shelves'] == number_of_shelves][self.SHELF_NUMBERS].values[0]
    #             except IndexError:
    #                 target = '3,4,5'
    #             target = map(lambda x: int(x), target.split(','))
    #             score = len(set(shelf_list) - set(target))
    #             if score > 0:
    #                 return False
    #     if not bay_shelf_list.empty:
    #         return True
    #     else:
    #         return False

    # def calculate_sos(self, params, **general_filters):
    #     type1 = params['Param Type (1)/ Numerator']
    #     value1 = map(unicode.strip, params['Param (1) Values'].split(','))
    #     type2 = params['Param Type (2)/ Denominator']
    #     value2 = map(unicode.strip, params['Param (2) Values'].split(','))
    #     type3 = params['Param (3)']
    #     value3 = params['Param (3) Values']
    #     target = params['Target Policy']
    #     try:
    #         target = int(target)/100.0
    #     except:
    #         Log.info('The target: {} cannot parse to int'.format(str(target)))
    #
    #     numerator_filters = {type1: value1, type2: value2, type3: value3}
    #     denominator_filters = {type2: value2}
    #
    #     numerator_width = self.tools.calculate_linear_share_of_display(numerator_filters,
    #                                                                    include_empty=True,
    #                                                                    **general_filters)
    #     denominator_width = self.tools.calculate_linear_share_of_display(denominator_filters,
    #                                                                      include_empty=True,
    #                                                                      **general_filters)
    #
    #     if denominator_width == 0:
    #         ratio = 0
    #     else:
    #         ratio = numerator_width / float(denominator_width)
    #     if ratio >= target:
    #         return True, str(ratio), str(target)
    #     else:
    #         return False, str(ratio), str(target)


    # def calculate_eye_level(self, params, **general_filters):
    #     type1 = params['Param Type (1)/ Numerator']
    #     value1 = map(unicode.strip, params['Param (1) Values'].split(','))
    #     type2 = params['Param Type (2)/ Denominator']
    #     value2 = map(unicode.strip, params['Param (2) Values'].split(','))
    #     type3 = params['Param (3)']
    #     value3 = params['Param (3) Values']
    #     target = float(params['Target Policy'])
    #
    #     skus_at_eye_lvl = 0
    #     if general_filters['scene_id']:
    #         filters = {type1: value1, type2: value2, type3: value3, 'scene_fk': general_filters['scene_id']}
    #         filters.update(**general_filters)
    #         matches_products = self.match_product_in_scene.merge(self.all_products, left_on='product_fk',
    #                                                              right_on='product_fk', how='left')
    #         scene_bays = matches_products[self.tools.get_filter_condition(matches_products, **filters)][[
    #             'scene_fk', 'bay_number']].drop_duplicates()
    #         # do we just select shelves that have products of relevant category?
    #         for index, row in scene_bays.iterrows():
    #             total_num_of_shelves = matches_products[(matches_products['bay_number'] == row.bay_number) &
    #                                                     (matches_products['scene_fk'] == row.scene_fk)][
    #                                                                 'shelf_number_from_bottom'].max()
    #             shelves_in_eye_lvl = self.get_eye_level_shelves(total_num_of_shelves, self.eye_level_args)
    #             if shelves_in_eye_lvl:
    #                 scene_shelf_bay_matches = matches_products[(matches_products['bay_number'] == row.bay_number)&
    #                                                            (matches_products['shelf_number_from_bottom'].isin(
    #                                                                                                         shelves_in_eye_lvl))&
    #                                                            (matches_products['scene_fk'] == row.scene_fk)]
    #                 skus_at_eye_lvl += len(scene_shelf_bay_matches[self.tools.get_filter_condition(scene_shelf_bay_matches,
    #                                                                                                **filters)])
    #     score = min(skus_at_eye_lvl/target, 1)
    #     return score, skus_at_eye_lvl, target
    #

    # def get_eye_level_shelves(self, shelves_num, eye_lvl_template):
    #     """
    #     :param shelves_num: num of shelves in specific bay
    #     :return: list of eye shelves
    #     """
    #     res_table = eye_lvl_template[(eye_lvl_template["Number of shelves max"] >= shelves_num) & (
    #                 eye_lvl_template["Number of shelves min"] <= shelves_num)][["Ignore from top",
    #                                                                               "Ignore from bottom"]]
    #     if res_table.empty:
    #         return []
    #     start_shelf = res_table['Ignore from bottom'].iloc[0] + 1
    #     end_shelf = shelves_num - res_table['Ignore from top'].iloc[0]
    #     final_shelves = range(start_shelf, end_shelf + 1)
    #     return final_shelves