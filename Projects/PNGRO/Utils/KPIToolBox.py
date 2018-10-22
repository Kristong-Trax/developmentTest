import os
from datetime import datetime

import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector

from KPIUtils_v2.DB.Common import Common
from Projects.PNGRO.Utils.Fetcher import PNGRO_PRODQueries
from Projects.PNGRO.Utils.GeneralToolBox import PNGRO_PRODGENERALToolBox
from Projects.PNGRO.Utils.ParseTemplates import parse_template
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

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

    SOS_FACING = 'SOS Facing' #Natalya
    EYE_LEVEL = 'Eye Level' #Natalya

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
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
        # self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = PNGRO_PRODGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.display_data = parse_template(TEMPLATE_PATH, 'display weight')
        # self.eye_level_target = self.get_shelf_level_target()
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        # self.sbd_kpis_data = parse_template(TEMPLATE_PATH, 'SBD_kpis', lower_headers_row_index=1)
        self.sbd_kpis_data = self.get_relevant_sbd_kpis()
        self.common = Common(self.data_provider)
        self.new_kpi_static_data = self.common.get_new_kpi_static_data()

        self.main_shelves = self.are_main_shelves()
        self.assortment = Assortment(self.data_provider, self.output, common=self.common)
        self.eye_level_args = self.get_eye_level_shelf_data() #Natalya

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
        sbd_kpis = parse_template(TEMPLATE_PATH, 'SBD_kpis', lower_headers_row_index=1)
        retailer_targets = parse_template(TEMPLATE_PATH, 'retailer_targets', lower_headers_row_index=1)
        retailer_targets = retailer_targets[retailer_targets['retailer'] == self.retailer]
        relevant_df = sbd_kpis.merge()

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGRO_PRODQueries.get_all_kpi_data()
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGRO_PRODQueries.get_match_display(self.session_uid)
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_match_stores_by_retailer(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = PNGRO_PRODQueries.get_match_stores_by_retailer()
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_template_fk_by_category_fk(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = PNGRO_PRODQueries.get_template_fk_by_category_fk()
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_status_session_by_display(self, session_uid):
        query = PNGRO_PRODQueries.get_status_session_by_display(session_uid)
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_status_session_by_category(self, session_uid):
        query = PNGRO_PRODQueries.get_status_session_by_category(session_uid)
        # self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        if not self.rds_conn.is_connected:
            self.rds_conn.connect_rds()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # Assortment(self.data_provider, self.output, common=self.common).main_assortment_calculation()
        self.calculate_assortment_main_shelf()
        # if not self.match_display.empty:
        #     if self.match_display['exclude_status_fk'][0] in (1, 4):
        self.calculate_linear_share_of_shelf_per_product_display()

        category_status_ok = self.get_status_session_by_category(self.session_uid)['category_fk'].tolist()
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
                        #Natalya
                        # score, result, threshold = self.calculate_sos(params, **general_filters)
                        score, result, threshold = self.calculate_sos(params, is_linear=True, **general_filters)
                    elif kpi_type == self.RELATIVE_POSITION:
                        score = self.calculate_relative_position(params, **general_filters)
                    elif kpi_type == self.AVAILABILITY:
                        score = self.calculate_availability(params, **general_filters)
                    elif kpi_type == self.SHELF_POSITION:
                        score = self.calculate_shelf_position(params, **general_filters)
                    elif kpi_type == self.SURVEY:
                        score = self.calculate_survey(params)
                    #Natalya
                    elif kpi_type == self.SOS_FACING:
                        score, result, threshold = self.calculate_sos(params, is_linear=False, **general_filters)
                    elif kpi_type == self.EYE_LEVEL:
                        score, result, threshold = self.calculate_eye_level(params, **general_filters)

                    atomic_kpi_fk = self.get_kpi_fk_by_kpi_name(params[self.SBD_KPI_NAME])
                    if atomic_kpi_fk is not None:
                        if result and threshold:
                            self.write_to_db_result(score=int(score), result=float(result), result_2=float(threshold),
                                                    level=self.LEVEL3, fk=atomic_kpi_fk)
                        else:
                            self.write_to_db_result(score=int(score), level=self.LEVEL3, fk=atomic_kpi_fk)

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

    #Natalya
    def calculate_eye_level(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = map(unicode.strip, params['Param (1) Values'].split(','))
        type2 = params['Param Type (2)/ Denominator']
        value2 = map(unicode.strip, params['Param (2) Values'].split(','))
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        target = float(params['Target Policy'])
        score = 0 #?? what should it be if there is no kpi data?

        filters = {type1: value1, type2: value2, type3: value3}
        filters.update(**general_filters)

        skus_at_eye_lvl=0
        scene_bays = self.matches[self.tools.get_filter_condition(self.matches, **filters)][[
            'scene_fk', 'bay_number']].drop_duplicates()  # do we just select shelves that have products of relevant category?
                                                          # do we calculate for the session as a whole?
        for index, row in scene_bays.iterrows():
            total_num_of_shelves = self.match_product_in_scene[(self.match_product_in_scene['bay_number'] ==
                                                                row.bay_number) & (
                                                                       self.match_product_in_scene['scene_fk'] == row.scene_fk)][
                                                                'shelf_number_from_bottom'].max()
            shelves_in_eye_lvl = self.get_eye_level_shelves(total_num_of_shelves, self.eye_level_args)
            if shelves_in_eye_lvl:
                scene_shelf_bay_matches = self.match_product_in_scene[(self.match_product_in_scene['bay_number'] == row.bay_number)&
                                                               (self.match_product_in_scene['shelf_number_from_bottom'].isin(
                                                                                                        shelves_in_eye_lvl))&
                                                               (self.match_product_in_scene['scene_fk'] == row.scene_fk)]
                skus_at_eye_lvl += scene_shelf_bay_matches[self.tools.get_filter_condition(scene_shelf_bay_matches, **filters)]['facings']
        score = skus_at_eye_lvl/target * 100
        #result =  # what should I return?
        return score, score, target

    # Natalya
    def get_eye_level_shelves(self, shelves_num, eye_lvl_template):
        """
        :param shelves_num: num of shelves in specific bay
        :return: list of eye shelves
        """
        res_table = eye_lvl_template[(eye_lvl_template["Number of shelves max"] >= shelves_num) & (
                    eye_lvl_template["Number of shelves min"] <= shelves_num)][["Ignore from top",
                                                                                  "Ignore from bottom"]]
        if res_table.empty:
            return []
        start_shelf = res_table['Ignore from bottom'].iloc[0] + 1
        end_shelf = shelves_num - res_table['Ignore from top'].iloc[0]
        final_shelves = range(start_shelf, end_shelf + 1)
        return final_shelves

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
                self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                                          numerator_result=result.in_store, result=score,
                                                          denominator_id=result.assortment_group_fk, denominator_result=1,
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
                self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl2, numerator_id=result.assortment_group_fk,
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
        return score

    # Natalya
    def calculate_sos(self, params, is_linear, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = map(unicode.strip, params['Param (1) Values'].split(','))
        type2 = params['Param Type (2)/ Denominator']
        value2 = map(unicode.strip, params['Param (2) Values'].split(','))
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        target = params['Target Policy']
        try:
            target = int(target)/100.0
        except:
            Log.info('The target: {} cannot parse to int'.format(str(target)))

        numerator_filters = {type1: value1, type2: value2, type3: value3}
        denominator_filters = {type2: value2}

        if is_linear:
            numerator_result = self.tools.calculate_linear_share_of_display(numerator_filters,
                                                                            include_empty=True,
                                                                            **general_filters)
            denominator_result = self.tools.calculate_linear_share_of_display(denominator_filters,
                                                                              include_empty=True,
                                                                              **general_filters)
        else:
            numerator_result = self.tools.calculate_facings_share_of_display(numerator_filters,
                                                                             include_empty=True,
                                                                             **general_filters)
            denominator_result = self.tools.calculate_facings_share_of_display(denominator_filters,
                                                                               include_empty=True,
                                                                               **general_filters)
        if denominator_result == 0:
            ratio = 0
        else:
            ratio = numerator_result / float(denominator_result)
        if ratio >= target:
            return True, str(ratio), str(target)
        else:
            return False, str(ratio), str(target)

    #Natalya commented out
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
        return score

    def calculate_availability(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']

        for value in map(unicode.strip, params['Param (1) Values'].split(',')):
            filters = {type1: value, type2: value2, type3: value3}
            if self.tools.calculate_availability(**dict(filters, **general_filters)) > 0:
                return True
        return False

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
            return False
        else:
            return True

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

    def calculate_linear_share_of_shelf_per_product_display(self):
        display_agg = self.get_display_agg()
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
                    self.common.write_to_db_result_new_tables(fk=level_fk, score=score, result=score,
                                                              numerator_result=product_width,
                                                              denominator_result=display_width,
                                                              numerator_id=product,
                                                              denominator_id=display_pd['pk'].values[0],
                                                              target=display_weight * display_count)

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

    def write_to_db_result(self, fk, level, score=None, result=None, result_2=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        # assert isinstance(fk, int), "fk is not a int: %r" % fk
        # assert isinstance(score, float), "score is not a float: %r" % score
        attributes = self.create_attributes_dict(fk, score, result, result_2, level)
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

    def create_attributes_dict(self, fk, score=None, result=None, result_2=None, level=None):
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
                                        score, result, result_2, kpi_fk, fk)],
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
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
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

    def get_display_agg(self):
        secondary_shelfs = self.scif.loc[self.scif['template_group'] == 'Secondary Shelf'][
            'scene_id'].unique().tolist()
        display_filter_from_scif = self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk']
            .isin(secondary_shelfs)]
        display_filter_from_scif['count'] = 0
        return \
            display_filter_from_scif.groupby(['scene_fk', 'display_name', 'pk'], as_index=False).agg({'count': np.size})

    # def get_shelf_level_target(self):
    #     eye_level_target = parse_template(TEMPLATE_PATH, 'Eye-level')
    #     return eye_level_target[eye_level_target['Retailer'] == self.retailer][[self.SHELF_NUMBERS,
    #                                                                             self.NUMBER_OF_SHELVES]]

    def get_eye_level_shelf_data(self):
        eye_level_targets = parse_template(TEMPLATE_PATH, 'eye_level_parameters')
        return eye_level_targets

