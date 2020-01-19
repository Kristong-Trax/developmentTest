# -*- coding: utf-8 -*-
# import ast
import json
import datetime as dt
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.Utils.Parsers import ParseInputKPI as Parser
from Projects.MARSRU_PROD.Utils.KPIFetcher import MARSRU_PRODKPIFetcher
# from Projects.MARSRU_PROD.Utils.PositionGraph import MARSRU_PRODPositionGraphs


__author__ = 'urid'


BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
MARS = 'Mars'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
EMPTY = 'Empty'
OTHER = 'Other'
ALLOWED_EMPTIES_RATIO = 0.2
ALLOWED_DEVIATION = 2
ALLOWED_DEVIATION_2018 = 3
NEGATIVE_ADJACENCY_RANGE = (2, 1000)
POSITIVE_ADJACENCY_RANGE = (0, 1)

OSA_KPI_NAME = 'OSA'

PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'
SESSION_FK = 'session_fk'
SCENE_FK = 'scene_fk'
PRODUCT_FK = 'product_fk'
IN_ASSORTMENT = 'in_assortment_osa'
IS_OOS = 'oos_osa'
OTHER_CUSTOM_SCIF_COLUMNS = ['length_mm_custom', 'mha_in_assortment', 'mha_oos']
OTHER_CUSTOM_SCIF_COLUMNS_VALUES = (0, 0, 0)

# MARS_FACINGS_PER_SCENE_TYPE
MARS_FACINGS_PER_SCENE_TYPE_KPI_NAME = 'MARS_FACINGS_PER_SCENE_TYPE'
MOTIVATION_PROGRAM_SCENE_TYPE_NAME = 'Мотивационная программа'

EXCLUDE_EMPTY = False
INCLUDE_EMPTY = True


class MARSRU_PRODKPIToolBox:

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, kpi_templates, data_provider, output, ignore_stacking=True):
        self.kpi_templates = kpi_templates
        self.data_provider = data_provider
        self.output = output
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        try:
            self.products['sub_brand'] = self.products['Sub Brand']  # the sub_brand column is empty
        except:
            pass
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manufacturer_id = int(
            self.data_provider[Data.OWN_MANUFACTURER][
                self.data_provider[Data.OWN_MANUFACTURER]['param_name'] == 'manufacturer_id'][
                'param_value'].tolist()[0])
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        try:
            self.scif['sub_brand'] = self.scif['Sub Brand']  # the sub_brand column is empty
        except:
            pass
        self.kpi_fetcher = MARSRU_PRODKPIFetcher(self.project_name, self.kpi_templates, self.scif,
                                                 self.match_product_in_scene,
                                                 self.products, self.session_uid)
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.survey_response_scene = self.kpi_fetcher.get_scene_survey_responses(self.session_fk)
        self.ignore_stacking = ignore_stacking
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'

        self.region = self.get_store_att10() if str(self.visit_date) >= '2019-12-29' else self.get_store_att5()

        self.attr6 = self.get_store_att6()
        self.store_num_1 = self.get_store_number_1_attribute()
        self.results_and_scores = {}
        self.result_df = []
        self.writing_to_db_time = dt.timedelta(0)
        self.kpi_results_queries = []
        # self.position_graphs = MARSRU_PRODPositionGraphs(self.data_provider, rds_conn=self.rds_conn)
        self.potential_products = {}
        self.custom_scif_queries = \
            pd.DataFrame(columns=[SESSION_FK, SCENE_FK, PRODUCT_FK, IN_ASSORTMENT, IS_OOS] + OTHER_CUSTOM_SCIF_COLUMNS)
        self.shelf_square_boundaries = {}
        self.object_type_conversion = {'SKUs': 'product_ean_code',
                                       'BRAND': 'brand_name',
                                       'BRAND in CAT': 'brand_name',
                                       'CAT': 'category',
                                       'MAN in CAT': 'category',
                                       'MAN': 'manufacturer_name'}
        self.common = Common(self.data_provider)
        self.osa_kpi_dict = {}
        self.kpi_count = {}
        self.assortment_products = self.get_assortment_for_store()
        self.parser = Parser

    def check_connection(self, rds_conn):
        try:
            rds_conn.db.cursor().execute(
                "select pk from probedata.session where session_uid = '{}';"
                    .format(self.session_uid))
        except:
            rds_conn.disconnect_rds()
            rds_conn.connect_rds()
            Log.debug('DB is reconnected')
            return False
        return True

    @kpi_runtime()
    def check_for_specific_display(self, params):
        """
        This function checks if a specific display( = scene type) exists in a store
        """
        formula_type = 'check_specific_display'
        for p in params:
            if p.get('Formula') != formula_type:
                continue

            result = 'TRUE'

            if p.get('Values'):
                filtered_scif = self.scif.loc[self.scif['template_name'].isin(p.get('Values').split('\n'))]
            else:
                filtered_scif = self.scif

            if p.get('Scene type'):
                filtered_scif = filtered_scif.loc[filtered_scif['template_name'].isin(p.get('Scene type').split(', '))]

            if p.get('Location type'):
                filtered_scif = filtered_scif.loc[filtered_scif['location_type'].isin(p.get('Location type').split(', '))]

            if filtered_scif.empty:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def get_product_fk(self, sku_list):
        """
        This function gets a list of SKU and returns a list of the product fk of those SKU list
        """
        product_fk_list = []
        for sku in sku_list:
            temp_df = self.products.loc[self.products['product_ean_code'] == sku]['product_fk']
            product_fk_list.append(int(temp_df.values[0]))
        return product_fk_list

    @kpi_runtime()
    def check_availability_on_golden_shelves(self, params):
        """
        This function is used to calculate availability for given SKU on golden shelves (#3,4 from bottom)
        """
        formula_type = 'availability_on_golden_shelves'
        for p in params:
            if p.get('Formula') != formula_type:
                continue

            result = 'TRUE'
            relevant_shelves = [3, 4]
            golden_shelves_filtered_df = self.match_product_in_scene.loc[
                (self.match_product_in_scene['shelf_number_from_bottom'].isin(relevant_shelves))]

            if golden_shelves_filtered_df.empty:
                Log.debug("In the session {} there are not shelves that stands in the "
                          "criteria for availability_on_golden_shelves KPI".format(self.session_uid))
                result = 'FALSE'
            else:
                sku_list = p.get('Values').split()
                product_fk_list = self.get_product_fk(sku_list)
                bays = golden_shelves_filtered_df['bay_number'].unique().tolist()
                for bay in bays:
                    current_bay_filter = golden_shelves_filtered_df.loc[golden_shelves_filtered_df['bay_number'] == bay]
                    shelves = current_bay_filter['shelf_number'].unique().tolist()
                    for shelf in shelves:
                        shelf_filter = current_bay_filter.loc[current_bay_filter['shelf_number'] == shelf]
                        filtered_shelf_by_products = shelf_filter.loc[(
                            shelf_filter['product_fk'].isin(product_fk_list))]
                        if len(filtered_shelf_by_products['product_fk'].unique()) != len(product_fk_list):
                            result = 'FALSE'
                            break
                    if result == 'FALSE':
                        break

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def get_store_att5(self):
        store_att = self.kpi_fetcher.get_store_att5(self.store_id)
        return store_att

    def get_store_att6(self):
        store_att = self.kpi_fetcher.get_store_att6(self.store_id)
        return store_att

    def get_store_att10(self):
        store_att = self.kpi_fetcher.get_store_att10(self.store_id)
        return store_att

    def get_store_number_1_attribute(self):
        store_number_1 = self.kpi_fetcher.get_store_number_1(self.store_id)
        return store_number_1

    def get_assortment_for_store(self):
        # assortment_products = self.kpi_fetcher.get_store_assortment(self.store_id, self.visit_date)
        assortment_products = Assortment(self.data_provider, self.output, common=self.common)\
            .get_lvl3_relevant_ass()
        if not assortment_products.empty:
            assortment_groups = [0] + assortment_products['assortment_group_fk'].unique().tolist()
            assortment_group = self.kpi_fetcher.get_relevant_assortment_group(assortment_groups)
            assortment_products = assortment_products[assortment_products['assortment_group_fk'] == assortment_group]

        if assortment_products.empty:
            Log.warning('Error. No relevant OSA Assortment was found. Store ID: {}'.format(self.store_id))

        return assortment_products

    def get_custom_query_values(self, scene_fk, product_fk, assortment, oos):
        """
        This gets the query for insertion to PS custom scif
        :param scene_fk:
        :param product_fk:
        :param assortment:
        :param oos:
        :return:
        """
        attributes_df = pd.DataFrame([(self.session_fk, scene_fk, product_fk, assortment, oos) +
                                      OTHER_CUSTOM_SCIF_COLUMNS_VALUES],
                                     columns=[SESSION_FK, SCENE_FK, PRODUCT_FK, IN_ASSORTMENT, IS_OOS] +
                                             OTHER_CUSTOM_SCIF_COLUMNS)
        self.custom_scif_queries = self.custom_scif_queries.append(attributes_df, ignore_index=True)

    def get_scenes_for_product(self, product_fk):
        """
        This function find all scene_fk where a product was in.
        :param product_fk:
        :return: a list of scenes_fks
        """
        product_scif = self.scif.loc[self.scif['product_fk'] == product_fk]
        scenes = product_scif['scene_fk'].unique().tolist()
        return scenes

    def commit_custom_scif(self):
        self.check_connection(self.rds_conn)
        delete_query = self.kpi_fetcher.get_delete_session_custom_scif(self.session_fk)
        cur = self.rds_conn.db.cursor()
        try:
            cur.execute(delete_query)
        except Exception as ex:
            Log.error('could not run delete query: {}, error {}'.format(delete_query, ex))
            return
        if self.custom_scif_queries.empty:
            return
        self.custom_scif_queries.drop_duplicates(inplace=True)
        values_dict = self.custom_scif_queries.to_dict()
        insert_query = insert(values_dict, PSERVICE_CUSTOM_SCIF)
        try:
            cur.execute(insert_query)
        except Exception as ex:
            Log.error('could not run insert query: {}, error {}'.format(insert_query, ex))
            return
        self.rds_conn.db.commit()

    @kpi_runtime()
    def handle_update_custom_scif(self):
        """
        This function updates the custom scif of PS with oos and assortment values for each product in each scene.
        :return:
        """
        if not self.store_num_1:
            return
        Log.debug("Updating PS Custom SCIF... ")
        if not self.assortment_products.empty:
            assortment_products = self.assortment_products['product_fk'].tolist()
            for scene in self.scif['scene_fk'].unique().tolist():
                products_in_scene = self.scif[(self.scif['scene_fk'] == scene) &
                                              (self.scif['facings'] > 0)]['product_fk'].unique().tolist()
                for product in assortment_products:
                    if product in products_in_scene:
                        # This means the product in assortment and is not oos (1,0)
                        self.get_custom_query_values(scene, product, 1, 0)
                    else:
                        # The product is in assortment list but is oos (1,1)
                        self.get_custom_query_values(scene, product, 1, 1)

                for product in products_in_scene:
                    if product not in assortment_products:
                        # The product is not in assortment list and not oos (0,0)
                        self.get_custom_query_values(scene, product, 0, 0)

        Log.debug("Done updating PS Custom SCIF... ")
        self.commit_custom_scif()

    def get_static_list(self, object_type):
        object_static_list = []
        if object_type == 'SKUs':
            object_static_list = self.products['product_ean_code'].values.tolist()
        elif object_type == 'CAT' or object_type == 'MAN in CAT':
            object_static_list = self.products['category'].values.tolist()
        elif object_type == 'BRAND':
            object_static_list = self.products['brand_name'].values.tolist()
        elif object_type == 'MAN':
            object_static_list = self.products['manufacturer_name'].values.tolist()
        else:
            Log.debug('The type {} does not exist in the data base'.format(object_type))
        return object_static_list

    @kpi_runtime()
    def check_availability(self, params):
        """
        This function is used to calculate availability given a set pf parameters

        """
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT', 'BRAND in CAT']
        formula_types = ['number of SKUs', 'number of facings', 'number of facings on the first scene']
        for p in params:
            if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
                continue

            result = self.calculate_availability(p)

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_availability(self, params, scenes=None, formula=None, values_list=None, object_type=None,
                               include_stacking=False):
        if not values_list:
            val = params.get('Values')
            val = str(val.encode('utf8')) if isinstance(val, (str, unicode)) else str(val)
            if '*' in val:
                values_list = val.split(', *')
            else:
                values_list = val.split(', ') if params.get('Values') else []

        if not formula:
            formula = params.get('Formula')

        if not scenes:
            scenes = self.get_relevant_scenes(params)

        if formula == 'number of facings on the first scene':
            scenes = [self.get_first_created_scene(scenes)]

        if params.get("Manufacturer"):
            manufacturers = [str(manufacturer)
                             for manufacturer in params.get("Manufacturer").split(", ")]
        else:
            manufacturers = []

        if params.get("Category"):
            categories = [str(categories)
                          for categories in params.get("Category").split(", ")]
        else:
            categories = []

        if params.get("Form Factor to include"):
            form_factors = [str(form_factor)
                            for form_factor in params.get("Form Factor to include").split(", ")]
        else:
            form_factors = []

        if params.get("Form Factor to exclude"):
            form_factors_to_exclude = [str(form_factor) for form_factor in
                                       params.get("Form Factor to exclude").split(", ")]
        else:
            form_factors_to_exclude = []

        if params.get('Sub brand to include'):
            sub_brands = [str(sub_brand)
                          for sub_brand in params.get('Sub brand to include').split(", ")]
        else:
            sub_brands = []

        if params.get('Sub brand to exclude'):
            sub_brands_to_exclude = [str(sub_brand)
                                     for sub_brand in params.get('Sub brand to exclude').split(", ")]
        else:
            sub_brands_to_exclude = []

        if params.get('Client Sub Category Name to include'):
            cl_sub_cats = [str(cl_sub_cat) for cl_sub_cat in params.get(
                'Client Sub Category Name to include').split(", ")]
        else:
            cl_sub_cats = []

        if params.get('Client Sub Category Name to exclude'):
            cl_sub_cats_to_exclude = [str(cl_sub_cat) for cl_sub_cat in params.get(
                'Client Sub Category Name to exclude').split(", ")]
        else:
            cl_sub_cats_to_exclude = []

        if params.get('Include Stacking'):
            include_stacking = True

        if object_type:
            availability_type = object_type
        else:
            availability_type = params.get('Type')

        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, availability_type,
                                                             formula=formula,
                                                             form_factors=form_factors,
                                                             manufacturers=manufacturers,
                                                             sub_brands=sub_brands,
                                                             sub_brands_to_exclude=sub_brands_to_exclude,
                                                             cl_sub_cats=cl_sub_cats,
                                                             cl_sub_cats_to_exclude=cl_sub_cats_to_exclude,
                                                             include_stacking=include_stacking,
                                                             form_factors_to_exclude=form_factors_to_exclude,
                                                             categories=categories)

        return object_facings

    def get_relevant_scenes(self, params):
        scif = self.scif

        template_groups = unicode(params.get('Template group')).split(', ') if params.get('Template group') else None
        if template_groups:
            scif = scif.loc[scif['template_group'].isin(template_groups)]

        locations = str(params.get('Location type')).split(', ') if params.get('Location type') else None
        if locations:
            scif = scif.loc[scif['location_type'].isin(locations)]

        scene_types = str(params.get('Scene type')).split(', ') if params.get('Scene type') else None
        if scene_types:
            scif = scif.loc[scif['template_name'].isin(scene_types)]

        scenes = scif['scene_id'].unique().tolist()

        return scenes

    @kpi_runtime()
    def check_number_of_scenes(self, params):
        """
        This function is used to calculate number of scenes

        """
        for p in params:
            if p.get('Formula') not in ['number of scenes', 'number of scenes vs target']:
                continue

            result = 0
            scenes = self.get_relevant_scenes(p)

            p_copy = p.copy()
            p_copy["Formula"] = "number of facings"
            for scene in scenes:
                if self.calculate_availability(p_copy,
                                               scenes=[scene],
                                               values_list=p_copy.get('Values').split(', ')) > 0:
                    res = 1
                else:
                    res = 0
                result += res

            if p.get('Formula') == 'number of scenes vs target':
                if p.get('Target') and result >= p.get('Target'):
                    result = 1
                else:
                    result = 0

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def check_survey_answer(self, params):
        """
        This function is used to calculate survey answer according to given format

        """
        d = {'Yes': [u'Да', u'ДА', u'да', u'Yes', u'YES', u'yes', u'True', u'TRUE', u'true', u'1'],
             'No': [u'Нет', u'НЕТ', u'нет', u'No', u'NO', u'no', u'False', u'FALSE', u'false', u'0']}
        for p in params:

            if p.get('Formula') not in ['answer for survey', 'answer for survey on the first scene']:
                continue

            survey_question_code = str(int(p.get('Values')))

            if p.get('Formula') == 'answer for survey on the first scene':
                scene = self.get_first_created_scene(self.get_relevant_scenes(p))
                survey_data = \
                    self.survey_response_scene.loc[(self.survey_response_scene['scene_fk'] == scene) &
                                                   (self.survey_response_scene['code'] == survey_question_code)]
            else:
                survey_data = self.survey_response.loc[self.survey_response['code'] == survey_question_code]

            if not survey_data.empty:

                if p.get('Type') == 'SURVEY BOOLEAN':
                    result = survey_data['selected_option_text'].values[0]
                    if result in d.get('Yes'):
                        result = 'TRUE'
                    elif result in d.get('No'):
                        result = 'FALSE'
                    else:
                        result = None

                elif p.get('Type') == 'SURVEY TEXT':
                    result = []
                    for answer in survey_data['selected_option_text'].unique().tolist():
                        result += str(answer)
                    result = ','.join([str(r) for r in result]) if result else None

                elif p.get('Type') == 'SURVEY TRANSLATED':
                    result = []
                    for answer in survey_data['selected_option_text'].unique().tolist():
                        result += [self.kpi_fetcher.get_survey_answers_translation(
                            survey_question_code, answer)]
                    result = ','.join([str(r) for r in result]) if result else None

                elif p.get('Type') == 'SURVEY NUMERIC':
                    try:
                        result = float(survey_data['number_value'].values[0])
                    except:
                        result = None

                else:
                    Log.debug('The answer type {} is not defined for surveys'.format(
                        params.get('Answer type')))
                    result = None

            else:
                Log.debug('No survey data with survey response code {} for this session'
                          ''.format(survey_question_code))
                result = None

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def check_price(self, params, scenes=None):
        for p in params:
            if p.get('Formula') != 'price':
                continue

            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            if not scenes:
                scenes = self.get_relevant_scenes(params)
            form_factors = [str(form_factor)
                            for form_factor in p.get('Form Factor to include').split(", ")]
            if p.get('Include Stacking'):
                max_price = self.kpi_fetcher.get_object_price(scenes, values_list, p.get('Type'),
                                                              self.match_product_in_scene, form_factors,
                                                              include_stacking=True)
            else:
                max_price = self.kpi_fetcher.get_object_price(scenes, values_list, p.get('Type'),
                                                              self.match_product_in_scene, form_factors)
            if not max_price:
                max_price_for_db = 0.0
            else:
                max_price_for_db = max_price

            result = max_price_for_db

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def custom_average_shelves(self, params):
        for p in params:
            if p.get('Formula') not in ('custom_average_shelves_1', 'custom_average_shelves_2'):
                continue

            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            scenes = self.get_relevant_scenes(p)

            if p.get('Formula') == 'custom_average_shelves_1':
                result = round(self.calculate_average_shelves_1(
                    scenes, p.get('Type'), values_list), 1)
            elif p.get('Formula') == 'custom_average_shelves_2':
                result = round(self.calculate_average_shelves_2(
                    scenes, p.get('Type'), values_list, p.get('Target')), 1)
            else:
                result = 0.0

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_average_shelves_1(self, scenes, object_type, values):
        object_field = self.object_type_conversion[object_type]
        scenes_linear_sos_dict = {}
        bay_counter = 0
        for scene in scenes:
            matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
            bays = matches['bay_number'].unique().tolist()
            bays_linear_sos_dict = {}
            for bay in bays:
                shelves_linear_sos_dict = {}
                bay_filter = matches.loc[matches['bay_number'] == bay]
                shelves = bay_filter['shelf_number'].unique().tolist()
                filtered_products = pd.merge(bay_filter, self.products, on=[
                                             'product_fk'], suffixes=['', '_1'])
                for shelf in shelves:
                    if object_type == 'MAN in CAT':
                        # Only MARS products with object type filters
                        shelf_data = filtered_products.loc[(filtered_products[object_field].isin(values)) & (
                            filtered_products['manufacturer_name'] == MARS) & (
                            filtered_products['shelf_number'] == shelf)]
                    else:
                        # All products with object type filters
                        shelf_data = filtered_products.loc[(filtered_products[object_field].isin(values)) & (
                            filtered_products['shelf_number'] == shelf)]

                    if not shelf_data.empty:
                        shelves_linear_sos_dict[shelf] = 1
                    else:
                        shelves_linear_sos_dict[shelf] = 0
                # Saving linear sos value per shelf
                bays_linear_sos_dict[bay] = sum(
                    shelves_linear_sos_dict.values())  # Saving sum of linear sos values per bay
                if bays_linear_sos_dict[bay] > 0:
                    bay_counter += 1
            if bays_linear_sos_dict.values():
                scenes_linear_sos_dict[scene] = sum(
                    bays_linear_sos_dict.values())  # Saving average of linear sos bay values per scene
            else:
                scenes_linear_sos_dict[scene] = 0
        if scenes and bay_counter > 0:
            final_linear_sos_values = sum(scenes_linear_sos_dict.values()) / float(bay_counter)
            #  Returning average of scenes linear sos values
        else:
            final_linear_sos_values = 0

        return final_linear_sos_values

    def calculate_average_shelves_2(self, scenes, object_type, values, target):
        object_field = self.object_type_conversion[object_type]
        matches = pd.merge(self.match_product_in_scene,
                           self.products,
                           on=['product_fk'],
                           suffixes=['', '_products'])
        matches = matches.loc[(matches['scene_fk'].isin(scenes)) &
                              (matches[object_field].isin(values))]

        shelves = matches.groupby(['bay_number'])['shelf_number'].nunique()\
            .reset_index(level=['bay_number'])\
            .rename(columns={'shelf_number': 'number_of_shelves'})

        if shelves['number_of_shelves'].max() - shelves['number_of_shelves'].min() > target:
            result = shelves['number_of_shelves'].max()
        else:
            result = shelves['number_of_shelves'].mean()

        return result

    @kpi_runtime()
    def custom_number_bays(self, params):
        for p in params:
            if p.get('Formula') != 'custom_number_bays':
                continue

            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            scenes = self.get_relevant_scenes(p)

            result = round(self.calculate_number_bays(
                scenes, p.get('Type'), values_list, p.get('Target')), 1)

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_number_bays(self, scenes, object_type, values, target):
        object_field = self.object_type_conversion[object_type]
        matches = pd.merge(self.match_product_in_scene,
                           self.products,
                           on=['product_fk'],
                           suffixes=['', '_products'])

        result = 0
        for scene in scenes:
            bays = matches.loc[(matches['scene_fk'] == scene) &
                               (matches[object_field].isin(values))]['bay_number'].unique().tolist()
            for bay in bays:
                total_length = matches.loc[(matches['scene_fk'] == scene) &
                                           (matches['bay_number'] == bay) &
                                           (matches['stacking_layer'] == 1)]['width_mm'].sum()

                object_length = matches.loc[(matches['scene_fk'] == scene) &
                                            (matches['bay_number'] == bay) &
                                            (matches['stacking_layer'] == 1) &
                                            (matches[object_field].isin(values))]['width_mm'].sum()
                if not target or object_length/float(total_length) > target:
                    result += 1

        return result

    @kpi_runtime()
    def check_layout_size(self, params):
        for p in params:
            if p.get('Formula') != 'layout size':
                continue

            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            manufacturers = str(p.get('Manufacturer')).split(', ')
            scenes = self.get_relevant_scenes(p)
            if p.get('Include Stacking'):
                if p.get('Include Stacking') == -1:
                    form_factor_filter = {"WET": 'gross_len_ign_stack',
                                          "DRY": 'gross_len_split_stack'}
                    linear_size, products = self.calculate_layout_size_by_form_factor(scenes,
                                                                                      p.get('Type'),
                                                                                      manufacturers,
                                                                                      values_list,
                                                                                      form_factor_filter)
                else:
                    linear_size, products = self.calculate_layout_size(scenes, p.get('Type'), manufacturers,
                                                                       values_list, include_stacking=True)
            else:
                linear_size, products = self.calculate_layout_size(scenes, p.get('Type'), manufacturers, values_list)

            if p.get('additional_attribute_for_specials'):
                allowed_linear_size = self.calculate_allowed_products(scenes, products,
                                                                      p.get(
                                                                          'additional_attribute_for_specials'),
                                                                      p.get('Include Stacking'))
                linear_size += allowed_linear_size

            result = round(linear_size, 1)

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_layout_size(self, scenes, object_type, manufacturers, values, include_stacking=False):
        object_field = self.object_type_conversion[object_type]
        manufacturers = manufacturers if manufacturers else [MARS]
        final_linear_size = 0
        products = []
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[
                    (self.scif['scene_id'] == scene) &
                    (self.scif[object_field].isin(values)) &
                    (self.scif['manufacturer_name'].isin(manufacturers))]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) &
                                              (self.scif[object_field].isin(values))]
            products.extend(filtered_scif['product_fk'].unique().tolist())
            if not include_stacking:
                final_linear_size += filtered_scif['gross_len_ign_stack'].sum()
            else:
                final_linear_size += filtered_scif['gross_len_split_stack'].sum()
        return float(final_linear_size / 1000), products

    def calculate_allowed_products(self, scenes, products, allowed_brands, include_stacking=False):
        allowed_brands = allowed_brands.split(', ')
        statuses = [1]
        if include_stacking:
            statuses.append(3)
        match_product_with_details = pd.merge(
            self.match_product_in_scene, self.products, on='product_fk')
        matches_to_include = []
        original_products_matches = match_product_with_details[match_product_with_details[
            'product_fk'].isin(products)]['scene_match_fk'].tolist()
        final_linear_size = 0
        for scene in scenes:
            match_product_current_scene = match_product_with_details[match_product_with_details['scene_fk'] == scene]
            for bay in match_product_current_scene['bay_number'].unique().tolist():
                match_product_current_bay = match_product_current_scene[
                    match_product_current_scene['bay_number'] == bay]
                for shelf in match_product_current_bay['shelf_number'].unique().tolist():
                    match_product_current_shelf = match_product_current_bay[
                        match_product_current_bay['shelf_number'] == shelf]
                    allowed_shelf_products = []
                    while True:
                        first_length = len(allowed_shelf_products)
                        for product in match_product_current_shelf[
                            (match_product_current_shelf['brand_name'].isin(allowed_brands)) &
                                (match_product_current_shelf['status'].isin(statuses))]['scene_match_fk'].tolist():
                            if self.neighbor(product, match_product_current_shelf, original_products_matches,
                                             allowed_shelf_products):
                                allowed_shelf_products.append(product)
                        if len(allowed_shelf_products) == first_length:
                            break
                    matches_to_include.extend(allowed_shelf_products)
            final_linear_size += match_product_current_scene[
                match_product_current_scene['scene_match_fk'].isin(matches_to_include)]['width_mm_advance'].sum()
        return float(final_linear_size) / 1000

    @staticmethod
    def neighbor(scene_match_fk, match_product_current_shelf, original_products, allowed_products):
        if scene_match_fk in allowed_products or scene_match_fk in original_products:
            return False
        filtered_good_sequences = match_product_current_shelf[
            (match_product_current_shelf['scene_match_fk'].isin(original_products + allowed_products))][
            'sku_sequence_number'].tolist()
        current_product = match_product_current_shelf[match_product_current_shelf['scene_match_fk'] == scene_match_fk]
        current_sequence = current_product['sku_sequence_number'].iloc[0]
        if (current_sequence + 1) in filtered_good_sequences or (current_sequence - 1) in filtered_good_sequences:
            return True
        return False

    def calculate_layout_size_by_filters(self, filters):
        filtered_scif = self.scif[filters]
        final_linear_size = filtered_scif['gross_len_ign_stack'].sum()
        return float(final_linear_size / 1000)

    def calculate_layout_size_by_form_factor(self, scenes, object_type, manufacturers, values, form_factor_filter):
        object_field = self.object_type_conversion[object_type]
        manufacturers = manufacturers if manufacturers else [MARS]
        final_linear_size = 0
        products = []
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) &
                                              (self.scif[object_field].isin(values)) &
                                              (self.scif['manufacturer_name'].isin(manufacturers))]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) &
                                              (self.scif[object_field].isin(values))]
            for form_factor in form_factor_filter:
                final_linear_size += filtered_scif[filtered_scif['form_factor'] == form_factor][form_factor_filter[
                    form_factor]].sum()
                products.extend(filtered_scif[filtered_scif['form_factor'] == form_factor][
                    'product_fk'].unique().tolist())
        return float(final_linear_size / 1000), products

    @kpi_runtime()
    def custom_marsru_1(self, params):
        """
        This function checks if a shelf size is more than 3 meters
        """
        for p in params:
            if p.get('Formula') != 'custom_mars_1':
                continue

            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            scenes = self.get_relevant_scenes(p)
            shelf_size_dict = self.calculate_shelf_size(scenes)
            check_product = False
            result = 'TRUE'
            for shelf in shelf_size_dict.values():
                if shelf >= 3000:
                    check_product = True

            if check_product:
                object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                     formula='number of facings')
                if object_facings > 0:
                    result = 'TRUE'
                else:
                    result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_shelf_size(self, scenes):
        scenes_dict = {}
        for scene in scenes:
            matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
            bays = matches['bay_number'].unique().tolist()
            bays_dict = {}
            for bay in bays:
                shelves_dict = {}
                bay_filter = matches.loc[matches['bay_number'] == bay]
                shelves = bay_filter['shelf_number'].unique().tolist()
                for shelf in shelves:
                    shelf_size = bay_filter.loc[(bay_filter['shelf_number'] == shelf)][
                        'width_mm'].sum()
                    shelves_dict[shelf] = shelf_size  # Saving shelf size value per shelf
                bays_dict[bay] = max(shelves_dict.values())  # Saving the max shelf per bay
            scenes_dict[scene] = sum(bays_dict.values())  # Summing all bays per scene

        return scenes_dict

    # @kpi_runtime()
    # def brand_blocked_in_rectangle(self, params):
    #     self.rds_conn.disconnect_rds()
    #     self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
    #     for p in params:
    #         if p.get('Formula') != 'custom_mars_2' and p.get('Formula') != 'custom_mars_2_2018':
    #             continue
    #
    #         brands_results_dict = {}
    #         values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
    #         scenes = self.get_relevant_scenes(p)
    #         object_field = self.object_type_conversion[p.get('Type')]
    #         self.check_connection(self.rds_conn)
    #         if p.get('Include Stacking'):
    #             matches = self.kpi_fetcher.get_filtered_matches()
    #         else:
    #             matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)
    #
    #         sub_brands = [str(sub_brand) for sub_brand in p.get('Sub brand to exclude').split(", ")]
    #         form_factors = [str(form_factor)
    #                         for form_factor in p.get("Form Factor to include").split(", ")]
    #         for value in values_list:
    #             if p.get('Formula') == 'custom_mars_2' and self.visit_date.year != 2018:
    #                 self.initial_mapping_of_square(scenes, matches, object_field, [value], p,
    #                                                form_factors=form_factors,
    #                                                sub_brands_to_exclude=sub_brands)
    #                 brand_result = self.check_brand_block(object_field, [value])
    #             else:
    #                 sub_brands_to_exclude_by_sub_cats = {'WET': [], 'DRY': sub_brands}
    #                 self.initial_mapping_of_square(scenes, matches, object_field, [value], p,
    #                                                form_factors=form_factors,
    #                                                sub_brands_by_sub_cat=sub_brands_to_exclude_by_sub_cats)
    #                 brand_result = self.check_brand_block_2018(object_field, [value], sub_brands, form_factors,
    #                                                            sub_brands_by_sub_cat=sub_brands_to_exclude_by_sub_cats)
    #             if brand_result == 'TRUE':
    #                 brands_results_dict[value] = 1
    #             else:
    #                 brands_results_dict[value] = 0
    #
    #         if sum(brands_results_dict.values()) == len(values_list):
    #             result = 'TRUE'
    #         else:
    #             result = 'FALSE'
    #
    #         self.store_results_and_scores(result, p)
    #
    #         self.store_to_old_kpi_tables(p)
    #         self.store_to_new_kpi_tables(p)
    #
    #     return

    # def check_brand_block(self, object_field, values_list):
    #     if not self.potential_products:
    #         result = 'FALSE'
    #     else:
    #         scenes_results_dict = {}
    #         sum_left = 0
    #         sum_right = 0
    #         potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
    #         scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
    #         for scene in scenes_to_check:
    #             shelves = potential_products_df.loc[potential_products_df['scene_fk'] == scene][
    #                 'shelf'].unique().tolist()
    #             for shelf in shelves:
    #                 temp = potential_products_df.loc[
    #                     (potential_products_df['scene_fk'] == scene) & (potential_products_df['shelf'] == shelf)]
    #                 # left side
    #                 temp.sort_values(by=['left'], inplace=True)
    #                 most_left_df = temp.loc[temp[object_field].isin(values_list)]
    #                 most_left_bay = min(most_left_df['bay_number'].unique().tolist())
    #                 most_left_value = most_left_df.loc[most_left_df['bay_number']
    #                                                    == most_left_bay]['left'].values[0]
    #                 left_most_candidates = most_left_df.loc[
    #                     (most_left_df['bay_number'] == most_left_bay) & (most_left_df['left'] <= most_left_value)]
    #                 if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
    #                     for i in reversed(left_most_candidates.index):
    #                         match = left_most_candidates.loc[[i]]
    #                         if match['product_type'][i] == EMPTY:
    #                             most_left_value = match['left'][i]
    #                         else:
    #                             break
    #                 sum_left += most_left_value
    #                 self.shelf_square_boundaries[shelf] = {'left': most_left_value}
    #                 # right side
    #                 temp.sort_values(by=['right'], inplace=True)
    #                 most_right_df = temp.loc[temp[object_field].isin(values_list)]
    #                 most_right_bay = max(most_right_df['bay_number'].unique().tolist())
    #                 most_right_value = most_right_df.loc[most_right_df['bay_number']
    #                                                      == most_right_bay]['right'].values[-1]
    #                 right_most_candidates = most_right_df.loc[
    #                     (most_right_df['bay_number'] == most_right_bay) & (most_right_df['right'] >= most_right_value)]
    #                 if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
    #                     for i in right_most_candidates.index:
    #                         match = right_most_candidates.loc[[i]]
    #                         if match['product_type'][i] == EMPTY:
    #                             most_right_value = match['right'][i]
    #                         else:
    #                             break
    #                 sum_right += most_right_value
    #                 self.shelf_square_boundaries[shelf]['right'] = most_right_value
    #             empties_ratio = 1  # Start condition: Rectangle should not be checked
    #             average_left = 0
    #             average_right = 0
    #             if shelves:
    #                 average_left = sum_left / float(len(shelves))
    #                 average_right = sum_right / float(len(shelves))
    #                 # todo: why this is the calculation?
    #                 initial_square_df = potential_products_df.loc[(potential_products_df['left'] > average_left) &
    #                                                               (potential_products_df['right'] < average_right)]
    #                 if not initial_square_df.empty:
    #                     total_products_in_square = len(initial_square_df.index)
    #                     # todo: remove stacking empty other
    #                     total_empties_in_square = \
    #                         len(potential_products_df.loc[(
    #                             potential_products_df['product_type'] == EMPTY)].index)
    #                     empties_ratio = total_empties_in_square / float(total_products_in_square)
    #             non_rect_conditions = (not initial_square_df.loc[
    #                 ~(initial_square_df[object_field].isin(values_list))].empty) \
    #                 or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
    #             if non_rect_conditions:
    #                 scenes_results_dict[scene] = 0
    #             else:
    #                 average_width = initial_square_df['width'].mean()
    #                 max_dev = ALLOWED_DEVIATION * average_width
    #                 square_shelves_counter = 0
    #                 for shelf in shelves:
    #                     if (abs(self.shelf_square_boundaries[shelf].get('left') - average_left)
    #                             + abs(self.shelf_square_boundaries[shelf].get('right') - average_right)) < max_dev:
    #                         square_shelves_counter += 1
    #                 if square_shelves_counter != len(shelves):
    #                     scenes_results_dict[scene] = 0
    #                 else:
    #                     scenes_results_dict[scene] = 1
    #         if sum(scenes_results_dict.values()) == len(scenes_to_check):
    #             result = 'TRUE'
    #         else:
    #             result = 'FALSE'
    #
    #     return result

    # def check_brand_block_2018(self, object_field, values_list, sub_brands_to_exclude, form_factors,
    #                            sub_brands_by_sub_cat={}):
    #     if not self.potential_products:
    #         result = 'FALSE'
    #     else:
    #         scenes_results_dict = {}
    #         self.shelf_square_boundaries = {}
    #         sum_left = 0
    #         sum_right = 0
    #         potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
    #         scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
    #         for scene in scenes_to_check:
    #             shelves = potential_products_df.loc[potential_products_df['scene_fk'] == scene][
    #                 'shelf'].unique().tolist()
    #             min_facings_on_shelf = 0
    #             relevant_shelf_counter = 0
    #             for shelf in shelves:
    #                 temp = potential_products_df.loc[
    #                     (potential_products_df['scene_fk'] == scene) & (potential_products_df['shelf'] == shelf)]
    #                 shelf_sub_category = temp['sub_category'].unique().tolist()
    #                 for category in shelf_sub_category:
    #                     if type(category) == str:
    #                         shelf_sub_category = category.upper()
    #                         break
    #                 if shelf_sub_category in sub_brands_by_sub_cat.keys() and not sub_brands_to_exclude:
    #                     sub_brands_to_exclude = sub_brands_by_sub_cat[shelf_sub_category]
    #                 # count facings on shelf:
    #                 facings_on_shelf = temp.loc[(temp['brand_name'].isin(values_list)) &
    #                                             (temp['stacking_layer'] == 1) &
    #                                             (temp['form_factor'].isin(form_factors)) &
    #                                             (~temp['sub_brand'].isin(sub_brands_to_exclude))]['brand_name'].count()
    #                 if facings_on_shelf:
    #                     relevant_shelf_counter += 1
    #                     if not min_facings_on_shelf:
    #                         min_facings_on_shelf = facings_on_shelf
    #                         min_shelf = shelf
    #                     else:
    #                         if facings_on_shelf < min_facings_on_shelf:
    #                             min_facings_on_shelf = facings_on_shelf
    #                             min_shelf = shelf
    #                     # left side
    #                     temp.sort_values(by=['left'], inplace=True)
    #                     most_left_df = temp.loc[(temp[object_field].isin(values_list)) &
    #                                             ((temp['form_factor'].isin(form_factors)) |
    #                                              (temp['product_type'].isin([OTHER]))) &
    #                                             (~temp['sub_brand'].isin(sub_brands_to_exclude))]
    #                     most_left_bay = min(most_left_df['bay_number'].unique().tolist())
    #                     most_left_value = most_left_df.loc[most_left_df['bay_number'] == most_left_bay][
    #                         'left'].values[0]
    #                     left_most_candidates = most_left_df.loc[(most_left_df['bay_number'] == most_left_bay) &
    #                                                             (most_left_df['left'] <= most_left_value)]
    #                     if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
    #                         for i in reversed(left_most_candidates.index):
    #                             match = left_most_candidates.loc[[i]]
    #                             if match['product_type'][i].isin(EMPTY):
    #                                 most_left_value = match['left'][i]
    #                             else:
    #                                 break
    #                     sum_left += most_left_value
    #                     self.shelf_square_boundaries[shelf] = {'left_boundary': most_left_value}
    #                     # right side
    #                     temp.sort_values(by=['right'], inplace=True)
    #                     most_right_df = temp.loc[(temp[object_field].isin(values_list)) &
    #                                              ((temp['form_factor'].isin(form_factors)) |
    #                                               (temp['product_type'].isin([OTHER]))) &
    #                                              (~temp['sub_brand'].isin(sub_brands_to_exclude))]
    #                     most_right_bay = max(most_right_df['bay_number'].unique().tolist())
    #                     most_right_value = most_right_df.loc[most_right_df['bay_number'] == most_right_bay][
    #                         'right'].values[-1]
    #                     right_most_candidates = most_right_df.loc[
    #                         (most_right_df['bay_number'] == most_right_bay) &
    #                         (most_right_df['right'] >= most_right_value)]
    #                     if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
    #                         for i in right_most_candidates.index:
    #                             match = right_most_candidates.loc[[i]]
    #                             if match['product_type'][i].isin(EMPTY):
    #                                 most_right_value = match['right'][i]
    #                             else:
    #                                 break
    #                     sum_right += most_right_value
    #                     self.shelf_square_boundaries[shelf]['right_boundary'] = most_right_value
    #             if relevant_shelf_counter == 1:
    #                 if min_facings_on_shelf > 4:
    #                     result = 'FALSE'
    #                     return result
    #             empties_ratio = 1  # Start condition: Rectangle should not be checked
    #             min_shelf_left = 0
    #             min_shelf_right = 0
    #             if relevant_shelf_counter == 0:
    #                 scenes_results_dict[scene] = 1
    #                 continue
    #             if shelves:
    #                 min_shelf_left = self.shelf_square_boundaries[min_shelf]['left_boundary']
    #                 min_shelf_right = self.shelf_square_boundaries[min_shelf]['right_boundary']
    #                 boundaries_list = list(self.shelf_square_boundaries.items())
    #                 boundaries_df = pd.DataFrame(boundaries_list, columns=[
    #                                              'shelf', 'left_right_boundaries'])
    #                 boundaries_df['left_right_boundaries'] = boundaries_df['left_right_boundaries'].astype(
    #                     str)
    #                 potential_products_df = pd.merge(
    #                     potential_products_df, boundaries_df, on='shelf')
    #                 left_right_boundaries_df = pd.DataFrame(
    #                     [ast.literal_eval(i) for i in potential_products_df.left_right_boundaries.values])
    #                 potential_products_df = potential_products_df.drop(
    #                     'left_right_boundaries', axis=1)
    #                 final_potential_products_df = pd.concat(
    #                     [potential_products_df, left_right_boundaries_df], axis=1)
    #                 initial_square_df = final_potential_products_df.loc[
    #                     (final_potential_products_df['left'] >= final_potential_products_df['left_boundary']) &
    #                     (final_potential_products_df['right'] <= final_potential_products_df['right_boundary'])]
    #                 if not initial_square_df.empty:
    #                     total_products_in_square = len(initial_square_df.index)
    #                     total_empties_in_square = \
    #                         len(initial_square_df.loc[(initial_square_df['product_type'].isin([EMPTY])) |
    #                                                   ((initial_square_df['product_type'].isin([OTHER])) &
    #                                                    (~initial_square_df['brand_name'].isin(values_list))) &
    #                                                   (initial_square_df['stacking_layer'] == 1)].index)
    #                     empties_ratio = total_empties_in_square / float(total_products_in_square)
    #             non_rect_conditions = (not initial_square_df.loc[
    #                 ~((initial_square_df[object_field].isin(values_list)) &
    #                   ((initial_square_df['form_factor'].isin(form_factors)) |
    #                    (initial_square_df['product_type'].isin([OTHER]))) &
    #                   (~initial_square_df['sub_brand'].isin(sub_brands_to_exclude)))].empty) \
    #                 or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
    #             if non_rect_conditions:
    #                 # scene_result = 'FALSE'
    #                 scenes_results_dict[scene] = 0
    #             else:
    #                 average_width = initial_square_df['width'].mean()
    #                 max_dev = ALLOWED_DEVIATION_2018 * average_width
    #                 square_shelves_counter = 0
    #                 relevant_shelves = self.shelf_square_boundaries.keys()
    #                 for shelf in relevant_shelves:
    #                     if (abs(self.shelf_square_boundaries[shelf].get('left_boundary') - min_shelf_left)
    #                         + abs(self.shelf_square_boundaries[shelf].get(
    #                             'right_boundary') - min_shelf_right)) < max_dev:
    #                         square_shelves_counter += 1
    #                 if square_shelves_counter != len(shelves):
    #                     scenes_results_dict[scene] = 0
    #                 else:
    #                     scenes_results_dict[scene] = 1
    #         if sum(scenes_results_dict.values()) == len(scenes_to_check):
    #             result = 'TRUE'
    #         else:
    #             result = 'FALSE'
    #     return result

    # def initial_mapping_of_square(self, scenes, matches, object_field, values_list, p, form_factors=None,
    #                               sub_brands_to_exclude=None, sub_brands_by_sub_cat={}):
    #     self.potential_products = {}
    #     if not scenes:
    #         return
    #     else:
    #         for scene in scenes:
    #             brand_counter = 0
    #             brands_presence_indicator = True
    #             scene_data = matches.loc[matches['scene_fk'] == scene]
    #             if p.get('Formula') != 'custom_mars_2_2018':
    #                 if form_factors:
    #                     scene_data = scene_data.loc[scene_data['form_factor'].isin(form_factors)]
    #                 scene_sub_category = scene_data['sub_category'].unique().tolist()
    #                 if scene_sub_category and scene_sub_category[0] is None:
    #                     scene_sub_category.remove(None)
    #                 if scene_sub_category:
    #                     scene_sub_category = scene_sub_category[0].upper()
    #                 if scene_sub_category in sub_brands_by_sub_cat.keys() and not sub_brands_to_exclude:
    #                     sub_brands_to_exclude = sub_brands_by_sub_cat[scene_sub_category]
    #                 if sub_brands_to_exclude:
    #                     scene_data = scene_data.loc[~scene_data['sub_brand'].isin(
    #                         sub_brands_to_exclude)]
    #             shelves = scene_data['shelf_number'].unique().tolist()
    #             # unified_scene_set = set(scene_data[object_field]) & set(values_list)
    #             unified_scene_set = scene_data.loc[scene_data[object_field].isin(values_list)]
    #             if len(values_list) > 1:
    #                 for brand in values_list:
    #                     brand_df = scene_data.loc[scene_data[object_field] == brand]
    #                     if not brand_df.empty:
    #                         brand_counter += 1
    #                 if brand_counter != len(values_list):
    #                     brands_presence_indicator = False
    #             if unified_scene_set.empty or not brands_presence_indicator:
    #                 continue
    #             else:
    #                 is_sequential_shelves = False
    #                 for shelf_number in shelves:
    #                     shelf_data = scene_data.loc[scene_data['shelf_number'] == shelf_number]
    #                     bays = shelf_data['bay_number'].unique().tolist()
    #                     # for bay in bays:
    #                     temp_shelf_data = shelf_data.reset_index()
    #                     unified_shelf_set = shelf_data.loc[shelf_data[object_field].isin(
    #                         values_list)]
    #                     # unified_shelf_set = set(shelf_data[object_field]) & set(values_list)
    #                     # if not unified_shelf_set:
    #                     if unified_shelf_set.empty:
    #                         if is_sequential_shelves:
    #                             # is_sequential_shelves = False
    #                             return
    #                         continue
    #                     else:
    #                         is_sequential_shelves = True
    #                         for i in temp_shelf_data.index:
    #                             match = temp_shelf_data.loc[[i]]
    #                             if match['bay_number'].values[0] == bays[0]:
    #                                 self.potential_products[match['scene_match_fk'].values[0]] = {
    #                                     'top': int(match[self.kpi_fetcher.TOP]),
    #                                     'bottom': int(
    #                                         match[self.kpi_fetcher.BOTTOM]),
    #                                     'left': int(
    #                                         match[self.kpi_fetcher.LEFT]),
    #                                     'right': int(
    #                                         match[self.kpi_fetcher.RIGHT]),
    #                                     object_field: match[object_field].values[0],
    #                                     'product_type': match['product_type'].values[0],
    #                                     'product_name': match['product_name'].values[0].encode('utf-8'),
    #                                     'shelf': shelf_number,
    #                                     'width': match['width_mm'].values[0],
    #                                     'bay_number': match['bay_number'].values[0],
    #                                     'scene_fk': scene,
    #                                     'form_factor': match['form_factor'].values[0],
    #                                     'sub_brand': match['sub_brand'].values[0],
    #                                     'stacking_layer': match['stacking_layer'].values[0],
    #                                     'shelf_px_total': int(match['shelf_px_total'].values[0]),
    #                                     'sub_category': match['sub_category'].values[0]}
    #                             else:
    #                                 sum_of_px_to_add = \
    #                                     temp_shelf_data.loc[
    #                                         temp_shelf_data['bay_number'] < match['bay_number'].values[0]][
    #                                         'shelf_px_total'].unique().sum()
    #                                 self.potential_products[match['scene_match_fk'].values[0]] = {
    #                                     'top': int(match[self.kpi_fetcher.TOP]),
    #                                     'bottom': int(
    #                                         match[self.kpi_fetcher.BOTTOM]),
    #                                     'left': int(
    #                                         match[self.kpi_fetcher.LEFT]) + int(sum_of_px_to_add),
    #                                     'right': int(
    #                                         match[self.kpi_fetcher.RIGHT]) + int(sum_of_px_to_add),
    #                                     object_field: match[object_field].values[0],
    #                                     'product_type': match['product_type'].values[0],
    #                                     'product_name': match['product_name'].values[0].encode('utf-8'),
    #                                     'shelf': shelf_number,
    #                                     'width': match['width_mm'].values[0],
    #                                     'bay_number': match['bay_number'].values[0],
    #                                     'scene_fk': scene,
    #                                     'form_factor': match['form_factor'].values[0],
    #                                     'sub_brand': match['sub_brand'].values[0],
    #                                     'stacking_layer': match['stacking_layer'].values[0],
    #                                     'shelf_px_total': int(match['shelf_px_total'].values[0])}

    # @kpi_runtime()
    # def multiple_brands_blocked_in_rectangle(self, params):
    #     for p in params:
    #         if p.get('Formula') != 'custom_mars_4' and p.get('Formula') != 'custom_mars_4_2018':
    #             continue
    #
    #         brands_results_dict = {}
    #         if '*' in str(p.get('Values')):
    #             values_list = str(p.get('Values')).split(', *')
    #         else:
    #             values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
    #         scenes = self.get_relevant_scenes(p)
    #         object_field = self.object_type_conversion[p.get('Type')]
    #         if p.get('Include Stacking'):
    #             matches = self.kpi_fetcher.get_filtered_matches()
    #         else:
    #             matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)
    #
    #         if p.get('Sub brand to exclude'):
    #             sub_brands = [str(sub_brand)
    #                           for sub_brand in p.get('Sub brand to exclude').split(", ")]
    #         else:
    #             sub_brands = []
    #
    #         if p.get("Form Factor to include"):
    #             form_factors = [str(form_factor)
    #                             for form_factor in p.get("Form Factor to include").split(", ")]
    #         else:
    #             form_factors = []
    #
    #         for value in values_list:
    #             self.potential_products = {}
    #             if ',' in value:
    #                 sub_values_list = str(value).split(', ')
    #                 self.initial_mapping_of_square(scenes, matches, object_field, sub_values_list, p,
    #                                                form_factors=form_factors,
    #                                                sub_brands_to_exclude=sub_brands)
    #                 if p.get('Formula') == 'custom_mars_4':
    #                     brand_result = self.check_brand_block(object_field, sub_values_list)
    #                 else:
    #                     brand_result = self.check_brand_block_2018(
    #                         object_field, sub_values_list, sub_brands, form_factors)
    #             else:
    #                 self.initial_mapping_of_square(scenes, matches, object_field, [value], p,
    #                                                form_factors=form_factors,
    #                                                sub_brands_to_exclude=sub_brands)
    #                 if p.get('Formula') == 'custom_mars_4':
    #                     brand_result = self.check_brand_block(object_field, [value])
    #                 else:
    #                     brand_result = self.check_brand_block_2018(object_field, [value], sub_brands,
    #                                                                form_factors)
    #             if brand_result == 'TRUE':
    #                 brands_results_dict[value] = 1
    #             else:
    #                 brands_results_dict[value] = 0
    #
    #         if sum(brands_results_dict.values()) == len(values_list):
    #             result = 'TRUE'
    #         else:
    #             result = 'FALSE'
    #
    #         self.store_results_and_scores(result, p)
    #
    #         self.store_to_old_kpi_tables(p)
    #         self.store_to_new_kpi_tables(p)
    #
    #     return

    @kpi_runtime()
    def golden_shelves(self, params):
        """
        This function checks if a predefined product is present in golden shelves
        """
        for p in params:
            if p.get('Formula') != 'custom_mars_5':
                continue

            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            scenes = self.get_relevant_scenes(p)
            scenes_results_dict = {}
            for scene in scenes:
                matches = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
                bays = matches['bay_number'].unique().tolist()
                bays_results_dict = {}
                for bay in bays:
                    shelves_result_dict = {}
                    bay_filter = matches.loc[matches['bay_number'] == bay]
                    shelves = bay_filter['shelf_number'].unique().tolist()
                    bay_golden_shelves = self.kpi_fetcher.get_golden_shelves(len(shelves))
                    for shelf in bay_golden_shelves:
                        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                             form_factors=[
                                                                                 p.get('Form Factor to include')],
                                                                             formula="number of SKUs",
                                                                             shelves=str(shelf))
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
            if 1 in scenes_results_dict.values():
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def facings_by_brand(self, params):
        for p in params:
            if p.get('Formula') != 'custom_mars_3' and p.get('Formula') != 'custom_mars_3_linear':
                continue

            brand_facings_dict = {}
            values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
            scenes = self.get_relevant_scenes(p)
            if p.get("Form Factor to include"):
                form_factors = [str(form_factor)
                                for form_factor in p.get("Form Factor to include").split(", ")]
            else:
                form_factors = []

            if p.get('Include Stacking'):
                include_stacking = True
            else:
                include_stacking = False

            linear = False
            if p.get('Formula') == 'custom_mars_3_linear':
                linear = True

            categories = p.get('Category').split(', ')

            object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                 formula='number of facings',
                                                                 form_factors=form_factors,
                                                                 categories=categories,
                                                                 include_stacking=include_stacking,
                                                                 linear=linear)
            brands_to_check = self.scif.loc[self.scif['scene_id'].isin(scenes) &
                                            ~(self.scif['brand_name'].isin(values_list))][
                'brand_name'].unique().tolist()
            for brand in brands_to_check:
                brand_facings = self.kpi_fetcher.get_object_facings(scenes, [brand], p.get('Type'),
                                                                    formula='number of facings',
                                                                    form_factors=form_factors,
                                                                    categories=categories,
                                                                    include_stacking=include_stacking,
                                                                    linear=linear)
                brand_facings_dict[brand] = brand_facings

            if brand_facings_dict.values():
                max_brand_facings = max(brand_facings_dict.values())
            else:
                max_brand_facings = 0

            if object_facings > max_brand_facings:
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def check_kpi_results(self, params):
        for p in params:
            if p.get('Formula') != 'kpi_results':
                continue

            kpi_set = '*'

            if p.get('#Mars KPI NAME') == 1013:  # Share of Shelf Nestle in Mars = (((1010+1011)/(4262+4266))/0,5)*0,5
                k1 = 0.5
                k2 = 0.5
                kpi_part_1 = self.results_and_scores[kpi_set]['1010']['result'] \
                             + self.results_and_scores[kpi_set]['1011']['result']
                kpi_part_2 = self.results_and_scores[kpi_set]['4262']['result'] \
                             + self.results_and_scores[kpi_set]['4266']['result']
                result = ((kpi_part_1/float(kpi_part_2))/k1)*k2 if kpi_part_2 else 0
                result = round(result if result < 1 else 1, 1)

            elif p.get('#Mars KPI NAME') == 1015:  # Nestle in Mars = ((1012/1014)/0.5)*0.5
                k1 = 0.5
                k2 = 0.5
                kpi_part_1 = self.results_and_scores[kpi_set]['1012']['result']
                kpi_part_2 = self.results_and_scores[kpi_set]['1014']['result']
                result = ((kpi_part_1/float(kpi_part_2))/k1)*k2 if kpi_part_2 else 0
                result = round(result if result < 1 else 1, 1)

            elif p.get('#Mars KPI NAME') == 1016:  # PSS Nestle = 1013+1015
                kpi_part_1 = self.results_and_scores[kpi_set]['1013']['result']
                kpi_part_2 = self.results_and_scores[kpi_set]['1015']['result']
                result = kpi_part_1 + kpi_part_2
                result = round(result if result < 1 else 1, 1)

            else:
                result = None

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

    @kpi_runtime()
    def check_range_kpis(self, params):
        for p in params:
            if p.get('Formula') != 'custom_mars_7':
                continue

            kpi_set = (p.get('#Mars KPI SET Old'), p.get('#Mars KPI SET New'))

            if p.get('#Mars KPI NAME') == 4704:  # If ((4269+4271)/(4270+4272))*100% >= 100% then TRUE
                kpi_part_1 = self.results_and_scores[kpi_set]['4269']['result'] \
                             + self.results_and_scores[kpi_set]['4271']['result']
                kpi_part_2 = self.results_and_scores[kpi_set]['4270']['result'] \
                             + self.results_and_scores[kpi_set]['4272']['result']
                ratio = kpi_part_1 / float(kpi_part_2) if kpi_part_2 else 0

                result = 'FALSE' if ratio < 1 else 'TRUE'

            else:

                kpi_parameters = self.kpi_fetcher.get_kpi_value_parameters(self.store_type, self.region, p,
                                                                           self.results_and_scores.get(kpi_set))
                scenes = self.get_relevant_scenes(p)
                result = None
                if kpi_parameters:
                    if p.get('#Mars KPI NAME') == 2317:

                        top_eans = kpi_parameters.get('eans')
                        top_products_in_store = self.scif[self.scif['product_ean_code'].isin(
                            top_eans)]['product_fk'].unique().tolist()

                        min_shelf, max_shelf = kpi_parameters.get('shelves').split('-')
                        min_shelf, max_shelf = int(min_shelf), int(max_shelf)
                        top_products_on_golden_shelf = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'].isin(scenes)) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] >= min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] <= max_shelf) &
                            (self.match_product_in_scene['product_fk'].isin(top_products_in_store))][
                            'product_fk'].unique().tolist()
                        top_products_outside_golden_shelf = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'].isin(scenes)) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] < min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] > max_shelf) &
                            (self.match_product_in_scene['product_fk'].isin(top_products_in_store))][
                            'product_fk'].unique().tolist()

                        if len(top_products_on_golden_shelf) < len(top_products_in_store) \
                                or len(top_products_outside_golden_shelf) > 0:
                            result = 'FALSE'
                        else:
                            result = 'TRUE'

                    if p.get('#Mars KPI NAME') in (4317, 4650):

                        top_eans = kpi_parameters.get('eans')
                        top_products_in_store = self.scif[
                            (self.scif['scene_fk'].isin(scenes)) &
                            (self.scif['product_ean_code'].isin(top_eans))]['product_fk'].unique().tolist()

                        min_shelf, max_shelf = kpi_parameters.get('shelves').split('-')
                        min_shelf, max_shelf = int(min_shelf), int(max_shelf)
                        top_products_on_golden_shelf = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'].isin(scenes)) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] >= min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] <= max_shelf) &
                            (self.match_product_in_scene['product_fk'].isin(top_products_in_store))][
                            'product_fk'].unique().tolist()

                        if len(top_products_on_golden_shelf) < len(top_products_in_store) \
                                or len(top_products_in_store) == 0:
                            result = 'FALSE'
                        else:
                            result = 'TRUE'

                    elif p.get('#Mars KPI NAME') == 2254:
                        if self.results_and_scores[kpi_set]['2264']['result'] \
                                or self.results_and_scores[kpi_set]['2351']['result']:
                            kpi_part_1 = self.results_and_scores[kpi_set]['2261']['result'] / \
                                         float(self.results_and_scores[kpi_set]['2264']['result']) \
                                if self.results_and_scores[kpi_set]['2264']['result'] > 0 else 0
                            kpi_part_2 = self.results_and_scores[kpi_set]['2265']['result'] / \
                                         float(self.results_and_scores[kpi_set]['2351']['result']) \
                                if self.results_and_scores[kpi_set]['2351']['result'] > 0 else 0
                            mars_shelf_size = kpi_part_1 + kpi_part_2
                            for row in kpi_parameters.get('shelf_length'):
                                if row['shelf from'] <= mars_shelf_size < row['shelf to']:
                                    result = str(row['result'])

                    elif p.get('#Mars KPI NAME') == 4254:
                        if self.results_and_scores[kpi_set]['4261']['result'] \
                                + self.results_and_scores[kpi_set]['4265']['result'] < p.get('Target'):
                            for row in kpi_parameters.get('shelf_length'):
                                if row['length_condition'] == '<' + str(int(p.get('Target'))):
                                    result = str(row['result'])
                                    break
                        elif self.results_and_scores[kpi_set]['4264']['result'] \
                                or self.results_and_scores[kpi_set]['4351']['result']:
                            kpi_part_1 = self.results_and_scores[kpi_set]['4261']['result'] / \
                                         float(self.results_and_scores[kpi_set]['4264']['result']) \
                                if self.results_and_scores[kpi_set]['4264']['result'] > 0 else 0
                            kpi_part_2 = self.results_and_scores[kpi_set]['4265']['result'] / \
                                         float(self.results_and_scores[kpi_set]['4351']['result']) \
                                if self.results_and_scores[kpi_set]['4351']['result'] > 0 else 0
                            mars_shelf_size = kpi_part_1 + kpi_part_2
                            for row in kpi_parameters.get('shelf_length'):
                                if row['shelf from'] <= mars_shelf_size < row['shelf to'] \
                                        and row['length_condition'] == '>=' + str(int(p.get('Target'))):
                                    result = str(row['result'])
                                    break

                    else:
                        sub_results = []
                        for eans in kpi_parameters.get('eans'):
                            kpi_res = self.calculate_availability(p, scenes, formula='number of SKUs',
                                                                  values_list=eans.split('/'),
                                                                  object_type='SKUs', include_stacking=True)
                            if kpi_res > 0:
                                sub_result = 1
                            else:
                                sub_result = 0
                            sub_results.append(sub_result)
                        sum_of_facings = sum(sub_results)
                        if sum_of_facings >= len(kpi_parameters.get('eans')):
                            result = 'TRUE'
                        else:
                            result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    # @kpi_runtime()
    # def negative_neighbors(self, params):
    #     for p in params:
    #         if p.get('Formula') != 'custom_mars_6':
    #             continue
    #
    #         object_field = self.object_type_conversion[p.get('Type')]
    #         values_list = str(p.get('Values')).split(', ') if p.get('Values') else []
    #         competitor_brands = str(p.get('competitor_brands')).split(', ')
    #         scenes = self.get_relevant_scenes(p)
    #         tested_filters = {object_field: values_list, 'category': p.get('Category')}
    #
    #         # First check - negative adjacency to MARS products
    #         mars_anchor_filters = {'manufacturer_name': MARS}
    #         negative_mars_adjacency_result = self.calculate_non_proximity(tested_filters,
    #                                                                       mars_anchor_filters,
    #                                                                       scene_fk=scenes)
    #
    #         # Second check - positive adjacency to competitor brands
    #         competitor_anchor_filters = {'brand_name': competitor_brands}
    #         direction_data2 = {'top': POSITIVE_ADJACENCY_RANGE,
    #                            'bottom': POSITIVE_ADJACENCY_RANGE,
    #                            'left': POSITIVE_ADJACENCY_RANGE,
    #                            'right': POSITIVE_ADJACENCY_RANGE}
    #         competitor_adjacency_result = self.calculate_relative_position(tested_filters,
    #                                                                        competitor_anchor_filters,
    #                                                                        direction_data2,
    #                                                                        scene_fk=scenes)
    #
    #         if negative_mars_adjacency_result and competitor_adjacency_result:
    #             result = 'TRUE'
    #         else:
    #             result = 'FALSE'
    #
    #         self.store_results_and_scores(result, p)
    #
    #         self.store_to_old_kpi_tables(p)
    #         self.store_to_new_kpi_tables(p)
    #
    #     return

    def parse_filter_from_template(self, line):
        result_filter = {}
        for target_filter_line in line.split('. '):
            is_include = self.INCLUDE_FILTER
            filter_name = target_filter_line.split(': ')[0].lower()
            filter_values = target_filter_line.split(': ')[1].split(', ')
            if 'exclude ' in filter_name:
                is_include = self.EXCLUDE_FILTER
                filter_name = filter_name.replace('exclude ', '')
            result_filter[filter_name] = (filter_values, is_include)
        return result_filter

    @kpi_runtime()
    def get_total_linear(self, params):
        for p in params:
            if p.get('Formula') != 'total_linear':
                continue

            result = 'TRUE'
            target_linear_size_total = 0
            others_linear_size_total = 0
            scenes = 'scene_id: ' + ', '.join([str(x) for x in self.get_relevant_scenes(p)]) + '. '
            for values in p.get('Values').split('\nOR\n'):
                targets, others, percent = values.split('\n')
                target_filter = self.get_filter_condition(
                    self.scif, **(self.parse_filter_from_template(scenes + targets)))
                other_filter = self.get_filter_condition(
                    self.scif, **(self.parse_filter_from_template(scenes + others)))
                target_linear_size = self.calculate_layout_size_by_filters(target_filter)
                others_linear_size = self.calculate_layout_size_by_filters(other_filter)
                target_linear_size_total += target_linear_size
                others_linear_size_total += others_linear_size
                if target_linear_size > 0 and target_linear_size >= float(percent) * others_linear_size:
                    pass
                else:
                    result = 'FALSE'
                    break

            if target_linear_size_total == 0:
                result = None

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def get_placed_near(self, params):
        for p in params:
            if p.get('Formula') != 'placed_near':
                continue

            scene_filter = {}
            scene_type = p.get('Scene type')
            if scene_type:
                scene_filter['template_name'] = scene_type
            location_type = p.get('Location type')
            if location_type:
                scene_filter['location_type'] = location_type

            targets, allowed = p.get('Values').split('\n')

            targets_filter = self.parse_filter_from_template(targets)
            targets_filter.update(scene_filter)
            targets_filter = self.get_filter_condition(self.scif, **targets_filter)
            filtered_targets_scif = self.scif[targets_filter]
            products_targets = filtered_targets_scif['product_fk'].tolist()
            filters = {'product_fk': products_targets}

            allowed_filter = self.parse_filter_from_template(allowed)
            allowed_filter.update(scene_filter)
            allowed_filter = self.get_filter_condition(self.scif, **allowed_filter)
            filtered_allowed_scif = self.scif[allowed_filter]
            products_allowed = filtered_allowed_scif['product_fk'].tolist()
            # allowed_products_filters = {'product_fk': products_allowed}

            scenes = filtered_targets_scif['scene_id'].unique().tolist()

            result = None
            if products_targets and scenes:
                for scene in scenes:
                    filters['scene_id'] = scene
                    results = Block(self.data_provider, rds_conn=self.rds_conn) \
                        .network_x_block_together(location={'scene_fk': scene},
                                                  population={'product_fk': products_targets},
                                                  additional={'minimum_block_ratio': 1,
                                                              'allowed_products_filters':
                                                                  {'product_fk': products_allowed}})
                    if results.empty:
                        continue
                    elif results['is_block'].all():
                        result = True
                    elif results['is_block'].all():
                        result = False
                        break

            if result:
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        relevant_scenes = self.scif[self.get_filter_condition(
            self.scif, **filters)]['scene_id'].unique()
        location_filters = {}
        for field in filters.keys():
            if field not in self.products.columns and field in self.scif.columns:
                location_filters[field] = filters.pop(field)
        return filters, relevant_scenes

    def get_scene_blocks(self, graph, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY, **filters):
        """
        This function is a sub-function for Block Together. It receives a graph and filters and returns a list of
        clusters.
        """
        relevant_vertices = set(self.filter_vertices_from_graph(graph, **filters))
        if allowed_products_filters:
            allowed_vertices = self.filter_vertices_from_graph(graph, **allowed_products_filters)
        else:
            allowed_vertices = set()

        if include_empty == EXCLUDE_EMPTY:
            empty_vertices = {v.index for v in graph.vs.select(product_type='Empty')}
            allowed_vertices = set(allowed_vertices).union(empty_vertices)

        all_vertices = {v.index for v in graph.vs}
        vertices_to_remove = all_vertices.difference(relevant_vertices.union(allowed_vertices))
        graph.delete_vertices(vertices_to_remove)
        # removing clusters including 'allowed' SKUs only
        blocks = [block for block in graph.clusters() if set(block).difference(allowed_vertices)]
        return blocks, graph

    # def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
    #                              minimum_block_ratio=1, result_by_scene=False, vertical=False, **filters):
    #     """
    #     :param vertical: if needed to check vertical block by average shelf
    #     :param allowed_products_filters: These are the parameters which are allowed to corrupt the block
    #                                      without failing it.
    #     :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
    #     :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
    #                                 in order for KPI to pass (if ratio=1, then only one block is allowed).
    #     :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
    #                             False - The result is True if at least one scene has a block, False - otherwise.
    #     :param filters: These are the parameters which the blocks are checked for.
    #     :return: see 'result_by_scene' above.
    #     """
    #     filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
    #     if len(relevant_scenes) == 0:
    #         if result_by_scene:
    #             return 0, 0
    #         else:
    #             Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
    #             return True
    #     number_of_blocked_scenes = 0
    #     cluster_ratios = []
    #     for scene in relevant_scenes:
    #         scene_graph = self.position_graphs.get(scene, horizontal_block_only=True).copy()
    #         clusters, scene_graph = self.get_scene_blocks(scene_graph,
    #                                                       allowed_products_filters=allowed_products_filters,
    #                                                       include_empty=include_empty, **filters)
    #         new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
    #         for cluster in clusters:
    #             relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
    #             if len(new_relevant_vertices) > 0:
    #                 cluster_ratio = len(relevant_vertices_in_cluster) / \
    #                     float(len(new_relevant_vertices))
    #             else:
    #                 cluster_ratio = 0
    #             cluster_ratios.append(cluster_ratio)
    #             if cluster_ratio >= minimum_block_ratio:
    #                 if result_by_scene:
    #                     number_of_blocked_scenes += 1
    #                     break
    #                 else:
    #                     if minimum_block_ratio == 1:
    #                         return True
    #                     else:
    #                         all_vertices = {v.index for v in scene_graph.vs}
    #                         non_cluster_vertices = all_vertices.difference(cluster)
    #                         scene_graph.delete_vertices(non_cluster_vertices)
    #                         if vertical:
    #                             return {'block': True, 'shelves': len(
    #                                 set(scene_graph.vs['shelf_number']))}
    #                         return cluster_ratio, scene_graph
    #     if result_by_scene:
    #         return number_of_blocked_scenes, len(relevant_scenes)
    #     else:
    #         if minimum_block_ratio == 1:
    #             return False
    #         elif cluster_ratios:
    #             return max(cluster_ratios)
    #         else:
    #             return None

    # def calculate_non_proximity(self, tested_filters, anchor_filters, allowed_diagonal=False, **general_filters):
    #     """
    #     :param tested_filters: The tested SKUs' filters.
    #     :param anchor_filters: The anchor SKUs' filters.
    #     :param allowed_diagonal: True - a tested SKU can be in a direct diagonal from an anchor SKU in order
    #                                     for the KPI to pass;
    #                              False - a diagonal proximity is NOT allowed.
    #     :param general_filters: These are the parameters which the general data frame is filtered by.
    #     :return:
    #     """
    #     direction_data = []
    #     if allowed_diagonal:
    #         direction_data.append({'top': (0, 1), 'bottom': (0, 1)})
    #         direction_data.append({'right': (0, 1), 'left': (0, 1)})
    #     else:
    #         direction_data.append({'top': (0, 1), 'bottom': (0, 1),
    #                                'right': (0, 1), 'left': (0, 1)})
    #     is_proximity = self.calculate_relative_position(tested_filters, anchor_filters, direction_data,
    #                                                     min_required_to_pass=1, **general_filters)
    #     return not is_proximity
    #
    # def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
    #                                 **general_filters):
    #     """
    #     :param tested_filters: The tested SKUs' filters.
    #     :param anchor_filters: The anchor SKUs' filters.
    #     :param direction_data: The allowed distance between the tested and anchor SKUs.
    #                            In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
    #                            Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
    #     :param min_required_to_pass: The number of appearances needed to be True for relative position
    #                                  in order for KPI
    #                                  to pass. If all appearances are required: ==a string or a big number.
    #     :param general_filters: These are the parameters which the general data frame is filtered by.
    #     :return: True if (at least) one pair of relevant SKUs fits the distance requirements;
    #              otherwise - returns False.
    #     """
    #     filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
    #     tested_scenes = filtered_scif[self.get_filter_condition(
    #         filtered_scif, **tested_filters)]['scene_id'].unique()
    #     anchor_scenes = filtered_scif[self.get_filter_condition(
    #         filtered_scif, **anchor_filters)]['scene_id'].unique()
    #     relevant_scenes = set(tested_scenes).intersection(anchor_scenes)
    #
    #     if relevant_scenes:
    #         pass_counter = 0
    #         reject_counter = 0
    #         for scene in relevant_scenes:
    #             scene_graph = self.position_graphs.get(scene)
    #             tested_vertices = self.filter_vertices_from_graph(scene_graph, **tested_filters)
    #             anchor_vertices = self.filter_vertices_from_graph(scene_graph, **anchor_filters)
    #             for tested_vertex in tested_vertices:
    #                 for anchor_vertex in anchor_vertices:
    #                     moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
    #                     path = scene_graph.get_shortest_paths(
    #                         anchor_vertex, tested_vertex, output='epath')
    #                     if path:
    #                         path = path[0]
    #                         for edge in path:
    #                             moves[scene_graph.es[edge]['direction']] += 1
    #                         if self.validate_moves(moves, direction_data):
    #                             pass_counter += 1
    #                             if isinstance(min_required_to_pass, int) and pass_counter >= min_required_to_pass:
    #                                 return True
    #                         else:
    #                             reject_counter += 1
    #                     else:
    #                         Log.debug('Tested and Anchor have no direct path')
    #         if pass_counter > 0 and reject_counter == 0:
    #             return True
    #         else:
    #             return False
    #     else:
    #         Log.debug('None of the scenes contain both anchor and tested SKUs')
    #         return False

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.debug('field {} is not in the Data Frame'.format(field))

        return filter_condition

    @staticmethod
    def filter_vertices_from_graph(graph, **filters):
        """
        This function is given a graph and returns a set of vertices calculated by a given set of filters.
        """
        vertices_indexes = None
        for field in filters.keys():
            field_vertices = set()
            values = filters[field] if isinstance(
                filters[field], (list, tuple)) else [filters[field]]
            for value in values:
                vertices = [v.index for v in graph.vs.select(**{field: value})]
                field_vertices = field_vertices.union(vertices)
            if vertices_indexes is None:
                vertices_indexes = field_vertices
            else:
                vertices_indexes = vertices_indexes.intersection(field_vertices)
        vertices_indexes = vertices_indexes if vertices_indexes is not None else [
            v.index for v in graph.vs]
        return list(vertices_indexes)

    @staticmethod
    def validate_moves(moves, direction_data):
        """
        This function checks whether the distance between the anchor and the tested SKUs fits the requirements.
        """
        direction_data = direction_data if isinstance(
            direction_data, (list, tuple)) else [direction_data]
        validated = False
        for data in direction_data:
            data_validated = True
            for direction in moves.keys():
                allowed_moves = data.get(direction, (0, 0))
                min_move, max_move = allowed_moves if isinstance(
                    allowed_moves, tuple) else (0, allowed_moves)
                if not min_move <= moves[direction] <= max_move:
                    data_validated = False
                    break
            if data_validated:
                validated = True
                break
        return validated

    def store_results_and_scores(self, result, params):
        """
        This function calculates score based on result and score type
        and stores result and score for the KPI

        """
        if result is None:
            result_value = None
        else:
            if params.get('Answer type') == 'Int':
                try:
                    result_value = int(result)
                except:
                    result_value = None
            elif params.get('Answer type') == 'Float':
                try:
                    result_value = float(result)
                except:
                    result_value = None
            elif params.get('Answer type') == 'Boolean':
                result_value = None if result is None else \
                    ('FALSE' if result == 'FALSE' else ('TRUE' if result else 'FALSE'))
            else:
                result_value = result

        score_value = None if result_value is None else (
            100 if result_value and result_value != 'FALSE' else 0)

        # Results by KPI Sets
        kpi_set = (params.get('#Mars KPI SET Old'), params.get('#Mars KPI SET New'))
        if not self.results_and_scores.get(kpi_set):
            self.results_and_scores[kpi_set] = {}
        self.results_and_scores[kpi_set][str(params.get('#Mars KPI NAME'))] = {
            'result': result_value, 'score': score_value}

        # All results
        kpi_set = '*'
        if not self.results_and_scores.get(kpi_set):
            self.results_and_scores[kpi_set] = {}
        self.results_and_scores[kpi_set][str(params.get('#Mars KPI NAME'))] = {
            'result': result_value, 'score': score_value}

    def transform_result(self, result, params):
        """
        This function result to store it in new kpi tables

        """
        if result is None:
            result_value = None
        elif params.get('Answer type') == 'Int':
            try:
                result_value = int(result)
            except:
                result_value = None
        elif params.get('Answer type') == 'Float':
            try:
                result_value = float(result)
            except:
                result_value = None
        elif params.get('Answer type') == 'Boolean':
            try:
                if result and result != 'FALSE':
                    result_value = self.kpi_fetcher.kpi_result_values[
                        (self.kpi_fetcher.kpi_result_values['kpi_result_type'] == 'Boolean') &
                        (self.kpi_fetcher.kpi_result_values['kpi_result_value'] == 'TRUE')][
                        'kpi_result_value_fk'].iloc[0]
                else:
                    result_value = self.kpi_fetcher.kpi_result_values[
                        (self.kpi_fetcher.kpi_result_values['kpi_result_type'] == 'Boolean') &
                        (self.kpi_fetcher.kpi_result_values['kpi_result_value'] == 'FALSE')][
                        'kpi_result_value_fk'].iloc[0]
            except:
                result_value = None
        else:
            result_value = result

        return result_value

    def write_to_db_result(self, df=None, level=None):
        """
        This function writes KPI results to old tables

        """
        if level == 3:
            df_dict = df.to_dict()
            query = insert(df_dict, KPI_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 2:
            df_dict = df.to_dict()
            query = insert(df_dict, KPK_RESULT)
            self.kpi_results_queries.append(query)
        elif level == 1:
            df_dict = df.to_dict()
            query = insert(df_dict, KPS_RESULT)
            self.kpi_results_queries.append(query)

    def insert_results_data(self, query):
        start_time = dt.datetime.utcnow()
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        self.rds_conn.db.commit()
        self.writing_to_db_time += dt.datetime.utcnow() - start_time
        return cur.lastrowid

    def write_to_db_result_level1(self, kpi_set_name):
        kpi_set_fk = self.kpi_fetcher.get_kpi_set_fk(kpi_set_name)

        if not kpi_set_fk:
            Log.error('KPI set <{0}> is not found in static.kpi_set table'
                      ''.format(kpi_set_name))

        attributes_for_table1 = pd.DataFrame([(kpi_set_name,
                                               self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               100,
                                               kpi_set_fk)],
                                             columns=['kps_name',
                                                      'session_uid',
                                                      'store_fk',
                                                      'visit_date',
                                                      'score_1',
                                                      'kpi_set_fk'])
        self.write_to_db_result(attributes_for_table1, level=1)

    def create_attributes_for_level2_df(self, params):
        """
        This function creates a data frame with all attributes needed for saving in level 2 tables

        """
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('#Mars KPI SET Old'),
                                             params.get('#Mars KPI NAME'))

        kpi_set = (params.get('#Mars KPI SET Old'), params.get('#Mars KPI SET New'))

        if self.results_and_scores[kpi_set].get(str(params.get('#Mars KPI NAME'))):
            score = self.results_and_scores[kpi_set][str(params.get('#Mars KPI NAME'))]['score']
        else:
            score = None

        if not kpi_fk:
            Log.error('KPI name <{0}> is not found for KPI set <{1}> in static.kpi table'
                      ''.format(params.get('#Mars KPI NAME'), params.get('#Mars KPI SET Old')))

        attributes_for_table2 = pd.DataFrame([(self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               kpi_fk,
                                               params.get('#Mars KPI NAME'),
                                               score)],
                                             columns=['session_uid',
                                                      'store_fk',
                                                      'visit_date',
                                                      'kpi_fk',
                                                      'kpk_name',
                                                      'score'])

        return attributes_for_table2

    def create_attributes_for_level3_df(self, params):
        """
        This function creates a data frame with all attributes needed for saving in level 3 tables

        """
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('#Mars KPI SET Old'),
                                             params.get('#Mars KPI NAME'))
        atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(params.get('#Mars KPI SET Old'),
                                                           params.get('#Mars KPI NAME'),
                                                           params.get('#Mars KPI NAME'))

        kpi_set = (params.get('#Mars KPI SET Old'), params.get('#Mars KPI SET New'))

        if self.results_and_scores[kpi_set].get(str(params.get('#Mars KPI NAME'))):
            result = self.results_and_scores[kpi_set][str(params.get('#Mars KPI NAME'))]['result']
            score = self.results_and_scores[kpi_set][str(params.get('#Mars KPI NAME'))]['score']
        else:
            result = None
            score = None

        if not atomic_kpi_fk:
            Log.error('Atomic KPI name <{0}> is not found for KPI <{0}> of KPI set <{1}> in static.atomic_kpi table'
                      ''.format(params.get('#Mars KPI NAME'), params.get('#Mars KPI SET Old')))

        attributes_for_table3 = pd.DataFrame([(params.get('KPI Display name RU').encode('utf-8').replace("'", "''"),
                                               self.session_uid,
                                               params.get('#Mars KPI SET Old'),
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               dt.datetime.utcnow().isoformat(),
                                               score,
                                               kpi_fk,
                                               atomic_kpi_fk,
                                               result)],
                                             columns=['display_text',
                                                      'session_uid',
                                                      'kps_name',
                                                      'store_fk',
                                                      'visit_date',
                                                      'calculation_time',
                                                      'score',
                                                      'kpi_fk',
                                                      'atomic_kpi_fk',
                                                      'result'])

        return attributes_for_table3

    def commit_results_data(self):
        self.rds_conn.disconnect_rds()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        delete_queries = self.kpi_fetcher.get_delete_session_results(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            try:
                cur.execute(query)
            except:
                Log.error('Execution failed for the following query: {}'.format(query))
        self.rds_conn.db.commit()

    def store_to_old_kpi_tables(self, p):
        attributes_for_table2 = self.create_attributes_for_level2_df(p)
        self.write_to_db_result(attributes_for_table2, level=2)
        attributes_for_table3 = self.create_attributes_for_level3_df(p)
        self.write_to_db_result(attributes_for_table3, level=3)

    def store_to_new_kpi_tables(self, p):

        kpi_set = (p.get('#Mars KPI SET Old'), p.get('#Mars KPI SET New'))

        # API KPIs
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(str(p.get('#Mars KPI NAME')))
        parent_fk = self.common.get_kpi_fk_by_kpi_type(str(p.get('#Mars KPI SET New')) + ' API')
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = self.transform_result(
            self.results_and_scores[kpi_set][str(p.get('#Mars KPI NAME'))]['result'], p)

        if not kpi_fk:
            Log.error('KPI name <{0}> is not found in static.kpi_level_2 table'
                      ''.format(str(p.get('#Mars KPI NAME'))))

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=numerator_id,
                                       numerator_result=0,
                                       denominator_id=denominator_id,
                                       denominator_result=0,
                                       result=result,
                                       score=0,
                                       identifier_result=identifier_result,
                                       identifier_parent=identifier_parent,
                                       should_enter=True)

        # Report KPIs
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(str(p.get('#Mars KPI NAME')) + '-RUS')
        parent_fk = self.common.get_kpi_fk_by_kpi_type(str(p.get('#Mars KPI SET New')) + ' RUS')
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = self.transform_result(
            self.results_and_scores[kpi_set][str(p.get('#Mars KPI NAME'))]['result'], p)

        if not kpi_fk:
            Log.error('KPI name <{0}> is not found in static.kpi_level_2 table'
                      ''.format(str(p.get('#Mars KPI NAME')) + '-RUS'))

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=numerator_id,
                                       numerator_result=0,
                                       denominator_id=denominator_id,
                                       denominator_result=0,
                                       result=result,
                                       score=0,
                                       identifier_result=identifier_result,
                                       identifier_parent=identifier_parent,
                                       should_enter=True)

        if not self.kpi_count.get(str(p.get('#Mars KPI SET New'))):
            self.kpi_count[str(p.get('#Mars KPI SET New'))] = 0
        self.kpi_count[str(p.get('#Mars KPI SET New'))] += 1

    def store_to_new_kpi_tables_level0(self, kpi_level_0_name):
        # API KPIs
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_level_0_name + ' API')
        parent_fk = 0
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = None

        if not kpi_fk:
            Log.error('KPI name <{0}> is not found in static.kpi_level_2 table'
                      ''.format(kpi_level_0_name + ' API'))

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=numerator_id,
                                       numerator_result=0,
                                       denominator_id=denominator_id,
                                       denominator_result=0,
                                       result=result,
                                       score=0,
                                       identifier_result=identifier_result,
                                       identifier_parent=identifier_parent,
                                       should_enter=True)

        # Report KPIs
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_level_0_name + ' RUS')
        parent_fk = 0
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = self.kpi_count[kpi_level_0_name]

        if not kpi_fk:
            Log.error('KPI name <{0}> is not found in static.kpi_level_2 table'
                      ''.format(kpi_level_0_name + ' RUS'))

        self.common.write_to_db_result(fk=kpi_fk,
                                       numerator_id=numerator_id,
                                       numerator_result=0,
                                       denominator_id=denominator_id,
                                       denominator_result=0,
                                       result=result,
                                       score=0,
                                       identifier_result=identifier_result,
                                       identifier_parent=identifier_parent,
                                       should_enter=True)

    @kpi_runtime()
    def calculate_osa(self):
        """
        This function calculates OSA kpi and writes to new KPI tables.
        :return:
        """
        if not self.assortment_products.empty:
            assortment_products = self.assortment_products['product_fk'].tolist()
            product_facings = self.scif.groupby('product_fk')['facings'].sum().reset_index()

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(OSA_KPI_NAME + ' - SKU')
            parent_fk = self.common.get_kpi_fk_by_kpi_type(OSA_KPI_NAME)
            identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
            identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
            denominator_id = self.store_id
            total_result = 0
            for product in assortment_products:
                numerator_id = product
                try:
                    numerator_result = product_facings[product_facings['product_fk'] == product]['facings'].iloc[0]
                except:
                    numerator_result = 0
                denominator_result = 1
                result = 1 if numerator_result >= denominator_result else 0
                score = result*100

                try:
                    if result:
                        result_value = self.kpi_fetcher.kpi_result_values[
                            (self.kpi_fetcher.kpi_result_values['kpi_result_type'] == 'PRESENCE') &
                            (self.kpi_fetcher.kpi_result_values['kpi_result_value'] == 'DISTRIBUTED')][
                            'kpi_result_value_fk'].iloc[0]
                    else:
                        result_value = self.kpi_fetcher.kpi_result_values[
                            (self.kpi_fetcher.kpi_result_values['kpi_result_type'] == 'PRESENCE') &
                            (self.kpi_fetcher.kpi_result_values['kpi_result_value'] == 'OOS')][
                            'kpi_result_value_fk'].iloc[0]
                except:
                    result_value = None

                self.common.write_to_db_result(fk=kpi_fk,
                                               numerator_id=numerator_id,
                                               numerator_result=numerator_result,
                                               denominator_id=denominator_id,
                                               denominator_result=denominator_result,
                                               result=result_value,
                                               score=score,
                                               identifier_result=identifier_result,
                                               identifier_parent=identifier_parent,
                                               should_enter=True)
                total_result += result

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(OSA_KPI_NAME)
            parent_fk = 0
            identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
            identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
            numerator_id = self.own_manufacturer_id
            numerator_result = total_result
            denominator_result = len(assortment_products)
            result = round(numerator_result / float(denominator_result), 3)
            score = result*100
            self.common.write_to_db_result(fk=kpi_fk,
                                           numerator_id=numerator_id,
                                           numerator_result=numerator_result,
                                           denominator_id=denominator_id,
                                           denominator_result=denominator_result,
                                           result=result,
                                           score=score,
                                           identifier_result=identifier_result,
                                           identifier_parent=identifier_parent,
                                           should_enter=True)

        return

    def get_first_created_scene(self, scenes):
        if not scenes:
            return None
        dict_to_calculate = {'population': {'include': [{'scene_fk': scenes}]}}
        df = self.parser.filter_df(dict_to_calculate, self.scif)
        if df.empty:
            return None
        first_creation_time = df['creation_time'].min()
        dict_to_calculate = {'population': {'include': [{'creation_time': first_creation_time}]}}
        df = self.parser.filter_df(dict_to_calculate, df)
        scene_id = df['scene_fk'].values[0]
        return scene_id

    @kpi_runtime()
    def check_block_and_neighbors_by_shelf(self, params):
        """
        The function checks core products to be a single block-together on all the scenes where they are found
        and checks the neighbors are as defined in parameters per each shelf where the core products are placed


        Single block (minimum_block_ratio = 1)
        Empties are ignored (ignore_empty = True)
        Block is 2 facings or more (minimum_facing_for_block = 2)
        Stacked products are ignored (include_stacking = False)

        :param params: KPI parameters row
        :return:
        """
        for p in params:
            if p.get('Formula') != 'custom_mars_8':
                continue

            result = None
            core_products = []
            neighbor_1_products = []
            neighbor_2_products = []

            kpi_parameters = json.loads(p.get('Values')) if p.get('Values') else {}
            if kpi_parameters:

                if kpi_parameters['core']:
                    for row in kpi_parameters['core']:
                        core_products += self.get_products_fks_by_attributes(row)
                    core_products = list(set(core_products))

                if kpi_parameters['neighbor_1']:
                    for row in kpi_parameters['neighbor_1']:
                        neighbor_1_products += self.get_products_fks_by_attributes(row)
                    neighbor_1_products = list(set(neighbor_1_products) - set(core_products))

                if kpi_parameters['neighbor_2']:
                    for row in kpi_parameters['neighbor_2']:
                        neighbor_2_products += self.get_products_fks_by_attributes(row)
                    neighbor_2_products = list(set(neighbor_2_products) - set(core_products))

            scenes = self.get_relevant_scenes(p)

            if core_products and scenes:
                minimum_facing_for_block = 1  # parameter
                block_together_results = Block(self.data_provider, rds_conn=self.rds_conn)\
                    .network_x_block_together(location={'scene_fk': scenes},
                                              population={'product_fk': core_products},
                                              additional={'minimum_block_ratio': 1,
                                                          'minimum_facing_for_block': minimum_facing_for_block})
                block_together_results['is_block_str'] = block_together_results['is_block'].astype(str)

                failed_scenes = \
                    block_together_results[(block_together_results['total_facings'] >= minimum_facing_for_block) &
                                           (block_together_results['is_block_str'] == 'False')]['scene_fk'].tolist()
                passed_scenes = \
                    block_together_results[(block_together_results['total_facings'] >= minimum_facing_for_block) &
                                           (block_together_results['is_block_str'] == 'True')]['scene_fk'].tolist()

                if failed_scenes:
                    result = False
                elif not passed_scenes:
                    result = None
                elif not neighbor_1_products and not neighbor_2_products:
                    result = True  # The core products are all blocked together and no neighbors defined to check
                else:

                    result = True
                    matches = self.match_product_in_scene[self.match_product_in_scene['stacking_layer'] == 1]

                    for scene in passed_scenes:

                        scene_matches = matches[matches['scene_fk'] == scene]
                        shelves = scene_matches[scene_matches['product_fk'].isin(core_products)][
                            'shelf_number'].unique().tolist()

                        for shelf in shelves:

                            shelf_matches = scene_matches[scene_matches['shelf_number'] == shelf]\
                                .merge(self.products[['product_fk', 'product_type']], how='left', on='product_fk')\
                                .sort_values(by=['bay_number', 'facing_sequence_number'])\
                                .copy()\
                                .reset_index(drop=True)
                            shelf_matches['position'] = shelf_matches.index + 1

                            min_position = shelf_matches['position'].min()
                            max_position = shelf_matches['position'].max()

                            core_left_position = shelf_matches[shelf_matches['product_fk'].isin(core_products)][
                                'position'].min()
                            core_right_position = shelf_matches[shelf_matches['product_fk'].isin(core_products)][
                                'position'].max()

                            if neighbor_1_products and not neighbor_2_products:
                                neighbor_1_left = self.check_block_neighbor_on_shelf('left',
                                                                                     core_left_position,
                                                                                     min_position,
                                                                                     neighbor_1_products,
                                                                                     shelf_matches)
                                if not neighbor_1_left:
                                    neighbor_1_right = self.check_block_neighbor_on_shelf('right',
                                                                                          core_right_position,
                                                                                          max_position,
                                                                                          neighbor_1_products,
                                                                                          shelf_matches)
                                    if not neighbor_1_right:
                                        result = False

                            elif neighbor_2_products and not neighbor_1_products:
                                neighbor_2_left = self.check_block_neighbor_on_shelf('left',
                                                                                     core_left_position,
                                                                                     min_position,
                                                                                     neighbor_2_products,
                                                                                     shelf_matches)
                                if not neighbor_2_left:
                                    neighbor_2_right = self.check_block_neighbor_on_shelf('right',
                                                                                          core_right_position,
                                                                                          max_position,
                                                                                          neighbor_2_products,
                                                                                          shelf_matches)
                                    if not neighbor_2_right:
                                        result = False

                            else:
                                neighbor_1_left = self.check_block_neighbor_on_shelf('left',
                                                                                     core_left_position,
                                                                                     min_position,
                                                                                     neighbor_1_products,
                                                                                     shelf_matches)
                                neighbor_2_left = self.check_block_neighbor_on_shelf('left',
                                                                                     core_left_position,
                                                                                     min_position,
                                                                                     neighbor_2_products,
                                                                                     shelf_matches)
                                if neighbor_1_left:
                                    neighbor_2_right = self.check_block_neighbor_on_shelf('right',
                                                                                          core_right_position,
                                                                                          max_position,
                                                                                          neighbor_2_products,
                                                                                          shelf_matches)
                                    if not neighbor_2_right:
                                        result = False

                                elif neighbor_2_left:
                                    neighbor_1_right = self.check_block_neighbor_on_shelf('right',
                                                                                          core_right_position,
                                                                                          max_position,
                                                                                          neighbor_1_products,
                                                                                          shelf_matches)
                                    if not neighbor_1_right:
                                        result = False

                                else:
                                    result = False

                            if not result:
                                break

                        if not result:
                            break

            if result:
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @staticmethod
    def check_block_neighbor_on_shelf(side, block_edge_position, end_shelf_position,
                                      expected_neighbours, shelf_matches):
        result = False
        step = -1 if side == 'left' else 1
        position = block_edge_position + step
        while position >= end_shelf_position and step == -1 or position <= end_shelf_position and step == 1:
            if shelf_matches[shelf_matches['position'] == position]['product_type'].values[0] == EMPTY:
                position = position + step
                continue
            elif shelf_matches[shelf_matches['position'] == position]['product_fk'].values[0] in expected_neighbours:
                result = True
                break
            else:
                break
        return result

    def get_products_fks_by_attributes(self, attributes):
        """
        The function gets a dictionary parameter with product table column names as keys
        and returns the list of product_fks
        :param attributes: dictionary of product column names and values:
            Exapmle:
            {"brand_name": ["Felix", "Perfect Fit"], "form_factor":  ["POUCH", "Pouch", "pouch"]}
        :return: list of product_fks
        """
        products = self.products
        for attribute in attributes.keys():
            products = products[products[attribute].isin(attributes[attribute])]
        product_fks = products['product_fk'].tolist()
        return product_fks
