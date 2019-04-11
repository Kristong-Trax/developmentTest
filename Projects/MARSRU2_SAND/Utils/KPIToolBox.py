# -*- coding: utf-8 -*-
import ast
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
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime

from Projects.MARSRU2_SAND.Utils.KPIFetcher import MARSRU2_SANDKPIFetcher
from Projects.MARSRU2_SAND.Utils.PositionGraph import MARSRU2_SANDPositionGraphs


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

IN_ASSORTMENT = 'in_assortment_osa'
IS_OOS = 'oos_osa'
PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'
PRODUCT_FK = 'product_fk'
SCENE_FK = 'scene_fk'

EXCLUDE_EMPTY = False
INCLUDE_EMPTY = True


class MARSRU2_SANDKPIToolBox:

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, kpi_templates, data_provider, output, set_name=None, ignore_stacking=True):
        self.kpi_templates = kpi_templates
        self.data_provider = data_provider
        self.output = output
        self.dict_for_planogram = {2261: 0, 2264: 0, 2265: 0,
                                   2351: 0, 4261: 0, 4264: 0, 4265: 0, 4351: 0,
                                   4269: 0, 4271: 0, 4270: 0, 4272: 0}
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
        self.own_manufacturer_id = int(self.data_provider.own_manufacturer[
            self.data_provider.own_manufacturer['param_name'] == 'manufacturer_id'][
            'param_value'].tolist()[0])
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        try:
            self.scif['sub_brand'] = self.scif['Sub Brand']  # the sub_brand column is empty
        except:
            pass
        if type(set_name) is tuple:
            self.set_name, self.kpi_level_0_name = set_name
        else:
            self.set_name = self.kpi_level_0_name = set_name
        self.kpi_fetcher = MARSRU2_SANDKPIFetcher(self.project_name, self.kpi_templates, self.scif, self.match_product_in_scene,
                                                 self.set_name, self.products, self.session_uid)
        self.kpi_set_fk = self.kpi_fetcher.get_kpi_set_fk()
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.ignore_stacking = ignore_stacking
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.region = self.get_store_Att5()
        self.attr6 = self.get_store_Att6()
        self.store_num_1 = self.get_store_number_1_attribute()
        self.results_and_scores = {}
        self.result_df = []
        self.writing_to_db_time = dt.timedelta(0)
        self.kpi_results_queries = []
        self.position_graphs = MARSRU2_SANDPositionGraphs(
            self.data_provider, rds_conn=self.rds_conn)
        self.potential_products = {}
        self.custom_scif_queries = []
        self.shelf_square_boundaries = {}
        self.object_type_conversion = {'SKUs': 'product_ean_code',
                                       'BRAND': 'brand_name',
                                       'BRAND in CAT': 'brand_name',
                                       'CAT': 'category',
                                       'MAN in CAT': 'category',
                                       'MAN': 'manufacturer_name'}
        self.common = Common(self.data_provider)
        self.osa_kpi_dict = {}
        self.kpi_count = 0

        self.assortment = Assortment(self.data_provider, self.output, common=self.common)
        self.block = Block(self.data_provider, rds_conn=self.rds_conn)

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
            scene_param = p.get('Values')
            filtered_scif = self.scif.loc[self.scif['template_name'] == scene_param]
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

    def get_store_Att5(self):
        store_att5 = self.kpi_fetcher.get_store_att5(self.store_id)
        return store_att5

    def get_store_Att6(self):
        store_att6 = self.kpi_fetcher.get_store_att6(self.store_id)
        return store_att6

    def get_store_number_1_attribute(self):
        store_number_1 = self.kpi_fetcher.get_store_number_1(self.store_id)
        return store_number_1

    def get_assortment_for_attribute(self):
        assortments = self.kpi_fetcher.get_store_assortment(self.store_num_1, self.visit_date)
        return assortments

    def get_assortment_for_store_id(self):
        assortments = self.kpi_fetcher.get_store_assortment(self.store_id, self.visit_date)
        return assortments

    def get_custom_query(self, scene_fk, product_fk, assortment, oos):
        """
        This gets the query for insertion to PS custom scif
        :param scene_fk:
        :param product_fk:
        :param assortment:
        :param oos:
        :return:
        """
        attributes = pd.DataFrame([(self.session_fk, scene_fk, product_fk, assortment, oos)],
                                  columns=['session_fk', 'scene_fk', 'product_fk', IN_ASSORTMENT, IS_OOS])

        query = insert(attributes.to_dict(), PSERVICE_CUSTOM_SCIF)
        self.custom_scif_queries.append(query)

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
        cur.execute(delete_query)
        self.rds_conn.db.commit()

        for query in self.custom_scif_queries:
            try:
                cur.execute(query)
            except:
                Log.error('could not run query: {}'.format(query))
                print 'could not run query: {}'.format(query)
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
        # assortment_products = self.get_assortment_for_store_id()
        assortment_products = self.assortment.get_lvl3_relevant_ass()
        if not assortment_products.empty:
            assortment_products = assortment_products[PRODUCT_FK].tolist()
            for scene in self.scif[SCENE_FK].unique().tolist():
                products_in_scene = self.scif[(self.scif[SCENE_FK] == scene) &
                                              (self.scif['facings'] > 0)][PRODUCT_FK].unique().tolist()
                for product in assortment_products:
                    if product in products_in_scene:
                        # This means the product in assortment and is not oos (1,0)
                        self.get_custom_query(scene, product, 1, 0)
                    else:
                        # The product is in assortment list but is oos (1,1)
                        self.get_custom_query(scene, product, 1, 1)

                for product in products_in_scene:
                    if product not in assortment_products:
                        # The product is not in assortment list and not oos (0,0)
                        self.get_custom_query(scene, product, 0, 0)

        Log.debug("Done updating PS Custom SCIF... ")
        self.commit_custom_scif()

    def get_static_list(self, type):
        object_static_list = []
        if type == 'SKUs':
            object_static_list = self.products['product_ean_code'].values.tolist()
        elif type == 'CAT' or type == 'MAN in CAT':
            object_static_list = self.products['category'].values.tolist()
        elif type == 'BRAND':
            object_static_list = self.products['brand_name'].values.tolist()
        elif type == 'MAN':
            object_static_list = self.products['manufacturer_name'].values.tolist()
        else:
            Log.debug('The type {} does not exist in the data base'.format(type))
        return object_static_list

    @kpi_runtime()
    def check_availability(self, params):
        """
        This function is used to calculate availability given a set pf parameters

        """
        availability_types = ['SKUs', 'BRAND', 'MAN', 'CAT', 'MAN in CAT', 'BRAND in CAT']
        formula_types = ['number of SKUs', 'number of facings']
        for p in params:
            if p.get('Type') not in availability_types or p.get('Formula') not in formula_types:
                continue

            result = self.calculate_availability(p)

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_availability(self, params, scenes=[], formula=None, values_list=None, object_type=None,
                               include_stacking=False):
        if not values_list:
            if '*' in str(params.get('Values')):
                values_list = str(params.get('Values')).split(', *')
            else:
                values_list = str(params.get('Values')).split(', ')
        if not formula:
            formula = params.get('Formula')
        if not scenes:
            scenes = self.get_relevant_scenes(params)

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
        if params.get('Brand Category value'):
            brand_category = params.get('Brand Category value')
        else:
            brand_category = None
        if object_type:
            availability_type = object_type
        else:
            availability_type = params.get('Type')

        object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, availability_type,
                                                             formula=formula, form_factor=form_factors,
                                                             sub_brands=sub_brands,
                                                             sub_brands_to_exclude=sub_brands_to_exclude,
                                                             cl_sub_cats=cl_sub_cats,
                                                             cl_sub_cats_to_exclude=cl_sub_cats_to_exclude,
                                                             include_stacking=include_stacking,
                                                             form_factor_to_exclude=form_factors_to_exclude,
                                                             brand_category=brand_category)

        return object_facings

    def get_relevant_scenes(self, params):
        scif = self.scif

        locations = str(params.get('Location type')).split(
            ', ') if params.get('Location type') else None
        if locations:
            scif = scif.loc[scif['location_type'].isin(locations)]

        scene_types = str(params.get('Scene type')).split(
            ', ') if params.get('Scene type') else None
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

            if p.get('Formula') != 'answer for survey':
                continue

            survey_question_code = str(int(p.get('Values')))
            survey_data = self.survey_response.loc[self.survey_response['code']
                                                   == survey_question_code]
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
                            .format(survey_question_code))
                result = None

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def check_price(self, params, scenes=[]):
        for p in params:
            if p.get('Formula') != 'price':
                continue

            values_list = str(p.get('Values')).split(', ')
            if not scenes:
                scenes = self.get_relevant_scenes(params)
            form_factors = [str(form_factor)
                            for form_factor in p.get('Form Factor to include').split(", ")]
            if p.get('Include Stacking'):
                max_price = self.kpi_fetcher.get_object_price(scenes, values_list, p.get('Type'),
                                                              self.match_product_in_scene, form_factors, include_stacking=True)
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

            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)

            if p.get('Formula') == 'custom_average_shelves_1':
                result = round(self.calculate_average_shelves_1(
                    scenes, p.get('Type'), values_list), 1)
            elif p.get('Formula') == 'custom_average_shelves_2':
                result = round(self.calculate_average_shelves_2(
                    scenes, p.get('Type'), values_list, p.get('Target')), 1)
            else:
                result = 0.0

            if p.get('#Mars KPI NAME') in (2264, 2351, 4264, 4351, 4269, 4271, 4270, 4272):
                self.dict_for_planogram[p.get('#Mars KPI NAME')] = float(result)

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

            values_list = str(p.get('Values')).split(', ')
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

            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            if p.get('Include Stacking'):
                if p.get('Include Stacking') == -1:
                    form_factor_filter = {"WET": 'gross_len_ign_stack',
                                          "DRY": 'gross_len_split_stack'}
                    linear_size, products = self.calculate_layout_size_by_form_factor(scenes, p.get('Type'),
                                                                                      values_list, form_factor_filter)
                else:
                    linear_size, products = self.calculate_layout_size(scenes, p.get('Type'), values_list,
                                                                       include_stacking=True)
            else:
                linear_size, products = self.calculate_layout_size(
                    scenes, p.get('Type'), values_list)

            if p.get('additional_attribute_for_specials'):
                allowed_linear_size = self.calculate_allowed_products(scenes, products,
                                                                      p.get(
                                                                          'additional_attribute_for_specials'),
                                                                      p.get('Include Stacking'))
                linear_size += allowed_linear_size

            result = round(linear_size, 1)

            if p.get('#Mars KPI NAME') in (2261, 2265, 4261, 4265, 4269, 4271, 4270, 4272):
                self.dict_for_planogram[p.get('#Mars KPI NAME')] = float(result)

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def calculate_layout_size(self, scenes, object_type, values, include_stacking=False):
        object_field = self.object_type_conversion[object_type]
        final_linear_size = 0
        products = []
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[
                    (self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values)) & (
                        self.scif['manufacturer_name'] == MARS)]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) & (
                    self.scif[object_field].isin(values))]
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

    def calculate_layout_size_by_form_factor(self, scenes, object_type, values, form_factor_filter):
        object_field = self.object_type_conversion[object_type]
        final_linear_size = 0
        products = []
        for scene in scenes:
            if object_type == 'MAN in CAT':
                filtered_scif = self.scif.loc[
                    (self.scif['scene_id'] == scene) & (self.scif[object_field].isin(values)) & (
                        self.scif['manufacturer_name'] == MARS)]
            else:
                filtered_scif = self.scif.loc[(self.scif['scene_id'] == scene) & (
                    self.scif[object_field].isin(values))]
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

            values_list = str(p.get('Values')).split(', ')
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

    @kpi_runtime()
    def brand_blocked_in_rectangle(self, params):
        self.rds_conn.disconnect_rds()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        for p in params:
            if p.get('Formula') != 'custom_mars_2' and p.get('Formula') != 'custom_mars_2_2018':
                continue

            brands_results_dict = {}
            values_list = str(p.get('Values')).split(', ')
            scenes = self.get_relevant_scenes(p)
            object_field = self.object_type_conversion[p.get('Type')]
            self.check_connection(self.rds_conn)
            if p.get('Include Stacking'):
                matches = self.kpi_fetcher.get_filtered_matches()
            else:
                matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)

            sub_brands = [str(sub_brand) for sub_brand in p.get('Sub brand to exclude').split(", ")]
            form_factors = [str(form_factor)
                            for form_factor in p.get("Form Factor to include").split(", ")]
            for value in values_list:
                if p.get('Formula') == 'custom_mars_2' and self.visit_date.year != 2018:
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], p, form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    brand_result = self.check_brand_block(object_field, [value])
                else:
                    sub_brands_to_exclude_by_sub_cats = {'WET': [], 'DRY': sub_brands}
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], p, form_factor=form_factors,
                                                   sub_brand_by_sub_cat=sub_brands_to_exclude_by_sub_cats)
                    brand_result = self.check_brand_block_2018(object_field, [value], sub_brands, form_factors,
                                                               sub_brand_by_sub_cat=sub_brands_to_exclude_by_sub_cats)
                if brand_result == 'TRUE':
                    brands_results_dict[value] = 1
                else:
                    brands_results_dict[value] = 0

            if sum(brands_results_dict.values()) == len(values_list):
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    def check_brand_block(self, object_field, values_list):
        if not self.potential_products:
            result = 'FALSE'
        else:
            scenes_results_dict = {}
            sum_left = 0
            sum_right = 0
            potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
            scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
            for scene in scenes_to_check:
                shelves = potential_products_df.loc[potential_products_df['scene_fk'] == scene][
                    'shelf'].unique().tolist()
                for shelf in shelves:
                    temp = potential_products_df.loc[
                        (potential_products_df['scene_fk'] == scene) & (potential_products_df['shelf'] == shelf)]
                    # left side
                    temp.sort_values(by=['left'], inplace=True)
                    most_left_df = temp.loc[temp[object_field].isin(values_list)]
                    most_left_bay = min(most_left_df['bay_number'].unique().tolist())
                    most_left_value = most_left_df.loc[most_left_df['bay_number']
                                                       == most_left_bay]['left'].values[0]
                    left_most_candidates = most_left_df.loc[
                        (most_left_df['bay_number'] == most_left_bay) & (most_left_df['left'] <= most_left_value)]
                    if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
                        for i in reversed(left_most_candidates.index):
                            match = left_most_candidates.loc[[i]]
                            if match['product_type'][i] == EMPTY:
                                most_left_value = match['left'][i]
                            else:
                                break
                    sum_left += most_left_value
                    self.shelf_square_boundaries[shelf] = {'left': most_left_value}
                    # right side
                    temp.sort_values(by=['right'], inplace=True)
                    most_right_df = temp.loc[temp[object_field].isin(values_list)]
                    most_right_bay = max(most_right_df['bay_number'].unique().tolist())
                    most_right_value = most_right_df.loc[most_right_df['bay_number']
                                                         == most_right_bay]['right'].values[-1]
                    right_most_candidates = most_right_df.loc[
                        (most_right_df['bay_number'] == most_right_bay) & (most_right_df['right'] >= most_right_value)]
                    if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
                        for i in right_most_candidates.index:
                            match = right_most_candidates.loc[[i]]
                            if match['product_type'][i] == EMPTY:
                                most_right_value = match['right'][i]
                            else:
                                break
                    sum_right += most_right_value
                    self.shelf_square_boundaries[shelf]['right'] = most_right_value
                empties_ratio = 1  # Start condition: Rectangle should not be checked
                average_left = 0
                average_right = 0
                if shelves:
                    average_left = sum_left / float(len(shelves))
                    average_right = sum_right / float(len(shelves))
                    initial_square_df = potential_products_df.loc[(potential_products_df['left'] > average_left) &
                                                                  (potential_products_df['right'] < average_right)]  # todo: why this is the calculation?
                    if not initial_square_df.empty:
                        total_products_in_square = len(initial_square_df.index)
                        total_empties_in_square = \
                            len(potential_products_df.loc[(
                                potential_products_df['product_type'] == EMPTY)].index)  # todo: remove stacking empty other
                        empties_ratio = total_empties_in_square / float(total_products_in_square)
                non_rect_conditions = (not initial_square_df.loc[
                    ~(initial_square_df[object_field].isin(values_list))].empty) \
                    or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
                if non_rect_conditions:
                    scenes_results_dict[scene] = 0
                else:
                    average_width = initial_square_df['width'].mean()
                    max_dev = ALLOWED_DEVIATION * average_width
                    square_shelves_counter = 0
                    for shelf in shelves:
                        if (abs(self.shelf_square_boundaries[shelf].get('left') - average_left)
                                + abs(self.shelf_square_boundaries[shelf].get('right') - average_right)) < max_dev:
                            square_shelves_counter += 1
                    if square_shelves_counter != len(shelves):
                        scenes_results_dict[scene] = 0
                    else:
                        scenes_results_dict[scene] = 1
            if sum(scenes_results_dict.values()) == len(scenes_to_check):
                result = 'TRUE'
            else:
                result = 'FALSE'

        return result

    def check_brand_block_2018(self, object_field, values_list, sub_brands_to_exclude, form_factors,
                               sub_brand_by_sub_cat={}):
        if not self.potential_products:
            result = 'FALSE'
        else:
            scenes_results_dict = {}
            self.shelf_square_boundaries = {}
            sum_left = 0
            sum_right = 0
            potential_products_df = pd.DataFrame.from_dict(self.potential_products, orient='index')
            scenes_to_check = potential_products_df['scene_fk'].unique().tolist()
            for scene in scenes_to_check:
                shelves = potential_products_df.loc[potential_products_df['scene_fk'] == scene][
                    'shelf'].unique().tolist()
                min_facings_on_shelf = 0
                relevant_shelf_counter = 0
                for shelf in shelves:
                    temp = potential_products_df.loc[
                        (potential_products_df['scene_fk'] == scene) & (potential_products_df['shelf'] == shelf)]
                    shelf_sub_category = temp['sub_category'].unique().tolist()
                    for category in shelf_sub_category:
                        if type(category) == str:
                            shelf_sub_category = category.upper()
                            break
                    if shelf_sub_category in sub_brand_by_sub_cat.keys() and not sub_brands_to_exclude:
                        sub_brands_to_exclude = sub_brand_by_sub_cat[shelf_sub_category]
                    # count facings on shelf:
                    facings_on_shelf = temp.loc[(temp['brand_name'].isin(values_list)) &
                                                (temp['stacking_layer'] == 1) &
                                                (temp['form_factor'].isin(form_factors)) &
                                                (~temp['sub_brand'].isin(sub_brands_to_exclude))]['brand_name'].count()
                    if facings_on_shelf:
                        relevant_shelf_counter += 1
                        if not min_facings_on_shelf:
                            min_facings_on_shelf = facings_on_shelf
                            min_shelf = shelf
                        else:
                            if facings_on_shelf < min_facings_on_shelf:
                                min_facings_on_shelf = facings_on_shelf
                                min_shelf = shelf
                        # left side
                        temp.sort_values(by=['left'], inplace=True)
                        most_left_df = temp.loc[(temp[object_field].isin(values_list)) &
                                                ((temp['form_factor'].isin(form_factors)) |
                                                 (temp['product_type'].isin([OTHER]))) &
                                                (~temp['sub_brand'].isin(sub_brands_to_exclude))]
                        most_left_bay = min(most_left_df['bay_number'].unique().tolist())
                        most_left_value = most_left_df.loc[most_left_df['bay_number'] == most_left_bay][
                            'left'].values[0]
                        left_most_candidates = most_left_df.loc[(most_left_df['bay_number'] == most_left_bay) &
                                                                (most_left_df['left'] <= most_left_value)]
                        if not left_most_candidates.loc[left_most_candidates['product_type'].isin([EMPTY])].empty:
                            for i in reversed(left_most_candidates.index):
                                match = left_most_candidates.loc[[i]]
                                if match['product_type'][i].isin(EMPTY):
                                    most_left_value = match['left'][i]
                                else:
                                    break
                        sum_left += most_left_value
                        self.shelf_square_boundaries[shelf] = {'left_boundary': most_left_value}
                        # right side
                        temp.sort_values(by=['right'], inplace=True)
                        most_right_df = temp.loc[(temp[object_field].isin(values_list)) &
                                                 ((temp['form_factor'].isin(form_factors)) |
                                                  (temp['product_type'].isin([OTHER]))) &
                                                 (~temp['sub_brand'].isin(sub_brands_to_exclude))]
                        most_right_bay = max(most_right_df['bay_number'].unique().tolist())
                        most_right_value = most_right_df.loc[most_right_df['bay_number'] == most_right_bay][
                            'right'].values[-1]
                        right_most_candidates = most_right_df.loc[
                            (most_right_df['bay_number'] == most_right_bay) &
                            (most_right_df['right'] >= most_right_value)]
                        if not right_most_candidates.loc[right_most_candidates['product_type'].isin([EMPTY])].empty:
                            for i in right_most_candidates.index:
                                match = right_most_candidates.loc[[i]]
                                if match['product_type'][i].isin(EMPTY):
                                    most_right_value = match['right'][i]
                                else:
                                    break
                        sum_right += most_right_value
                        self.shelf_square_boundaries[shelf]['right_boundary'] = most_right_value
                if relevant_shelf_counter == 1:
                    if min_facings_on_shelf > 4:
                        result = 'FALSE'
                        return result
                empties_ratio = 1  # Start condition: Rectangle should not be checked
                min_shelf_left = 0
                min_shelf_right = 0
                if relevant_shelf_counter == 0:
                    scenes_results_dict[scene] = 1
                    continue
                if shelves:
                    min_shelf_left = self.shelf_square_boundaries[min_shelf]['left_boundary']
                    min_shelf_right = self.shelf_square_boundaries[min_shelf]['right_boundary']
                    boundaries_list = list(self.shelf_square_boundaries.items())
                    boundaries_df = pd.DataFrame(boundaries_list, columns=[
                                                 'shelf', 'left_right_boundaries'])
                    boundaries_df['left_right_boundaries'] = boundaries_df['left_right_boundaries'].astype(
                        str)
                    potential_products_df = pd.merge(
                        potential_products_df, boundaries_df, on='shelf')
                    left_right_boundaries_df = pd.DataFrame(
                        [ast.literal_eval(i) for i in potential_products_df.left_right_boundaries.values])
                    potential_products_df = potential_products_df.drop(
                        'left_right_boundaries', axis=1)
                    final_potential_products_df = pd.concat(
                        [potential_products_df, left_right_boundaries_df], axis=1)
                    initial_square_df = final_potential_products_df.loc[
                        (final_potential_products_df['left'] >= final_potential_products_df['left_boundary']) &
                        (final_potential_products_df['right'] <= final_potential_products_df['right_boundary'])]
                    if not initial_square_df.empty:
                        total_products_in_square = len(initial_square_df.index)
                        total_empties_in_square = \
                            len(initial_square_df.loc[(initial_square_df['product_type'].isin([EMPTY])) |
                                                      ((initial_square_df['product_type'].isin([OTHER])) &
                                                       (~initial_square_df['brand_name'].isin(values_list))) &
                                                      (initial_square_df['stacking_layer'] == 1)].index)
                        empties_ratio = total_empties_in_square / float(total_products_in_square)
                non_rect_conditions = (not initial_square_df.loc[
                    ~((initial_square_df[object_field].isin(values_list)) &
                      ((initial_square_df['form_factor'].isin(form_factors)) |
                       (initial_square_df['product_type'].isin([OTHER]))) &
                      (~initial_square_df['sub_brand'].isin(sub_brands_to_exclude)))].empty) \
                    or empties_ratio > ALLOWED_EMPTIES_RATIO or not shelves
                if non_rect_conditions:
                    # scene_result = 'FALSE'
                    scenes_results_dict[scene] = 0
                else:
                    average_width = initial_square_df['width'].mean()
                    max_dev = ALLOWED_DEVIATION_2018 * average_width
                    square_shelves_counter = 0
                    relevant_shelves = self.shelf_square_boundaries.keys()
                    for shelf in relevant_shelves:
                        if (abs(self.shelf_square_boundaries[shelf].get('left_boundary') - min_shelf_left)
                            + abs(self.shelf_square_boundaries[shelf].get(
                                'right_boundary') - min_shelf_right)) < max_dev:
                            square_shelves_counter += 1
                    if square_shelves_counter != len(shelves):
                        scenes_results_dict[scene] = 0
                    else:
                        scenes_results_dict[scene] = 1
            if sum(scenes_results_dict.values()) == len(scenes_to_check):
                result = 'TRUE'
            else:
                result = 'FALSE'
        return result

    def initial_mapping_of_square(self, scenes, matches, object_field, values_list, p, form_factor=None,
                                  sub_brand_to_exclude=None, sub_brand_by_sub_cat={}):
        self.potential_products = {}
        if not scenes:
            return
        else:
            for scene in scenes:
                brand_counter = 0
                brands_presence_indicator = True
                scene_data = matches.loc[matches['scene_fk'] == scene]
                if p.get('Formula') != 'custom_mars_2_2018':
                    if form_factor:
                        scene_data = scene_data.loc[scene_data['form_factor'].isin(form_factor)]
                    scene_sub_category = scene_data['sub_category'].unique().tolist()
                    if scene_sub_category and scene_sub_category[0] is None:
                        scene_sub_category.remove(None)
                    if scene_sub_category:
                        scene_sub_category = scene_sub_category[0].upper()
                    if scene_sub_category in sub_brand_by_sub_cat.keys() and not sub_brand_to_exclude:
                        sub_brand_to_exclude = sub_brand_by_sub_cat[scene_sub_category]
                    if sub_brand_to_exclude:
                        scene_data = scene_data.loc[~scene_data['sub_brand'].isin(
                            sub_brand_to_exclude)]
                shelves = scene_data['shelf_number'].unique().tolist()
                # unified_scene_set = set(scene_data[object_field]) & set(values_list)
                unified_scene_set = scene_data.loc[scene_data[object_field].isin(values_list)]
                if len(values_list) > 1:
                    for brand in values_list:
                        brand_df = scene_data.loc[scene_data[object_field] == brand]
                        if not brand_df.empty:
                            brand_counter += 1
                    if brand_counter != len(values_list):
                        brands_presence_indicator = False
                if unified_scene_set.empty or not brands_presence_indicator:
                    continue
                else:
                    is_sequential_shelves = False
                    for shelf_number in shelves:
                        shelf_data = scene_data.loc[scene_data['shelf_number'] == shelf_number]
                        bays = shelf_data['bay_number'].unique().tolist()
                        # for bay in bays:
                        temp_shelf_data = shelf_data.reset_index()
                        unified_shelf_set = shelf_data.loc[shelf_data[object_field].isin(
                            values_list)]
                        # unified_shelf_set = set(shelf_data[object_field]) & set(values_list)
                        # if not unified_shelf_set:
                        if unified_shelf_set.empty:
                            if is_sequential_shelves:
                                # is_sequential_shelves = False
                                return
                            continue
                        else:
                            is_sequential_shelves = True
                            for i in temp_shelf_data.index:
                                match = temp_shelf_data.loc[[i]]
                                if match['bay_number'].values[0] == bays[0]:
                                    self.potential_products[match['scene_match_fk'].values[0]] = {
                                        'top': int(match[self.kpi_fetcher.TOP]),
                                        'bottom': int(
                                            match[self.kpi_fetcher.BOTTOM]),
                                        'left': int(
                                            match[self.kpi_fetcher.LEFT]),
                                        'right': int(
                                            match[self.kpi_fetcher.RIGHT]),
                                        object_field: match[object_field].values[0],
                                        'product_type': match['product_type'].values[0],
                                        'product_name': match['product_name'].values[0].encode('utf-8'),
                                        'shelf': shelf_number,
                                        'width': match['width_mm'].values[0],
                                        'bay_number': match['bay_number'].values[0],
                                        'scene_fk': scene,
                                        'form_factor': match['form_factor'].values[0],
                                        'sub_brand': match['sub_brand'].values[0],
                                        'stacking_layer': match['stacking_layer'].values[0],
                                        'shelf_px_total': int(match['shelf_px_total'].values[0]),
                                        'sub_category': match['sub_category'].values[0]}
                                else:
                                    sum_of_px_to_add = \
                                        temp_shelf_data.loc[
                                            temp_shelf_data['bay_number'] < match['bay_number'].values[0]][
                                            'shelf_px_total'].unique().sum()
                                    self.potential_products[match['scene_match_fk'].values[0]] = {
                                        'top': int(match[self.kpi_fetcher.TOP]),
                                        'bottom': int(
                                            match[self.kpi_fetcher.BOTTOM]),
                                        'left': int(
                                            match[self.kpi_fetcher.LEFT]) + int(sum_of_px_to_add),
                                        'right': int(
                                            match[self.kpi_fetcher.RIGHT]) + int(sum_of_px_to_add),
                                        object_field: match[object_field].values[0],
                                        'product_type': match['product_type'].values[0],
                                        'product_name': match['product_name'].values[0].encode('utf-8'),
                                        'shelf': shelf_number,
                                        'width': match['width_mm'].values[0],
                                        'bay_number': match['bay_number'].values[0],
                                        'scene_fk': scene,
                                        'form_factor': match['form_factor'].values[0],
                                        'sub_brand': match['sub_brand'].values[0],
                                        'stacking_layer': match['stacking_layer'].values[0],
                                        'shelf_px_total': int(match['shelf_px_total'].values[0])}

    @kpi_runtime()
    def multiple_brands_blocked_in_rectangle(self, params):
        for p in params:
            if p.get('Formula') != 'custom_mars_4' and p.get('Formula') != 'custom_mars_4_2018':
                continue

            brands_results_dict = {}
            values_list = str(p.get('Values')).split(', *')
            scenes = self.get_relevant_scenes(p)
            object_field = self.object_type_conversion[p.get('Type')]
            if p.get('Include Stacking'):
                matches = self.kpi_fetcher.get_filtered_matches()
            else:
                matches = self.kpi_fetcher.get_filtered_matches(include_stacking=False)

            if p.get('Sub brand to exclude'):
                sub_brands = [str(sub_brand)
                              for sub_brand in p.get('Sub brand to exclude').split(", ")]
            else:
                sub_brands = []

            if p.get("Form Factor to include"):
                form_factors = [str(form_factor)
                                for form_factor in p.get("Form Factor to include").split(", ")]
            else:
                form_factors = []

            for value in values_list:
                self.potential_products = {}
                if ',' in value:
                    sub_values_list = str(value).split(', ')
                    self.initial_mapping_of_square(scenes, matches, object_field, sub_values_list, p,
                                                   form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    if p.get('Formula') == 'custom_mars_4':
                        brand_result = self.check_brand_block(object_field, sub_values_list)
                    else:
                        brand_result = self.check_brand_block_2018(
                            object_field, sub_values_list, sub_brands, form_factors)
                else:
                    self.initial_mapping_of_square(scenes, matches, object_field, [value], p, form_factor=form_factors,
                                                   sub_brand_to_exclude=sub_brands)
                    if p.get('Formula') == 'custom_mars_4':
                        brand_result = self.check_brand_block(object_field, [value])
                    else:
                        brand_result = self.check_brand_block_2018(object_field, [value], sub_brands,
                                                                   form_factors)
                if brand_result == 'TRUE':
                    brands_results_dict[value] = 1
                else:
                    brands_results_dict[value] = 0

            if sum(brands_results_dict.values()) == len(values_list):
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def golden_shelves(self, params):
        """
        This function checks if a predefined product is present in golden shelves
        """
        for p in params:
            if p.get('Formula') != 'custom_mars_5':
                continue

            values_list = str(p.get('Values')).split(', ')
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
                                                                             form_factor=[
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
            values_list = str(p.get('Values')).split(', ')
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

            brand_category = p.get('Brand Category value')
            if brand_category and ', ' in brand_category:
                brand_category = brand_category.split(', ')

            object_facings = self.kpi_fetcher.get_object_facings(scenes, values_list, p.get('Type'),
                                                                 formula='number of facings',
                                                                 form_factor=form_factors,
                                                                 brand_category=brand_category,
                                                                 include_stacking=include_stacking,
                                                                 linear=linear)
            brands_to_check = self.scif.loc[self.scif['scene_id'].isin(scenes) &
                                            ~(self.scif['brand_name'].isin(values_list))][
                'brand_name'].unique().tolist()
            for brand in brands_to_check:
                brand_facings = self.kpi_fetcher.get_object_facings(scenes, [brand], p.get('Type'),
                                                                    formula='number of facings',
                                                                    form_factor=form_factors,
                                                                    brand_category=brand_category,
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
    def must_range_skus(self, params):
        for p in params:
            if p.get('Formula') != 'custom_mars_7':
                continue

            if p.get('#Mars KPI NAME') == 4704:  # If ((4269+4271)/(4270+4272))*100% >= 100% then TRUE
                kpi_part_1 = self.dict_for_planogram[4269] + self.dict_for_planogram[4271]
                kpi_part_2 = self.dict_for_planogram[4270] + self.dict_for_planogram[4272]
                ratio = kpi_part_1 / kpi_part_2 if kpi_part_2 else 0

                result = 'FALSE' if ratio < 1 else 'TRUE'

            else:

                values_list = self.kpi_fetcher.get_must_range_skus_by_region_and_store(self.store_type,
                                                                                       self.region,
                                                                                       p.get('#Mars KPI NAME'),
                                                                                       self.results_and_scores)
                scenes = self.get_relevant_scenes(p)
                result = None
                if values_list:
                    if p.get('#Mars KPI NAME') == 2317:

                        top_eans = p.get('Values').split('\n')
                        top_products_in_store = self.scif[self.scif['product_ean_code'].isin(
                            top_eans)]['product_fk'].unique().tolist()

                        min_shelf, max_shelf = values_list.split('-')
                        min_shelf, max_shelf = int(min_shelf), int(max_shelf)
                        top_products_on_golden_shelf = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'].isin(scenes)) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] >= min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] <= max_shelf) &
                            (self.match_product_in_scene['product_fk'].isin(top_products_in_store))]['product_fk'].unique().tolist()
                        top_products_outside_golden_shelf = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'].isin(scenes)) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] < min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] > max_shelf) &
                            (self.match_product_in_scene['product_fk'].isin(top_products_in_store))]['product_fk'].unique().tolist()

                        if len(top_products_on_golden_shelf) < len(top_products_in_store) or len(top_products_outside_golden_shelf) > 0:
                            result = 'FALSE'
                        else:
                            result = 'TRUE'

                    if p.get('#Mars KPI NAME') in (4317, 4650):

                        top_eans = p.get('Values').split('\n')
                        top_products_in_store = self.scif[
                            (self.scif['scene_fk'].isin(scenes)) &
                            (self.scif['product_ean_code'].isin(top_eans))]['product_fk'].unique().tolist()

                        min_shelf, max_shelf = values_list.split('-')
                        min_shelf, max_shelf = int(min_shelf), int(max_shelf)
                        top_products_on_golden_shelf = self.match_product_in_scene[
                            (self.match_product_in_scene['scene_fk'].isin(scenes)) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] >= min_shelf) &
                            (self.match_product_in_scene['shelf_number_from_bottom'] <= max_shelf) &
                            (self.match_product_in_scene['product_fk'].isin(top_products_in_store))]['product_fk'].unique().tolist()

                        if len(top_products_on_golden_shelf) < len(top_products_in_store):
                            result = 'FALSE'
                        else:
                            result = 'TRUE'

                    elif p.get('#Mars KPI NAME') == 2254:
                        if self.dict_for_planogram[2264] or self.dict_for_planogram[2351]:
                            kpi_part_1 = self.dict_for_planogram[2261] / self.dict_for_planogram[2264] \
                                if self.dict_for_planogram[2264] > 0 else 0
                            kpi_part_2 = self.dict_for_planogram[2265] / self.dict_for_planogram[2351] \
                                if self.dict_for_planogram[2351] > 0 else 0
                            mars_shelf_size = kpi_part_1 + kpi_part_2
                            for row in values_list:
                                if row['shelf from'] <= mars_shelf_size < row['shelf to']:
                                    result = str(row['result'])

                    elif p.get('#Mars KPI NAME') == 4254:
                        if self.dict_for_planogram[4261]+self.dict_for_planogram[4265] < p.get('Target'):
                            for row in values_list:
                                if row['length_condition'] == '<' + str(int(p.get('Target'))):
                                    result = str(row['result'])
                                    break
                        elif self.dict_for_planogram[4264] or self.dict_for_planogram[4351]:
                            kpi_part_1 = self.dict_for_planogram[4261] / self.dict_for_planogram[4264] \
                                if self.dict_for_planogram[4264] > 0 else 0
                            kpi_part_2 = self.dict_for_planogram[4265] / self.dict_for_planogram[4351] \
                                if self.dict_for_planogram[4351] > 0 else 0
                            mars_shelf_size = kpi_part_1 + kpi_part_2
                            for row in values_list:
                                if row['shelf from'] <= mars_shelf_size < row['shelf to'] \
                                        and row['length_condition'] == '>=' + str(int(p.get('Target'))):
                                    result = str(row['result'])
                                    break

                    else:
                        sub_results = []
                        for value in values_list:
                            kpi_res = self.calculate_availability(p, scenes, formula='number of SKUs',
                                                                  values_list=value.split('/'),
                                                                  object_type='SKUs', include_stacking=True)
                            if kpi_res > 0:
                                sub_result = 1
                            else:
                                sub_result = 0
                            sub_results.append(sub_result)
                        sum_of_facings = sum(sub_results)
                        if sum_of_facings >= len(values_list):
                            result = 'TRUE'
                        else:
                            result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

    @kpi_runtime()
    def negative_neighbors(self, params):
        for p in params:
            if p.get('Formula') != 'custom_mars_6':
                continue

            object_field = self.object_type_conversion[p.get('Type')]
            values_list = str(p.get('Values')).split(', ')
            competitor_brands = str(p.get('competitor_brands')).split(', ')
            scenes = self.get_relevant_scenes(p)
            tested_filters = {object_field: values_list, 'category': p.get('Brand Category value')}

            # First check - negative adjacency to MARS products
            mars_anchor_filters = {'manufacturer_name': MARS}
            negative_mars_adjacency_result = self.calculate_non_proximity(tested_filters,
                                                                          mars_anchor_filters,
                                                                          scene_fk=scenes)

            # Second check - positive adjacency to competitor brands
            competitor_anchor_filters = {'brand_name': competitor_brands}
            direction_data2 = {'top': POSITIVE_ADJACENCY_RANGE,
                               'bottom': POSITIVE_ADJACENCY_RANGE,
                               'left': POSITIVE_ADJACENCY_RANGE,
                               'right': POSITIVE_ADJACENCY_RANGE}
            competitor_adjacency_result = self.calculate_relative_position(tested_filters,
                                                                           competitor_anchor_filters,
                                                                           direction_data2,
                                                                           scene_fk=scenes)

            if negative_mars_adjacency_result and competitor_adjacency_result:
                result = 'TRUE'
            else:
                result = 'FALSE'

            self.store_results_and_scores(result, p)

            self.store_to_old_kpi_tables(p)
            self.store_to_new_kpi_tables(p)

        return

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
            for values in p.get('Values').split('\nOR\n'):
                targets, others, percent = values.split('\n')
                target_filter = self.get_filter_condition(
                    self.scif, **(self.parse_filter_from_template(targets)))
                other_filter = self.get_filter_condition(
                    self.scif, **(self.parse_filter_from_template(others)))
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
            allowed_products_filters = {'product_fk': products_allowed}

            scenes = filtered_targets_scif['scene_id'].unique().tolist()

            result = None
            if products_targets and scenes:
                for scene in scenes:
                    filters['scene_id'] = scene
                    # result = self.calculate_block_together(allowed_products_filters=allowed_products_filters,
                    #                                                        minimum_block_ratio=1, include_empty=True,
                    #                                                        **filters)
                    result = self.block.calculate_block_together(allowed_products_filters=allowed_products_filters,
                                                                 minimum_block_ratio=1, include_empty=True,
                                                                 **filters)
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

    def calculate_block_together(self, allowed_products_filters=None, include_empty=EXCLUDE_EMPTY,
                                 minimum_block_ratio=1, result_by_scene=False, vertical=False, **filters):
        """
        :param vertical: if needed to check vertical block by average shelf
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param minimum_block_ratio: The minimum (block number of facings / total number of relevant facings) ratio
                                    in order for KPI to pass (if ratio=1, then only one block is allowed).
        :param result_by_scene: True - The result is a tuple of (number of passed scenes, total relevant scenes);
                                False - The result is True if at least one scene has a block, False - otherwise.
        :param filters: These are the parameters which the blocks are checked for.
        :return: see 'result_by_scene' above.
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            if result_by_scene:
                return 0, 0
            else:
                Log.debug('Block Together: No relevant SKUs were found for these filters {}'.format(filters))
                return True
        number_of_blocked_scenes = 0
        cluster_ratios = []
        for scene in relevant_scenes:
            scene_graph = self.position_graphs.get(scene, horizontal_block_only=True).copy()
            clusters, scene_graph = self.get_scene_blocks(scene_graph, allowed_products_filters=allowed_products_filters,
                                                          include_empty=include_empty, **filters)
            new_relevant_vertices = self.filter_vertices_from_graph(scene_graph, **filters)
            for cluster in clusters:
                relevant_vertices_in_cluster = set(cluster).intersection(new_relevant_vertices)
                if len(new_relevant_vertices) > 0:
                    cluster_ratio = len(relevant_vertices_in_cluster) / \
                        float(len(new_relevant_vertices))
                else:
                    cluster_ratio = 0
                cluster_ratios.append(cluster_ratio)
                if cluster_ratio >= minimum_block_ratio:
                    if result_by_scene:
                        number_of_blocked_scenes += 1
                        break
                    else:
                        if minimum_block_ratio == 1:
                            return True
                        else:
                            all_vertices = {v.index for v in scene_graph.vs}
                            non_cluster_vertices = all_vertices.difference(cluster)
                            scene_graph.delete_vertices(non_cluster_vertices)
                            if vertical:
                                return {'block': True, 'shelves': len(
                                    set(scene_graph.vs['shelf_number']))}
                            return cluster_ratio, scene_graph
        if result_by_scene:
            return number_of_blocked_scenes, len(relevant_scenes)
        else:
            if minimum_block_ratio == 1:
                return False
            elif cluster_ratios:
                return max(cluster_ratios)
            else:
                return None

    def calculate_non_proximity(self, tested_filters, anchor_filters, allowed_diagonal=False, **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param allowed_diagonal: True - a tested SKU can be in a direct diagonal from an anchor SKU in order
                                        for the KPI to pass;
                                 False - a diagonal proximity is NOT allowed.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return:
        """
        direction_data = []
        if allowed_diagonal:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1)})
            direction_data.append({'right': (0, 1), 'left': (0, 1)})
        else:
            direction_data.append({'top': (0, 1), 'bottom': (0, 1),
                                   'right': (0, 1), 'left': (0, 1)})
        is_proximity = self.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                        min_required_to_pass=1, **general_filters)
        return not is_proximity

    def calculate_relative_position(self, tested_filters, anchor_filters, direction_data, min_required_to_pass=1,
                                    **general_filters):
        """
        :param tested_filters: The tested SKUs' filters.
        :param anchor_filters: The anchor SKUs' filters.
        :param direction_data: The allowed distance between the tested and anchor SKUs.
                               In form: {'top': 4, 'bottom: 0, 'left': 100, 'right': 0}
                               Alternative form: {'top': (0, 1), 'bottom': (1, 1000), ...} - As range.
        :param min_required_to_pass: The number of appearances needed to be True for relative position in order for KPI
                                     to pass. If all appearances are required: ==a string or a big number.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: True if (at least) one pair of relevant SKUs fits the distance requirements; otherwise - returns False.
        """
        filtered_scif = self.scif[self.get_filter_condition(self.scif, **general_filters)]
        tested_scenes = filtered_scif[self.get_filter_condition(
            filtered_scif, **tested_filters)]['scene_id'].unique()
        anchor_scenes = filtered_scif[self.get_filter_condition(
            filtered_scif, **anchor_filters)]['scene_id'].unique()
        relevant_scenes = set(tested_scenes).intersection(anchor_scenes)

        if relevant_scenes:
            pass_counter = 0
            reject_counter = 0
            for scene in relevant_scenes:
                scene_graph = self.position_graphs.get(scene)
                tested_vertices = self.filter_vertices_from_graph(scene_graph, **tested_filters)
                anchor_vertices = self.filter_vertices_from_graph(scene_graph, **anchor_filters)
                for tested_vertex in tested_vertices:
                    for anchor_vertex in anchor_vertices:
                        moves = {'top': 0, 'bottom': 0, 'left': 0, 'right': 0}
                        path = scene_graph.get_shortest_paths(
                            anchor_vertex, tested_vertex, output='epath')
                        if path:
                            path = path[0]
                            for edge in path:
                                moves[scene_graph.es[edge]['direction']] += 1
                            if self.validate_moves(moves, direction_data):
                                pass_counter += 1
                                if isinstance(min_required_to_pass, int) and pass_counter >= min_required_to_pass:
                                    return True
                            else:
                                reject_counter += 1
                        else:
                            Log.debug('Tested and Anchor have no direct path')
            if pass_counter > 0 and reject_counter == 0:
                return True
            else:
                return False
        else:
            Log.debug('None of the scenes contain both anchor and tested SKUs')
            return False

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
        self.results_and_scores[str(params.get('#Mars KPI NAME'))] = {
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

    def write_to_db_result_level1(self):
        attributes_for_table1 = pd.DataFrame([(self.set_name,
                                               self.session_uid,
                                               self.store_id,
                                               self.visit_date.isoformat(),
                                               100,
                                               self.kpi_set_fk)],
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
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('#Mars KPI NAME'))

        if self.results_and_scores.get(str(params.get('#Mars KPI NAME'))):
            score = self.results_and_scores[str(params.get('#Mars KPI NAME'))]['score']
        else:
            score = None

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
        kpi_fk = self.kpi_fetcher.get_kpi_fk(params.get('#Mars KPI NAME'))
        atomic_kpi_fk = self.kpi_fetcher.get_atomic_kpi_fk(params.get('#Mars KPI NAME'))

        if self.results_and_scores.get(str(params.get('#Mars KPI NAME'))):
            result = self.results_and_scores[str(params.get('#Mars KPI NAME'))]['result']
            score = self.results_and_scores[str(params.get('#Mars KPI NAME'))]['score']
        else:
            result = None
            score = None

        attributes_for_table3 = pd.DataFrame([(params.get('KPI Display name RU').encode('utf-8').replace("'", "''"),
                                               self.session_uid,
                                               self.set_name,
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
        # API KPIs
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(str(p.get('#Mars KPI NAME')))
        parent_fk = self.common.get_kpi_fk_by_kpi_type(self.kpi_level_0_name + ' API')
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = self.transform_result(
            self.results_and_scores[str(p.get('#Mars KPI NAME'))]['result'], p)
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
        parent_fk = self.common.get_kpi_fk_by_kpi_type(self.kpi_level_0_name + ' RUS')
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = self.transform_result(
            self.results_and_scores[str(p.get('#Mars KPI NAME'))]['result'], p)
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

        self.kpi_count += 1

    def store_to_new_kpi_tables_level0(self, kpi_level_0_name):
        # API KPIs
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_level_0_name + ' API')
        parent_fk = 0
        numerator_id = self.own_manufacturer_id
        denominator_id = self.store_id
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_parent = self.common.get_dictionary(kpi_fk=parent_fk)
        result = None
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
        result = self.kpi_count
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
        # assortment_products = self.get_assortment_for_store_id()
        assortment_products = self.assortment.get_lvl3_relevant_ass()
        if assortment_products.empty:
            return

        assortment_products = assortment_products['product_fk'].tolist()
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
                numerator_result = product_facings[product_facings['product_fk']
                                                   == product]['facings'].iloc[0]
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
