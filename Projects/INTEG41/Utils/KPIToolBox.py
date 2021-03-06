import os
import pandas as pd
import numpy as np
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.INTEG41.Utils.Const import INTEG41Const
from Projects.INTEG41.Utils.Fetcher import INTEG41Queries
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.INTEG41Calculations.AvailabilityINTEG41Calculations import Availability
# from KPIUtils_v2.INTEG41Calculations.NumberOfScenesINTEG41Calculations import NumberOfScenes
# from KPIUtils_v2.INTEG41Calculations.PositionGraphsINTEG41Calculations import PositionGraphs
# from KPIUtils_v2.INTEG41Calculations.SOSINTEG41Calculations import SOS
# from KPIUtils_v2.INTEG41Calculations.SequenceINTEG41Calculations import Sequence
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'Elyashiv'

OFF_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'off_premise Template.xlsx')
ON_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'on_premise Template.xlsx')


class INTEG41ToolBox:
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.assortment = Assortment(self.data_provider, self.output)
        self.fetcher = INTEG41Queries
        self.survey = Survey(self.data_provider, self.output)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif_without_emptys = self.scif[~(self.scif['product_type'] == "Empty")]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.state = self.get_state()
        self.sub_brands = self.get_sub_brands()
        # this function is temporary
        self.refresh_sub_brands()
        #
        self.result_values = self.get_result_values()
        self.products_with_prices = self.get_products_prices()
        self.kpi_static_data = self.common.kpi_static_data
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'DIAGEO']['manufacturer_fk'].iloc[0]
        store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.on_premise = True if store_type in ('Dining', 'Bar/Nightclub') else False
        self.templates = {}
        self.get_templates()
        self.kpi_results_queries = []
        if self.on_premise:
            self.sales_data = self.get_sales_data()
            self.no_menu_allowed = self.survey.check_survey_answer(survey_text=INTEG41Const.NO_MENU_ALLOWED_QUESTION,
                                                                   target_answer=INTEG41Const.SURVEY_ANSWER)
        else:
            self.scenes = self.scif_without_emptys['scene_fk'].unique().tolist()
            self.scenes_with_shelves = {}
            for scene in self.scenes:
                shelf = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene][
                    'shelf_number'].max()
                self.scenes_with_shelves[scene] = shelf
            self.converted_groups = self.convert_groups_from_template()
            self.calculated_price, self.calculated_shelf_facings = [], []
            self.no_display_allowed = self.survey.check_survey_answer(survey_text=INTEG41Const.NO_DISPLAY_ALLOWED_QUESTION,
                                                                      target_answer=INTEG41Const.SURVEY_ANSWER)
        self.assortment_products = self.assortment.get_lvl3_relevant_ass()

    # initialize:

    def get_sales_data(self):
        """
        returns the list of the sub_brands relevant for the store and date
        :return: list of strings
        """
        query = self.fetcher.get_sales_data(self.store_id, self.visit_date)
        df = pd.read_sql_query(query, self.rds_conn.db)
        products_list = df['product_fk'].tolist()
        return products_list

    def get_sub_brands(self):
        """
        returns the DF of the sub_brands
        :return:
        """
        query = self.fetcher.get_sub_brands()
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def refresh_sub_brands(self):
        """
        temporary function - checks if there are new sub_brands that don't exist in the DB, and uploads them
        :return:
        """
        all_sub_brands = self.all_products['Sub Brand'].unique().tolist()
        current_sub_brand = self.sub_brands['name'].unique().tolist()
        subs_not_in_db = set(all_sub_brands) - set(current_sub_brand)
        if subs_not_in_db:
            self.insert_new_subs(subs_not_in_db)

    def insert_new_subs(self, new_subs):
        """
        Temporary function: Gets a list of all sub_brands not in the DB and inserts them.
        After that - reloads the local DF.
        :param new_subs: list
        :return:
        """
        queries = []
        all_new_subs = self.all_products[self.all_products['Sub Brand'].isin(new_subs)]
        for i, line in all_new_subs.iterrows():
            sub_brand = line['Sub Brand']
            brand = line['brand_fk']
            if sub_brand:
                queries.append(self.fetcher.insert_new_sub_brands(sub_brand, brand))
        merge_queries = self.common.merge_insert_queries(queries)
        cur = self.rds_conn.db.cursor()
        for query in merge_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.sub_brands = self.get_sub_brands()

    def get_templates(self):
        """
        Reads the template (and makes the EANs be Strings)
        """
        if self.on_premise:
            for sheet in INTEG41Const.ON_SHEETS:
                self.templates[sheet] = pd.read_excel(ON_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                      keep_default_na=False)
        else:
            for sheet in INTEG41Const.OFF_SHEETS:
                if sheet in ([INTEG41Const.SHELF_FACING_SHEET, INTEG41Const.PRICING_SHEET]):
                    converters = {INTEG41Const.OUR_EAN_CODE: lambda x: str(x).replace(".0", ""),
                                  INTEG41Const.COMP_EAN_CODE: lambda x: str(x).replace(".0", "")}
                    self.templates[sheet] = pd.read_excel(OFF_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                          converters=converters, keep_default_na=False)
                elif sheet == INTEG41Const.SHELF_PLACMENTS_SHEET:
                    converters = {INTEG41Const.PRODUCT_EAN_CODE: lambda x: str(x).replace(".0", "")}
                    self.templates[sheet] = pd.read_excel(OFF_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                          converters=converters, keep_default_na=False)
                else:
                    self.templates[sheet] = pd.read_excel(OFF_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                          keep_default_na=False)

    def get_state(self):
        """
        :return: state_fk
        """
        if not self.data_provider[Data.STORE_INFO]['state_fk'][0]:
            Log.error("session '{}' does not have a state".format(self.session_uid))
            return INTEG41Const.OTHER
        query = self.fetcher.get_state().format(self.data_provider[Data.STORE_INFO]['state_fk'][0])
        state = pd.read_sql_query(query, self.rds_conn.db)
        return state.values[0][0]

    def get_result_values(self):
        """
        gets the static.kpi_result_value table from DB
        :return: DF
        """
        query = self.fetcher.get_result_values()
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_products_prices(self):
        """
        Gets all the session's products and prices from DB
        :return:
        """
        query = self.fetcher.get_prices_dataframe().format(self.session_uid)
        products_with_prices = pd.read_sql_query(query, self.rds_conn.db)
        return products_with_prices[~products_with_prices['price_value'].isnull()]

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        total_store_score, segment_store_score, national_store_score = 0, 0, 0
        if self.on_premise:
            total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_TOTAL)
            segment_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_SEGMENT)
            national_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NATIONAL)
        else:
            total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_TOTAL)
            segment_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_SEGMENT)
            national_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NATIONAL)
        for i, kpi_line in self.templates[INTEG41Const.KPIS_SHEET].iterrows():
            total_weighted_score, segment_weighted_score, national_weighted_score = self.calculate_set(kpi_line)
            if kpi_line[INTEG41Const.KPI_GROUP]:
                total_store_score += total_weighted_score
                segment_store_score += segment_weighted_score
                national_store_score += national_weighted_score
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, result=total_store_score,
            identifier_result=self.common.get_dictionary(name=INTEG41Const.TOTAL), score=total_store_score)
        if segment_kpi_fk and national_kpi_fk:
            nothing = self.get_pks_of_result(' ')
            self.common.write_to_db_result(
                fk=segment_kpi_fk, numerator_id=self.manufacturer_fk, result=nothing,
                identifier_result=self.common.get_dictionary(name=INTEG41Const.SEGMENT), score=segment_store_score)
            self.common.write_to_db_result(
                fk=national_kpi_fk, numerator_id=self.manufacturer_fk, result=nothing,
                identifier_result=self.common.get_dictionary(name=INTEG41Const.NATIONAL), score=national_store_score)

    def calculate_set(self, kpi_line):
        """
        Gets a line from the main sheet, and transports it to the match function
        :param kpi_line: series - {KPI Name, Template Group/ Scene Type, Target, Weight}
        :return: 3 scores (total, segment, national)
        """
        kpi_name = kpi_line[INTEG41Const.KPI_NAME]
        scene_types = kpi_line[INTEG41Const.TEMPLATE_GROUP]
        target = kpi_line[INTEG41Const.TARGET]
        weight = kpi_line[INTEG41Const.WEIGHT]
        if not self.does_exist(weight):
            weight = 1
        if kpi_name == INTEG41Const.SHELF_PLACEMENT:
            total_score, segment_score, national_score = self.calculate_total_shelf_placement(
                scene_types, kpi_name, weight)
        elif kpi_name == INTEG41Const.SHELF_FACINGS:
            total_score, segment_score, national_score = self.calculate_total_shelf_facings(
                scene_types, kpi_name, weight)
        elif kpi_name == INTEG41Const.MSRP:
            total_score, segment_score, national_score = self.calculate_total_msrp(scene_types, kpi_name, weight)
        elif kpi_name == INTEG41Const.DISPLAY_SHARE:
            total_score, segment_score, national_score = self.calculate_total_display_share(scene_types, weight, target)
        elif kpi_name in (INTEG41Const.POD, INTEG41Const.DISPLAY_BRAND, INTEG41Const.BACK_BAR):
            if self.on_premise:
                total_score, segment_score, national_score = self.calculate_on_assortment(scene_types, kpi_name, weight)
            else:
                total_score, segment_score, national_score = self.calculate_assortment(scene_types, kpi_name, weight)
        elif kpi_name == INTEG41Const.MENU:
            total_score, segment_score, national_score = self.calculate_menu(scene_types, weight, target)
        else:
            Log.warning("Set {} is not defined".format(kpi_name))
            return 0, 0, 0
        return total_score, segment_score, national_score

    def survey_display_write_to_db(self, weight):
        """
        In case we don't have display (buy the survey question) we need to pass the KPI.
        :param weight: float
        :return: True if no display
        """
        if not self.no_display_allowed:
            return False
        score = 100
        dict_of_fks = {
            INTEG41Const.TOTAL: self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_BRAND][INTEG41Const.TOTAL]),
            INTEG41Const.NATIONAL: self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_BRAND][
                                                                   INTEG41Const.NATIONAL]),
            INTEG41Const.SEGMENT: self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_BRAND][
                                                                  INTEG41Const.SEGMENT])
        }
        for kpi in dict_of_fks:
            self.common.write_to_db_result(
                fk=dict_of_fks[kpi], numerator_id=self.manufacturer_fk,
                identifier_parent=self.common.get_dictionary(name=kpi),
                result=score, should_enter=True, weight=weight * 100, score=score
            )
        return True

    # assortments:

    def calculate_on_assortment(self, scene_types, kpi_name, weight):
        """
        Gets assortment type, and calculates it with the match function.
        It's working until sub_brand level (without sku)
        :param scene_types: string from template
        :param kpi_name: str ("Back Bar" or "POD")
        :param weight: float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        kpi_db_names = INTEG41Const.DB_ON_NAMES[kpi_name]
        total_on_trade_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ASSORTMENTS_NAMES[INTEG41Const.DB_ON])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_on_trade_fk]
        all_results = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in relevant_assortment.iterrows():
            additional_attrs = json.loads(product_line['additional_attributes'])
            standard_type = additional_attrs[INTEG41Const.NATIONAL_SEGMENT]
            result_line = self.calculate_pod_on(
                product_line['product_fk'], relevant_scif, standard_type, kpi_name)
            all_results = all_results.append(result_line, ignore_index=True)
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, with_standard_type=True, sub_brand_numeric=True)
        # add extra products to DB:
        if kpi_name == INTEG41Const.POD:
            self.calculate_extras_on(relevant_assortment, relevant_scif)
        return total_result, segment_result, national_result

    def calculate_extras_on(self, relevant_assortment, filtered_scif):
        """
        add the extra products (products not shown in the template) to DB.
        :param relevant_assortment: DF of assortment with all the PODs
        :param filtered_scif: scif in the match scenes
        :return:
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NAMES[INTEG41Const.POD][INTEG41Const.SKU])
        all_diageo_products = filtered_scif[
            filtered_scif['manufacturer_fk'] == self.manufacturer_fk]['product_fk'].unique().tolist()
        assortment_products = relevant_assortment['product_fk'].unique().tolist()
        products_not_in_list = set(all_diageo_products) - set(assortment_products)
        result = INTEG41Const.EXTRA
        for product in products_not_in_list:
            self.common.write_to_db_result(
                fk=sku_kpi_fk, numerator_id=product, result=self.get_pks_of_result(result))

    def calculate_pod_on(self, product_fk, relevant_scif, standard_type, kpi_name):
        """
        Checks if specific product's sub_brand exists in the filtered scif
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :param kpi_name: POD or Back Bar - to know if we should check the product in the sales data
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NAMES[kpi_name][INTEG41Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NAMES[kpi_name][INTEG41Const.TOTAL])
        brand, sub_brand = self.get_product_details(product_fk)
        facings = relevant_scif[(relevant_scif['product_fk'] == product_fk)]['facings'].sum()
        if facings > 0 or (product_fk in self.sales_data and kpi_name == INTEG41Const.POD):
            result, passed = INTEG41Const.DISTRIBUTED, 1
        else:
            result, passed = INTEG41Const.OOS, 0
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: passed,
                          INTEG41Const.BRAND: brand, INTEG41Const.SUB_BRAND: sub_brand, INTEG41Const.STANDARD_TYPE: standard_type}
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        return product_result

    def calculate_assortment(self, scene_types, kpi_name, weight):
        """
        Gets assortment type, and calculates it with the match function
        :param scene_types: string from template
        :param kpi_name: POD or Display Brand
        :param weight:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        if kpi_name == INTEG41Const.POD:
            calculate_function = self.calculate_pod_sku
        elif kpi_name == INTEG41Const.DISPLAY_BRAND:
            if self.survey_display_write_to_db(weight):
                Log.info("There is no display, Display Brand got 100")
                return 100 * weight, 100 * weight, 100 * weight
            calculate_function = self.calculate_display_compliance_sku
            relevant_scif = relevant_scif[relevant_scif['location_type'] == 'Secondary Shelf']
        else:
            Log.error("Assortment '{}' is not defined in the code".format(kpi_name))
            return 0, 0, 0
        kpi_db_names = INTEG41Const.DB_OFF_NAMES[kpi_name]
        total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ASSORTMENTS_NAMES[INTEG41Const.DB_OFF])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_off_trade_fk]
        all_results = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in relevant_assortment.iterrows():
            additional_attrs = json.loads(product_line['additional_attributes'])
            if kpi_name == INTEG41Const.DISPLAY_BRAND and additional_attrs[INTEG41Const.DISPLAY] == 0:
                continue
            standard_type = additional_attrs[INTEG41Const.NATIONAL_SEGMENT]
            result_line = calculate_function(product_line['product_fk'], relevant_scif, standard_type)
            all_results = all_results.append(result_line, ignore_index=True)
        total_result, segment_result, national_result = self.insert_all_levels_to_db(all_results, kpi_db_names, weight,
                                                                                     with_standard_type=True)
        # add extra products to DB:
        if kpi_name == INTEG41Const.POD:
            self.calculate_extras(relevant_assortment)
        return total_result, segment_result, national_result

    def calculate_extras(self, relevant_assortment):
        """
        add the extra products (products not shown in the template) to DB.
        :param relevant_assortment: DF of assortment with all the PODs
        :return:
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.POD][INTEG41Const.SKU])
        all_diageo_products = self.scif_without_emptys[
            self.scif_without_emptys['manufacturer_fk'] == self.manufacturer_fk]['product_fk'].unique().tolist()
        assortment_products = relevant_assortment['product_fk'].unique().tolist()
        products_not_in_list = set(all_diageo_products) - set(assortment_products)
        result = INTEG41Const.EXTRA
        for product in products_not_in_list:
            self.common.write_to_db_result(
                fk=sku_kpi_fk, numerator_id=product, result=self.get_pks_of_result(result))

    def calculate_pod_sku(self, product_fk, relevant_scif, standard_type):
        """
        Checks if specific product exists in the filtered scif
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.POD][INTEG41Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.POD][INTEG41Const.TOTAL])
        facings = relevant_scif[relevant_scif['product_fk'] == product_fk]['facings'].sum()
        if facings > 0:
            result, passed = INTEG41Const.DISTRIBUTED, 1
        else:
            result, passed = INTEG41Const.OOS, 0
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: passed,
                          INTEG41Const.BRAND: brand, INTEG41Const.SUB_BRAND: sub_brand, INTEG41Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_display_compliance_sku(self, product_fk, relevant_scif, standard_type):
        """
        Checks if specific product passes the condition of the display
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_BRAND][INTEG41Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_BRAND][INTEG41Const.TOTAL])
        facings = self.calculate_passed_display(product_fk, relevant_scif)
        if facings > 0:
            result, passed = INTEG41Const.DISTRIBUTED, 1
        else:
            result, passed = INTEG41Const.OOS, 0
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: passed,
                          INTEG41Const.BRAND: brand, INTEG41Const.SUB_BRAND: sub_brand, INTEG41Const.STANDARD_TYPE: standard_type}
        return product_result

    # menu

    def calculate_menu(self, scene_types, weight, target):
        """
        calculates the share of all brands and manufacturers in the menu, and
        checks if Diageo result is bigger than target
        :param scene_types: str
        :param weight: float
        :param target: float
        :return:
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NAMES[INTEG41Const.MENU][INTEG41Const.TOTAL])
        if self.no_menu_allowed:
            Log.info("There is no menu, Menu got 100")
            score = 100
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target * 100,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=self.common.get_dictionary(name=INTEG41Const.TOTAL))
            return score * weight, 0, 0
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NAMES[INTEG41Const.MENU][INTEG41Const.MANUFACTURER])
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ON_NAMES[INTEG41Const.MENU][INTEG41Const.SUB_BRAND])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[(self.scif_without_emptys['scene_id'].isin(relevant_scenes)) &
                                                 (self.scif_without_emptys['product_type'] == 'POS')]
        all_sub_brands = relevant_scif['Sub Brand'].unique().tolist()
        all_manufacturers = relevant_scif['manufacturer_fk'].unique().tolist()
        den_res = relevant_scif['facings'].sum()
        diageo_facings = 0
        for sub_brand_name in all_sub_brands:
            num_res = relevant_scif[relevant_scif['Sub Brand'] == sub_brand_name]['facings'].sum()
            result = self.get_score(num_res, den_res)
            sub_brand_fk = self.get_sub_brand_fk(sub_brand_name)
            self.common.write_to_db_result(
                fk=sub_brand_kpi_fk, numerator_id=sub_brand_fk, numerator_result=num_res, denominator_result=den_res,
                result=result, identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        for manufacturer_fk in all_manufacturers:
            num_res = relevant_scif[relevant_scif['manufacturer_fk'] == manufacturer_fk]['facings'].sum()
            if manufacturer_fk == self.manufacturer_fk:
                diageo_facings = num_res
            result = self.get_score(num_res, den_res)
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer_fk, numerator_result=num_res, result=result,
                denominator_result=den_res, identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        diageo_result = self.get_score(diageo_facings, den_res)
        result = 100 if diageo_result / 100 >= target else 0
        score = result * weight
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_facings,
            denominator_result=den_res, result=diageo_result, score=score, weight=weight * 100,
            identifier_result=self.common.get_dictionary(kpi_fk=total_kpi_fk), target=target * 100,
            identifier_parent=self.common.get_dictionary(name=INTEG41Const.TOTAL), should_enter=True)
        return score, 0, 0

    # display share:

    def calculate_total_display_share(self, scene_types, weight, target):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :param target: for the score
        :param weight: float
        :return: total_result
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_SHARE][INTEG41Const.TOTAL])
        total_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        if self.no_display_allowed:
            Log.info("There is no display, Display Share got 100")
            score = 100
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target * 100,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=self.common.get_dictionary(name=INTEG41Const.TOTAL))
            return score * weight, 0, 0
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[
                                                                     INTEG41Const.DISPLAY_SHARE][INTEG41Const.MANUFACTURER])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif_without_emptys[(self.scif_without_emptys['scene_fk'].isin(relevant_scenes)) &
                                                     (self.scif_without_emptys['location_type'] == 'Secondary Shelf')]
        all_results = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_DISPLAY)
        for product_fk in relevant_products['product_fk'].unique().tolist():
            product_result = self.calculate_display_share_of_sku(product_fk, relevant_products, manufacturer_kpi_fk)
            all_results = all_results.append(product_result, ignore_index=True)
        den_res = all_results[INTEG41Const.PASSED].sum()
        diageo_results, diageo_result = 0, 0
        for manufacturer in all_results[INTEG41Const.MANUFACTURER].unique().tolist():
            num_res = all_results[all_results[INTEG41Const.MANUFACTURER] == manufacturer][INTEG41Const.PASSED].sum()
            result = self.get_score(num_res, den_res)
            target_manufacturer = None
            if manufacturer == self.manufacturer_fk:
                diageo_result, diageo_results = result, num_res
                target_manufacturer = target * 100
            result_dict = self.common.get_dictionary(manufacturer_fk=manufacturer, kpi_fk=manufacturer_kpi_fk)
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer, numerator_result=num_res,
                target=target_manufacturer,
                denominator_result=den_res, result=result, identifier_parent=total_dict, identifier_result=result_dict)
        score = 100 if (diageo_results >= target * den_res) else 0
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_results, target=target * 100,
            denominator_result=den_res, result=diageo_result, should_enter=True, weight=weight * 100, score=score,
            identifier_result=total_dict, identifier_parent=self.common.get_dictionary(name=INTEG41Const.TOTAL))
        return score * weight, 0, 0

    def calculate_display_share_of_sku(self, product_fk, relevant_products, manufacturer_kpi_fk):
        """
        calculates a specific product if it passes the condition of display
        :param product_fk:
        :param relevant_products: DF (scif of the display)
        :param manufacturer_kpi_fk: for write_to_db
        :return: a line for the results DF
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.DISPLAY_SHARE][INTEG41Const.SKU])
        manufacturer = self.get_manufacturer(product_fk)
        sum_scenes_passed = self.calculate_passed_display(product_fk, relevant_products)
        parent_dict = self.common.get_dictionary(kpi_fk=manufacturer_kpi_fk, manufacturer_fk=manufacturer)
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk,
            result=sum_scenes_passed, identifier_parent=parent_dict)
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: sum_scenes_passed,
                          INTEG41Const.MANUFACTURER: manufacturer}
        return product_result

    # shelf facings:

    def calculate_total_shelf_facings(self, scene_types, kpi_name, weight):
        """
        Calculates if facings of Diageo products are more than targets (competitors products or objective target)
        :param scene_types: str
        :param kpi_name: str
        :param weight: float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[INTEG41Const.SHELF_FACING_SHEET]
        if self.state in relevant_competitions[INTEG41Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[relevant_competitions[INTEG41Const.STATE] == self.state]
        else:
            relevant_competitions = relevant_competitions[relevant_competitions[INTEG41Const.STATE] == INTEG41Const.OTHER]
        all_results = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, competition in relevant_competitions.iterrows():
            result_dict = self.calculate_shelf_facings_of_competition(competition, relevant_scenes, i)
            all_results = all_results.append(result_dict, ignore_index=True)
        kpi_db_names = INTEG41Const.DB_OFF_NAMES[kpi_name]
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, with_standard_type=True)
        return total_result, segment_result, national_result

    def calculate_shelf_facings_of_competition(self, competition, relevant_scenes, index):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :param index: for hierarchy
        :return: passed, product_fk, standard_type
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.SHELF_FACINGS][INTEG41Const.COMPETITION])
        our_eans = competition[INTEG41Const.OUR_EAN_CODE].split(', ')
        our_lines = self.all_products[self.all_products['product_ean_code'].isin(our_eans)]
        if our_lines.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_eans))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        product_fk = our_fks[0]
        total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ASSORTMENTS_NAMES[INTEG41Const.DB_OFF])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_off_trade_fk]
        product_assortment_line = relevant_assortment[relevant_assortment['product_fk'] == product_fk]
        if product_assortment_line.empty:
            return None
        additional_attrs = json.loads(product_assortment_line.iloc[0]['additional_attributes'])
        standard_type = additional_attrs[INTEG41Const.NATIONAL_SEGMENT]
        result_identifier = self.common.get_dictionary(kpi_fk=kpi_fk, product_fk=product_fk, index=index)
        our_facings = self.calculate_shelf_facings_of_sku(our_fks, relevant_scenes, result_identifier)
        if self.does_exist(competition[INTEG41Const.COMP_EAN_CODE]):
            comp_eans = competition[INTEG41Const.COMP_EAN_CODE].split(', ')
            comp_lines = self.all_products[self.all_products['product_ean_code'].isin(comp_eans)]
            if comp_lines.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(comp_eans))
                return None
            comp_fks = comp_lines['product_fk'].unique().tolist()
            comp_facings = self.calculate_shelf_facings_of_sku(comp_fks, relevant_scenes, result_identifier)
            bench_value = competition[INTEG41Const.BENCH_VALUE]
            target = comp_facings * bench_value
        else:
            target = competition[INTEG41Const.BENCH_VALUE]
        comparison = 1 if (our_facings >= target and our_facings > 0) else 0
        brand, sub_brand = self.get_product_details(product_fk)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.SHELF_FACINGS][INTEG41Const.TOTAL])
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk,
            result=comparison * 100, identifier_result=result_identifier,
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: comparison,
                          INTEG41Const.BRAND: brand, INTEG41Const.SUB_BRAND: sub_brand, INTEG41Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_shelf_facings_of_sku(self, product_fks, relevant_scenes, parent_identifier):
        """
        Gets product(s) and counting its facings.
        :param product_fks: list of FKs
        :param relevant_scenes: list
        :param parent_identifier: for write_to_db
        :return: amount of facings
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.SHELF_FACINGS][INTEG41Const.SKU])
        amount_of_facings = 0
        for product_fk in product_fks:
            product_facing = self.scif_without_emptys[
                (self.scif_without_emptys['product_fk'] == product_fk) &
                (self.scif_without_emptys['scene_id'].isin(relevant_scenes))]['facings'].sum()
            if product_facing is None or np.isnan(product_facing):
                product_facing = 0
            amount_of_facings += product_facing
            # if product_fk in self.calculated_shelf_facings:
            if 0 > 2:
                continue
            else:
                self.calculated_shelf_facings.append(product_fk)
                self.common.write_to_db_result(
                    fk=kpi_fk, numerator_id=product_fk, result=product_facing,
                    should_enter=True, identifier_parent=parent_identifier)
        return amount_of_facings

    # shelf placement:

    def calculate_total_shelf_placement(self, scene_types, kpi_name, weight):
        """
        Takes list of products and their shelf groups, and calculate if the're pass the target.
        :param scene_types: str
        :param kpi_name: str
        :param weight float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[INTEG41Const.SHELF_PLACMENTS_SHEET]
        all_results = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in all_products_table.iterrows():
            result = self.calculate_shelf_placement_of_sku(product_line, relevant_scenes)
            all_results = all_results.append(result, ignore_index=True)
        kpi_db_names = INTEG41Const.DB_OFF_NAMES[kpi_name]
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, with_standard_type=True)
        return total_result, segment_result, national_result

    def convert_groups_from_template(self):
        """
        Creates dict that contains every number in the template and its shelves
        :return: dict of lists
        """
        shelf_groups = self.templates[INTEG41Const.SHELF_GROUPS_SHEET]
        shelves_groups = {}
        for i, group in shelf_groups.iterrows():
            shelves_groups[group[INTEG41Const.NUMBER_GROUP]] = group[INTEG41Const.SHELF_GROUP].split(', ')
        return shelves_groups

    def calculate_shelf_placement_of_sku(self, product_line, relevant_scenes):
        """
        Gets a product (line from template) and checks if it has more facings than targets in the eye level
        :param product_line: series
        :param relevant_scenes: list
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.SHELF_PLACEMENT][INTEG41Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.SHELF_PLACEMENT][INTEG41Const.TOTAL])
        product_fk = self.all_products[self.all_products['product_ean_code'] == product_line[
            INTEG41Const.PRODUCT_EAN_CODE]]['product_fk']
        if product_fk.empty:
            Log.warning("Product_ean '{}' does not exist".format(product_line[INTEG41Const.PRODUCT_EAN_CODE]))
            return None
        product_fk = product_fk.iloc[0]
        total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_ASSORTMENTS_NAMES[INTEG41Const.DB_OFF])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_off_trade_fk]
        product_assortment_line = relevant_assortment[relevant_assortment['product_fk'] == product_fk]
        if product_assortment_line.empty:
            return None
        additional_attrs = json.loads(product_assortment_line.iloc[0]['additional_attributes'])
        standard_type = additional_attrs[INTEG41Const.NATIONAL_SEGMENT]
        min_shelf_loc = product_line[INTEG41Const.MIN_SHELF_LOCATION]
        product_fk_with_substs = [product_fk]
        product_fk_with_substs += self.all_products[self.all_products['substitution_product_fk'] == product_fk][
            'product_fk'].tolist()
        relevant_products = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'].isin(product_fk_with_substs)) &
            (self.match_product_in_scene['scene_fk'].isin(relevant_scenes))]
        if relevant_products.empty:
            return None
        shelf_groups = self.converted_groups[min_shelf_loc]
        all_shelves_placements = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_PRODUCT_PLACEMENT)
        passed, result = 0, None
        for i, product in relevant_products.iterrows():
            is_passed, shelf_name = self.calculate_specific_product_shelf_placement(product, shelf_groups)
            if is_passed == 1:
                result, passed = shelf_name, 1
                if shelf_name != INTEG41Const.OTHER:
                    break
            if all_shelves_placements[all_shelves_placements[INTEG41Const.SHELF_NAME] == shelf_name].empty:
                all_shelves_placements = all_shelves_placements.append(
                    {INTEG41Const.SHELF_NAME: shelf_name, INTEG41Const.PASSED: is_passed, INTEG41Const.FACINGS: 1}, ignore_index=True)
            else:
                all_shelves_placements[all_shelves_placements[INTEG41Const.SHELF_NAME] == shelf_name][
                    INTEG41Const.FACINGS] += 1
        if passed == 0:
            all_shelves_placements = all_shelves_placements.sort_values(by=[INTEG41Const.FACINGS])
            result = all_shelves_placements[INTEG41Const.SHELF_NAME].iloc[0]
        shelf_groups = self.templates[INTEG41Const.SHELF_GROUPS_SHEET]
        target = shelf_groups[shelf_groups[INTEG41Const.NUMBER_GROUP] == min_shelf_loc][INTEG41Const.SHELF_GROUP].iloc[0]
        target_fk = self.get_pks_of_result(target)
        score = 100 * passed
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, score=score, result=self.get_pks_of_result(result),
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk), target=target_fk)
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: passed,
                          INTEG41Const.BRAND: brand, INTEG41Const.SUB_BRAND: sub_brand, INTEG41Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_specific_product_shelf_placement(self, match_product_line, shelf_groups):
        """
        takes a line of match_product and the group shleves it should be on, and returns if it does (and which group)
        :param match_product_line: series - specific line from match_product_in_scene
        :param shelf_groups: list of the match group_names (['E', 'T'])
        :return: couple: if passed or not, and the location ("E")
        """
        min_max_shleves = self.templates[INTEG41Const.MINIMUM_SHELF_SHEET]
        shelf_from_bottom = match_product_line['shelf_number_from_bottom']
        scene = match_product_line['scene_fk']
        shelf_groups_for_scene = min_max_shleves[
            (min_max_shleves[INTEG41Const.NUM_SHLEVES_MIN] <= self.scenes_with_shelves[scene]) &
            (min_max_shleves[INTEG41Const.NUM_SHLEVES_MAX] >= self.scenes_with_shelves[scene])]
        group_for_product = shelf_groups_for_scene[shelf_groups_for_scene[
            INTEG41Const.SHELVES_FROM_BOTTOM] == shelf_from_bottom]
        if group_for_product.empty:
            group_names = [INTEG41Const.OTHER]
        else:
            group_names = group_for_product[INTEG41Const.SHELF_NAME].tolist()
        if "ALL" in shelf_groups:
            answer_couple = 1, group_names[0]
        else:
            answer_couple = 0, group_names[0]
            common_shelves = set(group_names) & set(shelf_groups)
            if common_shelves:
                answer_couple = 1, common_shelves.pop()
        return answer_couple

    # msrp:

    def calculate_total_msrp(self, scene_types, kpi_name, weight):
        """
        Compares the prices of Diageo products to the competitors' (or absolute values).
        :param scene_types: str
        :param kpi_name: str
        :param weight: float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[INTEG41Const.PRICING_SHEET]
        if self.state in all_products_table[INTEG41Const.STATE].unique().tolist():
            all_products_table = all_products_table[all_products_table[INTEG41Const.STATE] == self.state]
        else:
            all_products_table = all_products_table[all_products_table[INTEG41Const.STATE] == INTEG41Const.OTHER]
        all_competes = pd.DataFrame(columns=INTEG41Const.COLUMNS_FOR_PRODUCT)
        for i, competition in all_products_table.iterrows():
            compete_result_dict = self.calculate_msrp_of_competition(competition, relevant_scenes, i)
            all_competes = all_competes.append(compete_result_dict, ignore_index=True)
        kpi_db_names = INTEG41Const.DB_OFF_NAMES[kpi_name]
        result, segment_result, national_result = self.insert_all_levels_to_db(
            all_competes, kpi_db_names, weight, write_numeric=True)
        return result, 0, 0

    def calculate_msrp_of_competition(self, competition, relevant_scenes, index):
        """
        Takes competition between the price of Diageo product and Comp's product.
        The result is the distance between the objected to the observed
        :param competition: line of the template
        :param relevant_scenes:
        :param index: for hierarchy
        :return: 1/0
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.MSRP][INTEG41Const.COMPETITION])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.MSRP][INTEG41Const.TOTAL])
        our_ean = competition[INTEG41Const.OUR_EAN_CODE]
        our_line = self.all_products[self.all_products['product_ean_code'] == our_ean]
        min_relative, max_relative = competition[INTEG41Const.MIN_MSRP_RELATIVE], competition[INTEG41Const.MAX_MSRP_RELATIVE]
        min_absolute, max_absolute = competition[INTEG41Const.MIN_MSRP_ABSOLUTE], competition[INTEG41Const.MAX_MSRP_ABSOLUTE]
        if our_line.empty:
            Log.warning("The products {} in MSRP don't exist in DB".format(our_ean))
            return None
        product_fk = our_line['product_fk'].iloc[0]
        result_dict = self.common.get_dictionary(kpi_fk=kpi_fk, product_fk=product_fk, index=index)
        our_price = self.calculate_sku_price(product_fk, relevant_scenes, result_dict)
        if our_price is None:
            return None
        is_competitor = (self.does_exist(competition[INTEG41Const.COMP_EAN_CODE]) and
                         self.does_exist(min_relative) and self.does_exist(max_relative))
        is_absolute = self.does_exist(min_absolute) and self.does_exist(max_absolute)
        if is_competitor:
            comp_ean = competition[INTEG41Const.COMP_EAN_CODE]
            comp_line = self.all_products[self.all_products['product_ean_code'] == comp_ean]
            if comp_line.empty:
                Log.warning("The products {} in MSRP don't exist in DB".format(our_ean))
                return None
            comp_fk = comp_line['product_fk'].iloc[0]
            comp_price = self.calculate_sku_price(comp_fk, relevant_scenes, result_dict)
            if comp_price is None:
                return None
            range_price = (round(comp_price + competition[INTEG41Const.MIN_MSRP_RELATIVE], 2),
                           round(comp_price + competition[INTEG41Const.MAX_MSRP_RELATIVE], 2))
        elif is_absolute:
            range_price = (competition[INTEG41Const.MIN_MSRP_ABSOLUTE], competition[INTEG41Const.MAX_MSRP_ABSOLUTE])
        else:
            Log.warning("In MSRP product {} does not have clear competitor".format(product_fk))
            return None
        if our_price < range_price[0]:
            result = range_price[0] - our_price
        elif our_price > range_price[1]:
            result = our_price - range_price[1]
        else:
            result = 0
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result,
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk), identifier_result=result_dict)
        product_result = {INTEG41Const.PRODUCT_FK: product_fk, INTEG41Const.PASSED: (result == 0) * 1,
                          INTEG41Const.BRAND: brand, INTEG41Const.SUB_BRAND: sub_brand}
        return product_result

    def calculate_sku_price(self, product_fk, scenes, parent_dict):
        """
        Takes product, checks its price and writes it in the DB.
        :param product_fk:
        :param scenes: list of fks
        :param parent_dict: identifier dictionary
        :return: price
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(INTEG41Const.DB_OFF_NAMES[INTEG41Const.MSRP][INTEG41Const.SKU])
        price = self.products_with_prices[(self.products_with_prices['product_fk'] == product_fk) &
                                          (self.products_with_prices['scene_fk'].isin(scenes))]['price_value']
        if price.empty:
            return None
        result = round(price.iloc[0], 2)
        # if product_fk not in self.calculated_price:
        if 2 > 0:
            self.calculated_price.append(product_fk)
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=product_fk, result=result,
                identifier_parent=parent_dict, should_enter=True)
        return result

    # help functions:

    def calculate_passed_display(self, product_fk, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet).
        :param product_fk:
        :param relevant_products: relevant scif?
        :return: number of scenes. int.
        """
        template = self.templates[INTEG41Const.DISPLAY_TARGET_SHEET]
        sum_scenes_passed, sum_facings = 0, 0
        for scene in relevant_products['scene_fk'].unique().tolist():
            scene_product = relevant_products[(relevant_products['scene_fk'] == scene) &
                                              (relevant_products['product_fk'] == product_fk)]
            if scene_product.empty:
                continue
            scene_type = scene_product['template_name'].iloc[0]
            minimum_products = template[template[INTEG41Const.SCENE_TYPE] == scene_type]
            if minimum_products.empty:
                minimum_products = template[template[INTEG41Const.SCENE_TYPE] == INTEG41Const.OTHER]
            minimum_products = minimum_products[INTEG41Const.MIN_FACINGS].iloc[0]
            facings = scene_product['facings'].iloc[0]
            sum_scenes_passed += 1 * (facings >= minimum_products)  # if the condition is failed, it will "add" 0.
        return sum_scenes_passed

    def get_relevant_scenes(self, scene_types):
        """
        :param scene_types: cell in the template
        :return: list of all the scenes contains the cell
        """
        if self.does_exist(scene_types):
            scene_type_list = scene_types.split(", ")
            # scene_type_list += list(map(lambda x: x + " - OLD", scene_type_list))
            return self.scif_without_emptys[self.scif_without_emptys["template_name"].isin(scene_type_list)][
                "scene_id"].unique().tolist()
        return self.scif_without_emptys["scene_id"].unique().tolist()

    def get_product_details(self, product_fk):
        """
        :param product_fk:
        :return: its details for assortment (brand, sub_brand)
        """
        brand = self.all_products[self.all_products['product_fk'] == product_fk]['brand_fk'].iloc[0]
        sub_brand = self.all_products[self.all_products['product_fk'] == product_fk]['Sub Brand'].iloc[0]
        if not sub_brand:
            sub_brand_fk = None
        else:
            sub_brand_fk = self.get_sub_brand_fk(sub_brand)
        return brand, sub_brand_fk

    def get_sub_brand_fk(self, sub_brand):
        """
        takes sub_brand and returns its pk
        :param sub_brand: str
        :return: int
        """
        sub_brand_line = self.sub_brands[self.sub_brands['name'] == sub_brand]
        if sub_brand_line.empty:
            return None
        else:
            return sub_brand_line.iloc[0]['pk']

    def get_manufacturer(self, product_fk):
        """
        :param product_fk:
        :return: manufacturer_fk
        """
        return self.all_products[self.all_products['product_fk'] == product_fk]['manufacturer_fk'].iloc[0]

    @staticmethod
    def does_exist(cell):
        """
        Checks if there's data in specific cell in the templates
        :param cell:
        :return: True/False
        """
        if cell in (["", "N/A", None]):
            return False
        return True

    @staticmethod
    def get_score(num, den):
        """
        :param num: number
        :param den: number
        :return: the percent of the num/den
        """
        if den == 0:
            return 0
        return round((float(num) * 100) / den, 2)

    def get_pks_of_result(self, result):
        """
        converts string result to its pk (in static.kpi_result_value)
        :param result: str
        :return: int
        """
        pk = self.result_values[self.result_values['value'] == result]['pk'].iloc[0]
        return pk

    # main insert to DB functions:

    def insert_all_levels_to_db(self, all_results, kpi_db_names, weight, with_standard_type=False,
                                should_enter=True, write_numeric=False, sub_brand_numeric=False):
        """
        This function gets all the sku results (with details) and puts in DB all the way up (sub_brand, brand, total,
        and segment-national if exist).
        :param all_results: DF with product_fk and its details - passed, sub_brand, brand, standard_type.
        :param kpi_db_names: name as it's shown in the main sheet of the template.
        :param weight:
        :param with_standard_type: in KPIs that include standard_type we need to know for calculation their total
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param sub_brand_numeric: write in the sub_brand if one product passed or not (like sku level)
        :param should_enter: if the total should enter the hierarchy table
        :return: the scores of all
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[INTEG41Const.TOTAL])
        total_identifier = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        for brand in all_results[INTEG41Const.BRAND].unique().tolist():
            if brand is None or np.isnan(brand):
                continue
            brand_results = all_results[all_results[INTEG41Const.BRAND] == brand]
            self.insert_brand_and_subs_to_db(brand_results, kpi_db_names, brand, total_identifier,
                                             write_numeric=write_numeric, sub_brand_numeric=sub_brand_numeric)
        all_passed_results = all_results[INTEG41Const.PASSED]
        total_result = self.insert_totals_to_db(all_passed_results, kpi_db_names, INTEG41Const.TOTAL, weight, total_identifier,
                                                should_enter=should_enter, write_numeric=write_numeric)
        segment_result, national_result = 0, 0
        if with_standard_type:
            national_results = all_results[all_results[INTEG41Const.STANDARD_TYPE] == INTEG41Const.NATIONAL][INTEG41Const.PASSED]
            national_result = self.insert_totals_to_db(national_results, kpi_db_names, INTEG41Const.NATIONAL, weight,
                                                       should_enter=should_enter, write_numeric=write_numeric)
            segment_results = all_results[all_results[INTEG41Const.STANDARD_TYPE] == INTEG41Const.SEGMENT][INTEG41Const.PASSED]
            segment_result = self.insert_totals_to_db(segment_results, kpi_db_names, INTEG41Const.SEGMENT, weight,
                                                      should_enter=should_enter, write_numeric=write_numeric)
        return total_result, segment_result, national_result

    def insert_brand_and_subs_to_db(self, brand_results, kpi_db_names, brand, total_identifier,
                                    write_numeric=False, sub_brand_numeric=False):
        """
        Inserting all brand and sub_brand results
        :param brand_results: DF from all_results
        :param kpi_db_names:
        :param brand: fk
        :param total_identifier: for hierarchy
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param sub_brand_numeric: write in the sub_brand if one product passed or not (like sku level)
        """
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[INTEG41Const.BRAND])
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
        for sub_brand in brand_results[brand_results[INTEG41Const.BRAND] == brand][INTEG41Const.SUB_BRAND].unique().tolist():
            if sub_brand is None or np.isnan(sub_brand):
                continue
            sub_brand_results = brand_results[(brand_results[INTEG41Const.BRAND] == brand) &
                                              (brand_results[INTEG41Const.SUB_BRAND] == sub_brand)]
            self.insert_sub_brands_to_db(sub_brand_results, kpi_db_names, brand, sub_brand, brand_dict,
                                         write_numeric=write_numeric, sub_brand_numeric=sub_brand_numeric)
        results = brand_results[INTEG41Const.PASSED]
        if write_numeric:
            num_res, den_res = 0, 0
            result = results.sum()
        else:
            num_res, den_res = results.sum(), results.count()
            result = self.get_score(num_res, den_res)
        self.common.write_to_db_result(
            fk=brand_kpi_fk, numerator_id=brand, numerator_result=num_res,
            denominator_result=den_res, result=result,
            identifier_parent=total_identifier, identifier_result=brand_dict)

    def insert_sub_brands_to_db(self, sub_brand_results, kpi_db_names, brand, sub_brand, brand_identifier,
                                write_numeric=False, sub_brand_numeric=False):
        """
        inserting sub_brand results into DB
        :param sub_brand_results: DF from all_products
        :param kpi_db_names:
        :param brand: fk
        :param sub_brand: fk
        :param brand_identifier: for hierarchy
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param sub_brand_numeric: write in the sub_brand if one product passed or not (like sku level)
        """
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[INTEG41Const.SUB_BRAND])
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        results = sub_brand_results[INTEG41Const.PASSED]
        num_res, den_res = 0, 0
        if sub_brand_numeric:
            result = INTEG41Const.DISTRIBUTED if results.sum() > 0 else INTEG41Const.OOS
            result = self.get_pks_of_result(result)
        elif write_numeric:
            result = results.sum()
        else:
            num_res, den_res = results.sum(), results.count()
            result = self.get_score(num_res, den_res)
        self.common.write_to_db_result(
            fk=sub_brand_kpi_fk, numerator_id=sub_brand, numerator_result=num_res,
            denominator_result=den_res, result=result,
            identifier_parent=brand_identifier, identifier_result=sub_brand_dict)

    def insert_totals_to_db(self, all_passed_results, kpi_db_names, total_kind, weight, identifier_result=None,
                            should_enter=True, write_numeric=False):
        """
        inserting all total level (includes segment and national) into DB
        :param all_passed_results: 'passed' column from all_results
        :param kpi_db_names:
        :param weight: float
        :param total_kind: TOTAL/SEGMENT/NATIONAL
        :param identifier_result: optional, if has children
        :param should_enter: if the total should enter the hierarchy table
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :return: the score
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[total_kind])
        if write_numeric:
            num_res, den_res = 0, 0
            result = all_passed_results.sum()
            score = 0
        else:
            num_res, den_res = all_passed_results.sum(), all_passed_results.count()
            result = self.get_score(num_res, den_res)
            score = result * weight
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_res, should_enter=should_enter,
            denominator_result=den_res, result=result, identifier_result=identifier_result,
            identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight * 100, score=score)
        return score
