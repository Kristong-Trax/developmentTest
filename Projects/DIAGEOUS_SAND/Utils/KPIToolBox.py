import os
import pandas as pd
import numpy as np
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.DIAGEOUS_SAND.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

# from Projects.DIAGEOUS.Utils.Fetcher import Queries
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from KPIUtils_v2.DIAGEOUSCalculations.AvailabilityDIAGEOUSCalculations import Availability
# from KPIUtils_v2.DIAGEOUSCalculations.NumberOfScenesDIAGEOUSCalculations import NumberOfScenes
# from KPIUtils_v2.DIAGEOUSCalculations.PositionGraphsDIAGEOUSCalculations import PositionGraphs
# from KPIUtils_v2.DIAGEOUSCalculations.SOSDIAGEOUSCalculations import SOS
# from KPIUtils_v2.DIAGEOUSCalculations.SequenceDIAGEOUSCalculations import Sequence
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class DIAGEOUSSANDToolBox:
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif_without_emptys = self.scif[~(self.scif['product_type'] == "Empty") &
                                             (self.scif['substitution_product_fk'].isnull())]
        self.all_products_sku = self.all_products[(self.all_products['product_type'] == 'SKU') &
                                                  (self.all_products['category'] == 'SPIRITS') &
                                                  (self.all_products['is_active'] == 1)]
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'DIAGEO']['manufacturer_fk'].iloc[0]
        store_number_1 = self.store_info['store_number_1'].iloc[0]
        if self.store_info['additional_attribute_6'].iloc[0]:
            self.on_off = Const.ON if self.store_info['additional_attribute_6'].iloc[0] in (
                'On-Premise') else Const.OFF
        else:
            Log.error("The store for this session has no attribute6. Set temporary as Off-premise, fix ASAP")
            self.on_off = Const.OFF
        self.templates = {}
        self.get_templates()
        self.kpi_results_queries = []
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.state = self.ps_data.get_state_name()
        self.sub_brands = self.ps_data.get_custom_entities(1002)
        self.result_values = self.ps_data.get_result_values()
        self.products_with_prices = self.ps_data.get_products_prices()
        self.assortment = Assortment(self.data_provider, self.output, ps_data_provider=self.ps_data,
                                     assortment_filter=store_number_1)
        if self.on_off == Const.ON:
            self.sales_data = self.ps_data.get_sales_data()
            self.no_menu_allowed = self.survey.check_survey_answer(survey_text=Const.NO_MENU_ALLOWED_QUESTION,
                                                                   target_answer=Const.SURVEY_ANSWER)
        else:
            scenes = self.scene_info['scene_fk'].unique().tolist()
            self.scenes_with_shelves = {}
            for scene in scenes:
                shelf = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene][
                    'shelf_number_from_bottom'].max()
                self.scenes_with_shelves[scene] = shelf
            self.converted_groups = self.convert_groups_from_template()
            self.no_display_allowed = self.survey.check_survey_answer(survey_text=Const.NO_DISPLAY_ALLOWED_QUESTION,
                                                                      target_answer=Const.SURVEY_ANSWER)
        self.assortment_products = self.assortment.get_lvl3_relevant_ass()
        if self.on_off == Const.OFF:
            total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(
                Const.DB_ASSORTMENTS_NAMES[Const.OFF])
            if not self.assortment_products.empty:
                self.relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] ==
                                                                    total_off_trade_fk]
            else:
                self.relevant_assortment = self.assortment_products  # we need this to prevent undefined errors

    # initialize:

    def get_templates(self):
        """
        Reads the template (and makes the EANs be Strings)
        """
        for sheet in Const.SHEETS[self.on_off]:
            if sheet in ([Const.SHELF_FACING_SHEET, Const.PRICING_SHEET]):
                converters = {Const.OUR_EAN_CODE: lambda x: str(x).replace(".0", ""),
                              Const.COMP_EAN_CODE: lambda x: str(x).replace(".0", "")}
                self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet,
                                                      converters=converters, keep_default_na=False)
            elif sheet == Const.SHELF_PLACMENTS_SHEET:
                converters = {Const.PRODUCT_EAN_CODE: lambda x: str(x).replace(".0", "")}
                self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet,
                                                      converters=converters, keep_default_na=False)
            else:
                self.templates[sheet] = pd.read_excel(
                    TEMPLATE_PATH, sheetname=sheet, keep_default_na=False)

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if self.on_off == Const.OFF:
            if self.relevant_assortment.empty:
                return

        total_store_score, segment_store_score, national_store_score = 0, 0, 0
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_TOTAL_KPIS[self.on_off][Const.TOTAL])
        segment_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_TOTAL_KPIS[self.on_off][Const.SEGMENT])
        national_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_TOTAL_KPIS[self.on_off][Const.NATIONAL])
        for i, kpi_line in self.templates[Const.SHEETS[self.on_off][0]].iterrows():
            total_weighted_score, segment_weighted_score, national_weighted_score = self.calculate_set(
                kpi_line)
            if kpi_line[Const.KPI_GROUP]:
                total_store_score += total_weighted_score
                segment_store_score += segment_weighted_score
                national_store_score += national_weighted_score
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, result=self.round_result(
                total_store_score),
            denominator_id=self.store_id,
            identifier_result=self.common.get_dictionary(name=Const.TOTAL), score=self.round_result(total_store_score))
        self.common.write_to_db_result(fk=segment_kpi_fk, numerator_id=self.manufacturer_fk, result=0,
                                       identifier_result=self.common.get_dictionary(
                                           name=Const.SEGMENT),
                                       score=self.round_result(segment_store_score))
        self.common.write_to_db_result(fk=national_kpi_fk, numerator_id=self.manufacturer_fk, result=0,
                                       identifier_result=self.common.get_dictionary(
                                           name=Const.NATIONAL),
                                       score=self.round_result(national_store_score))

    def calculate_set(self, kpi_line):
        """
        Gets a line from the main sheet, and transports it to the match function
        :param kpi_line: series - {KPI Name, Template Group/ Scene Type, Target, Weight}
        :return: 3 scores (total, segment, national)
        """
        kpi_name, scene_types = kpi_line[Const.KPI_NAME], kpi_line[Const.TEMPLATE_GROUP]
        target, weight = kpi_line[Const.TARGET], kpi_line[Const.WEIGHT]
        if not self.does_exist(weight):
            weight = 0
        if kpi_name == Const.SHELF_PLACEMENT:
            total_score, segment_score, national_score = self.calculate_total_shelf_placement(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.SHELF_FACINGS:
            total_score, segment_score, national_score = self.calculate_total_shelf_facings(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.MSRP:
            total_score, segment_score, national_score = self.calculate_total_msrp(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.DISPLAY_SHARE:
            total_score, segment_score, national_score = self.calculate_total_display_share(
                scene_types, weight, target)
        elif kpi_name in (Const.POD, Const.DISPLAY_BRAND, Const.BACK_BAR):
            if self.on_off == Const.ON:
                total_score, segment_score, national_score = self.calculate_on_assortment(
                    scene_types, kpi_name, weight)
            else:
                total_score, segment_score, national_score = self.calculate_assortment(
                    scene_types, kpi_name, weight)
        elif kpi_name == Const.MENU:
            total_score, segment_score, national_score = self.calculate_menu(
                scene_types, weight, target)
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
        score = 1
        dict_of_fks = {
            Const.TOTAL: self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][Const.TOTAL]),
            Const.NATIONAL: self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][
                Const.NATIONAL]),
            Const.SEGMENT: self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][
                Const.SEGMENT])
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
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(
            relevant_scenes)]
        kpi_db_names = Const.DB_ON_NAMES[kpi_name]
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SKU])
        total_on_trade_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ASSORTMENTS_NAMES[Const.ON])
        if self.assortment_products.empty:
            return 0, 0, 0
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2']
                                                       == total_on_trade_fk]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in relevant_assortment.iterrows():
            additional_attrs = json.loads(product_line['additional_attributes'])
            standard_type = additional_attrs[Const.NATIONAL_SEGMENT]
            result_line = self.calculate_ass_on_sku(
                product_line['product_fk'], relevant_scif, standard_type, kpi_name)
            if not result_line:
                continue
            sub_brand = result_line[Const.SUB_BRAND]
            sub_brands_results = all_results[all_results[Const.SUB_BRAND] == sub_brand]
            if sub_brands_results.empty:
                all_results = all_results.append(result_line, ignore_index=True)
            elif result_line[Const.PASSED] > 0:
                all_results.loc[all_results[Const.SUB_BRAND] == sub_brand, Const.PASSED] = 1
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, with_standard_type=True, sub_brand_numeric=True)
        # add extra products to DB:
        if kpi_name == Const.POD:
            self.calculate_extras(relevant_assortment, relevant_scif, sku_kpi_fk)
        return total_result, segment_result, national_result

    def calculate_extras(self, relevant_assortment, filtered_scif, sku_kpi_fk):
        """
        add the extra products (products not shown in the template) to DB.
        :param relevant_assortment: DF of assortment with all the PODs
        :param filtered_scif: DF (scif in the scenes)
        :param sku_kpi_fk: on or off
        :return:
        """
        all_diageo_products = filtered_scif[
            (filtered_scif['manufacturer_fk'] == self.manufacturer_fk) &
            (filtered_scif['facings'] > 0)]['product_fk'].unique().tolist()
        assortment_products = relevant_assortment['product_fk'].unique().tolist()
        products_not_in_list = set(all_diageo_products) - set(assortment_products)
        result = Const.EXTRA
        for product in products_not_in_list:
            self.common.write_to_db_result(
                fk=sku_kpi_fk, numerator_id=product, result=self.get_pks_of_result(result))

    def calculate_assortment(self, scene_types, kpi_name, weight):
        """
        Gets assortment type, and calculates it with the match function
        :param scene_types: string from template
        :param kpi_name: POD or Display Brand
        :param weight:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(
            relevant_scenes)]
        if kpi_name == Const.POD:
            calculate_function = self.calculate_pod_off_sku
        elif kpi_name == Const.DISPLAY_BRAND:
            if self.survey_display_write_to_db(weight):
                Log.debug("There is no display, Display Brand got 100")
                return 1 * weight, 1 * weight, 1 * weight
            calculate_function = self.calculate_display_compliance_sku
            relevant_scif = relevant_scif[relevant_scif['location_type'] == 'Secondary Shelf']
        else:
            Log.error("Assortment '{}' is not defined in the code".format(kpi_name))
            return 0, 0, 0
        if self.assortment_products.empty:
            return 0, 0, 0
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SKU])
        total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_ASSORTMENTS_NAMES[Const.OFF])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2']
                                                       == total_off_trade_fk]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in relevant_assortment.iterrows():
            additional_attrs = json.loads(product_line['additional_attributes'])
            if kpi_name == Const.DISPLAY_BRAND and additional_attrs[Const.DISPLAY] in (0, '0'):
                continue
            standard_type = additional_attrs[Const.NATIONAL_SEGMENT]
            result_line = calculate_function(
                product_line['product_fk'], relevant_scif, standard_type)
            all_results = all_results.append(result_line, ignore_index=True)
        total_result, segment_result, national_result = self.insert_all_levels_to_db(all_results, kpi_db_names, weight,
                                                                                     with_standard_type=True)
        # add extra products to DB:
        if kpi_name == Const.POD:
            self.calculate_extras(relevant_assortment, relevant_scif, sku_kpi_fk)
        return total_result, segment_result, national_result

    def calculate_ass_on_sku(self, product_fk, relevant_scif, standard_type, kpi_name):
        """
        Checks if specific product's sub_brand exists in the filtered scif
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :param kpi_name: POD or Back Bar - to know if we should check the product in the sales data
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[kpi_name][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[kpi_name][Const.TOTAL])
        brand, sub_brand = self.get_product_details(product_fk)
        if sub_brand is None or self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None
        facings = relevant_scif[(relevant_scif['product_fk'] == product_fk)]['facings'].sum()
        if facings > 0 or (product_fk in self.sales_data and kpi_name == Const.POD):
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: passed,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        return product_result

    def calculate_pod_off_sku(self, product_fk, relevant_scif, standard_type):
        """
        Checks if specific product exists in the filtered scif
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        if self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.POD][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.POD][Const.TOTAL])
        facings = relevant_scif[relevant_scif['product_fk'] == product_fk]['facings'].sum()
        if facings > 0:
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: passed,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_display_compliance_sku(self, product_fk, relevant_scif, standard_type):
        """
        Checks if specific product passes the condition of the display
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        if self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][Const.TOTAL])
        facings = self.calculate_passed_display_without_subst(product_fk, relevant_scif)
        if facings > 0:
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: passed,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
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
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_ON_NAMES[Const.MENU][Const.TOTAL])
        if self.no_menu_allowed:
            Log.debug("There is no menu, Menu got 100")
            score = 1
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
            return score * weight, 0, 0
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_ON_NAMES[Const.MENU][Const.MANUFACTURER])
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_ON_NAMES[Const.MENU][Const.SUB_BRAND])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[(self.scif_without_emptys['scene_id'].isin(relevant_scenes)) &
                                                 (self.scif_without_emptys['product_type'] == 'POS')]
        all_manufacturers = relevant_scif['manufacturer_fk'].unique().tolist()
        den_res = relevant_scif['facings'].sum()
        diageo_facings = 0
        for products in relevant_scif[['sub_brand', 'brand_fk']].drop_duplicates().itertuples():
            sub_brand = products.sub_brand
            brand_fk = products.brand_fk
            if not sub_brand or not brand_fk:
                continue
            num_res = relevant_scif[(relevant_scif['sub_brand'] == sub_brand) &
                                    (relevant_scif['brand_fk'] == brand_fk)]['facings'].sum()
            result = self.get_score(num_res, den_res)
            sub_brand_fk = self.get_sub_brand_fk(sub_brand, brand_fk)
            if sub_brand_fk == 0:
                continue
            self.common.write_to_db_result(
                fk=sub_brand_kpi_fk, numerator_id=sub_brand_fk, numerator_result=num_res, denominator_result=den_res,
                result=result, identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        for manufacturer_fk in all_manufacturers:
            num_res = relevant_scif[relevant_scif['manufacturer_fk']
                                    == manufacturer_fk]['facings'].sum()
            manufacturer_target = None
            if manufacturer_fk == self.manufacturer_fk:
                diageo_facings = num_res
                manufacturer_target = target
            result = self.get_score(num_res, den_res)
            if manufacturer_fk == 0:
                continue
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer_fk, numerator_result=num_res, result=result,
                denominator_result=den_res, identifier_parent=self.common.get_dictionary(
                    kpi_fk=total_kpi_fk),
                target=manufacturer_target)
        result = self.get_score(diageo_facings, den_res)
        score = 1 if result >= target else 0
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_facings,
            denominator_result=den_res, result=score, score=result, weight=weight * 100,
            identifier_result=self.common.get_dictionary(kpi_fk=total_kpi_fk), target=target,
            identifier_parent=self.common.get_dictionary(name=Const.TOTAL), should_enter=True)
        return score * weight, 0, 0

    # display share:

    def calculate_total_display_share(self, scene_types, weight, target):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :param target: for the score
        :param weight: float
        :return: total_result
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.TOTAL])
        total_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        if self.no_display_allowed:
            Log.debug("There is no display, Display Share got 100")
            score = 1
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
            return score * weight, 0, 0
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[
            Const.DISPLAY_SHARE][Const.MANUFACTURER])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif_without_emptys[(self.scif_without_emptys['scene_fk'].isin(relevant_scenes)) &
                                                     (self.scif_without_emptys['location_type'] == 'Secondary Shelf') &
                                                     (self.scif_without_emptys['product_type'] == 'SKU')]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        for product_fk in relevant_products['product_fk'].unique().tolist():
            product_result = self.calculate_display_share_of_sku(
                product_fk, relevant_products, manufacturer_kpi_fk)
            all_results = all_results.append(product_result, ignore_index=True)
        den_res = all_results[Const.PASSED].sum()
        diageo_results, diageo_result = 0, 0
        for manufacturer in all_results[Const.MANUFACTURER].unique().tolist():
            num_res = all_results[all_results[Const.MANUFACTURER]
                                  == manufacturer][Const.PASSED].sum()
            result = self.get_score(num_res, den_res)
            target_manufacturer = None
            if manufacturer == self.manufacturer_fk:
                diageo_result, diageo_results = result, num_res
                target_manufacturer = target
            result_dict = self.common.get_dictionary(
                manufacturer_fk=manufacturer, kpi_fk=manufacturer_kpi_fk)
            if manufacturer == 0:
                continue
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer, numerator_result=num_res,
                target=target_manufacturer,
                denominator_result=den_res, result=result, identifier_parent=total_dict, identifier_result=result_dict)
        if den_res == 0:
            score = 0
        else:
            score = 1 if (diageo_results >= target * den_res) else 0
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_results, target=target,
            denominator_result=den_res, result=score, should_enter=True, weight=weight * 100, score=diageo_result,
            identifier_result=total_dict, identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
        return score * weight, 0, 0

    def calculate_display_share_of_sku(self, product_fk, relevant_products, manufacturer_kpi_fk):
        """
        calculates a specific product if it passes the condition of display
        :param product_fk:
        :param relevant_products: DF (scif of the display)
        :param manufacturer_kpi_fk: for write_to_db
        :return: a line for the results DF
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.SKU])
        manufacturer = self.get_manufacturer(product_fk)
        sum_scenes_passed = self.calculate_passed_display_without_subst(
            product_fk, relevant_products)
        parent_dict = self.common.get_dictionary(
            kpi_fk=manufacturer_kpi_fk, manufacturer_fk=manufacturer)
        if sum_scenes_passed == 0 or product_fk == 0:
            return None
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk,
            result=sum_scenes_passed, identifier_parent=parent_dict)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: sum_scenes_passed,
                          Const.MANUFACTURER: manufacturer}
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
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        if self.state in relevant_competitions[Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == self.state]
        else:
            relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == Const.OTHER]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, competition in relevant_competitions.iterrows():
            result_dict = self.calculate_shelf_facings_of_competition_per_scene(
                competition, relevant_scenes, i)
            all_results = all_results.append(result_dict, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, with_standard_type=True)
        return total_result, segment_result, national_result

    def calculate_shelf_facings_of_competition_per_scene(self, competition, relevant_scenes, index):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :param index: for hierarchy
        :return: passed, product_fk, standard_type
        """

        kpi_fk_total = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.SHELF_FACINGS][Const.COMPETITION])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.SHELF_FACINGS][Const.TOTAL])
        our_eans = str(competition[Const.OUR_EAN_CODE]).split(', ')
        our_lines = self.all_products_sku[self.all_products_sku['product_ean_code'].isin(our_eans)]
        if our_lines.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_eans))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        product_fk = our_fks[0]
        product_assortment_line = self.relevant_assortment[self.relevant_assortment['product_fk'] == product_fk]
        if product_assortment_line.empty or product_fk == 0:
            return None
        additional_attrs = json.loads(product_assortment_line.iloc[0]['additional_attributes'])
        standard_type = additional_attrs[Const.NATIONAL_SEGMENT]
        result_identifier = self.common.get_dictionary(
            kpi_fk=kpi_fk_total, product_fk=product_fk, index=index)
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.SHELF_FACINGS][Const.SKU])
        flag = False
        target = 1
        comparison_result = comp_facings_df = pd.DataFrame()
        comp_product_fk = -1
        if self.does_exist(competition[Const.COMP_EAN_CODE]):
            comp_eans = competition[Const.COMP_EAN_CODE].split(', ')
            comp_lines = self.all_products_sku[self.all_products_sku['product_ean_code'].isin(
                comp_eans)]
            if comp_lines.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(comp_eans))
                target = 0
            else:
                comp_fks = comp_lines['product_fk'].unique().tolist()
                comp_product_fk = comp_fks[0]
                comp_facings_df = self.calculate_shelf_facings_of_sku_per_scene(
                    comp_fks, relevant_scenes)
                bench_value = competition[Const.BENCH_VALUE]
                if type(bench_value) in (unicode, str):
                    bench_value = float(bench_value.replace("%", "")) / 100
                comp_facings_df['target'] = comp_facings_df['facings'] * bench_value
                comp_facings_df = comp_facings_df.rename(columns={'facings': 'facings_comp'})
                flag = True
        elif self.does_exist(competition[Const.BENCH_VALUE]):
            target = competition[Const.BENCH_VALUE]
        else:
            Log.warning("Product {} has no target in shelf facings".format(our_eans))
            target = 0
        our_facings_df = self.calculate_shelf_facings_of_sku_per_scene(our_fks, relevant_scenes)
        if flag and not our_facings_df.empty:
            comparison_df = pd.merge(our_facings_df, comp_facings_df,
                                     how="left", on='template_name').fillna(0)
            comparison_df = comparison_df.iloc[:1]
            comparison_result = comparison_df[(comparison_df['facings'] >= comparison_df['facings_comp']) &
                                              (comparison_df['facings'] > 0)]
            comparison_len = len(comparison_result)
            if comparison_len > 0:
                comparison = 1
                comparison_result = comparison_result.sort_values(by=['template_name'])
            else:
                comparison = 0
        else:
            if our_facings_df.empty:
                comparison = 0
            else:
                comparison_df = comparison_result = our_facings_df.sort_values(by=['template_name'])
                comparison = 1 if len(comparison_df[comparison_df['facings'] >= target]) else 0

        product_facings_comp = product_facings_ours = 0
        if flag and not comparison_result.empty:
            product_facings_comp = comparison_result.iloc[0]['facings_comp']
            product_facings_ours = comparison_result.iloc[0]['facings']
        else:
            if not comp_facings_df.empty:
                product_facings_comp = comp_facings_df.iloc[0]['facings_comp']
            if not our_facings_df.empty:
                product_facings_ours = our_facings_df.iloc[0]['facings']
        if self.does_exist(competition[Const.COMP_EAN_CODE]):
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=comp_product_fk, result=product_facings_comp, denominator_id=None,
                should_enter=True, identifier_parent=result_identifier, target=target)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=product_facings_ours, denominator_id=self.manufacturer_fk,
            should_enter=True, identifier_parent=result_identifier, target=target)

        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk_total, numerator_id=product_fk, score=comparison * 100,
            result=product_facings_ours, identifier_result=result_identifier,
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: comparison,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_shelf_facings_of_sku_per_scene(self, product_fks, relevant_scenes):
        """
        Gets product(s) and counting its facings.
        :param product_fks: list of FKs
        :param relevant_scenes: list
        :param parent_identifier: for write_to_db
        :param target: float, for the main product
        :param diageo_product: bool, if it's the main product
        :return: amount of facings
        """
        product_facing = pd.DataFrame
        for product_fk in product_fks:
            product_facing = self.scif_without_emptys[
                (self.scif_without_emptys['product_fk'] == product_fk) &
                (self.scif_without_emptys['scene_id'].isin(relevant_scenes))][['template_name',
                                                                               'facings']].groupby('template_name').sum().reset_index()
        return product_facing

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
        all_products_table = self.templates[Const.SHELF_PLACMENTS_SHEET]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in all_products_table.iterrows():
            result = self.calculate_shelf_placement_of_sku(product_line, relevant_scenes)
            all_results = all_results.append(result, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, with_standard_type=True)
        return total_result, segment_result, national_result

    def convert_groups_from_template(self):
        """
        Creates dict that contains every number in the template and its shelves
        :return: dict of lists
        """
        shelf_groups = self.templates[Const.SHELF_GROUPS_SHEET]
        shelves_groups = {}
        for i, group in shelf_groups.iterrows():
            shelves_groups[group[Const.NUMBER_GROUP]] = group[Const.SHELF_GROUP].split(', ')
        return shelves_groups

    def calculate_shelf_placement_of_sku(self, product_line, relevant_scenes):
        """
        Gets a product (line from template) and checks if it has more facings than targets in the eye level
        :param product_line: series
        :param relevant_scenes: list
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.SHELF_PLACEMENT][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.SHELF_PLACEMENT][Const.TOTAL])
        product_fk = self.all_products_sku[self.all_products_sku['product_ean_code'] == product_line[
            Const.PRODUCT_EAN_CODE]]['product_fk']
        if product_fk.empty:
            Log.warning("Product_ean '{}' does not exist".format(
                product_line[Const.PRODUCT_EAN_CODE]))
            return None
        product_fk = product_fk.iloc[0]
        product_assortment_line = self.relevant_assortment[self.relevant_assortment['product_fk'] == product_fk]
        if product_assortment_line.empty or product_fk == 0:
            return None
        additional_attrs = json.loads(product_assortment_line.iloc[0]['additional_attributes'])
        standard_type = additional_attrs[Const.NATIONAL_SEGMENT]
        min_shelf_loc = product_line[Const.MIN_SHELF_LOCATION]
        product_fk_with_substs = [product_fk]
        product_fk_with_substs += self.all_products[self.all_products['substitution_product_fk'] == product_fk][
            'product_fk'].tolist()
        relevant_products = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'].isin(product_fk_with_substs)) &
            (self.match_product_in_scene['scene_fk'].isin(relevant_scenes))]
        if relevant_products.empty:
            passed, result = 0, Const.NO_PLACEMENT
        else:
            relevant_products = pd.merge(relevant_products, self.scif[['scene_id', 'template_name']], how='left',
                                         left_on='scene_fk', right_on='scene_id').drop_duplicates()
            relevant_products = relevant_products.sort_values(by=['template_name'])
            shelf_groups = self.converted_groups[min_shelf_loc]
            all_shelves_placements = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_PLACEMENT)
            passed, result = 0, None
            for i, product in relevant_products.iterrows():
                is_passed, shelf_name = self.calculate_specific_product_shelf_placement(
                    product, shelf_groups)
                if is_passed == 1:
                    result, passed = shelf_name, 1
                    if shelf_name != Const.OTHER:
                        break
                if all_shelves_placements[all_shelves_placements[Const.SHELF_NAME] == shelf_name].empty:
                    all_shelves_placements = all_shelves_placements.append(
                        {Const.SHELF_NAME: shelf_name, Const.PASSED: is_passed, Const.FACINGS: 1}, ignore_index=True)
                else:
                    all_shelves_placements[all_shelves_placements[Const.SHELF_NAME] == shelf_name][
                        Const.FACINGS] += 1
            if passed == 0:
                all_shelves_placements = all_shelves_placements.sort_values(by=[Const.FACINGS])
                result = all_shelves_placements[Const.SHELF_NAME].iloc[0]
        shelf_groups = self.templates[Const.SHELF_GROUPS_SHEET]
        target = shelf_groups[shelf_groups[Const.NUMBER_GROUP]
                              == min_shelf_loc][Const.SHELF_GROUP].iloc[0]
        target_fk, result_fk = self.get_pks_of_result(target), self.get_pks_of_result(result)
        score = passed * 100
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, score=score, result=self.get_pks_of_result(result),
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk), target=target_fk)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: passed,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_specific_product_shelf_placement(self, match_product_line, shelf_groups):
        """
        takes a line of match_product and the group shleves it should be on, and returns if it does (and which group)
        :param match_product_line: series - specific line from match_product_in_scene
        :param shelf_groups: list of the match group_names (['E', 'T'])
        :return: couple: if passed or not, and the location ("E")
        """
        min_max_shleves = self.templates[Const.MINIMUM_SHELF_SHEET]
        shelf_from_bottom = match_product_line['shelf_number_from_bottom']
        scene = match_product_line['scene_fk']
        if shelf_from_bottom > len(min_max_shleves):
            shelf_from_bottom = len(min_max_shleves)
        amount_of_shelves = self.scenes_with_shelves[scene] \
            if self.scenes_with_shelves[scene] <= len(min_max_shleves.columns) else len(min_max_shleves.columns)
        group_for_product = min_max_shleves[amount_of_shelves].iloc[shelf_from_bottom - 1]
        if "ALL" in shelf_groups:
            answer_couple = 1, group_for_product
        else:
            answer_couple = 0, group_for_product
            if group_for_product in shelf_groups:
                answer_couple = 1, group_for_product
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
        all_products_table = self.templates[Const.PRICING_SHEET]
        if self.state in all_products_table[Const.STATE].unique().tolist():
            all_products_table = all_products_table[all_products_table[Const.STATE] == self.state]
        else:
            all_products_table = all_products_table[all_products_table[Const.STATE] == Const.OTHER]
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in all_products_table.iterrows():
            compete_result_dict = self.calculate_msrp_of_competition(
                competition, relevant_scenes, i)
            all_competes = all_competes.append(compete_result_dict, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
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
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.MSRP][Const.COMPETITION])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.MSRP][Const.TOTAL])
        our_ean, comp_ean = competition[Const.OUR_EAN_CODE], competition[Const.COMP_EAN_CODE]
        min_relative, max_relative = competition[Const.MIN_MSRP_RELATIVE], competition[Const.MAX_MSRP_RELATIVE]
        min_absolute, max_absolute = competition[Const.MIN_MSRP_ABSOLUTE], competition[Const.MAX_MSRP_ABSOLUTE]
        our_line = self.all_products_sku[self.all_products_sku['product_ean_code'] == our_ean]
        if our_line.empty:
            Log.warning("The products {} in MSRP don't exist in DB".format(our_ean))
            return None
        product_fk = our_line['product_fk'].iloc[0]
        result_dict = self.common.get_dictionary(kpi_fk=kpi_fk, product_fk=product_fk, index=index)
        our_price = self.calculate_sku_price(product_fk, relevant_scenes, result_dict)
        if our_price is None or product_fk == 0:
            return None
        is_competitor = (self.does_exist(comp_ean) and
                         self.does_exist(min_relative) and self.does_exist(max_relative))
        is_absolute = self.does_exist(min_absolute) and self.does_exist(max_absolute)
        if is_competitor:
            comp_line = self.all_products_sku[self.all_products_sku['product_ean_code'] == comp_ean]
            if comp_line.empty:
                Log.warning("The products {} in MSRP don't exist in DB".format(our_ean))
                comp_price = our_price + competition[Const.MIN_MSRP_RELATIVE]
            else:
                comp_fk = comp_line['product_fk'].iloc[0]
                comp_price = self.calculate_sku_price(comp_fk, relevant_scenes, result_dict)
                if comp_price is None:
                    comp_price = our_price + min_relative
            range_price = (round(comp_price + min_relative, 1),
                           round(comp_price + max_relative, 1))
        elif is_absolute:
            range_price = (min_absolute, max_absolute)
        else:
            Log.warning("In MSRP product {} does not have a clear competitor".format(product_fk))
            range_price = (our_price, our_price)
        result = 0
        if our_price < range_price[0]:
            result = range_price[0] - our_price
        elif our_price > range_price[1]:
            result = range_price[1] - our_price
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result,
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk), identifier_result=result_dict)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: (result == 0) * 1,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand}
        return product_result

    def calculate_sku_price(self, product_fk, scenes, parent_dict):
        """
        Takes product, checks its price and writes it in the DB.
        :param product_fk:
        :param scenes: list of fks
        :param parent_dict: identifier dictionary
        :return: price
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.SKU])
        price = self.products_with_prices[(self.products_with_prices['product_fk'] == product_fk) &
                                          (self.products_with_prices['scene_fk'].isin(scenes))]['price_value']
        if price.empty or product_fk == 0:
            return None
        result = round(price.iloc[0], 1)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result,
            identifier_parent=parent_dict, should_enter=True)
        return result

    # help functions:

    def calculate_passed_display(self, product_fk, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet).
        :param product_fk:
        :param relevant_products: relevant scif
        :return: number of scenes. int.
        """
        template = self.templates[Const.DISPLAY_TARGET_SHEET]
        sum_scenes_passed, sum_facings = 0, 0
        for scene in relevant_products['scene_fk'].unique().tolist():
            scene_product = relevant_products[(relevant_products['scene_fk'] == scene) &
                                              (relevant_products['product_fk'] == product_fk)]
            if scene_product.empty:
                continue
            scene_type = scene_product['template_name'].iloc[0]
            minimum_products = template[template[Const.SCENE_TYPE] == scene_type]
            if minimum_products.empty:
                minimum_products = template[template[Const.SCENE_TYPE] == Const.OTHER]
            minimum_products = minimum_products[Const.MIN_FACINGS].iloc[0]
            facings = scene_product['facings'].iloc[0]
            # if the condition is failed, it will "add" 0.
            sum_scenes_passed += 1 * (facings >= minimum_products)
        return sum_scenes_passed

    def calculate_passed_display_without_subst(self, product_fk, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet),
        every time it should pass the condition ONLY with the same product_fk (without the similar products).
        :param product_fk:
        :param relevant_products: relevant scif
        :return: number of scenes. int.
        """
        template = self.templates[Const.DISPLAY_TARGET_SHEET]
        sum_scenes_passed, sum_facings = 0, 0
        product_fk_with_substs = [product_fk]
        product_fk_with_substs += self.all_products[self.all_products['substitution_product_fk'] == product_fk][
            'product_fk'].tolist()
        for product in product_fk_with_substs:
            for scene in relevant_products['scene_fk'].unique().tolist():
                scene_products = self.match_product_in_scene[
                    (self.match_product_in_scene['scene_fk'] == scene) &
                    (self.match_product_in_scene['product_fk'] == product)]
                if scene_products.empty:
                    continue
                scene_type = self.get_scene_type(scene)
                minimum_products = template[template[Const.SCENE_TYPE] == scene_type]
                if minimum_products.empty:
                    minimum_products = template[template[Const.SCENE_TYPE] == Const.OTHER]
                minimum_products = minimum_products[Const.MIN_FACINGS].iloc[0]
                facings = len(scene_products)
                # if the condition is failed, it will "add" 0.
                sum_scenes_passed += 1 * (facings >= minimum_products)
        return sum_scenes_passed

    def get_scene_type(self, scene_fk):
        return self.scif[self.scif['scene_fk'] == scene_fk]['template_name'].iloc[0]

    def get_relevant_scenes(self, scene_types):
        """
        :param scene_types: cell in the template
        :return: list of all the scenes contains the cell
        """
        if self.does_exist(scene_types):
            scene_type_list = scene_types.split(", ")
            return self.scif_without_emptys[self.scif_without_emptys["template_name"].isin(scene_type_list)][
                "scene_id"].unique().tolist()
        return self.scif_without_emptys["scene_id"].unique().tolist()

    def get_product_details(self, product_fk):
        """
        :param product_fk:
        :return: its details for assortment (brand, sub_brand)
        """
        if self.all_products[self.all_products['product_fk'] == product_fk].empty:
            return None, None
        brand = self.all_products[self.all_products['product_fk'] == product_fk]['brand_fk'].iloc[0]
        sub_brand = self.all_products[self.all_products['product_fk']
                                      == product_fk]['sub_brand'].iloc[0]
        if not sub_brand:
            sub_brand_fk = None
        else:
            sub_brand_fk = self.get_sub_brand_fk(sub_brand, brand)
        return brand, sub_brand_fk

    def get_sub_brand_fk(self, sub_brand, brand_fk):
        """
        takes sub_brand and returns its pk
        :param sub_brand: str
        :param brand_fk: we need it for the parent_id (different brands can have common sub_brand)
        :return: pk
        """
        sub_brand_line = self.sub_brands[(self.sub_brands['name'] == sub_brand) &
                                         (self.sub_brands['parent_id'] == brand_fk)]
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

    def get_score(self, num, den):
        """
        :param num: number
        :param den: number
        :return: the percent of the num/den
        """
        if den == 0:
            return 0
        return self.round_result(float(num) / den)

    def get_pks_of_result(self, result):
        """
        converts string result to its pk (in static.kpi_result_value)
        :param result: str
        :return: int
        """
        pk = self.result_values[self.result_values['value'] == result]['pk'].iloc[0]
        return pk

    @staticmethod
    def round_result(result):
        return round(result, 3)

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
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.TOTAL])
        total_identifier = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        for brand in all_results[Const.BRAND].unique().tolist():
            if brand is None or np.isnan(brand):
                continue
            brand_results = all_results[all_results[Const.BRAND] == brand]
            self.insert_brand_and_subs_to_db(brand_results, kpi_db_names, brand, total_identifier,
                                             write_numeric=write_numeric, sub_brand_numeric=sub_brand_numeric)
        all_passed_results = all_results[Const.PASSED]
        total_result = self.insert_totals_to_db(all_passed_results, kpi_db_names, Const.TOTAL, weight, total_identifier,
                                                should_enter=should_enter, write_numeric=write_numeric)
        segment_result, national_result = 0, 0
        if with_standard_type:
            national_results = all_results[all_results[Const.STANDARD_TYPE]
                                           == Const.NATIONAL][Const.PASSED]
            national_result = self.insert_totals_to_db(national_results, kpi_db_names, Const.NATIONAL, weight,
                                                       should_enter=should_enter, write_numeric=write_numeric)
            segment_results = all_results[all_results[Const.STANDARD_TYPE]
                                          == Const.SEGMENT][Const.PASSED]
            segment_result = self.insert_totals_to_db(segment_results, kpi_db_names, Const.SEGMENT, weight,
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
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.BRAND])
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
        for sub_brand in brand_results[brand_results[Const.BRAND] == brand][Const.SUB_BRAND].unique().tolist():
            if sub_brand is None or np.isnan(sub_brand) or sub_brand == 0:
                continue
            sub_brand_results = brand_results[(brand_results[Const.BRAND] == brand) &
                                              (brand_results[Const.SUB_BRAND] == sub_brand)]
            self.insert_sub_brands_to_db(sub_brand_results, kpi_db_names, brand, sub_brand, brand_dict,
                                         write_numeric=write_numeric, sub_brand_numeric=sub_brand_numeric)
        results = brand_results[Const.PASSED]
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
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SUB_BRAND])
        sub_brand_dict = self.common.get_dictionary(
            kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        results = sub_brand_results[Const.PASSED]
        num_res, den_res = 0, 0
        if sub_brand_numeric:
            result = Const.DISTRIBUTED if results.sum() > 0 else Const.OOS
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
            score = result * weight
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_res, should_enter=should_enter,
                denominator_result=den_res, result=self.round_result(score), identifier_result=identifier_result,
                identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight * 100, score=result)
        else:
            num_res, den_res = all_passed_results.sum(), all_passed_results.count()
            result = self.get_score(num_res, den_res)
            score = result * weight
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_res, should_enter=should_enter,
                denominator_result=den_res, result=result, identifier_result=identifier_result,
                identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight * 100,
                score=self.round_result(score))
        return score
