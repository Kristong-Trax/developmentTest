import os
import pandas as pd
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.DIAGEOUS_SAND2.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from datetime import datetime

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class ToolBox:
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
        try:
            self.manufacturer_fk = int(self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0])
        except Exception as e:
            self.manufacturer_fk = self.all_products[
                self.all_products['manufacturer_name'] == 'DIAGEO']['manufacturer_fk'].iloc[0]
        if self.store_info['additional_attribute_6'].iloc[0]:
            self.attr6 = Const.ON if self.store_info['additional_attribute_6'].iloc[0] == 'On-Premise' else Const.OFF
        else:
            Log.error("The store for this session has no attribute6. Set temporary as Off-premise, fix ASAP")
            self.attr6 = Const.OFF
        if self.store_info['additional_attribute_11'].iloc[0]:
            self.attr11 = self.store_info['additional_attribute_11'].iloc[0]
        else:
            Log.error("The store for this session has no attribute11. Set temporary as Open, fix ASAP")
            self.attr11 = Const.OPEN
        if self.store_info['additional_attribute_2'].iloc[0]:
            self.attr2 = self.store_info['additional_attribute_2'].iloc[0]
        else:
            Log.warning("The store for this session has no attribute2. Set temporary as Other, please fix")
            self.attr2 = Const.OTHER
        self.templates = {}
        self.get_templates()
        self.kpi_results_queries = []
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.state_fk = self.store_info['state_fk'][0]
        self.sub_brands = self.ps_data.get_custom_entities(1002)
        self.result_values = self.ps_data.get_result_values()
        self.products_with_prices = self.ps_data.get_products_prices()
        self.relevant_assortment = pd.DataFrame()
        self.init_dfs()

    # initialize:

    def get_templates(self):
        """
        Reads the template (and makes the EANs be Strings)
        """
        for sheet in Const.SHEETS[self.attr11][self.attr6]:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet, keep_default_na=False)

    def init_dfs(self):
        self.sub_brands.rename(
            inplace=True, index=str, columns={"pk": "sub_brand_fk", "parent_id": "brand_fk", "name": "sub_brand"})
        self.all_products = self.all_products.merge(self.sub_brands, on=["brand_fk", "sub_brand"], how="left")
        self.scif = self.scif.merge(self.sub_brands, on=["brand_fk", "sub_brand"], how="left")
        self.scif_without_emptys = self.scif[~(self.scif['product_type'] == "Empty") &
                                             (self.scif['substitution_product_fk'].isnull())]
        self.all_products_sku = self.all_products[(self.all_products['product_type'] == 'SKU') &
                                                  (self.all_products['category'] == 'SPIRITS') &
                                                  (self.all_products['is_active'] == 1)]
        self.match_product_in_scene = self.match_product_in_scene.merge(self.all_products, on="product_fk")
        self.store_number_1 = self.store_info['store_number_1'].iloc[0]
        self.no_menu_allowed = self.survey.check_survey_answer(
            survey_text=Const.NO_MENU_ALLOWED_QUESTION, target_answer=Const.SURVEY_ANSWER)
        self.no_back_bar_allowed = self.survey.check_survey_answer(
            survey_text=Const.NO_BACK_BAR_ALLOWED_QUESTION, target_answer=Const.SURVEY_ANSWER)
        self.no_display_allowed = self.survey.check_survey_answer(
            survey_text=Const.NO_DISPLAY_ALLOWED_QUESTION, target_answer=Const.SURVEY_ANSWER)
        if self.attr11 in Const.NOT_INDEPENDENT_STORES:
            self.init_assortment(self.store_number_1)
            if self.attr6 == Const.ON:
                self.sales_data = self.ps_data.get_sales_data()
            elif self.attr11 == Const.OPEN:
                scenes = self.scene_info['scene_fk'].unique().tolist()
                self.scenes_with_shelves = {}
                for scene in scenes:
                    shelves = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene][[
                        'shelf_number_from_bottom', 'shelf_number']].max()
                    self.scenes_with_shelves[scene] = max(shelves)
                self.converted_groups = self.convert_groups_from_template()
                self.external_targets = self.ps_data.get_kpi_external_targets(
                    kpi_operation_types=Const.OPEN_OPERATION_TYPES,
                    key_fields=[Const.EX_PRODUCT_FK, Const.EX_STATE_FK, Const.EX_STORE_NUMBER, Const.EX_SCENE_TYPE,
                                Const.EX_ATTR2],
                    data_fields=[Const.EX_MIN_FACINGS, Const.EX_MINIMUM_SHELF, Const.EX_BENCHMARK_VALUE,
                                 Const.EX_TARGET_MIN, Const.EX_COMPETITOR_FK, Const.EX_RELATIVE_MAX,
                                 Const.EX_RELATIVE_MIN, Const.EX_TARGET_MAX])
                self.external_targets = self.external_targets.fillna("N/A")
            else:
                self.external_targets = self.ps_data.get_kpi_external_targets(
                    kpi_operation_types=Const.OPEN_OPERATION_TYPES,
                    key_fields=[Const.EX_PRODUCT_FK, Const.EX_STATE_FK, Const.EX_STORE_NUMBER, Const.EX_SCENE_TYPE,
                                Const.EX_ATTR2],
                    data_fields=[Const.EX_MIN_FACINGS, Const.EX_MINIMUM_SHELF, Const.EX_BENCHMARK_VALUE,
                                 Const.EX_COMPETITOR_FK])
                self.external_targets = self.external_targets.fillna("N/A")
        elif self.attr6 != Const.ON:
                self.init_assortment()
                self.external_targets = self.ps_data.get_kpi_external_targets(
                    kpi_operation_types=Const.INDEPENDENT_OPERATION_TYPES,
                    key_fields=[Const.EX_SCENE_TYPE, Const.EX_ATTR2], data_fields=[Const.EX_MIN_FACINGS])
                self.external_targets = self.external_targets.fillna("N/A")
        if self.attr6 == Const.OFF:
            total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(
                Const.DB_ASSORTMENTS_NAMES[Const.OFF]) if self.attr11 in Const.NOT_INDEPENDENT_STORES else \
                self.common.get_kpi_fk_by_kpi_name(Const.DB_ASSORTMENTS_NAMES[Const.INDEPENDENT])
            if not self.assortment_products.empty:
                self.relevant_assortment = self.assortment_products[
                    self.assortment_products['kpi_fk_lvl2'] == total_off_trade_fk]
            else:
                self.relevant_assortment = self.assortment_products  # we need this to prevent undefined errors

    def init_assortment(self, store_number_1=None):
        self.assortment = Assortment(self.data_provider, self.output, ps_data_provider=self.ps_data,
                                     assortment_filter=store_number_1)
        self.assortment_products = self.assortment.get_lvl3_relevant_ass()
        if 'product_fk' not in self.assortment_products.columns:
            self.assortment_products['product_fk'] = None
        if 'additional_attributes' not in self.assortment_products.columns:
            self.assortment_products['additional_attributes'] = '{}'
        self.assortment_products['additional_attributes'] = self.assortment_products[
            'additional_attributes'].apply(json.loads)
        self.assortment_products[Const.DISPLAY] = self.assortment_products['additional_attributes'].apply(
            lambda x: x[Const.DISPLAY] if Const.DISPLAY in x.keys() else None)
        self.assortment_products[Const.STANDARD_TYPE] = self.assortment_products['additional_attributes'].apply(
            lambda x: x[Const.NATIONAL_SEGMENT] if Const.NATIONAL_SEGMENT in x.keys() else None)
        self.assortment_products = self.assortment_products.merge(self.all_products[[
            'product_fk', 'manufacturer_fk', 'brand_fk', 'sub_brand_fk']], on="product_fk")

    # main functions:

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if self.relevant_assortment.empty and self.attr6 == Const.OFF:
            return
        total_store_score, segment_store_score, national_store_score = 0, 0, 0
        for i, kpi_line in self.templates[Const.SHEETS[self.attr11][self.attr6][0]].iterrows():
            total_weighted_score, segment_weighted_score, national_weighted_score = self.calculate_set(kpi_line)
            if kpi_line[Const.KPI_GROUP]:
                total_store_score += total_weighted_score
                segment_store_score += segment_weighted_score
                national_store_score += national_weighted_score
        total_store_score = self.round_result(total_store_score)
        national_store_score = self.round_result(national_store_score)
        segment_store_score = self.round_result(segment_store_score)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_TOTAL_KPIS[self.attr6][Const.TOTAL])
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, result=total_store_score, denominator_id=self.store_id,
            identifier_result=self.common.get_dictionary(name=Const.TOTAL), score=total_store_score)
        if self.attr11 == Const.OPEN:
            segment_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_TOTAL_KPIS[self.attr6][Const.SEGMENT])
            national_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_TOTAL_KPIS[self.attr6][Const.NATIONAL])
            self.common.write_to_db_result(
                fk=segment_kpi_fk, numerator_id=self.manufacturer_fk, score=segment_store_score,
                identifier_result=self.common.get_dictionary(name=Const.SEGMENT), result=segment_store_score)
            self.common.write_to_db_result(
                fk=national_kpi_fk, numerator_id=self.manufacturer_fk, score=national_store_score,
                identifier_result=self.common.get_dictionary(name=Const.NATIONAL), result=national_store_score)

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
            calculation = self.calculate_total_shelf_placement
        elif kpi_name == Const.SHELF_FACINGS:
            calculation = self.calculate_total_shelf_facings
        elif kpi_name == Const.MSRP:
            calculation = self.calculate_total_msrp
        elif kpi_name == Const.DISPLAY_SHARE:
            calculation = self.calculate_total_display_share
        elif kpi_name in (Const.POD, Const.DISPLAY_BRAND, Const.BACK_BAR):
            if self.attr6 == Const.ON:
                calculation = self.calculate_on_assortment
            else:
                calculation = self.calculate_off_assortment
        elif kpi_name == Const.MENU:
            calculation = self.calculate_menu
        else:
            Log.warning("Set {} is not defined".format(kpi_name))
            return 0, 0, 0
        total_score, segment_score, national_score = calculation(scene_types, kpi_name, weight, target)
        return total_score, segment_score, national_score

    def survey_display_back_bar_write_to_db(self, weight, kpi_dict):
        """
        In case we don't have display (buy the survey question) we need to pass the KPI.
        :param weight: float
        :param kpi_dict: dict
        :return: True if no display/back_bar
        """
        score = 1
        dict_of_fks = {
            Const.TOTAL: kpi_dict[Const.TOTAL],
            Const.NATIONAL: kpi_dict[Const.NATIONAL],
            Const.SEGMENT: kpi_dict[Const.SEGMENT]
        }
        for kpi in dict_of_fks:
            self.common.write_to_db_result(
                fk=dict_of_fks[kpi], numerator_id=self.manufacturer_fk,
                identifier_parent=self.common.get_dictionary(name=kpi),
                result=score, should_enter=True, weight=weight * 100, score=score
            )
        return True

    # assortments:

    def calculate_on_assortment(self, scene_types, kpi_name, weight, target):
        """
        Gets assortment type, and calculates it with the match function.
        It's working until sub_brand level (without sku)
        :param scene_types: string from template
        :param kpi_name: str ("Back Bar" or "POD")
        :param weight: float
        :return:
        """
        kpi_db_names = self.pull_kpi_fks_from_names(Const.DB_ON_NAMES[kpi_name])
        if kpi_name == Const.BACK_BAR and self.no_back_bar_allowed:
            self.survey_display_back_bar_write_to_db(weight, kpi_db_names)
            Log.debug("There is no back bar, Back Bar got 100")
            return 1 * weight, 1 * weight, 1 * weight
        if self.assortment_products.empty:
            return 0, 0, 0
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        assortment_name = Const.BACK_BAR if kpi_name == Const.BACK_BAR and self.attr11 == Const.OPEN else Const.ON
        total_on_trade_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ASSORTMENTS_NAMES[assortment_name])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_on_trade_fk]
        standard_types_results = {Const.SEGMENT: [], Const.NATIONAL: []} if self.attr11 == Const.OPEN else {}
        total_results = []
        if self.attr11 == Const.NATIONAL_STORE and kpi_name == Const.BACK_BAR:
            kpi_db_names = self.pull_kpi_fks_from_names(Const.DB_ON_NAMES[Const.BACK_BAR_NATIONAL])
            for scene_type in relevant_scif['template_name'].unique().tolist():
                temp_scif = relevant_scif[relevant_scif['template_name'] == scene_type]
                temp_results = self.calculate_back_bar_national_template(
                    temp_scif, relevant_assortment, kpi_db_names, weight, target)
                total_results += temp_results
        else:
            for brand_fk in relevant_assortment['brand_fk'].unique().tolist():
                brand_assortment = relevant_assortment[relevant_assortment['brand_fk'] == brand_fk]
                standard_types_results, brand_results = self.generic_brand_calculator(
                    brand_assortment, relevant_scif, standard_types_results, kpi_db_names)
                total_results += brand_results
        standard_types_results[Const.TOTAL] = total_results
        scores = self.insert_final_results_avg(standard_types_results, kpi_db_names, weight)
        # add extra products to DB:
        if kpi_name == Const.POD:
            self.calculate_extras(relevant_assortment, relevant_scif, kpi_db_names)
        return scores[Const.TOTAL], scores[Const.SEGMENT], scores[Const.NATIONAL]

    def calculate_back_bar_national_template(self, relevant_scif, relevant_assortment, kpi_db_names, weight, target):
        total_results = []
        template_kpi_fk = kpi_db_names[Const.TEMPLATE]
        template_fk = relevant_scif['template_fk'].iloc[0]
        for brand_fk in relevant_assortment['brand_fk'].unique().tolist():
            brand_assortment = relevant_assortment[relevant_assortment['brand_fk'] == brand_fk]
            brand_results = self.calculate_back_bar_national_brand(
                brand_assortment, relevant_scif, kpi_db_names, template_fk)
            total_results += brand_results
        num, den = sum(total_results), len(total_results)
        result = self.get_score(num, den)
        scene_type_dict = self.common.get_dictionary(kpi_fk=template_kpi_fk, template_fk=template_fk)
        parent_dict = self.common.get_dictionary(kpi_fk=kpi_db_names[Const.TOTAL])
        self.common.write_to_db_result(
            fk=template_kpi_fk, numerator_id=template_fk,
            denominator_result=den, result=result, weight=weight * 100,
            identifier_result=scene_type_dict, target=target, numerator_result=num,
            identifier_parent=parent_dict, should_enter=True)
        return total_results

    def calculate_back_bar_national_brand(self, brand_list, relevant_df, kpi_db_names, template_fk):
        """
        Calculates the brand with calculating the sub_brand and SKUs scores and inserts it to DB.
        :param brand_list: assortment or template list
        :param relevant_df: matches/scif/prices
        :param kpi_db_names: dict
        :param template_fk: for the identifier parent of template level KPIs
        :return: dict of lists, list
        """
        brand_results = []
        brand = brand_list.iloc[0]['brand_fk']
        for sub_brand_fk in brand_list[~(brand_list['sub_brand_fk'].isnull())]['sub_brand_fk'].unique().tolist():
            sub_brand_list = brand_list[brand_list['sub_brand_fk'] == sub_brand_fk]
            sub_brand_results = self.calculate_back_bar_national_sub_brand(
                sub_brand_list, relevant_df, kpi_db_names, template_fk)
            brand_results += sub_brand_results
        num, den = sum(brand_results), len(brand_results)
        result = self.get_score(num, den)
        brand_kpi_fk = kpi_db_names[Const.BRAND]
        template_kpi_fk = kpi_db_names[Const.TEMPLATE]
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand, template_fk=template_fk)
        template_dict = self.common.get_dictionary(kpi_fk=template_kpi_fk, template_fk=template_fk)
        self.common.write_to_db_result(
            fk=brand_kpi_fk, numerator_id=brand, result=result, numerator_result=num, denominator_result=den,
            identifier_parent=template_dict, should_enter=True, identifier_result=brand_dict)
        return brand_results

    def calculate_back_bar_national_sub_brand(self, sub_brand_list, relevant_df, kpi_db_names, template):
        """
        Calculates the sub_brand with calculating the SKUs scores and inserts it to DB.
        :param relevant_df: matches/scif/prices
        :param kpi_db_names: dict
        :param template: for the identifier parent of template level KPIs
        :return: dict of lists, list
        """
        sub_brand_results = []
        brand, sub_brand = sub_brand_list.iloc[0][['brand_fk', 'sub_brand_fk']]
        for i, line in sub_brand_list.iterrows():
            passed, standard_type = self.calculate_back_bar_national_sku(line, relevant_df, kpi_db_names, i, template)
            if passed is None:
                continue
            sub_brand_results.append(passed)
        num, den = 0, 0
        if sum(sub_brand_results) > 0:
            result = Const.DISTRIBUTED
            answer_list = [1]
        else:
            result = Const.OOS
            answer_list = [0]
        result = self.get_pks_of_result(result)
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        brand_kpi_fk = kpi_db_names[Const.BRAND]
        sub_brand_dict = self.common.get_dictionary(
            kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand, template_fk=template)
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand, template_fk=template)
        self.common.write_to_db_result(
            fk=sub_brand_kpi_fk, numerator_id=sub_brand, result=result, numerator_result=num, denominator_result=den,
            identifier_parent=brand_dict, should_enter=True, identifier_result=sub_brand_dict)
        return answer_list

    def calculate_back_bar_national_sku(self, product_line, relevant_scif, kpi_db_names, i, template_fk):
        """
        Checks if specific product's sub_brand exists in the filtered scif
        :param relevant_scif: filtered scif
        :param template_fk: back_bar national should write the template also
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        sku_kpi_fk = kpi_db_names[Const.SKU]
        product_fk, brand, sub_brand, standard_type = product_line[[
            'product_fk', 'brand_fk', 'sub_brand_fk', Const.STANDARD_TYPE]]
        if sub_brand is None or self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None, None
        facings = relevant_scif[relevant_scif['product_fk'] == product_fk]['facings'].sum()
        if facings > 0:
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(
            kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand, template_fk=template_fk)
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk, result=self.get_pks_of_result(result),
            identifier_parent=sub_brand_dict, should_enter=True)
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk, result=self.get_pks_of_result(result), denominator_id=template_fk)
        return passed, standard_type

    def calculate_ass_on_sku(self, product_line, relevant_scif, kpi_db_names, i):
        """
        Checks if specific product's sub_brand exists in the filtered scif
        :param standard_type: S or N
        :param product_fk: int
        :param relevant_scif: filtered scif
        :param kpi_name: POD or Back Bar - to know if we should check the product in the sales data
        :param template_fk: back_bar national should write the template also
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        sku_kpi_fk = kpi_db_names[Const.SKU]
        product_fk, brand, sub_brand, standard_type = product_line[[
            'product_fk', 'brand_fk', 'sub_brand_fk', Const.STANDARD_TYPE]]
        if sub_brand is None or self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None, None
        facings = relevant_scif[relevant_scif['product_fk'] == product_fk]['facings'].sum()
        if facings > 0 or (product_fk in self.sales_data and kpi_db_names[Const.KPI_NAME] == Const.POD):
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(
            kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk, result=self.get_pks_of_result(result),
            identifier_parent=sub_brand_dict, should_enter=False)
        return passed, standard_type

    def calculate_off_assortment(self, scene_types, kpi_name, weight, target):
        """
        Gets assortment type, and calculates it with the match function
        :param scene_types: string from template
        :param kpi_name: POD or Display Brand
        :param weight:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        relevant_assortment = self.relevant_assortment
        kpi_db_names = self.pull_kpi_fks_from_names(Const.DB_OFF_NAMES[kpi_name])
        if kpi_name == Const.DISPLAY_BRAND:
            if self.no_display_allowed:
                self.survey_display_back_bar_write_to_db(weight, kpi_db_names)
                Log.debug("There is no display, Display Brand got 100")
                return 1 * weight, 1 * weight, 1 * weight
            if self.attr11 in Const.NOT_INDEPENDENT_STORES and kpi_name == Const.DISPLAY_BRAND:
                relevant_assortment = relevant_assortment[relevant_assortment[Const.DISPLAY].isin([1, '1'])]
            relevant_scif = relevant_scif[relevant_scif['location_type'] == 'Secondary Shelf']
        if self.assortment_products.empty:
            return 0, 0, 0
        standard_types_results = {Const.SEGMENT: [], Const.NATIONAL: []} if self.attr11 == Const.OPEN else {}
        total_results = []
        for brand_fk in relevant_assortment['brand_fk'].unique().tolist():
            brand_assortment = relevant_assortment[relevant_assortment['brand_fk'] == brand_fk]
            standard_types_results, brand_results = self.generic_brand_calculator(
                brand_assortment, relevant_scif, standard_types_results, kpi_db_names)
            total_results += brand_results
        standard_types_results[Const.TOTAL] = total_results
        scores = self.insert_final_results_avg(standard_types_results, kpi_db_names, weight)
        # add extra products to DB:
        if kpi_name == Const.POD:
            self.calculate_extras(relevant_assortment, relevant_scif, kpi_db_names)
        return scores[Const.TOTAL], scores[Const.SEGMENT], scores[Const.NATIONAL]

    def calculate_pod_off_sku(self, product_line, relevant_scif, kpi_db_names, i):
        """
        Checks if specific product exists in the filtered scif
        :param relevant_scif: filtered scif
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        product_fk, brand, sub_brand, standard_type = product_line[[
            'product_fk', 'brand_fk', 'sub_brand_fk', Const.STANDARD_TYPE]]
        if self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None, None
        facings = relevant_scif[relevant_scif['product_fk'] == product_fk]['facings'].sum()
        if facings > 0:
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        kpi_fk = kpi_db_names[Const.SKU]
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=self.get_pks_of_result(result),
            identifier_parent=sub_brand_dict, should_enter=False)
        return passed, standard_type

    def calculate_display_compliance_sku(self, product_line, relevant_scif, kpi_db_names, i):
        """
        Checks if specific product passes the condition of the display
        :param standard_type: S or N
        :param product_fk:
        :param relevant_scif: filtered scif
        :return: a line for the DF - {product: 8, passed: 1/0, standard: N/S, brand: 5, sub: 12}
        """
        product_fk, brand, sub_brand, standard_type = \
            product_line[['product_fk', 'brand_fk', 'sub_brand_fk', Const.STANDARD_TYPE]]
        if self.all_products_sku[self.all_products_sku['product_fk'] == product_fk].empty:
            return None, None
        facings = self.calculate_passed_display_without_subst_sep_scenes(product_fk, relevant_scif)
        if facings > 0:
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        kpi_fk = kpi_db_names[Const.SKU]
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=self.get_pks_of_result(result),
            identifier_parent=sub_brand_dict, should_enter=False)
        return passed, standard_type

    def calculate_extras(self, relevant_assortment, relevant_scif, kpi_db_names):
        """
        add the extra products (products not shown in the template) to DB.
        :param relevant_assortment: DF of assortment with all the PODs
        :param relevant_scif: DF (scif in the scenes)
        :param sku_kpi_fk: on or off
        :return:
        """
        sku_kpi_fk = kpi_db_names[Const.SKU]
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        assortment_products = relevant_assortment['product_fk'].unique().tolist()
        all_diageo_products = relevant_scif[
            (relevant_scif['manufacturer_fk'] == self.manufacturer_fk) &
            (relevant_scif['facings'] > 0) &
            ~(relevant_scif['product_fk'].isin(assortment_products))][[
            'product_fk', 'sub_brand_fk', 'brand_fk']].drop_duplicates()
        result = Const.EXTRA
        for i, product_line in all_diageo_products.iterrows():
            product, sub_brand, brand = product_line[['product_fk', 'sub_brand_fk', 'brand_fk']]
            sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
            self.common.write_to_db_result(
                fk=sku_kpi_fk, numerator_id=product, result=self.get_pks_of_result(result),
                identifier_parent=sub_brand_dict, should_enter=False)

    # menu

    def calculate_menu(self, scene_types, kpi_name, weight, target):
        """
        calculates the share of all brands and manufacturers in the menu, and
        checks if Diageo result is bigger than target
        :param scene_types: str
        :param weight: float
        :param target: float
        :return:
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.TOTAL])
        result_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        total_dict = self.common.get_dictionary(name=Const.TOTAL)
        if self.no_menu_allowed:
            Log.debug("There is no menu, Menu got 100")
            score = 1
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=total_dict)
            return score * weight, 0, 0
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[
            (self.scif_without_emptys['scene_id'].isin(relevant_scenes)) &
            (self.scif_without_emptys['product_type'] == 'POS') &
            ~(self.scif_without_emptys['sub_category_local_name'].isin(Const.MENU_EXCLUDE_SUB_CATEGORIES))]
        diageo_facings, den_res = 0, 0
        if self.attr11 == Const.NATIONAL_STORE:
            total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU_NATIONAL][Const.TOTAL])
            result_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
            template_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU_NATIONAL][Const.TEMPLATE])
            for template_group in relevant_scif['template_group'].unique().tolist():
                template_scif = relevant_scif[
                    relevant_scif['template_group'].str.encode("utf-8") == template_group.encode("utf-8")]
                template_id = template_scif['template_fk'].iloc[0]
                scene_type_dict = self.common.get_dictionary(kpi_fk=template_kpi_fk, template_fk=template_id)
                template_diageo_facings, temp_den_res = self.calculate_menu_scene_type(
                    template_scif, scene_type_dict, target, template_id)
                diageo_facings += template_diageo_facings
                den_res += temp_den_res
                result = self.get_score(template_diageo_facings, temp_den_res)
                score = 1 if result >= target else 0
                self.common.write_to_db_result(
                    fk=template_kpi_fk, numerator_id=template_id,
                    denominator_result=temp_den_res, result=score, score=result, weight=weight * 100,
                    identifier_result=scene_type_dict, target=target, numerator_result=template_diageo_facings,
                    identifier_parent=result_dict, should_enter=True)
        else:
            diageo_facings, den_res = self.calculate_menu_scene_type(relevant_scif, result_dict, target, Const.ALL)
        result = self.get_score(diageo_facings, den_res)
        score = 1 if result >= target else 0
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_facings,
            denominator_result=den_res, result=score, score=result, weight=weight * 100,
            identifier_result=result_dict, target=target, identifier_parent=total_dict, should_enter=True)
        return score * weight, 0, 0

    def calculate_menu_scene_type(self, relevant_scif, parent_dict, target, template_fk):
        """
        calculates the share of all brands and manufacturers in the menu, and
        checks if Diageo result is bigger than target
        :param weight: float
        :param target: float
        :return:
        """
        if template_fk == Const.ALL:
            menu_type = Const.MENU
            should_enter = False
        else:
            menu_type = Const.MENU_NATIONAL
            should_enter = True
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[menu_type][Const.MANUFACTURER])
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[menu_type][Const.SUB_BRAND])
        all_manufacturers = relevant_scif['manufacturer_fk'].unique().tolist()
        den_res = relevant_scif['facings'].sum()
        diageo_facings = 0
        for manufacturer_fk in all_manufacturers:
            if manufacturer_fk == 0:
                continue
            manufacturer_num = 0
            manufacturer_scif = relevant_scif[(relevant_scif['manufacturer_fk'] == manufacturer_fk) &
                                              ~(relevant_scif['sub_brand_fk'].isnull())]
            manufacturer_dict = self.common.get_dictionary(
                kpi_fk=manufacturer_kpi_fk, manufacturer_fk=manufacturer_fk, template_fk=template_fk)
            for products in manufacturer_scif[['sub_brand_fk', 'brand_fk']].drop_duplicates().itertuples():
                sub_brand_fk = products.sub_brand_fk
                brand_fk = products.brand_fk
                num_res = manufacturer_scif[(manufacturer_scif['sub_brand_fk'] == sub_brand_fk) &
                                            (manufacturer_scif['brand_fk'] == brand_fk)]['facings'].sum()
                manufacturer_num += num_res
                result = self.get_score(num_res, den_res)
                self.common.write_to_db_result(
                    fk=sub_brand_kpi_fk, numerator_id=sub_brand_fk, numerator_result=num_res, result=result,
                    denominator_result=den_res, should_enter=should_enter, identifier_parent=manufacturer_dict)
                if should_enter:
                    self.common.write_to_db_result(
                        fk=sub_brand_kpi_fk, numerator_id=sub_brand_fk, numerator_result=num_res,
                        denominator_result=den_res, result=result, denominator_id=template_fk)
            manufacturer_target = None
            if manufacturer_fk == self.manufacturer_fk:
                diageo_facings = manufacturer_num
                manufacturer_target = target
            result = self.get_score(manufacturer_num, den_res)
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer_fk, numerator_result=manufacturer_num, result=result,
                denominator_result=den_res, target=manufacturer_target, should_enter=should_enter,
                identifier_parent=parent_dict, identifier_result=manufacturer_dict)
        return diageo_facings, den_res

    # display share:

    def calculate_total_display_share(self, scene_types, kpi_name, weight, target):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :param target: for the score
        :param weight: float
        :return: total_result
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.TOTAL])
        total_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(
            Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.MANUFACTURER])
        if self.no_display_allowed:
            Log.debug("There is no display, Display Share got 100")
            score = 1
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
            return score * weight, 0, 0
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif_without_emptys[(self.scif_without_emptys['scene_fk'].isin(relevant_scenes)) &
                                                     (self.scif_without_emptys['location_type'] == 'Secondary Shelf') &
                                                     (self.scif_without_emptys['product_type'].isin(['SKU', 'Other']))]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        for product_fk in relevant_products['product_fk'].unique().tolist():
            product_result = self.calculate_display_share_of_sku(
                product_fk, relevant_products, manufacturer_kpi_fk)
            all_results = all_results.append(product_result, ignore_index=True)
        den_res = all_results[Const.PASSED].sum()
        if not den_res:
            den_res = 0
        diageo_results, diageo_result = 0, 0
        for manufacturer in all_results[Const.MANUFACTURER].unique().tolist():
            num_res = all_results[all_results[Const.MANUFACTURER] == manufacturer][Const.PASSED].sum()
            if not num_res:
                num_res = 0
            result = self.get_score(num_res, den_res)
            target_manufacturer = None
            if manufacturer == self.manufacturer_fk:
                diageo_result, diageo_results = result, num_res
                target_manufacturer = target
            result_dict = self.common.get_dictionary(manufacturer_fk=manufacturer, kpi_fk=manufacturer_kpi_fk)
            if manufacturer == 0:
                continue
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer, numerator_result=num_res,
                target=target_manufacturer, should_enter=False, denominator_result=den_res,
                result=result, identifier_parent=total_dict, identifier_result=result_dict)
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
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.SKU])
        manufacturer = self.all_products[self.all_products['product_fk'] == product_fk]['manufacturer_fk'].iloc[0]
        if self.visit_date >= datetime.strptime("2019-07-01", '%Y-%m-%d').date():
            display_function = self.calculate_passed_display_without_subst_sep_scenes
        else:
            display_function = self.calculate_passed_display_without_subst
        sum_scenes_passed = display_function(product_fk, relevant_products)
        parent_dict = self.common.get_dictionary(
            kpi_fk=manufacturer_kpi_fk, manufacturer_fk=manufacturer)
        if sum_scenes_passed == 0 or product_fk == 0:
            return None
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk,
            result=sum_scenes_passed, identifier_parent=parent_dict, should_enter=False)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: sum_scenes_passed,
                          Const.MANUFACTURER: manufacturer}
        return product_result

    # shelf facings:

    def calculate_total_shelf_facings(self, scene_types, kpi_name, weight, target):
        """
        Calculates if facings of Diageo products are more than targets (competitors products or objective target)
        :param scene_types: str
        :param kpi_name: str
        :param weight: float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.external_targets[
            self.external_targets[Const.EX_OPERATION_TYPE] == Const.SHELF_FACINGS_OP]
        if relevant_competitions.empty:
            Log.warning("No shelf_facings list for this visit_date.")
            return 0, 0, 0
        if self.attr11 == Const.NATIONAL_STORE:
            if self.store_number_1 in relevant_competitions[Const.EX_STORE_NUMBER].unique().tolist():
                relevant_competitions = relevant_competitions[
                    relevant_competitions[Const.EX_STORE_NUMBER] == self.store_number_1]
            else:
                Log.error("The store has no products for shelf_facings.")
                return 0, 0, 0
        else:
            if self.state_fk in relevant_competitions[Const.EX_STATE_FK].unique().tolist():
                relevant_competitions = relevant_competitions[relevant_competitions[Const.EX_STATE_FK] == self.state_fk]
            else:
                default_state = relevant_competitions[Const.EX_STATE_FK].iloc[0]
                Log.error("The store's state has no products, shelf_facings is calculated with state '{}'.".format(
                    default_state))
                relevant_competitions = relevant_competitions[relevant_competitions[Const.EX_STATE_FK] == default_state]
        relevant_competitions = relevant_competitions[Const.SHELF_FACINGS_COLUMNS]
        relevant_competitions = relevant_competitions.merge(self.relevant_assortment, on="product_fk")
        kpi_db_names = self.pull_kpi_fks_from_names(Const.DB_OFF_NAMES[kpi_name])
        total_results = []
        standard_types_results = {Const.SEGMENT: [], Const.NATIONAL: []} if self.attr11 == Const.OPEN else {}
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        for brand_fk in relevant_competitions['brand_fk'].unique().tolist():
            brand_competitions = relevant_competitions[relevant_competitions['brand_fk'] == brand_fk]
            standard_types_results, brand_results = self.generic_brand_calculator(
                brand_competitions, relevant_scif, standard_types_results, kpi_db_names)
            total_results += brand_results
        standard_types_results[Const.TOTAL] = total_results
        scores = self.insert_final_results_avg(standard_types_results, kpi_db_names, weight)
        return scores[Const.TOTAL], scores[Const.SEGMENT], scores[Const.NATIONAL]

    def calculate_shelf_facings_of_competition(self, competition, relevant_scif, kpi_db_names, index):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :param index: for hierarchy
        :return: passed, product_fk, standard_type
        """
        competition_kpi_fk = kpi_db_names[Const.COMPETITION]
        kpi_fk = kpi_db_names[Const.SKU]
        product_fk, brand, sub_brand = competition[[Const.EX_PRODUCT_FK, 'brand_fk', 'sub_brand_fk']]
        if product_fk == 0:
            return None, None
        standard_type = competition[Const.STANDARD_TYPE]
        result_identifier = self.common.get_dictionary(kpi_fk=competition_kpi_fk, product_fk=product_fk, index=index)
        diageo_facings, comp_facings, score, target, comp_product_fk = self.calculate_facings_competition(
            competition, relevant_scif, product_fk)
        if comp_product_fk:
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=comp_product_fk, result=comp_facings, denominator_id=self.manufacturer_fk,
                should_enter=True, identifier_parent=result_identifier, target=target)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=diageo_facings, denominator_id=self.manufacturer_fk,
            should_enter=True, identifier_parent=result_identifier, target=target)
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=competition_kpi_fk, numerator_id=product_fk, score=score * 100,
            result=diageo_facings, identifier_result=result_identifier,
            identifier_parent=sub_brand_dict, should_enter=False)
        return score, standard_type

    def calculate_facings_competition(self, competition, relevant_scif, product_fk):
        if self.does_exist(competition[Const.EX_BENCHMARK_VALUE]):
            bench_value = float(competition[Const.EX_BENCHMARK_VALUE])
        else:
            Log.warning("Product {} has no target in shelf facings".format(product_fk))
            bench_value = 0
        target = 0
        if self.does_exist(competition[Const.EX_COMPETITOR_FK]):
            comp_product_fk = competition[Const.EX_COMPETITOR_FK]
        else:
            comp_product_fk = 0
            target = bench_value
        diageo_facings, comp_facings, temp_comp_facings, temp_target = 0, 0, 0, target
        for template_name in relevant_scif['template_name'].unique():
            template_scif = relevant_scif[relevant_scif['template_name'] == template_name]
            temp_diageo_facings = template_scif[template_scif['product_fk'] == product_fk]['facings'].sum()
            if comp_product_fk:
                temp_comp_facings = template_scif[template_scif['product_fk'] == comp_product_fk]['facings'].sum()
                temp_target = bench_value * temp_comp_facings
            if temp_diageo_facings >= temp_target and temp_diageo_facings > 0:
                return temp_diageo_facings, temp_comp_facings, 1, temp_target, comp_product_fk
            if temp_diageo_facings >= diageo_facings:
                diageo_facings = temp_diageo_facings
                target = temp_target
                comp_facings = temp_comp_facings
        return diageo_facings, comp_facings, 0, target, comp_product_fk

    # shelf placement:

    def calculate_total_shelf_placement(self, scene_types, kpi_name, weight, target):
        """
        Takes list of products and their shelf groups, and calculate if the're pass the target.
        :param scene_types: str
        :param kpi_name: str
        :param weight float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.external_targets[
            self.external_targets[Const.EX_OPERATION_TYPE] == Const.SHELF_PLACEMENT_OP][Const.SHELF_PLACEMENT_COLUMNS]
        if all_products_table.empty:
            Log.warning("No shelf_placement list for this visit_date.")
            return 0, 0, 0
        all_products_table = all_products_table.merge(self.relevant_assortment, on="product_fk")
        kpi_db_names = self.pull_kpi_fks_from_names(Const.DB_OFF_NAMES[kpi_name])
        total_results = []
        standard_types_results = {Const.SEGMENT: [], Const.NATIONAL: []} if self.attr11 == Const.OPEN else {}
        relevant_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'].isin(relevant_scenes)]
        for brand_fk in all_products_table['brand_fk'].unique().tolist():
            brand_competitions = all_products_table[all_products_table['brand_fk'] == brand_fk]
            standard_types_results, brand_results = self.generic_brand_calculator(
                brand_competitions, relevant_matches, standard_types_results, kpi_db_names)
            total_results += brand_results
        standard_types_results[Const.TOTAL] = total_results
        scores = self.insert_final_results_avg(standard_types_results, kpi_db_names, weight)
        return scores[Const.TOTAL], scores[Const.SEGMENT], scores[Const.NATIONAL]

    def calculate_shelf_placement_of_sku(self, product_line, relevant_matches, kpi_db_names, index):
        """
        Gets a product (line from template) and checks if it has more facings than targets in the eye level
        :param product_line: series
        :param relevant_scenes: list
        :return:
        """
        kpi_fk = kpi_db_names[Const.SKU]
        product_fk, brand, sub_brand = product_line[[Const.EX_PRODUCT_FK, 'brand_fk', 'sub_brand_fk']]
        if product_fk == 0:
            return None, None
        standard_type = product_line[Const.STANDARD_TYPE]
        min_shelf_loc = product_line[Const.EX_MINIMUM_SHELF]
        product_fk_with_substs = [product_fk]
        product_fk_with_substs += self.all_products[self.all_products['substitution_product_fk'] == product_fk][
            'product_fk'].tolist()
        relevant_products = relevant_matches[relevant_matches['product_fk'].isin(product_fk_with_substs)]
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
                is_passed, shelf_name = self.calculate_specific_product_shelf_placement(product, shelf_groups)
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
        target = shelf_groups[shelf_groups[
                                  Const.NUMBER_GROUP] == min_shelf_loc][Const.SHELF_GROUP].iloc[0]
        target_fk, result_fk = self.get_pks_of_result(target), self.get_pks_of_result(result)
        score = passed * 100
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, score=score, result=self.get_pks_of_result(result),
            identifier_parent=sub_brand_dict, target=target_fk, should_enter=False)
        return passed, standard_type

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
        if Const.ALL in shelf_groups:
            answer_couple = 1, group_for_product
        else:
            answer_couple = 0, group_for_product
            if group_for_product in shelf_groups:
                answer_couple = 1, group_for_product
        return answer_couple

    # msrp:

    def calculate_total_msrp(self, scene_types, kpi_name, weight, target):
        """
        Compares the prices of Diageo products to the competitors' (or absolute values).
        :param scene_types: str
        :param kpi_name: str
        :param weight: float
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.external_targets[
            self.external_targets[Const.EX_OPERATION_TYPE] == Const.MSRP_OP]
        if relevant_competitions.empty:
            Log.warning("No MSRP list for this visit_date.")
            return 0, 0, 0
        if self.state_fk in relevant_competitions[Const.EX_STATE_FK].unique().tolist():
            relevant_competitions = relevant_competitions[relevant_competitions[Const.EX_STATE_FK] == self.state_fk]
        else:
            default_state = relevant_competitions[Const.EX_STATE_FK].iloc[0]
            Log.error("The store has no state, MSRP is calculated with state '{}'.".format(default_state))
            relevant_competitions = relevant_competitions[relevant_competitions[Const.EX_STATE_FK] == default_state]
        kpi_db_names = self.pull_kpi_fks_from_names(Const.DB_OFF_NAMES[kpi_name])
        all_products_table = relevant_competitions[Const.MSRP_COLUMNS]
        all_products_table = all_products_table.merge(self.all_products, on="product_fk")
        relevant_prices = self.products_with_prices[self.products_with_prices['scene_fk'].isin(relevant_scenes)]
        standard_types_results, total_results = {}, []
        for brand_fk in all_products_table['brand_fk'].unique().tolist():
            brand_competitions = all_products_table[all_products_table['brand_fk'] == brand_fk]
            standard_types_results, brand_results = self.generic_brand_calculator(
                brand_competitions, relevant_prices, standard_types_results, kpi_db_names)
            total_results += brand_results
        standard_types_results[Const.TOTAL] = total_results
        scores = self.insert_final_results_avg(standard_types_results, kpi_db_names, weight)
        return scores[Const.TOTAL], 0, 0

    def calculate_msrp_of_competition(self, competition, relevant_prices, kpi_db_names, index):
        """
        Takes competition between the price of Diageo product and Comp's product.
        The result is the distance between the objected to the observed
        :param competition: line of the template
        :param relevant_prices: df
        :param kpi_db_names: dict
        :param index: for hierarchy
        :param template: for hierarchy
        :return: 1/0
        """
        kpi_fk = kpi_db_names[Const.COMPETITION]
        comp_fk = competition[Const.EX_COMPETITOR_FK]
        product_fk, brand, sub_brand = competition[[Const.EX_PRODUCT_FK, 'brand_fk', 'sub_brand_fk']]
        min_relative, max_relative = competition[[Const.EX_RELATIVE_MIN, Const.EX_RELATIVE_MAX]]
        min_absolute, max_absolute = competition[[Const.EX_TARGET_MIN, Const.EX_TARGET_MAX]]
        result_dict = self.common.get_dictionary(kpi_fk=kpi_fk, product_fk=product_fk, index=index)
        our_price = self.calculate_sku_price(product_fk, relevant_prices, result_dict, kpi_db_names)
        if our_price is None or product_fk == 0:
            return None, None
        is_competitor = (self.does_exist(comp_fk) and
                         self.does_exist(min_relative) and self.does_exist(max_relative))
        is_absolute = self.does_exist(min_absolute) and self.does_exist(max_absolute)
        if is_competitor:
            comp_price = self.calculate_sku_price(comp_fk, relevant_prices, result_dict, kpi_db_names)
            if comp_price is None:
                comp_price = our_price + min_relative
            range_price = (round(comp_price + min_relative, 1), round(comp_price + max_relative, 1))
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
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result, should_enter=False,
            identifier_parent=sub_brand_dict, identifier_result=result_dict)
        passed = (result == 0) * 1
        return passed, None

    def calculate_sku_price(self, product_fk, prices, parent_dict, kpi_db_tables):
        """
        Takes product, checks its price and writes it in the DB.
        :param product_fk:
        :param scenes: list of fks
        :param parent_dict: identifier dictionary
        :return: price
        """
        kpi_fk = kpi_db_tables[Const.SKU]
        price = prices[prices['product_fk'] == product_fk]['price_value']
        if price.empty or product_fk == 0:
            return None
        result = round(price.iloc[0], 1)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result, identifier_parent=parent_dict, should_enter=True)
        return result

    # help functions:

    def pull_kpi_fks_from_names(self, kpi_db_names):
        """
        Takes a dict with level names, convert them to PKs and insert the SKU function
        :param kpi_db_names: dict with all the KPI's level names
        :return: converted dict
        """
        kpis_db = kpi_db_names.copy()
        for key in kpis_db.keys():
            if key != Const.KPI_NAME:
                kpis_db[key] = self.common.get_kpi_fk_by_kpi_name(kpis_db[key])
        kpis_db[Const.FUNCTION] = self.get_function_from_kpi_names(kpi_db_names)
        return kpis_db

    def get_function_from_kpi_names(self, kpi_db_names):
        """
        Gets a kpi dict and returns the match SKU function
        """
        if kpi_db_names[Const.KPI_NAME] == Const.SHELF_FACINGS:
            return self.calculate_shelf_facings_of_competition
        if kpi_db_names[Const.KPI_NAME] == Const.SHELF_PLACEMENT:
            return self.calculate_shelf_placement_of_sku
        if kpi_db_names[Const.KPI_NAME] == Const.MSRP:
            return self.calculate_msrp_of_competition
        if kpi_db_names[Const.KPI_NAME] == Const.DISPLAY_BRAND:
            return self.calculate_display_compliance_sku
        if kpi_db_names == Const.DB_OFF_NAMES[Const.POD]:
            return self.calculate_pod_off_sku
        if self.attr6 == Const.ON:
            return self.calculate_ass_on_sku
        return None

    def insert_final_results_avg(self, standard_types_results, kpi_db_names, weight):
        """
        Gets a dict of lists of all the scores, calculates the total score and inserts them to the DB
        :param standard_types_results: dict
        :param kpi_db_names: dict
        :param weight: float
        :return: dict of scores
        """
        scores = {}
        for standard_type in standard_types_results.keys():
            if kpi_db_names[Const.KPI_NAME] == Const.MSRP:
                num, den = 0, 0
                result = sum(standard_types_results[standard_type])
            else:
                num, den = sum(standard_types_results[standard_type]), len(standard_types_results[standard_type])
                result = self.get_score(num, den)
            kpi_fk = kpi_db_names[standard_type]
            total_dict = self.common.get_dictionary(kpi_fk=kpi_fk)
            score = result * weight
            scores[standard_type] = score
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num, should_enter=True,
                denominator_result=den, result=result, identifier_result=total_dict,
                identifier_parent=self.common.get_dictionary(name=standard_type), weight=weight * 100,
                score=self.round_result(score))
        if Const.SEGMENT not in scores.keys():
            scores[Const.SEGMENT] = 0
        if Const.NATIONAL not in scores.keys():
            scores[Const.NATIONAL] = 0
        return scores

    def generic_brand_calculator(self, brand_list, relevant_df, standard_types_results, kpi_db_names):
        """
        Calculates the brand with calculating the sub_brand and SKUs scores and inserts it to DB.
        :param brand_list: assortment or template list
        :param relevant_df: matches/scif/prices
        :param standard_types_results: dict with all the standard types scores
        :param kpi_db_names: dict
        :param template_fk: for the identifier parent of template level KPIs
        :return: dict of lists, list
        """
        brand_results = []
        brand = brand_list.iloc[0]['brand_fk']
        for sub_brand_fk in brand_list[~(brand_list['sub_brand_fk'].isnull())]['sub_brand_fk'].unique().tolist():
            sub_brand_list = brand_list[brand_list['sub_brand_fk'] == sub_brand_fk]
            sub_brand_results, standard_types_results = self.generic_sub_brand_calculator(
                sub_brand_list, relevant_df, standard_types_results, kpi_db_names)
            brand_results += sub_brand_results
        if kpi_db_names[Const.KPI_NAME] == Const.MSRP:
            num, den = 0, 0
            result = sum(brand_results)
        else:
            num, den = sum(brand_results), len(brand_results)
            result = self.get_score(num, den)
        brand_kpi_fk = kpi_db_names[Const.BRAND]
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
        total_kpi_fk = kpi_db_names[Const.TOTAL]
        parent_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        self.common.write_to_db_result(
            fk=brand_kpi_fk, numerator_id=brand, result=result, numerator_result=num, denominator_result=den,
            identifier_parent=parent_dict, should_enter=False, identifier_result=brand_dict)
        return standard_types_results, brand_results

    def generic_sub_brand_calculator(self, sub_brand_list, relevant_df, standard_types_results, kpi_db_names):
        """
        Calculates the sub_brand with calculating the SKUs scores and inserts it to DB.
        :param relevant_df: matches/scif/prices
        :param standard_types_results: dict with all the standard types scores
        :param kpi_db_names: dict
        :return: dict of lists, list
        """
        sub_brand_results = []
        brand, sub_brand = sub_brand_list.iloc[0][['brand_fk', 'sub_brand_fk']]
        calculate_function = kpi_db_names[Const.FUNCTION]
        for i, line in sub_brand_list.iterrows():
            passed, standard_type = calculate_function(line, relevant_df, kpi_db_names, i)
            if passed is None:
                continue
            sub_brand_results.append(passed)
            if standard_type in standard_types_results.keys():
                standard_types_results[standard_type].append(passed)
        num, den = 0, 0
        sub_brand_results_for_brand = sub_brand_results[:]
        if kpi_db_names[Const.KPI_NAME] == Const.MSRP:
            result = sum(sub_brand_results)
        elif self.attr6 == Const.ON:
            if sum(sub_brand_results) > 0:
                result = Const.DISTRIBUTED
                sub_brand_results_for_brand = [1]
            else:
                result = Const.OOS
                sub_brand_results_for_brand = [0]
            result = self.get_pks_of_result(result)
        else:
            num, den = sum(sub_brand_results), len(sub_brand_results)
            result = self.get_score(num, den)
        sub_brand_kpi_fk = kpi_db_names[Const.SUB_BRAND]
        brand_kpi_fk = kpi_db_names[Const.BRAND]
        sub_brand_dict = self.common.get_dictionary(
            kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
        self.common.write_to_db_result(
            fk=sub_brand_kpi_fk, numerator_id=sub_brand, result=result, numerator_result=num, denominator_result=den,
            identifier_parent=brand_dict, should_enter=False, identifier_result=sub_brand_dict)
        return sub_brand_results_for_brand, standard_types_results

    def calculate_passed_display_without_subst(self, product_fk, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet),
        every time it should pass the condition ONLY with the same product_fk (without the similar products).
        :param product_fk:
        :param relevant_products: relevant scif
        :return: number of scenes. int.
        """
        external_template = self.external_targets[self.external_targets["operation_type"] == Const.DISPLAY_TARGET_OP][
            Const.DISPLAY_TARGET_COLUMNS]
        template = external_template[external_template[Const.EX_ATTR2] == self.attr2]
        if template.empty:
            template = external_template[external_template[Const.EX_ATTR2] == Const.OTHER]
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
                scene_type = self.scif[self.scif['scene_fk'] == scene]['template_name'].iloc[0]
                minimum_products = template[template[Const.EX_SCENE_TYPE] == scene_type]
                if minimum_products.empty:
                    minimum_products = template[template[Const.EX_SCENE_TYPE] == Const.OTHER]
                minimum_products = minimum_products[Const.EX_MIN_FACINGS].iloc[0]
                facings = len(scene_products)
                # if the condition is failed, it will "add" 0.
                sum_scenes_passed += 1 * (facings >= minimum_products)
        return sum_scenes_passed

    def calculate_passed_display_without_subst_sep_scenes(self, product_fk, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet),
        every time it should pass the condition ONLY with the same product_fk (without the similar products).
        :param product_fk:
        :param relevant_products: relevant scif
        :return: number of scenes. int.
        """
        external_template = self.external_targets[self.external_targets["operation_type"] == Const.DISPLAY_TARGET_OP][
            Const.DISPLAY_TARGET_COLUMNS]
        template = external_template[external_template[Const.EX_ATTR2] == self.attr2]
        if template.empty:
            template = external_template[external_template[Const.EX_ATTR2] == Const.OTHER]
        sum_scenes_passed, sum_facings = 0, 0
        product_fk_with_substs = [product_fk]
        product_fk_with_substs += self.all_products[self.all_products['substitution_product_fk'] == product_fk][
            'product_fk'].tolist()
        for scene in relevant_products['scene_fk'].unique().tolist():
            for product in product_fk_with_substs:
                scene_products = self.match_product_in_scene[
                    (self.match_product_in_scene['scene_fk'] == scene) &
                    (self.match_product_in_scene['product_fk'] == product)]
                if scene_products.empty:
                    continue
                scene_type = self.scif[self.scif['scene_fk'] == scene]['template_name'].iloc[0]
                minimum_products = template[template[Const.EX_SCENE_TYPE] == scene_type]
                if minimum_products.empty:
                    minimum_products = template[template[Const.EX_SCENE_TYPE] == Const.OTHER]
                minimum_products = minimum_products[Const.EX_MIN_FACINGS].iloc[0]
                facings = len(scene_products)
                if facings >= minimum_products:
                    sum_scenes_passed += 1
                    break
        return sum_scenes_passed

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
