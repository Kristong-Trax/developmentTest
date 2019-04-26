from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np
import os
import json

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.Queries import Queries
from Projects.DIAGEOBEERUS.Data.Const import Const
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'DBC KPI Templates Final (2-19-19).xlsx')


class DIAGEOBEERUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.all_products_sku = self.all_products[(self.all_products['product_type'] == 'SKU') &
                                                  (self.all_products['is_active'] == 1)]
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'Diageo Plc']['manufacturer_fk'].iloc[0]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.match_display_in_scene = self.get_match_display()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.get_scene_info()
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.display_info = self.get_display_info()
        if self.store_info['store_type'].str.lower().values[0] == Const.ON:
            self.on_off = Const.ON
        elif self.store_info['store_type'].str.lower().values[0] == Const.OFF:
            self.on_off = Const.OFF
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif_without_empties = self.scif[~(self.scif['product_type'] == "Empty") &
                                              (self.scif['substitution_product_fk'].isnull())]
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.state = self.ps_data.get_state_name()
        self.result_values = self.ps_data.get_result_values()
        self.sub_brands = self.ps_data.get_custom_entities(1002)
        self.products_with_prices = self.ps_data.get_products_prices()
        self.templates = {}
        self.get_templates()
        self.assortment = Assortment(self.data_provider, self.output, ps_data_provider=self.ps_data)
        self.assortment_products = self.assortment.get_lvl3_relevant_ass()
        self.relevant_assortment = self.assortment_products
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)
        self.calculated_sub_brands_list = []

        self.survey = Survey(self.data_provider, self.output)
        if self.on_off == Const.ON:
            self.no_menu_allowed = self.survey.check_survey_answer(survey_text=Const.NO_MENU_ALLOWED_QUESTION,
                                                                   target_answer=Const.SURVEY_ANSWER)

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
            # elif sheet == Const.TAP_HANDLE_BEER_GLASSES_SHEET:
            #     converters = {Const.DISPLAY_BRAND: lambda x: x.replace('\u2019', "'")}
            #     self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet, keep_default_na=False,
            #                                           encoding='utf-8')
            else:
                self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet, keep_default_na=False)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_TOTAL_KPIS[self.on_off][Const.TOTAL])
        total_store_score = 0
        for i, kpi_line in self.templates[Const.SHEETS[self.on_off][0]].iterrows():
            total_weighted_score = self.calculate_set(kpi_line)
            if kpi_line[Const.KPI_GROUP]:
                total_store_score += total_weighted_score
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, result=self.round_result(total_store_score),
            denominator_id=self.store_id,
            identifier_result=self.common.get_dictionary(name=Const.TOTAL), score=self.round_result(total_store_score))

    def calculate_set(self, kpi_line):
        """
        Gets a line from the main sheet, and transports it to the match function
        :param kpi_line: series - {KPI Name, Template Group/ Scene Type, Target, Weight}
        :return: score (total)
        """
        kpi_name, scene_types = kpi_line[Const.KPI_NAME], kpi_line[Const.TEMPLATE_GROUP]
        target, weight = kpi_line[Const.TARGET], kpi_line[Const.WEIGHT]
        if not self.does_exist(weight):
            weight = 0
        if kpi_name == Const.SHELF_FACINGS_MAIN_SHELF:
            total_score = self.calculate_total_shelf_facings(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.SHELF_FACINGS_COLD_BOX:
            total_score = self.calculate_total_shelf_facings(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.NUMBER_OF_DISPLAYS:
            total_score = self.calculate_number_of_displays(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.MPA:
            total_score = self.calculate_assortment(
                scene_types, kpi_name, weight
            )
        elif kpi_name == Const.MSRP:
            total_score = self.calculate_total_msrp(scene_types, kpi_name, weight)
        elif kpi_name == Const.DISPLAY_SHARE:
            total_score = self.calculate_total_display_share(scene_types, weight, target)
        elif kpi_name == Const.MENU:  # on premise
            total_score = self.calculate_menu(scene_types, weight, target)
        elif kpi_name == Const.TAP_HANDLE:  # on premise
            total_score = self.calculate_tap_handles_and_beer_glasses(scene_types, kpi_name, weight)
        elif kpi_name == Const.BEER_GLASSES:  # on premise
            total_score = self.calculate_tap_handles_and_beer_glasses(scene_types, kpi_name, weight)
        else:
            Log.warning("Set {} is not defined".format(kpi_name))
            return 0
        return total_score

    # mpa
    def calculate_assortment(self, scene_types, kpi_name, weight):
        total_skus = 0
        passed_skus = 0
        result_dict_list = self.diageo_generator.diageo_global_assortment_function_v2()
        mpa_fk = self.common.get_kpi_fk_by_kpi_name(Const.MPA)
        mpa_sku_fk = self.common.get_kpi_fk_by_kpi_name(Const.MPA_SKU)
        total_identifier = self.common.get_dictionary(name=Const.TOTAL)
        mpa_identifier = self.common.get_dictionary(name=Const.MPA)
        for result_dict in result_dict_list:
            if result_dict['fk'] == mpa_fk:
                result_dict.update({'identifier_parent': total_identifier, 'should_enter': True,
                                    'weight': weight * 100, 'identifier_result': mpa_identifier})
            if result_dict['fk'] == mpa_sku_fk:
                total_skus = total_skus + 1
                if result_dict['result'] == 100:
                    passed_skus = passed_skus + 1
                result_dict.update({'identifier_parent': mpa_identifier, 'should_enter': True})
                if result_dict['result'] == 100:
                    result_dict.update({'result': self.get_pks_of_result(Const.ON_SHELF)})
                elif result_dict['result'] == 0:
                    result_dict.update({'result': self.get_pks_of_result(Const.NOT_ON_SHELF)})
            self.common.write_to_db_result(**result_dict)

        return (passed_skus / float(total_skus)) * weight * 100 if total_skus > 0 else 0

    # display share:
    def calculate_total_display_share(self, scene_types, weight, target):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :param target: for the score
        :param weight: float
        :return: total_result
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.TOTAL])
        total_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        # if self.no_display_allowed:
        #     Log.info("There is no display, Display Share got 100")
        #     score = 1
        #     self.common.write_to_db_result(
        #         fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target,
        #         result=score, should_enter=True, weight=weight * 100, score=score,
        #         identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
        #     return score * weight, 0, 0
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[
                                                                     Const.DISPLAY_SHARE][Const.MANUFACTURER])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif_without_empties[(self.scif_without_empties['scene_fk'].isin(relevant_scenes)) &
                                                      (self.scif_without_empties['product_type'] == 'SKU')]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        for product_fk in relevant_products['product_fk'].unique().tolist():
            product_result = self.calculate_display_share_of_sku(product_fk, relevant_products, manufacturer_kpi_fk)
            all_results = all_results.append(product_result, ignore_index=True)
        den_res = all_results[Const.PASSED].sum()
        diageo_results, diageo_result = 0, 0
        for manufacturer in all_results[Const.MANUFACTURER].unique().tolist():
            num_res = all_results[all_results[Const.MANUFACTURER] == manufacturer][Const.PASSED].sum()
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
                target=target_manufacturer,
                denominator_result=den_res, result=result, identifier_parent=total_dict,
                identifier_result=result_dict, should_enter=True)
        if den_res == 0:
            score = 0
        else:
            score = 1 if (diageo_results >= target * den_res) else 0
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_results, target=target,
            denominator_result=den_res, result=score * 100, weight=weight * 100, score=diageo_result * 100,
            identifier_result=total_dict)
        return score * weight

    def calculate_display_share_of_sku(self, product_fk, relevant_products, manufacturer_kpi_fk):
        """
        calculates a specific product if it passes the condition of display
        :param product_fk:
        :param relevant_products: DF (scif of the display)
        :param manufacturer_kpi_fk: for write_to_db
        :return: a line for the results DF
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.SKU])
        manufacturer = self.get_manufacturer(product_fk)
        sum_scenes_passed = self.calculate_passed_display_without_subst(product_fk, relevant_products)
        parent_dict = self.common.get_dictionary(kpi_fk=manufacturer_kpi_fk, manufacturer_fk=manufacturer)
        if sum_scenes_passed == 0 or product_fk == 0:
            return None
        self.common.write_to_db_result(
            fk=sku_kpi_fk, numerator_id=product_fk,
            result=sum_scenes_passed, identifier_parent=parent_dict, should_enter=True)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: sum_scenes_passed,
                          Const.MANUFACTURER: manufacturer}
        return product_result

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
                sum_scenes_passed += 1 * (facings >= minimum_products)  # if the condition is failed, it will "add" 0.
        return sum_scenes_passed

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
            all_products_table = all_products_table[((all_products_table[Const.STATE] == self.state) |
                                                     (all_products_table[Const.STATE] == 'All'))]
        else:
            all_products_table = all_products_table[((all_products_table[Const.STATE] == Const.OTHER) |
                                                     (all_products_table[Const.STATE] == 'All'))]
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in all_products_table.iterrows():
            compete_result_dict = self.calculate_msrp_of_competition(competition, relevant_scenes, i)
            all_competes = all_competes.append(compete_result_dict, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        result = self.insert_all_levels_to_db(
            all_competes, kpi_db_names, weight, write_numeric=False)  # result should be %, not count
        return result

    def calculate_msrp_of_competition(self, competition, relevant_scenes, index):
        """
        Takes competition between the price of Diageo product and Comp's product.
        The result is the distance between the objected to the observed
        :param competition: line of the template
        :param relevant_scenes:
        :param index: for hierarchy
        :return: 1/0
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.COMPETITION])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.TOTAL])
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
                return None
            else:
                comp_fk = comp_line['product_fk'].iloc[0]
                comp_price = self.calculate_sku_price(comp_fk, relevant_scenes, result_dict)
                if comp_price is None:
                    return None
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
        sub_brand_level_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.SUB_BRAND])
        identifier_parent = self.common.get_dictionary(kpi_fk=sub_brand_level_kpi_fk,
                                                       brand_fk=brand, sub_brand_fk=sub_brand)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result, should_enter=True,
            identifier_parent=identifier_parent, identifier_result=result_dict)
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
        result = price.min()  # get the lowest applicable price (suppresses the effects of errors in recognition)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, result=result,
            identifier_parent=parent_dict, should_enter=True)
        return round(result, 1)

    # shelf facings:

    def calculate_total_shelf_facings(self, scene_types, kpi_name, weight):
        """
        Calculates if facings of Diageo products are more than targets (competitors products or objective target)
        :param scene_types: str
        :param kpi_name: str
        :param weight: float
        :return:
        """
        self.calculated_sub_brands_list = []
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        relevant_competitions = relevant_competitions[relevant_competitions['KPI type'] == kpi_name]
        if self.state in relevant_competitions[Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[((relevant_competitions[Const.STATE] == self.state) |
                                                           (relevant_competitions[Const.STATE] == 'All'))]
        else:
            relevant_competitions = relevant_competitions[((relevant_competitions[Const.STATE] == Const.OTHER) |
                                                           (relevant_competitions[Const.STATE] == 'All'))]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        for i, competition in relevant_competitions.iterrows():
            result_dict = self.calculate_shelf_facings_of_competition(competition, relevant_scenes, i, kpi_db_names)
            all_results = all_results.append(result_dict, ignore_index=True)
        total_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, weighted_result=True)
        return total_result

    def calculate_shelf_facings_of_competition(self, competition, relevant_scenes, index, kpi_db_names):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :param index: for hierarchy
        :return: passed, product_fk, standard_type
        """
        kpi_type = competition['KPI type']
        weight_of_competition = competition[Const.WEIGHT]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SUB_BRAND])
        our_sub_brands = competition[Const.OUR_SUB_BRAND].split(', ')
        our_lines = self.all_products_sku[self.all_products_sku['sub_brand'].isin(our_sub_brands)]
        if our_lines.empty:
            Log.warning("The sub brands {} in shelf facings don't exist in DB".format(our_sub_brands))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        product_fk = our_fks[0]
        brand_fk, sub_brand_fk = self.get_product_details(product_fk)
        result_identifier = self.common.get_dictionary(kpi_fk=kpi_fk, brand_fk=brand_fk, sub_brand_fk=sub_brand_fk)
        if self.does_exist(competition[Const.COMP_SUB_BRAND]):
            comp_sub_brands = competition[Const.COMP_SUB_BRAND].split(', ')
            comp_lines = self.all_products_sku[self.all_products_sku['sub_brand'].isin(comp_sub_brands)]
            if comp_lines.empty:
                Log.warning("The sub brands {} in shelf facings don't exist in DB".format(comp_sub_brands))
                target = 0
            else:
                comp_facings = self.calculate_shelf_facings_of_sub_brand(comp_sub_brands, relevant_scenes,
                                                                         result_identifier, kpi_db_names=kpi_db_names)
                bench_value = competition[Const.BENCH_VALUE]
                if type(bench_value) in (unicode, str):
                    bench_value = float(bench_value.replace("%", "")) / 100
                target = comp_facings * bench_value
        elif self.does_exist(competition[Const.BENCH_VALUE]):
            target = competition[Const.BENCH_VALUE]
        else:
            Log.warning("Product {} has no target in shelf facings".format(our_sub_brands))
            target = 0
        our_facings = self.calculate_shelf_facings_of_sub_brand(our_sub_brands, relevant_scenes, result_identifier,
                                                                target=target,
                                                                diageo_product=True, kpi_db_names=kpi_db_names,
                                                                weight=weight_of_competition)
        # comparison = 1 if (our_facings >= target and our_facings > 0) else 0  # used for count instead of weight
        comparison = weight_of_competition if (our_facings >= target and our_facings > 0) else 0
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: comparison,
                          Const.BRAND: brand_fk, Const.SUB_BRAND: sub_brand_fk}
        return product_result

    def calculate_shelf_facings_of_sub_brand(self, sub_brand_names, relevant_scenes, parent_identifier, target=None,
                                             diageo_product=False, kpi_db_names=None, weight=None):
        amount_of_facings = None
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.VARIANT])
        denominator_id = self.manufacturer_fk if diageo_product else None
        for sub_brand in sub_brand_names:
            amount_of_facings = 0
            sub_brand_scif = self.scif_without_empties[self.scif_without_empties['sub_brand'] == sub_brand]
            scene_scif = sub_brand_scif[sub_brand_scif['scene_id'].isin(relevant_scenes)]
            sub_brand_facing = scene_scif['facings'].sum()
            if sub_brand_facing is None or np.isnan(sub_brand_facing):
                sub_brand_facing = 0
            amount_of_facings += sub_brand_facing
            if (sub_brand in self.calculated_sub_brands_list) and diageo_product:
                continue
            if amount_of_facings == 0:
                brand_fk = self.all_products_sku[self.all_products_sku['sub_brand'] == sub_brand]['brand_fk'].values[0]
            else:
                brand_fk = sub_brand_scif['brand_fk'].values[0]
            sub_brand_fk = self.get_sub_brand_fk(sub_brand, brand_fk)
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=sub_brand_fk, result=sub_brand_facing, denominator_id=denominator_id,
                should_enter=True, identifier_parent=parent_identifier, target=target, weight=weight)
            self.calculated_sub_brands_list.append(sub_brand)
        return amount_of_facings

    # number of displays
    def calculate_number_of_displays(self, scene_types, kpi_name, weight):
        """
        Calculates if facings of Diageo products are more than targets (competitors products or objective target)
        :param scene_types: str
        :param kpi_name: str
        :param weight: float
        :return:
        """
        self.calculated_sub_brands_list = []
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        relevant_competitions = relevant_competitions[relevant_competitions['KPI type'] == kpi_name]
        if self.state in relevant_competitions[Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[((relevant_competitions[Const.STATE] == self.state) |
                                                           (relevant_competitions[Const.STATE] == 'All'))]
        else:
            relevant_competitions = relevant_competitions[((relevant_competitions[Const.STATE] == Const.OTHER) |
                                                           (relevant_competitions[Const.STATE] == 'All'))]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        for i, competition in relevant_competitions.iterrows():
            result_dict = self.calculate_number_of_displays_of_competition(competition, relevant_scenes, i,
                                                                           kpi_db_names)
            all_results = all_results.append(result_dict, ignore_index=True)
        total_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight)
        return total_result

    def calculate_number_of_displays_of_competition(self, competition, relevant_scenes, index, kpi_db_names):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :param index: for hierarchy
        :return: passed, product_fk, standard_type
        """
        kpi_type = competition['KPI type']
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SUB_BRAND])
        our_sub_brands = competition[Const.OUR_SUB_BRAND].split(', ')
        our_lines = self.all_products_sku[self.all_products_sku['sub_brand'].isin(our_sub_brands)]
        if our_lines.empty:
            Log.warning("The sub brands {} in shelf facings don't exist in DB".format(our_sub_brands))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        product_fk = our_fks[0]
        brand_fk, sub_brand_fk = self.get_product_details(product_fk)
        result_identifier = self.common.get_dictionary(kpi_fk=kpi_fk, brand_fk=brand_fk, sub_brand_fk=sub_brand_fk)
        if self.does_exist(competition[Const.COMP_SUB_BRAND]):
            comp_sub_brands = competition[Const.COMP_SUB_BRAND].split(', ')
            comp_lines = self.all_products_sku[self.all_products_sku['sub_brand'].isin(comp_sub_brands)]
            if comp_lines.empty:
                Log.warning("The sub brands {} in shelf facings don't exist in DB".format(comp_sub_brands))
                target = 0
            else:
                comp_facings = self.calculate_displays_of_sub_brand(comp_sub_brands, relevant_scenes, result_identifier,
                                                                    kpi_db_names=kpi_db_names)
                bench_value = competition[Const.BENCH_VALUE]
                if type(bench_value) in (unicode, str):
                    bench_value = float(bench_value.replace("%", "")) / 100
                target = comp_facings * bench_value
        elif self.does_exist(competition[Const.BENCH_VALUE]):
            target = competition[Const.BENCH_VALUE]
        else:
            Log.warning("Product {} has no target in shelf facings".format(our_sub_brands))
            target = 0
        our_facings = self.calculate_displays_of_sub_brand(our_sub_brands, relevant_scenes, result_identifier,
                                                           target=target, diageo_product=True,
                                                           kpi_db_names=kpi_db_names)
        comparison = 1 if (our_facings >= target and our_facings > 0) else 0
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: comparison,
                          Const.BRAND: brand_fk, Const.SUB_BRAND: sub_brand_fk}
        return product_result

    def calculate_displays_of_sub_brand(self, sub_brand_names, relevant_scenes, parent_identifier, target=None,
                                        diageo_product=False, kpi_db_names=None):
        number_of_displays = None
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.VARIANT])
        denominator_id = self.manufacturer_fk if diageo_product else None
        for sub_brand in sub_brand_names:
            number_of_displays = 0
            sub_brand_scif = self.scif_without_empties[self.scif_without_empties['sub_brand'] == sub_brand]
            for scene in relevant_scenes:
                scene_scif = self.scif_without_empties[self.scif_without_empties['scene_id'] == scene]
                scene_type = scene_scif['template_name'].values[0]
                template = self.templates[Const.DISPLAY_TARGET_SHEET]
                facings_minimum = template[template[Const.SCENE_TYPE] == scene_type][Const.MIN_FACINGS].values[0]
                passing_products = scene_scif[(scene_scif['sub_brand'] == sub_brand) &
                                              (scene_scif['facings'] >= facings_minimum)]['product_fk'].count()
                if passing_products is None or np.isnan(passing_products):
                    passing_products = 0
                number_of_displays += passing_products
            if (sub_brand in self.calculated_sub_brands_list) and diageo_product:
                continue
            if number_of_displays == 0:
                brand_fk = self.all_products_sku[self.all_products_sku['sub_brand'] == sub_brand]['brand_fk'].values[0]
            else:
                brand_fk = sub_brand_scif['brand_fk'].values[0]
            sub_brand_fk = self.get_sub_brand_fk(sub_brand, brand_fk)
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=sub_brand_fk, result=number_of_displays, denominator_id=denominator_id,
                should_enter=True, identifier_parent=parent_identifier, target=target)
            self.calculated_sub_brands_list.append(sub_brand)
        return number_of_displays

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
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.TOTAL])
        if self.no_menu_allowed:
            Log.info("There is no menu, Menu got 100")
            score = 1
            self.common.write_to_db_result(
                fk=total_kpi_fk, numerator_id=self.manufacturer_fk, target=target,
                result=score, should_enter=True, weight=weight * 100, score=score,
                identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
            return score * weight
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.MANUFACTURER])
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.BRAND])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_empties[(self.scif_without_empties['scene_id'].isin(relevant_scenes)) &
                                                  (self.scif_without_empties['product_type'] == 'POS')]
        all_manufacturers = relevant_scif['manufacturer_fk'].unique().tolist()
        den_res = relevant_scif['facings'].sum()
        diageo_facings = 0
        for products in relevant_scif[['manufacturer_fk', 'brand_fk']].drop_duplicates().itertuples():
            brand_fk = products.brand_fk
            if not brand_fk:
                continue
            num_res = relevant_scif[relevant_scif['brand_fk'] == brand_fk]['facings'].sum()
            # result = self.get_score(num_res, den_res) # for % share of menu
            result = num_res  # for number of appearances
            identifier_parent = self.common.get_dictionary(kpi_fk=total_kpi_fk,
                                                           manufacturer_fk=products.manufacturer_fk)
            self.common.write_to_db_result(
                fk=brand_kpi_fk, numerator_id=brand_fk, numerator_result=num_res,
                denominator_id=products.manufacturer_fk, denominator_result=den_res,
                result=result, identifier_parent=identifier_parent, should_enter=True)
        for manufacturer_fk in all_manufacturers:
            num_res = relevant_scif[relevant_scif['manufacturer_fk'] == manufacturer_fk]['facings'].sum()
            manufacturer_target = None
            if manufacturer_fk == self.manufacturer_fk:
                diageo_facings = num_res
                manufacturer_target = target
            # result = self.get_score(num_res, den_res)  # for % share of menu
            result = num_res  # for number of appearances
            if manufacturer_fk == 0:
                continue
            identifier_result = self.common.get_dictionary(kpi_fk=total_kpi_fk, manufacturer_fk=manufacturer_fk)
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer_fk, numerator_result=num_res, result=result,
                denominator_result=den_res, identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk),
                identifier_result=identifier_result, should_enter=True, target=manufacturer_target)
        result = self.get_score(diageo_facings, den_res)
        score = 1 if result > target else 0
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_facings,
            denominator_result=den_res, result=score * 100, score=result * 100, weight=weight * 100,
            identifier_result=self.common.get_dictionary(kpi_fk=total_kpi_fk), target=target,
            identifier_parent=self.common.get_dictionary(name=Const.TOTAL), should_enter=True)
        return score * weight * 100

    # tap handles and beer glasses
    def calculate_tap_handles_and_beer_glasses(self, scene_types, kpi_name, weight):
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[kpi_name][Const.TOTAL])
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[kpi_name][Const.BRAND])
        display_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[kpi_name][Const.DISPLAY])

        store_format = self.store_info[Const.ATT1].iloc[0]
        template = self.templates[Const.TAP_HANDLE_BEER_GLASSES_SHEET]
        relevant_template = template[(template[Const.TYPE] == kpi_name) &
                                     (template[Const.ATT1].str.contains(store_format))]

        relevant_scenes = self.scene_info[self.scene_info['name'] == scene_types]['scene_fk'].tolist()

        mdis = self.match_display_in_scene[self.match_display_in_scene['scene_fk'].isin(relevant_scenes)]
        brands_present = mdis['name'].unique().tolist()
        total_score = 0
        for row in relevant_template[[Const.DISPLAY_BRAND, Const.WEIGHT]].drop_duplicates().itertuples():
            display_brand_fk = self.get_display_brand_fk(row.display_brand)
            if not display_brand_fk:
                continue
            result = 0
            score = 0
            if row.display_brand in brands_present:
                result = 1
                score = result * row.Weight
                total_score += score

                for matching_display in mdis[mdis['name'] == row.display_brand].itertuples():
                    display_fk = self.get_display_fk(matching_display.display_name)
                    # write displays present by brand
                    self.common.write_to_db_result(
                        fk=display_kpi_fk, numerator_id=display_fk, numerator_result=result, result=result * 100,
                        score=score * 100, denominator_result=1,
                        identifier_parent=self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=display_brand_fk),
                        weight=row.Weight * 100, should_enter=True)
            # write brand result
            self.common.write_to_db_result(
                fk=brand_kpi_fk, numerator_id=display_brand_fk, numerator_result=result, result=result * 100,
                score=score * 100, denominator_result=1,
                identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk),
                identifier_result=self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=display_brand_fk),
                weight=row.Weight * 100, should_enter=True)
        # write total result
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=total_score,
            denominator_result=len(relevant_template[Const.DISPLAY_BRAND].unique().tolist()), result=total_score * 100,
            score=total_score * weight * 100, weight=weight * 100,
            identifier_result=self.common.get_dictionary(kpi_fk=total_kpi_fk),
            identifier_parent=self.common.get_dictionary(name=Const.TOTAL), should_enter=True)
        return total_score * weight * 100

    # helpers
    def insert_all_levels_to_db(self, all_results, kpi_db_names, weight, should_enter=True, write_numeric=False,
                                sub_brand_numeric=False, weighted_result=False):
        """
        This function gets all the sku results (with details) and puts in DB all the way up (sub_brand, brand, total).
        :param all_results: DF with product_fk and its details - passed, sub_brand, brand, standard_type.
        :param kpi_db_names: name as it's shown in the main sheet of the template.
        :param weight:
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param weighted_result: assume values in all_results[Const.PASSED] are weighted
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
                                             write_numeric=write_numeric, sub_brand_numeric=sub_brand_numeric,
                                             weighted_result=weighted_result)
        all_passed_results = all_results[Const.PASSED]
        total_result = self.insert_totals_to_db(all_passed_results, kpi_db_names, Const.TOTAL, weight, total_identifier,
                                                should_enter=should_enter, write_numeric=write_numeric,
                                                weighted_result=weighted_result)

        return total_result

    def insert_brand_and_subs_to_db(self, brand_results, kpi_db_names, brand, total_identifier,
                                    write_numeric=False, sub_brand_numeric=False, weighted_result=False):
        """
        Inserting all brand and sub_brand results
        :param brand_results: DF from all_results
        :param kpi_db_names:
        :param brand: fk
        :param total_identifier: for hierarchy
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param sub_brand_numeric: write in the sub_brand if one product passed or not (like sku level)
        :param weighted_result: assume values in brand_results[Const.PASSED] are weighted
        """
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.BRAND])
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
        for sub_brand in brand_results[brand_results[Const.BRAND] == brand][Const.SUB_BRAND].unique().tolist():
            if sub_brand is None or np.isnan(sub_brand) or sub_brand == 0:
                continue
            sub_brand_results = brand_results[(brand_results[Const.BRAND] == brand) &
                                              (brand_results[Const.SUB_BRAND] == sub_brand)]
            self.insert_sub_brands_to_db(sub_brand_results, kpi_db_names, brand, sub_brand, brand_dict,
                                         write_numeric=write_numeric, sub_brand_numeric=sub_brand_numeric,
                                         weighted_result=weighted_result)
        results = brand_results[Const.PASSED]
        if write_numeric:
            num_res, den_res = 0, 0
            result = results.sum()
        elif weighted_result:
            num_res, den_res = len(results.nonzero()[0]), results.count()
            result = results.sum()
        else:
            num_res, den_res = results.sum(), results.count()
            result = self.get_score(num_res, den_res)
        self.common.write_to_db_result(
            fk=brand_kpi_fk, numerator_id=brand, numerator_result=num_res,
            denominator_result=den_res, result=result * 100, should_enter=True,
            identifier_parent=total_identifier, identifier_result=brand_dict)

    def insert_sub_brands_to_db(self, sub_brand_results, kpi_db_names, brand, sub_brand, brand_identifier,
                                write_numeric=False, sub_brand_numeric=False, weighted_result=False):
        """
        inserting sub_brand results into DB
        :param sub_brand_results: DF from all_products
        :param kpi_db_names:
        :param brand: fk
        :param sub_brand: fk
        :param brand_identifier: for hierarchy
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param sub_brand_numeric: write in the sub_brand if one product passed or not (like sku level)
        :param weighted_result: assume values in sub_brand_results[Const.PASSED] are weighted
        """
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SUB_BRAND])
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        results = sub_brand_results[Const.PASSED]
        num_res, den_res = 0, 0
        if sub_brand_numeric:
            result = Const.DISTRIBUTED if results.sum() > 0 else Const.OOS
            result = self.get_pks_of_result(result)
        elif write_numeric:
            result = results.sum()
        elif weighted_result:
            num_res, den_res = len(results.nonzero()[0]), results.count()
            result = results.sum()
        else:
            num_res, den_res = results.sum(), results.count()
            result = self.get_score(num_res, den_res)
        self.common.write_to_db_result(
            fk=sub_brand_kpi_fk, numerator_id=sub_brand, numerator_result=num_res,
            denominator_result=den_res, result=result * 100, should_enter=True,
            identifier_parent=brand_identifier, identifier_result=sub_brand_dict)

    def insert_totals_to_db(self, all_passed_results, kpi_db_names, total_kind, weight, identifier_result=None,
                            should_enter=True, write_numeric=False, weighted_result=False):
        """
        inserting all total level (includes segment and national) into DB
        :param all_passed_results: 'passed' column from all_results
        :param kpi_db_names:
        :param weight: float
        :param total_kind: TOTAL/SEGMENT/NATIONAL
        :param identifier_result: optional, if has children
        :param should_enter: if the total should enter the hierarchy table
        :param write_numeric: for MSRP - writing only the amount of passed in the result, without percentage
        :param weighted_result: assume values in all_passed_results are weighted
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
        elif weighted_result:
            num_res, den_res = len(all_passed_results.nonzero()[0]), all_passed_results.count()
            result = all_passed_results.sum()
            score = result * weight * 100
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_res, should_enter=should_enter,
                denominator_result=den_res, result=result * 100, identifier_result=identifier_result,
                identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight * 100,
                score=self.round_result(score))
        else:
            num_res, den_res = all_passed_results.sum(), all_passed_results.count()
            result = self.get_score(num_res, den_res)
            score = result * weight * 100
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_res, should_enter=should_enter,
                denominator_result=den_res, result=result * 100, identifier_result=identifier_result,
                identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight * 100,
                score=self.round_result(score))
        return score

    def get_pks_of_result(self, result):
        """
        converts string result to its pk (in static.kpi_result_value)
        :param result: str
        :return: int
        """
        pk = self.result_values[self.result_values['value'] == result]['pk'].iloc[0]
        return pk

    def get_relevant_scenes(self, scene_types):
        """
        :param scene_types: cell in the template
        :return: list of all the scenes contains the cell
        """
        if self.does_exist(scene_types):
            scene_type_list = scene_types.split(", ")
            return self.scif_without_empties[self.scif_without_empties["template_name"].isin(scene_type_list)][
                "scene_id"].unique().tolist()
        return self.scif_without_empties["scene_id"].unique().tolist()

    def get_scene_type(self, scene_fk):
        return self.scif[self.scif['scene_fk'] == scene_fk]['template_name'].iloc[0]

    def get_product_details(self, product_fk):
        """
        :param product_fk:
        :return: its details for assortment (brand, sub_brand)
        """
        if self.all_products[self.all_products['product_fk'] == product_fk].empty:
            return None, None
        brand = self.all_products[self.all_products['product_fk'] == product_fk]['brand_fk'].iloc[0]
        sub_brand = self.all_products[self.all_products['product_fk'] == product_fk]['sub_brand'].iloc[0]
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

    def get_score(self, num, den):
        """
        :param num: number
        :param den: number
        :return: the percent of the num/den
        """
        if den == 0:
            return 0
        return self.round_result(float(num) / den)

    def get_manufacturer(self, product_fk):
        """
        :param product_fk:
        :return: manufacturer_fk
        """
        return self.all_products[self.all_products['product_fk'] == product_fk]['manufacturer_fk'].iloc[0]

    def get_brand_fk(self, brand_name):
        try:
            return self.all_products[self.all_products['brand_name'] == str(brand_name)]['brand_fk'].iloc[0]
        except IndexError:
            Log.error('No brand_fk exists for {}'.format(brand_name))
            return None

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = Queries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_display_info(self):
        query = """select d.pk, d.display_type_fk, d.display_brand_fk, d.display_name, b.name as display_brand 
                    from static.display d left join static.display_brand b on d.display_brand_fk = b.pk;"""
        display_info = pd.read_sql_query(query, self.rds_conn.db)
        return display_info

    def get_display_fk(self, display_name):
        return self.display_info[self.display_info['display_name'] == display_name]['pk'].iloc[0]

    def get_display_brand_fk(self, display_brand_name):
        return self.display_info[self.display_info['display_brand'] == display_brand_name]['display_brand_fk'].iloc[0]

    def get_scene_info(self):
        base = self.data_provider[Data.SCENES_INFO]
        template_table = pd.read_sql_query('select pk, name from static.template', self.rds_conn.db)
        return pd.merge(base, template_table, how='left', left_on='template_fk', right_on='pk')

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
    def round_result(result):
        return round(result, 3)

    def commit_results_data(self):
        self.common.commit_results_data()
