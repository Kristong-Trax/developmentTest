
import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.DIAGEOUS_SAND.Utils.Const import Const
from KPIUtils_v2.DB.CommonNew import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class DIAGEOUSToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.assortment = Assortment(self.data_provider, self.output)
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.state = self.get_state()
        self.products_with_prices = self.get_products_prices()
        self.kpi_static_data = self.common.kpi_static_data
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'DIAGEO']['manufacturer_fk'].iloc[0]
        self.templates = {}
        self.get_templates()
        self.kpi_results_queries = []
        self.scenes = self.scif['scene_fk'].unique().tolist()
        self.scenes_with_shelves = {}
        for scene in self.scenes:
            shelf = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]['shelf_number'].max()
            self.scenes_with_shelves[scene] = shelf
        self.converted_groups = self.convert_groups_from_template()
        self.hierarchy = {Const.TOTAL: {Const.KPI: Const.TOTAL},
                          Const.SEGMENT: {Const.KPI: Const.SEGMENT},
                          Const.NATIONAL: {Const.KPI: Const.NATIONAL}}
        self.assortment_products = self.assortment.get_lvl3_relevant_ass()

# main functions:

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        t_store_score, s_store_score, n_store_score = 0, 0, 0
        t_score_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SCORE_TOTAL)
        s_score_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SCORE_SEGMENT)
        n_score_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SCORE_NATIONAL)
        for i, kpi_line in self.templates[Const.KPIS_SHEET].iterrows():
            t_weighted_score, s_weighted_score, n_weighted_score = self.calculate_set(kpi_line)
            if kpi_line[Const.KPI_GROUP]:
                t_store_score += t_weighted_score
                s_store_score += s_weighted_score
                n_store_score += n_weighted_score
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_score_fk, numerator_id=self.manufacturer_fk, result=t_store_score, numerator_result=t_store_score,
            result_dict=self.hierarchy[Const.TOTAL])
        self.common.write_to_db_result_new_tables_with_tree(
            fk=s_score_fk, numerator_id=self.manufacturer_fk, result=s_store_score, numerator_result=s_store_score,
            result_dict=self.hierarchy[Const.SEGMENT])
        self.common.write_to_db_result_new_tables_with_tree(
            fk=n_score_fk, numerator_id=self.manufacturer_fk, result=n_store_score, numerator_result=n_store_score,
            result_dict=self.hierarchy[Const.NATIONAL])

    def calculate_set(self, kpi_line):
        """
        Gets line from the main sheet, and transports it to the match function
        :param kpi_line: series - {KPI Name, Template Group/ Scene Type, Target, Weight}
        :return: 3 scores (total, segment, national)
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        scene_types = kpi_line[Const.TEMPLATE_GROUP]
        target = kpi_line[Const.TARGET]
        weight = kpi_line[Const.WEIGHT]
        if kpi_name == Const.SHELF_PLACEMENT:
            t_score, s_score, n_score = self.calculate_total_shelf_placement(scene_types)
        elif kpi_name == Const.SHELF_FACINGS:
            t_score, s_score, n_score = self.calculate_total_shelf_facings(scene_types)
        elif kpi_name == Const.MSRP:
            t_score, s_score, n_score = self.calculate_total_msrp(scene_types)
        elif kpi_name == Const.DISPLAY_SHARE:
            t_score, s_score, n_score = self.calculate_total_display_share(scene_types)
        elif kpi_name in (Const.POD, Const.DISPLAY_BRAND):
            t_score, s_score, n_score = self.calculate_assortment(kpi_name, scene_types)
        elif kpi_name == Const.STORE_SCORE:
            return 0, 0, 0
        else:
            Log.warning("Set {} isn't defined in the code yet".format(kpi_name))
            return 0, 0, 0
        if self.is_exist(target):
            t_score, s_score, n_score = 100 * (t_score >= target), 100 * (s_score >= target), 100 * (n_score >= target)
        if not self.is_exist(weight):
            weight = 1
        return t_score * weight, s_score * weight, n_score * weight

# assortments:

    def calculate_assortment(self, kpi_name, scene_types):
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif[self.scif['scene_id'].isin(relevant_scenes)]
        if kpi_name == Const.POD:
            brand_level, sub_brand_level = Const.DB_POD_BRAND, Const.DB_POD_SUB_BRAND
            segment_level, national_level = Const.DB_POD_SEGMENT, Const.DB_POD_NATIONAL
            calculate_function, total_level = self.calculate_pod_sku, Const.DB_POD_TOTAL
        elif kpi_name == Const.DISPLAY_BRAND:
            brand_level, sub_brand_level = Const.DB_COMPLIANCE_BRAND, Const.DB_COMPLIANCE_SUB_BRAND
            segment_level, national_level = Const.DB_COMPLIANCE_SEGMENT, Const.DB_COMPLIANCE_NATIONAL
            calculate_function, total_level = self.calculate_display_compliance_sku, Const.DB_COMPLIANCE_TOTAL
            relevant_scif = relevant_scif[relevant_scif['location_type'] == 'Secondary Shelf']
        else:
            Log.error("Assortment '{}' is not defined in the code".format(kpi_name))
            return 0, 0, 0
        total_level_fk = self.common.get_kpi_fk_by_kpi_name(total_level)
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_level_fk]
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in relevant_assortment.iterrows():
            result_dict = calculate_function(competition, relevant_scif)
            all_competes = all_competes.append(result_dict, ignore_index=True)
        t_result, s_result, n_result = self.enter_sub_brands_and_brands_to_db(
            all_competes, brand_level, sub_brand_level,
            total_kpi=total_level, segment_kpi=segment_level, national_kpi=national_level)
        return t_result, s_result, n_result

    def calculate_pod_sku(self, competition, relevant_scif):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_POD_SKU)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_POD_TOTAL)
        product_fk = competition['product_fk']
        facings = relevant_scif[relevant_scif['product_fk'] == product_fk]['facings'].sum()
        result = 1 * (facings > 0)
        brand, sub_brand, standard_type = self.get_product_details(product_fk)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, numerator_id=product_fk, numerator_result=facings,
            result=result, parent_dict={Const.KPI: total_kpi_fk})
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: result,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_display_compliance_sku(self, competition, relevant_scif):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_COMPLIANCE_SKU)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_COMPLIANCE_TOTAL)
        product_fk = competition['product_fk']
        facings = self.calculate_passed_display(product_fk, relevant_scif)
        result = 1 * (facings > 0)
        brand, sub_brand, standard_type = self.get_product_details(product_fk)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, numerator_id=product_fk, numerator_result=facings,
            result=result, parent_dict={Const.KPI: total_kpi_fk})
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: result,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

# display share:

    def calculate_total_display_share(self, scene_types):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :return: total_result
        """
        t_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_DISPLAY_SHARE_TOTAL)
        m_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_DISPLAY_SHARE_MANUFACTURER)
        total_dict = {Const.KPI: t_fk}
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif[(self.scif['scene_fk'].isin(relevant_scenes)) &
                                      (self.scif['location_type'] == 'Secondary Shelf')]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        for product_fk in relevant_products['product_fk'].unique().tolist():
            product_result = self.calculate_display_share_of_sku(product_fk, relevant_products, m_fk)
            all_results = all_results.append(product_result, ignore_index=True)
        den_res = all_results[Const.PASSED].sum()
        for manufacturer in all_results[Const.MANUFACTURER].unique().tolist():
            num_res = all_results[all_results[Const.MANUFACTURER] == manufacturer][Const.PASSED].sum()
            if manufacturer == self.manufacturer_fk:
                result = Const.TARGET_FOR_DISPLAY_SHARE
            else:
                result = self.get_score(num_res, den_res)
            result_dict = {Const.KPI: m_fk, Const.MANUFACTURER: manufacturer}
            self.common.write_to_db_result_new_tables_with_tree(
                fk=m_fk, numerator_id=manufacturer, numerator_result=num_res,
                denominator_result=den_res, result=result, parent_dict=total_dict, result_dict=result_dict)
        diageo_results = all_results[all_results[Const.MANUFACTURER] == self.manufacturer_fk][Const.PASSED].sum()
        result = 100 * (diageo_results >= Const.TARGET_FOR_DISPLAY_SHARE * den_res)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_results,
            denominator_result=den_res, result=result,
            result_dict=total_dict, parent_dict=self.hierarchy[Const.TOTAL], necessary=True)
        return result, 0, 0

    def calculate_display_share_of_sku(self, product_fk, relevant_products, manufacturer_kpi_fk):
        """

        :param product_fk:
        :param relevant_products: DF (scif of the display)
        :param manufacturer_kpi_fk: for write_to_db
        :return: DF of all the results of the manufacturer's products
        """
        s_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_DISPLAY_SHARE_SKU)
        manufacturer = self.get_manufacturer(product_fk)
        sum_scenes_passed = self.calculate_passed_display(product_fk, relevant_products)
        parent_dict = {Const.KPI: manufacturer_kpi_fk, Const.MANUFACTURER: manufacturer}
        self.common.write_to_db_result_new_tables_with_tree(
            fk=s_fk, numerator_id=product_fk, numerator_result=None,
            result=sum_scenes_passed, parent_dict=parent_dict)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: sum_scenes_passed,
                          Const.MANUFACTURER: manufacturer}
        return product_result

# shelf facings:

    def calculate_total_shelf_facings(self, scene_types):
        """
        Calculates if facings of Diageo products are more than targets (competitors products or objective target)
        :param scene_types:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        if self.state in relevant_competitions[Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == self.state]
        else:
            Log.warning("There are no shelf facing competitions for state {}".format(self.state))
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in relevant_competitions.iterrows():
            result_dict = self.calculate_shelf_facings_of_competition(competition, relevant_scenes)
            all_competes = all_competes.append(result_dict, ignore_index=True)
        t_result, s_result, n_result = self.enter_sub_brands_and_brands_to_db(
            all_competes, Const.DB_SHELF_FACING_BRAND, Const.DB_SHELF_FACING_SUB_BRAND,
            total_kpi=Const.DB_SHELF_FACING_TOTAL, segment_kpi=Const.DB_SHELF_FACING_SEGMENT,
            national_kpi=Const.DB_SHELF_FACING_NATIONAL)
        return t_result, s_result, n_result

    def calculate_shelf_facings_of_competition(self, competition, relevant_scenes):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :return: passed, product_fk, standard_type
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SHELF_FACING_SKU_COMPETITION)
        our_eans = competition[Const.OUR_EAN_CODE].split(', ')
        our_lines = self.all_products[self.all_products['product_ean_code'].isin(our_eans)]
        if our_lines.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_eans))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        product_fk = our_fks[0]
        result_dict = {Const.KPI: kpi_fk, Const.PRODUCT_FK: product_fk}
        our_facings = self.calculate_shelf_facings_of_sku(our_fks, relevant_scenes, result_dict)
        all_facings = our_facings
        if self.is_exist(competition[Const.COMP_EAN_CODE]):
            comp_eans = competition[Const.COMP_EAN_CODE].split(', ')
            comp_lines = self.all_products[self.all_products['product_ean_code'].isin(comp_eans)]
            if comp_lines.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(comp_eans))
                return None
            comp_fks = comp_lines['product_fk'].unique().tolist()
            comp_facings = self.calculate_shelf_facings_of_sku(comp_fks, relevant_scenes, result_dict)
            all_facings += comp_facings
            target = comp_facings * competition[Const.BENCH_VALUE]
        else:
            target = competition[Const.BENCH_VALUE]
        comparison = 100 * (our_facings >= target)
        result = self.get_score(our_facings, all_facings)
        brand, sub_brand, standard_type = self.get_product_details(product_fk)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SHELF_FACING_TOTAL)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, numerator_id=product_fk, numerator_result=our_facings, denominator_result=all_facings,
            result=result, score=comparison, result_dict=result_dict, parent_dict={Const.KPI: total_kpi_fk})
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: comparison / 100,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_shelf_facings_of_sku(self, product_fks, relevant_scenes, parent_dict):
        """
        Gets product(s) and counting its facings.
        :param product_fks: list of FKs
        :param relevant_scenes: list
        :param parent_dict: for write_to_db
        :return: amount of facings
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SHELF_FACING_SKU)
        amount_of_facings = 0
        for product_fk in product_fks:
            product_facing = self.scif[
                (self.scif['product_fk'] == product_fk) &
                (self.scif['scene_id'].isin(relevant_scenes))]['facings'].sum()
            amount_of_facings += product_facing
            self.common.write_to_db_result_new_tables_with_tree(
                fk=kpi_fk, numerator_id=product_fk, numerator_result=product_facing, result=product_facing,
                necessary=True, parent_dict=parent_dict)
        return amount_of_facings

# shelf placement:

    def convert_groups_from_template(self):
        """
        Create dict that contains every number in the template and its shelves
        :return: dict of lists
        """
        shelf_groups = self.templates[Const.SHELF_GROUPS_SHEET]
        shelves_groups = {}
        for i, group in shelf_groups.iterrows():
            shelves_groups[group[Const.NUMBER_GROUP]] = group[Const.SHELF_GROUP].split(', ')
        return shelves_groups

    def calculate_total_shelf_placement(self, scene_types):
        """
        Takes list of products and their shelf groups, and calculate if the're pass the target.
        :param scene_types:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[Const.SHELF_PLACMENTS_SHEET]
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in all_products_table.iterrows():
            result_dict = self.calculate_shelf_placement_of_sku(competition, relevant_scenes)
            all_competes = all_competes.append(result_dict, ignore_index=True)
        t_result, s_result, n_result = self.enter_sub_brands_and_brands_to_db(
            all_competes, Const.DB_SHELF_PLACEMENT_BRAND, Const.DB_SHELF_PLACEMENT_SUB_BRAND,
            total_kpi=Const.DB_SHELF_PLACEMENT_TOTAL, segment_kpi=Const.DB_SHELF_PLACEMENT_SEGMENT,
            national_kpi=Const.DB_SHELF_PLACEMENT_NATIONAL)
        return t_result, s_result, n_result

    def calculate_shelf_placement_of_sku(self, product_line, relevant_scenes):
        """
        Gets product (line from template) and checks if it has more facings than targets in the eye level
        :param product_line: series
        :param relevant_scenes: list
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SHELF_PLACEMENT_SKU)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_SHELF_PLACEMENT_TOTAL)
        product_fk = self.get_product_fk(product_line[Const.PRODUCT_EAN_CODE])
        min_shelf_loc = product_line[Const.MIN_SHELF_LOCATION]
        shelf_groups = self.converted_groups[min_shelf_loc]
        min_max_shleves = self.templates[Const.MINIMUM_SHELF_SHEET]
        if "ALL" in shelf_groups:
            relevant_groups = min_max_shleves
        else:
            relevant_groups = min_max_shleves[min_max_shleves[Const.SHELF_NAME].isin(shelf_groups)]
        relevant_products = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'] == product_fk) &
            (self.match_product_in_scene['scene_fk'].isin(relevant_scenes))]
        if relevant_products.empty:
            return None
        eye_level_product_facings = 0
        for i, product in relevant_products.iterrows():
            eye_level_product_facings += self.calculate_specific_product_eye_level(product, relevant_groups)
        all_facings = len(relevant_products)
        result = 100 * (eye_level_product_facings > all_facings * Const.PERCENT_FOR_EYE_LEVEL)
        brand, sub_brand, standard_type = self.get_product_details(product_fk)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, numerator_id=product_fk, numerator_result=eye_level_product_facings,
            denominator_result=all_facings, result=result, parent_dict={Const.KPI: total_kpi_fk})
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: result / 100,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_specific_product_eye_level(self, match_product_line, relevant_groups):
        """
        Takes one facing of product and checks if it passed its scene definition.
        :param match_product_line: series, line of match_product_in_scene.
        :param relevant_groups: filtered template of groups
        :return: 1/0
        """
        scene = match_product_line['scene_fk']
        relevant_groups_for_scene = relevant_groups[
            (relevant_groups[Const.NUM_SHLEVES_MIN] <= self.scenes_with_shelves[scene]) &
            (relevant_groups[Const.NUM_SHLEVES_MAX] >= self.scenes_with_shelves[scene])]
        if relevant_groups_for_scene.empty:
            Log.info("Scene {} has {} shelves, not in the template".format(scene, self.scenes_with_shelves[scene]))
            return 0
        relevant_shelves = map(int, str(relevant_groups_for_scene[Const.SHELVES_FROM_BOTTOM].iloc[0]).split(', '))
        shelf_from_bottom = match_product_line['shelf_number_from_bottom']
        if shelf_from_bottom in relevant_shelves:
            return 1
        return 0

# msrp:

    def calculate_total_msrp(self, scene_types):
        """
        Compares the prices of Diageo products to the competitors' (or absolute values).
        :param scene_types:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[Const.PRICING_SHEET]
        if self.state in all_products_table[Const.STATE].unique().tolist():
            all_products_table = all_products_table[all_products_table[Const.STATE] == self.state]
        else:
            Log.warning("There are no pricing competitions for state {}".format(self.state))
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in all_products_table.iterrows():
            result_dict = self.calculate_msrp_of_competition(competition, relevant_scenes)
            all_competes = all_competes.append(result_dict, ignore_index=True)
        result, s_result, n_result = self.enter_sub_brands_and_brands_to_db(
            all_competes, Const.DB_MSRP_BRAND, Const.DB_MSRP_SUB_BRAND, total_kpi=Const.DB_MSRP_TOTAL)
        return result, 0, 0

    def calculate_msrp_of_competition(self, competition, relevant_scenes):
        """
        Takes competition between the price of Diageo product and Comp's product.
        The result is the distance between the objected to the observed
        :param competition: line of the template
        :param relevant_scenes:
        :return: 1/0
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_MSRP_COMPETITION)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_MSRP_TOTAL)
        our_ean = competition[Const.OUR_EAN_CODE]
        our_line = self.all_products[self.all_products['product_ean_code'] == our_ean]
        if our_line.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_ean))
            return None
        product_fk = our_line['product_fk'].iloc[0]
        result_dict = {Const.KPI: kpi_fk, Const.PRODUCT_FK: product_fk}
        our_price = self.calculate_sku_price(product_fk, relevant_scenes, result_dict)
        if our_price is None:
            return None
        if self.is_exist(competition[Const.COMP_EAN_CODE]):
            comp_ean = competition[Const.COMP_EAN_CODE]
            comp_line = self.all_products[self.all_products['product_ean_code'] == comp_ean]
            if comp_line.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(our_ean))
                return None
            comp_fk = comp_line['product_fk'].iloc[0]
            comp_price = self.calculate_sku_price(comp_fk, relevant_scenes, result_dict)
            if comp_price is None:
                return None
            range_price = (comp_price + competition[Const.MIN_MSRP_RELATIVE],
                           comp_price + competition[Const.MAX_MSRP_RELATIVE])
        else:
            range_price = (competition[Const.MIN_MSRP_ABSOLUTE], competition[Const.MAX_MSRP_ABSOLUTE])
        if our_price < range_price[0]:
            result = range_price[0] - our_price
        elif our_price > range_price[1]:
            result = our_price - range_price[1]
        else:
            result = 0
        brand, sub_brand, standard_type = self.get_product_details(product_fk)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, numerator_id=product_fk, numerator_result=(result == 0) * 100, result=result,
            parent_dict={Const.KPI: total_kpi_fk}, result_dict=result_dict)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: (result == 0) * 1,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        return product_result

    def calculate_sku_price(self, product_fk, scenes, parent_dict):
        """
        Takes product, checks its price and writes it in the DB.
        :param product_fk:
        :param scenes:
        :param parent_dict:
        :return: price
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_MSRP_PRICE)
        price = self.products_with_prices[(self.products_with_prices['product_fk'] == product_fk) &
                                          (self.products_with_prices['scene_fk'].isin(scenes))]['price_value']
        if price.empty:
            Log.warning("Product {} has no price in scenes {}".format(product_fk, scenes))
            return None
        result = round(price.iloc[0], 2)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, numerator_id=product_fk, numerator_result=result, result=result,
            parent_dict=parent_dict, necessary=True)
        return result

# help functions:

    def enter_sub_brands_and_brands_to_db(self, all_competes, brand_kpi, sub_brand_kpi,
                                          total_kpi, segment_kpi=None, national_kpi=None):
        """

        :param all_competes:
        :param brand_kpi:
        :param sub_brand_kpi:
        :param total_kpi:
        :param segment_kpi:
        :param national_kpi:
        :return:
        """
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(brand_kpi)
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(sub_brand_kpi)
        t_kpi_fk = self.common.get_kpi_fk_by_kpi_name(total_kpi)
        total_dict = {Const.KPI: t_kpi_fk}
        for brand in all_competes[Const.BRAND].unique().tolist():
            brand_dict = {Const.KPI: brand_kpi_fk, Const.BRAND: brand}
            for sub_brand in all_competes[all_competes[Const.BRAND] == brand][Const.SUB_BRAND].unique().tolist():
                sub_brand_dict = {Const.KPI: sub_brand_kpi_fk, Const.BRAND: brand, Const.SUB_BRAND: sub_brand}
                results = all_competes[(all_competes[Const.BRAND] == brand) &
                                       (all_competes[Const.SUB_BRAND] == sub_brand)][Const.PASSED]
                num_res, den_res = results.sum(), results.count()
                self.common.write_to_db_result_new_tables_with_tree(
                    fk=brand_kpi_fk, numerator_id=sub_brand, numerator_result=num_res,
                    denominator_result=den_res, result=self.get_score(num_res, den_res),
                    parent_dict=brand_dict, result_dict=sub_brand_dict)
            results = all_competes[all_competes[Const.BRAND] == brand][Const.PASSED]
            num_res, den_res = results.sum(), results.count()
            self.common.write_to_db_result_new_tables_with_tree(
                fk=sub_brand_kpi_fk, numerator_id=brand, numerator_result=num_res,
                denominator_result=den_res, result=self.get_score(num_res, den_res),
                parent_dict=total_dict, result_dict=brand_dict)
        national_competes = all_competes[all_competes[Const.STANDARD_TYPE] == Const.NATIONAL][Const.PASSED]
        segment_competes = all_competes[all_competes[Const.STANDARD_TYPE] == Const.SEGMENT][Const.PASSED]
        all_competes = all_competes[Const.PASSED]
        t_num_result, t_den_result = all_competes.sum(), all_competes.count()
        t_result = self.get_score(t_num_result, t_den_result)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=t_num_result,
            denominator_result=t_den_result, result=t_result,
            result_dict=total_dict, parent_dict=self.hierarchy[Const.TOTAL], necessary=True)
        s_result, n_result = 0, 0
        if segment_kpi and national_kpi:
            s_kpi_fk = self.common.get_kpi_fk_by_kpi_name(segment_kpi)
            n_kpi_fk = self.common.get_kpi_fk_by_kpi_name(national_kpi)
            s_num_result, s_den_result = segment_competes.sum(), segment_competes.count()
            n_num_result, n_den_result = national_competes.sum(), national_competes.count()
            s_result = self.get_score(s_num_result, s_den_result)
            n_result = self.get_score(n_num_result, n_den_result)
            self.common.write_to_db_result_new_tables_with_tree(
                fk=s_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=s_num_result,
                denominator_result=s_den_result, result=s_result,
                parent_dict=self.hierarchy[Const.SEGMENT])
            self.common.write_to_db_result_new_tables_with_tree(
                fk=n_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=n_num_result,
                denominator_result=n_den_result, result=n_result,
                parent_dict=self.hierarchy[Const.NATIONAL])
        return t_result, s_result, n_result

    def calculate_passed_display(self, sku, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet).
        :param sku: fk
        :param relevant_products:
        :return: number of scenes.
        """
        template = self.templates[Const.DISPLAY_TARGET_SHEET]
        sum_scenes_passed, sum_facings = 0, 0
        for scene in relevant_products['scene_fk'].unique().tolist():
            scene_product = relevant_products[(relevant_products['scene_fk'] == scene) &
                                              (relevant_products['product_fk'] == sku)]
            if scene_product.empty:
                continue
            scene_type = scene_product['template_name'].iloc[0]
            minimum_products = template[template[Const.SCENE_TYPE] == scene_type]
            if minimum_products.empty:
                Log.warning("scene type {} doesn't exist in the template".format(scene_type))
                continue
            minimum_products = minimum_products[Const.MIN_FACINGS].iloc[0]
            facings = scene_product['facings'].iloc[0]
            sum_scenes_passed += 1 * (facings >= minimum_products)
        return sum_scenes_passed

    def get_templates(self):
        """
        Reads the template (and makes the EANs Strings)
        """
        for sheet in Const.SHEETS:
            if sheet in ([Const.SHELF_FACING_SHEET, Const.PRICING_SHEET]):
                converters = {Const.OUR_EAN_CODE: lambda x: str(x).replace(".0", ""),
                              Const.COMP_EAN_CODE: lambda x: str(x).replace(".0", "")}
                self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                      converters=converters, keep_default_na=False)
            elif sheet == Const.SHELF_PLACMENTS_SHEET:
                converters = {Const.PRODUCT_EAN_CODE: lambda x: str(x).replace(".0", "")}
                self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                      converters=converters, keep_default_na=False)
            else:
                self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet, skiprows=2, keep_default_na=False)

    def get_standard_type(self, product_fk):
        """
        :param product_fk:
        :return: standard_type of the given product
        """
        if product_fk % 3 == 0:
            return "segment"
        else:
            return "national"

    def get_relevant_scenes(self, scene_types):
        """
        :param scene_types: cell of template
        :return: list of all the scenes contains the cell
        """
        if self.is_exist(scene_types):
            scene_type_list = scene_types.split(", ")
            # scene_type_list += list(map(lambda x: x + " - OLD", scene_type_list))
            return self.scif[self.scif["template_name"].isin(scene_type_list)][
                "scene_id"].unique().tolist()
        return self.scif["scene_id"].unique().tolist()

    def get_state(self):
        query = "select name from static.state where pk = {};".format(self.data_provider[Data.STORE_INFO]['state_fk'][0])
        state = pd.read_sql_query(query, self.rds_conn.db)
        return state.values[0][0]

    def get_product_details(self, product_fk):
        """
        :param product_fk:
        :return: its details for assortment
        """
        brand = self.all_products[self.all_products['product_fk'] == product_fk]['brand_fk'].iloc[0]
        sub_brand = self.all_products[self.all_products['product_fk'] == product_fk]['sub_brand'].iloc[0]
        standard_type = self.get_standard_type(product_fk)
        return brand, sub_brand, standard_type

    def get_manufacturer(self, product_fk):
        return self.all_products[self.all_products['product_fk'] == product_fk]['manufacturer_fk'].iloc[0]

    def get_product_fk(self, product_ean_code):
        return self.all_products[self.all_products['product_ean_code'] == product_ean_code]['product_fk'].iloc[0]

    def get_products_prices(self):
        query = """SELECT p.scene_fk as scene_fk, mpip.product_fk as product_fk,
                    mpn.match_product_in_probe_fk as probe_match_fk,
                    mpn.value as price_value, mpas.state as number_attribute_state
            FROM
                    probedata.match_product_in_probe_price_attribute_value mpn
                    LEFT JOIN static.match_product_in_probe_attributes_state mpas ON mpn.attribute_state_fk = mpas.pk
                    JOIN probedata.match_product_in_probe mpip ON mpn.match_product_in_probe_fk = mpip.pk
                    JOIN probedata.probe p ON p.pk = mpip.probe_fk
            WHERE
                    p.session_uid = "{}";""".format(self.session_uid)
        products_with_prices = pd.read_sql_query(query, self.rds_conn.db)
        return products_with_prices[~products_with_prices['price_value'].isnull()]

    @staticmethod
    def is_exist(cell):
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
        :param num:
        :param den:
        :return: the percent of the num/den
        """
        if den == 0:
            Log.error("Zero cannot be divided")
            return 0
        return round((float(num) * 100) / den, 2)
