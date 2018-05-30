
import os
import pandas as pd
# import numpy as np
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEOUS_SAND.Utils.Const import Const
from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
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
        self.common = Common(self.data_provider, levels=6)
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
        self.state_fk = self.data_provider[Data.STORE_INFO]['state_fk']
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_new_kpi_static_data()
        self.state = "AAA"######
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

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        level = 1
        t_store_score, s_store_score, n_store_score = 0, 0, 0
        t_score_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SCORE_TOTAL)
        s_score_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SCORE_SEGMENT)
        n_score_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SCORE_NATIONAL)
        for i, kpi_line in self.templates[Const.KPIS_SHEET].iterrows():
            t_weighted_score, s_weighted_score, n_weighted_score = self.calculate_set(kpi_line)
            if kpi_line[Const.KPI_GROUP]:
                t_store_score += t_weighted_score
                s_store_score += s_weighted_score
                n_store_score += n_weighted_score
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_score_fk, level=level, numerator_id=self.manufacturer_fk, result=t_store_score,
            numerator_result=t_store_score, has_children=True)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=s_score_fk, level=level, numerator_id=self.manufacturer_fk, result=s_store_score,
            numerator_result=s_store_score, has_children=True)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=n_score_fk, level=level, numerator_id=self.manufacturer_fk, result=n_store_score,
            numerator_result=n_store_score, has_children=True)

    def calculate_set(self, kpi_line):
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
        else:
            return 0, 0, 0
        if self.is_exist(target):
            t_score, s_score, n_score = 100 * (t_score >= target), 100 * (s_score >= target), 100 * (n_score >= target)
        if not self.is_exist(weight):
            weight = 1
        return t_score * weight, s_score * weight, n_score * weight

    def calculate_total_display_share(self, scene_types):
        level = 2
        t_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_DISPLAY_SHARE_TOTAL)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        targets_template = self.templates[Const.DISPLAY_TARGET_SHEET]
        relevant_products = self.scif[(self.scif['scene_fk'].isin(relevant_scenes)) &
                                      (self.scif['location_type'] == 'Secondary Shelf')]
        manufacturers = relevant_products['manufacturer_fk'].unique().tolist()
        total_products_in_display = relevant_products['facings'].sum()
        diageo_passed = 0
        for manufacturer in manufacturers:
            passed_skus = self.calculate_manufacturer_display_share(manufacturer, relevant_products,
                                                                    targets_template, total_products_in_display)
            if manufacturer == self.manufacturer_fk:
                diageo_passed = passed_skus
        result = 100 * (diageo_passed >= Const.TARGET_FOR_DISPLAY_SHARE * total_products_in_display)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=diageo_passed,
            denominator_result=total_products_in_display, result=result,
            has_children=True, necessary=True, has_parent=True)
        return result, 0, 0

    def calculate_manufacturer_display_share(self, manufacturer, relevant_products, template, total_products):
        level = 3
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_DISPLAY_SHARE_MANUFACTURER)
        manufacturer_products = relevant_products[relevant_products['manufacturer_fk'] == manufacturer]
        passed_products = 0
        for product_fk in manufacturer_products['product_fk'].unique().tolist():
            passed_products += self.calculate_sku_display_share(product_fk, manufacturer_products, template)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=manufacturer, numerator_result=passed_products,
            denominator_result=total_products, result=self.get_score(passed_products, total_products),
            has_children=True, necessary=True, has_parent=True)
        return passed_products

    def calculate_sku_display_share(self, sku, relevant_products, template):
        level = 4
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_DISPLAY_SHARE_SKU)
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
            sum_facings += facings
            sum_scenes_passed += 1 * (facings >= minimum_products)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=sku, numerator_result=sum_scenes_passed, result=sum_facings,
            has_children=True, necessary=True, has_parent=True)
        return sum_scenes_passed

    def get_templates(self):
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

    def calculate_total_msrp(self, scene_types):
        level = 1
        kpi_name = Const.DB_MSRP_TOTAL
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[Const.PRICING_SHEET]
        products_ean = all_products_table[Const.PRODUCT_EAN_CODE].unique().tolist()
        relevant_products = self.all_products[self.all_products[Const.PRODUCT_EAN_CODE_DB].isin(products_ean)]
        brands = relevant_products['brand_fk'].unique().tolist()
        passed_competes, all_competes = 0, 0
        for brand_fk in brands:
            brand_products = relevant_products[relevant_products['brand_fk'] == brand_fk]
            brand_table = all_products_table[all_products_table[Const.OUR_EAN_CODE].isin(
                brand_products['product_ean_code'].tolist())]
            brand_passed_competes, brand_all_competes = self.calculate_msrp_of_brand(
                brand_fk, brand_products, brand_table, relevant_scenes)
            passed_competes += brand_passed_competes
            all_competes += brand_all_competes
        result = self.get_score(passed_competes, all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=passed_competes,
            denominator_result=all_competes, result=result,
            has_children=True, necessary=True, has_parent=True)
        return result, 0, 0

    def calculate_msrp_of_brand(self, brand_fk, brand_products, brand_table, scene_types):
        level = 2
        kpi_name = Const.DB_MSRP_BRAND
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        sub_brands = brand_products['sub_brand'].unique().tolist()
        passed_competes, all_competes = 0, 0
        for sub_brand in sub_brands:
            sub_brand_products = brand_products[brand_products['sub_brand'] == sub_brand]
            sub_brand_competitions = brand_table[brand_table[Const.OUR_EAN_CODE].isin(
                sub_brand_products['product_ean_code'].tolist())]
            sub_passed_competes, sub_all_competes = self.calculate_msrp_of_sub_brand(
                sub_brand, sub_brand_competitions, scene_types)
            passed_competes += sub_passed_competes
            all_competes += sub_all_competes
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=brand_fk, numerator_result=passed_competes,
            denominator_result=all_competes, result=self.get_score(passed_competes, all_competes),
            has_children=True, necessary=True, has_parent=True)
        return passed_competes, all_competes

    def calculate_msrp_of_sub_brand(self, sub_brand, sub_brand_competitions, scene_types):
        level = 3
        kpi_name = Const.DB_MSRP_SUB_BRAND
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        all_competes, passed_competes = 0, 0
        for i, competition in sub_brand_competitions.iterrows():
            competition_score = self.calculate_msrp_of_competition(competition, scene_types)
            if competition_score is None:
                continue
            all_competes += 1
            passed_competes += competition_score
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=sub_brand, numerator_result=passed_competes,
            denominator_result=all_competes, result=self.get_score(passed_competes, all_competes),
            has_children=True, necessary=True, has_parent=True)
        return passed_competes, all_competes

    def calculate_msrp_of_competition(self, competition, relevant_scenes):
        level = 4
        kpi_name = Const.DB_MSRP_COMPETITION
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        our_ean = competition[Const.OUR_EAN_CODE]
        our_line = self.all_products[self.all_products['product_ean_code'] == our_ean]
        if our_line.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_ean))
            return None
        our_fk = our_line['product_fk'].iloc[0]
        our_price = self.calculate_sku_price(our_fk, relevant_scenes)
        if self.is_exist(competition[Const.COMP_EAN_CODE]):
            comp_ean = competition[Const.COMP_EAN_CODE]
            comp_line = self.all_products[self.all_products['product_ean_code'] == comp_ean]
            if comp_line.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(our_ean))
                return None
            comp_fk = comp_line['product_fk'].iloc[0]
            comp_price = self.calculate_sku_price(comp_fk, relevant_scenes)
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
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=our_fk, numerator_result=(result == 0) * 100, result=result,
            has_children=True, necessary=True, has_parent=True)
        return (result == 0) * 1

    def calculate_sku_price(self, product_fk, scenes):
        level = 5
        kpi_name = Const.DB_MSRP_PRICE
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        price = self.scif[(self.scif['product_fk'] == product_fk) &
                          (self.scif['scene_fk'].isin(scenes))]['price_recognition']
        if price.empty:
            Log.warning("the product {} doesn't exist in scenes {}".format(product_fk, scenes))
            return None
        result = price.iloc[0]
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=product_fk, numerator_result=result, result=result,
            necessary=True, has_parent=True)
        return result

    @staticmethod
    def is_exist(cell):
        if cell in (["", "N/A", None]):
            return False
        return True

    def get_relevant_scenes(self, scene_types):
        if self.is_exist(scene_types):
            return self.scif[self.scif["template_name"].isin(scene_types.split(", "))]["scene_id"].unique().tolist()
        return self.scif["scene_id"].unique().tolist()

    def calculate_total_shelf_facings(self, scene_types):
        level = 2
        t_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_TOTAL)
        s_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_SEGMENT)
        n_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_NATIONAL)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == self.state]
        if relevant_competitions.empty:
            Log.warning("There are no shelf facing competitions for state {}".format(self.state))
            return
        products_ean = relevant_competitions[Const.OUR_EAN_CODE].unique().tolist()
        relevant_products = self.all_products[self.all_products[Const.PRODUCT_EAN_CODE_DB].isin(products_ean)]
        brands = relevant_products['brand_fk'].unique().tolist()
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for brand_fk in brands:
            brand_products = relevant_products[relevant_products['brand_fk'] == brand_fk]
            brand_competitions = relevant_competitions[relevant_competitions[Const.OUR_EAN_CODE].isin(
                brand_products['product_ean_code'].tolist())]
            all_competes.append(self.calculate_shelf_facings_of_brand(
                brand_fk, brand_products, brand_competitions, relevant_scenes))
        national_competes = all_competes[all_competes[Const.STANDARD_TYPE] == 'national']
        segment_competes = all_competes[all_competes[Const.STANDARD_TYPE] == 'segment']
        t_num_result, t_den_result = all_competes[Const.PASSED].sum(), all_competes[Const.PASSED].count()
        s_num_result, s_den_result = segment_competes[Const.PASSED].sum(), segment_competes[Const.PASSED].count()
        n_num_result, n_den_result = national_competes[Const.PASSED].sum(), national_competes[Const.PASSED].count()
        t_result = self.get_score(t_num_result, t_den_result)
        s_result = self.get_score(s_num_result, s_den_result)
        n_result = self.get_score(n_num_result, n_den_result)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=t_num_result,
            denominator_result=t_den_result, result=t_result,
            has_children=True, necessary=True, has_parent=True)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=s_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=t_num_result,
            denominator_result=t_den_result, result=s_result,
            has_children=False, necessary=True, has_parent=True)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=n_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=t_num_result,
            denominator_result=t_den_result, result=n_result,
            has_children=False, necessary=True, has_parent=True)
        return t_result, s_result, n_result

    def calculate_shelf_facings_of_brand(self, brand_fk, brand_products, brand_competitions, scene_types):
        level = 3
        kpi_name = Const.DB_SHELF_FACING_BRAND
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        sub_brands = brand_products['sub_brand'].unique().tolist()
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        sub_brands.remove(None)
        for sub_brand in sub_brands:
            sub_brand_products = brand_products[brand_products['sub_brand'] == sub_brand]
            sub_brand_competitions = brand_competitions[brand_competitions[Const.OUR_EAN_CODE].isin(
                sub_brand_products['product_ean_code'].tolist())]
            all_competes.append(self.calculate_shelf_facings_of_sub_brand(
                sub_brand, sub_brand_competitions, scene_types))
        num_result, den_result = all_competes[Const.PASSED].sum(), len(all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=brand_fk, numerator_result=num_result,
            denominator_result=den_result, result=self.get_score(num_result, den_result),
            has_children=True, necessary=True, has_parent=True)
        return all_competes

    def calculate_shelf_facings_of_sub_brand(self, sub_brand, sub_brand_competitions, scene_types):
        level = 4
        kpi_name = Const.DB_SHELF_FACING_SUB_BRAND
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in sub_brand_competitions.iterrows():
            competition_score, product_fk, standard_type = self.calculate_shelf_facings_of_competition(
                competition, scene_types)
            all_competes.append({Const.STANDARD_TYPE: standard_type, Const.PRODUCT_FK: product_fk,
                                 Const.PASSED: competition_score}, ignore_index=True)
        num_result, den_result = all_competes[Const.PASSED].sum(), len(all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=sub_brand, numerator_result=num_result,
            denominator_result=den_result, result=self.get_score(num_result, den_result),
            has_children=True, necessary=True, has_parent=True)
        return all_competes

    def calculate_shelf_facings_of_competition(self, competition, relevant_scenes):
        level = 5
        kpi_name = Const.DB_SHELF_FACING_SKU_COMPETITION
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        our_eans = competition[Const.OUR_EAN_CODE].split(', ')
        our_lines = self.all_products[self.all_products['product_ean_code'].isin(our_eans)]
        if our_lines.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_eans))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        our_facings = self.calculate_shelf_facings_of_sku(our_fks, relevant_scenes)
        all_facings = our_facings
        if self.is_exist(competition[Const.COMP_EAN_CODE]):
            comp_eans = competition[Const.COMP_EAN_CODE].split(', ')
            comp_lines = self.all_products[self.all_products['product_ean_code'].isin(comp_eans)]
            if comp_lines.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(comp_eans))
                return None
            comp_fks = comp_lines['product_fk'].unique().tolist()
            comp_facings = self.calculate_shelf_facings_of_sku(comp_fks, relevant_scenes)
            all_facings += comp_facings
            target = comp_facings * competition[Const.BENCH_VALUE]
        else:
            target = competition[Const.BENCH_VALUE]
        comparison = 100 * (our_facings >= target)
        num_id = our_fks[0]
        result = round(float(our_facings) / all_facings, 2)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=num_id, numerator_result=our_facings, denominator_result=all_facings,
            result=result, has_children=True, necessary=True, has_parent=True, score=comparison)
        return comparison / 100, num_id, self.get_standard_type(num_id)

    def calculate_shelf_facings_of_sku(self, product_fks, relevant_scenes):
        level = 6
        kpi_name = Const.DB_SHELF_FACING_SKU
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        amount_of_facings = 0
        for product_fk in product_fks:
            product_facing = self.scif[
                (self.scif['product_fk'] == product_fk) &
                (self.scif['scene_id'].isin(relevant_scenes))]['facings'].sum()
            amount_of_facings += product_facing
            self.common.write_to_db_result_new_tables_with_tree(
                fk=kpi_fk, level=level, numerator_id=product_fk, numerator_result=product_facing, result=product_facing,
                necessary=True)
        return amount_of_facings

    def convert_groups_from_template(self):
        shelf_groups = self.templates[Const.SHELF_GROUPS_SHEET]
        shelves_groups = {}
        for i, group in shelf_groups.iterrows():
            shelves_groups[group[Const.NUMBER_GROUP]] = group[Const.SHELF_GROUP].split(', ')
        return shelves_groups

    def calculate_total_shelf_placement(self, scene_types):
        level = 2
        t_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_PLACEMENT_TOTAL)
        s_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_PLACEMENT_SEGMENT)
        n_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_PLACEMENT_NATIONAL)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[Const.SHELF_PLACMENTS_SHEET]
        products_ean = all_products_table[Const.PRODUCT_EAN_CODE].unique().tolist()
        relevant_products = self.all_products[self.all_products[Const.PRODUCT_EAN_CODE_DB].isin(products_ean)]
        brands = relevant_products['brand_fk'].unique().tolist()
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        converted_groups = self.convert_groups_from_template()
        for brand_fk in brands:
            brand_products = relevant_products[relevant_products['brand_fk'] == brand_fk]
            brand_table = all_products_table[all_products_table[Const.OUR_EAN_CODE].isin(
                brand_products['product_ean_code'].tolist())]
            all_competes.append(self.calculate_shelf_placement_of_brand(
                brand_fk, brand_products, brand_table, converted_groups, relevant_scenes))
        national_competes = all_competes[all_competes[Const.STANDARD_TYPE] == 'national']
        segment_competes = all_competes[all_competes[Const.STANDARD_TYPE] == 'segment']
        t_num_result, t_den_result = all_competes[Const.PASSED].sum(), all_competes[Const.PASSED].count()
        s_num_result, s_den_result = segment_competes[Const.PASSED].sum(), segment_competes[Const.PASSED].count()
        n_num_result, n_den_result = national_competes[Const.PASSED].sum(), national_competes[Const.PASSED].count()
        t_result = self.get_score(t_num_result, t_den_result)
        s_result = self.get_score(s_num_result, s_den_result)
        n_result = self.get_score(n_num_result, n_den_result)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=t_num_result,
            denominator_result=t_den_result, result=t_result,
            has_children=True, necessary=True, has_parent=True)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=s_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=s_num_result,
            denominator_result=t_den_result, result=s_result,
            has_children=False, necessary=True, has_parent=True)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=n_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=n_num_result,
            denominator_result=t_den_result, result=n_result,
            has_children=False, necessary=True, has_parent=True)
        return t_result, s_result, n_result

    def calculate_shelf_placement_of_brand(self, brand_fk, brand_products, brand_table, converted_groups, scene_types):
        level = 3
        kpi_name = Const.DB_SHELF_PLACEMENT_BRAND
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        sub_brands = brand_products['sub_brand'].unique().tolist()
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for sub_brand in sub_brands:
            sub_brand_products = brand_products[brand_products['sub_brand'] == sub_brand]
            sub_brand_competitions = brand_table[brand_table[Const.PRODUCT_EAN_CODE].isin(
                sub_brand_products['product_ean_code'].tolist())]
            all_competes.append(self.calculate_shelf_placement_of_sub_brand(
                sub_brand, sub_brand_competitions, converted_groups, scene_types))
        num_result, den_result = all_competes[Const.PASSED].sum(), len(all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=brand_fk, numerator_result=num_result,
            denominator_result=den_result, result=self.get_score(num_result, den_result),
            has_children=True, necessary=True, has_parent=True)
        return all_competes

    def calculate_shelf_placement_of_sub_brand(self, sub_brand, sub_brand_competitions, converted_groups, scene_types):
        level = 4
        kpi_name = Const.DB_SHELF_PLACEMENT_SUB_BRAND
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in sub_brand_competitions.iterrows():
            competition_score, product_fk, standard_type = self.calculate_shelf_placement_of_sku(
                competition, converted_groups, scene_types)
            if competition_score is None:
                continue
            all_competes.append({Const.STANDARD_TYPE: standard_type, Const.PRODUCT_FK: product_fk,
                                 Const.PASSED: competition_score}, ignore_index=True)
        num_result, den_result = all_competes[Const.PASSED].sum(), len(all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=sub_brand, numerator_result=num_result,
            denominator_result=den_result, result=self.get_score(num_result, den_result),
            has_children=True, necessary=True, has_parent=True)
        return all_competes

    def calculate_shelf_placement_of_sku(self, product_line, converted_groups, scene_types):
        level = 5
        kpi_name = Const.DB_SHELF_PLACEMENT_SKU
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(kpi_name)
        product_fk = self.all_products[
            self.all_products['product_ean_code'] == product_line[Const.PRODUCT_EAN_CODE]]['product_fk'].iloc[0]
        min_shelf_loc = product_line[Const.MIN_SHELF_LOCATION]
        shelf_groups = converted_groups[min_shelf_loc]
        min_max_shleves = self.templates[Const.MINIMUM_SHELF_SHEET]
        relevant_groups = min_max_shleves[min_max_shleves[Const.SHELF_NAME].isin(shelf_groups)]
        relevant_products = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'] == product_fk) &
            (self.match_product_in_scene['scene_fk'].isin(scene_types))]
        if relevant_products.empty:
            return None, None, None
        eye_level_product_facings = 0
        for product_line in relevant_products:
            eye_level_product_facings += self.calculate_specific_product_eye_level(product_line, relevant_groups)
        all_facings = len(relevant_products)
        result = 100 * (eye_level_product_facings > all_facings * Const.PERCENT_FOR_EYE_LEVEL)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=product_fk, numerator_result=eye_level_product_facings,
            denominator_result=all_facings, result=result,
            has_children=False, necessary=True, has_parent=True)
        return result / 100, product_fk, self.get_standard_type(product_fk)

    def calculate_specific_product_eye_level(self, match_product_line, relevant_groups):
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

    @staticmethod
    def get_score(num, den):
        if den == 0:
            Log.error("Cannot devide 0")
            return 0
        return round((float(num) * 100) / den, 2)

    def get_standard_type(self, product_fk):
        if product_fk % 2 == 0:
            return "segment"
        else:
            return "national"
