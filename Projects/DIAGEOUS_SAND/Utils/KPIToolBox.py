
import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from Projects.DIAGEOUS_SAND.Utils.Const import Const
from KPIUtils_v2.DB.Common import Common
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
        self.common = Common(self.data_provider, levels=6)
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
        self.kpi_static_data = self.common.get_new_kpi_static_data()
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'DIAGEO']['manufacturer_fk'].iloc[0]
        self.templates = {}
        self.get_templates()
        self.kpi_results_queries = []
        self.assortment_scores = {Const.DISPLAY_BRAND: {}, Const.POD: {}}
        self.scenes = self.scif['scene_fk'].unique().tolist()
        self.scenes_with_shelves = {}
        for scene in self.scenes:
            shelf = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]['shelf_number'].max()
            self.scenes_with_shelves[scene] = shelf
        self.converted_groups = self.convert_groups_from_template()

# main functions:

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        level = 1
        t_store_score, s_store_score, n_store_score = 0, 0, 0
        t_score_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SCORE_TOTAL)
        s_score_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SCORE_SEGMENT)
        n_score_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SCORE_NATIONAL)
        self.main_assortment_calculation()
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
            t_score, s_score, n_score = self.get_scores_of_assortments(kpi_name)
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

    def get_assortment_fks(self):
        """
        Creates dict of dicts that contains all the necessary kpi_fk of the assortments
        :return:
        """
        pod_sku_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_POD_SKU)
        display_sku_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_COMPLIANCE_SKU)
        pod_sub_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_POD_SUB_BRAND)
        display_sub_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_COMPLIANCE_SUB_BRAND)
        pod_brand_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_POD_BRAND)
        display_brand_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_COMPLIANCE_BRAND)
        pod_total_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_POD_TOTAL)
        display_total_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_COMPLIANCE_TOTAL)
        pod_segment_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_POD_SEGMENT)
        display_segment_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_COMPLIANCE_SEGMENT)
        pod_national_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_POD_NATIONAL)
        display_national_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_COMPLIANCE_NATIONAL)
        fks_list = {
            Const.POD: {
                Const.AS_BRAND: pod_brand_fk, Const.AS_SUB: pod_sub_fk, Const.AS_SKU: pod_sku_fk,
                Const.AS_SEGMENT: pod_segment_fk, Const.AS_NATIONAL: pod_national_fk, Const.AS_TOTAL: pod_total_fk},
            Const.DISPLAY_BRAND: {
                Const.AS_BRAND: display_brand_fk, Const.AS_SUB: display_sub_fk, Const.AS_SKU: display_sku_fk,
                Const.AS_SEGMENT: display_segment_fk, Const.AS_NATIONAL: display_national_fk,
                Const.AS_TOTAL: display_total_fk}}
        return fks_list

    def insert_assortment_results_to_db(self, fks_list, dfs):
        """
        Gets all the kpi_fks and the results of the assortments and writes the scores in DB and in
            "self.assortment_scores" dict of dicts
        :param fks_list: dict of dicts
        :param dfs: dict with two cells, both are dataframes with all the products' results
        """
        for assortment in fks_list:
            sku_fk = fks_list[assortment][Const.AS_SKU]
            results = dfs[sku_fk]
            for brand in results[Const.AS_BRAND].unique().tolist():
                for sub_brand in results[results[Const.AS_BRAND] == brand][Const.AS_SUB].unique().tolist():
                    for product in results[
                                (results[Const.AS_BRAND] == brand) & (results[Const.AS_SUB] == sub_brand)
                    ][Const.PRODUCT_FK].unique().tolist():
                        self.common.write_to_db_result_new_tables_with_tree(
                            fk=sku_fk, level=5, numerator_id=product, numerator_result=None,
                            result=results[results[Const.PRODUCT_FK == product]][Const.PASSED].iloc[0],
                            has_children=False, necessary=False, has_parent=True)
                    num_res = results[(results[Const.AS_BRAND] == brand) & (results[Const.AS_SUB] == sub_brand)][
                        Const.PASSED].sum()
                    den_res = results[(results[Const.AS_BRAND] == brand) & (results[Const.AS_SUB] == sub_brand)][
                        Const.PASSED].count()
                    self.common.write_to_db_result_new_tables_with_tree(
                        fk=fks_list[assortment][Const.AS_SUB], level=4, numerator_id=sub_brand,
                        numerator_result=num_res, denominator_result=den_res, result=self.get_score(num_res, den_res),
                        has_children=True, necessary=False, has_parent=True)
                num_res = results[results[Const.AS_BRAND] == brand][Const.PASSED].sum()
                den_res = results[results[Const.AS_BRAND] == brand][Const.PASSED].count()
                self.common.write_to_db_result_new_tables_with_tree(
                    fk=fks_list[assortment][Const.AS_BRAND], level=3, numerator_id=brand, numerator_result=num_res,
                    denominator_result=den_res, result=self.get_score(num_res, den_res),
                    has_children=True, necessary=False, has_parent=True)
            num_res = results[Const.PASSED].sum()
            den_res = results[Const.PASSED].count()
            self.common.write_to_db_result_new_tables_with_tree(
                fk=fks_list[assortment][Const.AS_TOTAL], level=2, numerator_id=self.manufacturer_fk,
                numerator_result=num_res, denominator_result=den_res, result=self.get_score(num_res, den_res),
                has_children=True, necessary=False, has_parent=True)
            self.assortment_scores[assortment][Const.AS_TOTAL] = self.get_score(num_res, den_res)
            num_res = results[results[Const.STANDARD_TYPE] == Const.AS_SEGMENT][Const.PASSED].sum()
            den_res = results[results[Const.STANDARD_TYPE] == Const.AS_SEGMENT][Const.PASSED].count()
            self.common.write_to_db_result_new_tables_with_tree(
                fk=fks_list[assortment][Const.AS_SEGMENT], level=2, numerator_id=self.manufacturer_fk,
                numerator_result=num_res, denominator_result=den_res, result=self.get_score(num_res, den_res))
            self.assortment_scores[assortment][Const.AS_SEGMENT] = self.get_score(num_res, den_res)
            num_res = results[results[Const.STANDARD_TYPE] == Const.AS_NATIONAL][Const.PASSED].sum()
            den_res = results[results[Const.STANDARD_TYPE] == Const.AS_NATIONAL][Const.PASSED].count()
            self.common.write_to_db_result_new_tables_with_tree(
                fk=fks_list[assortment][Const.AS_NATIONAL], level=2, numerator_id=self.manufacturer_fk,
                numerator_result=num_res, denominator_result=den_res, result=self.get_score(num_res, den_res))
            self.assortment_scores[assortment][Const.AS_NATIONAL] = self.get_score(num_res, den_res)

    def main_assortment_calculation(self):
        """
        This function calculates the assortments, using the global Assortment.
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        fks_list = self.get_assortment_fks()
        display_sku_fk = fks_list[Const.DISPLAY_BRAND][Const.AS_SKU]
        pod_sku_fk = fks_list[Const.POD][Const.AS_SKU]
        assortment_dfs = {pod_sku_fk: pd.DataFrame(columns=Const.COLUMNS_FOR_AS),
                          display_sku_fk: pd.DataFrame(columns=Const.COLUMNS_FOR_AS)}
        relevant_products = self.scif[self.scif['location_type'] == 'Secondary Shelf']
        for result in lvl3_result.itertuples():
            assortment_result = 0
            brand, sub_brand, standard_type = self.get_product_details(result.product_fk)
            if result.kpi_fk_lvl3 == display_sku_fk: # if it's display, it should pass the scenes' targets
                if result.in_store:
                    passed_display = self.calculate_passed_display(result.product_fk, relevant_products)
                    assortment_result = 1 * (passed_display > 0)
            else:
                assortment_result = result.in_store
            assortment_dfs[result.kpi_fk_lvl3].append(
                {Const.PRODUCT_FK: result.product_fk, Const.AS_BRAND: brand,
                 Const.PASSED: assortment_result, Const.AS_SUB: sub_brand,
                 Const.STANDARD_TYPE: standard_type}, ignore_index=True)
        # DB insertion:
        self.insert_assortment_results_to_db(fks_list, assortment_dfs)
        # calculate extras
        all_diageo_products = self.scif[self.scif['manufacturer_fk'] == self.manufacturer_fk][
            'product_fk'].unique().tolist()
        assortment_products = lvl3_result.product_fk.tolist()#TODO
        for product in all_diageo_products:
            if product not in assortment_products:
                self.common.write_to_db_result_new_tables_with_tree(
                    fk=pod_sku_fk, level=5, numerator_id=product, numerator_result=None,
                    result=Const.EXTRA, has_children=False, necessary=False, has_parent=False)


# assortments:

    def get_scores_of_assortments(self, kpi_name):
        """
        :param kpi_name: assortment type
        :return: 3 scores from self.assortment_scores[kpi_name]
        """
        scores = self.assortment_scores[kpi_name]
        return scores[Const.AS_TOTAL], scores[Const.AS_SEGMENT], scores[Const.AS_NATIONAL]

    def get_product_details(self, product_fk):
        """
        :param product_fk:
        :return: its details for assortment
        """
        brand = self.all_products[self.all_products['product_fk'] == product_fk]['brand_fk'].iloc[0]
        sub_brand = self.all_products[self.all_products['product_fk'] == product_fk]['sub_brand'].iloc[0]
        standard_type = self.get_standard_type(product_fk)
        return brand, sub_brand, standard_type

# display share:

    def calculate_total_display_share(self, scene_types):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :return: total_result
        """
        t_level, m_level, s_level = 2, 3, 4
        t_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_DISPLAY_SHARE_TOTAL)
        m_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_DISPLAY_SHARE_MANUFACTURER)
        s_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_DISPLAY_SHARE_SKU)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif[(self.scif['scene_fk'].isin(relevant_scenes)) &
                                      (self.scif['location_type'] == 'Secondary Shelf')]
        manufacturers = relevant_products['manufacturer_fk'].unique().tolist()
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        for manufacturer in manufacturers:
            passed_skus = self.calculate_manufacturer_display_share(manufacturer, relevant_products)
            all_results.append(passed_skus)
        den_res = all_results[Const.PASSED].sum()
        for manufacturer in all_results[Const.MANUFACTURER].unique().tolist():
            for product in all_results[
                        all_results[Const.MANUFACTURER] == manufacturer]:
                self.common.write_to_db_result_new_tables_with_tree(
                    fk=s_fk, level=s_level, numerator_id=product[Const.PRODUCT_FK].iloc[0], numerator_result=None,
                    result=product[Const.PASSED].iloc[0], necessary=False, has_parent=True)
            num_res = all_results[all_results[Const.MANUFACTURER] == manufacturer][Const.PASSED].sum()
            if manufacturer == self.manufacturer_fk:
                result = Const.TARGET_FOR_DISPLAY_SHARE
            else:
                result = self.get_score(num_res, den_res)
            self.common.write_to_db_result_new_tables_with_tree(
                fk=m_fk, level=m_level, numerator_id=manufacturer, numerator_result=num_res,
                denominator_result=den_res, result=result,
                has_children=True, necessary=False, has_parent=True)
        diageo_results = all_results[all_results[Const.MANUFACTURER] == self.manufacturer_fk][Const.PASSED].sum()
        result = 100 * (diageo_results >= Const.TARGET_FOR_DISPLAY_SHARE * den_res)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=t_fk, level=t_level, numerator_id=self.manufacturer_fk, numerator_result=diageo_results,
            denominator_result=den_res, result=result,
            has_children=True, necessary=True, has_parent=True)
        return result, 0, 0

    def calculate_manufacturer_display_share(self, manufacturer, relevant_products):
        """
        :param manufacturer:
        :param relevant_products: DF (scif of the display)
        :return: DF of all the results of the manufacturer's products
        """
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        manufacturer_products = relevant_products[relevant_products['manufacturer_fk'] == manufacturer]
        for product_fk in manufacturer_products['product_fk'].unique().tolist():
            sum_scenes_passed = self.calculate_passed_display(product_fk, relevant_products)
            all_results.append({Const.PRODUCT_FK: product_fk, Const.PASSED: sum_scenes_passed,
                                Const.MANUFACTURER: manufacturer}, ignore_index=True)
        return all_results

# shelf facings:

    def calculate_total_shelf_facings(self, scene_types):
        """
        Calculates if facings of Diageo products are more than targets (competitors products or objective target)
        :param scene_types:
        :return:
        """
        level = 2
        t_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_TOTAL)
        s_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_SEGMENT)
        n_kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_NATIONAL)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        if self.state in relevant_competitions[Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == self.state]
        else:
            Log.warning("There are no shelf facing competitions for state {}".format(self.state))
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
            has_children=False, necessary=False, has_parent=False)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=n_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=t_num_result,
            denominator_result=t_den_result, result=n_result,
            has_children=False, necessary=False, has_parent=False)
        return t_result, s_result, n_result

    def calculate_shelf_facings_of_brand(self, brand_fk, brand_products, brand_competitions, scene_types):
        """

        :param brand_fk:
        :param brand_products: filtered scif
        :param brand_competitions: filtered template
        :param scene_types:
        :return:
        """
        level = 3
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_BRAND)
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
            has_children=True, necessary=False, has_parent=True)
        return all_competes

    def calculate_shelf_facings_of_sub_brand(self, sub_brand, sub_brand_competitions, scene_types):
        """

        :param sub_brand: fk
        :param sub_brand_competitions: filtered template
        :param scene_types:
        :return:
        """
        level = 4
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_SUB_BRAND)
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
            has_children=True, necessary=False, has_parent=True)
        return all_competes

    def calculate_shelf_facings_of_competition(self, competition, relevant_scenes):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :return: passed, product_fk, standard_type
        """
        level = 5
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_SKU_COMPETITION)
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
            result=result, has_children=True, necessary=False, has_parent=True, score=comparison)
        return comparison / 100, num_id, self.get_standard_type(num_id)

    def calculate_shelf_facings_of_sku(self, product_fks, relevant_scenes):
        """
        Gets product(s) and counting its facings.
        :param product_fks:
        :param relevant_scenes:
        :return: amount of facings
        """
        level = 6
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_FACING_SKU)
        amount_of_facings = 0
        for product_fk in product_fks:
            product_facing = self.scif[
                (self.scif['product_fk'] == product_fk) &
                (self.scif['scene_id'].isin(relevant_scenes))]['facings'].sum()
            amount_of_facings += product_facing
            self.common.write_to_db_result_new_tables_with_tree(
                fk=kpi_fk, level=level, numerator_id=product_fk, numerator_result=product_facing, result=product_facing,
                necessary=True, has_parent=True)
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
        for brand_fk in brands:
            brand_products = relevant_products[relevant_products['brand_fk'] == brand_fk]
            brand_table = all_products_table[all_products_table[Const.OUR_EAN_CODE].isin(
                brand_products['product_ean_code'].tolist())]
            all_competes.append(self.calculate_shelf_placement_of_brand(
                brand_fk, brand_products, brand_table, relevant_scenes))
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
            has_children=False, necessary=False, has_parent=False)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=n_kpi_fk, level=level, numerator_id=self.manufacturer_fk, numerator_result=n_num_result,
            denominator_result=t_den_result, result=n_result,
            has_children=False, necessary=False, has_parent=False)
        return t_result, s_result, n_result

    def calculate_shelf_placement_of_brand(self, brand_fk, brand_products, brand_table, scene_types):
        """
        :param brand_fk:
        :param brand_products: filtered all_products
        :param brand_table: filtered template
        :param scene_types:
        :return:
        """
        level = 3
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_PLACEMENT_BRAND)
        sub_brands = brand_products['sub_brand'].unique().tolist()
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for sub_brand in sub_brands:
            sub_brand_products = brand_products[brand_products['sub_brand'] == sub_brand]
            sub_brand_competitions = brand_table[brand_table[Const.PRODUCT_EAN_CODE].isin(
                sub_brand_products['product_ean_code'].tolist())]
            all_competes.append(self.calculate_shelf_placement_of_sub_brand(
                sub_brand, sub_brand_competitions, scene_types))
        num_result, den_result = all_competes[Const.PASSED].sum(), len(all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=brand_fk, numerator_result=num_result,
            denominator_result=den_result, result=self.get_score(num_result, den_result),
            has_children=True, necessary=False, has_parent=True)
        return all_competes

    def calculate_shelf_placement_of_sub_brand(self, sub_brand, sub_brand_competitions, scene_types):
        """
        :param sub_brand: fk
        :param sub_brand_competitions: filtered template
        :param scene_types:
        :return:
        """
        level = 4
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_PLACEMENT_SUB_BRAND)
        all_competes = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in sub_brand_competitions.iterrows():
            competition_score, product_fk, standard_type = self.calculate_shelf_placement_of_sku(
                competition, scene_types)
            if competition_score is None:
                continue
            all_competes.append({Const.STANDARD_TYPE: standard_type, Const.PRODUCT_FK: product_fk,
                                 Const.PASSED: competition_score}, ignore_index=True)
        num_result, den_result = all_competes[Const.PASSED].sum(), len(all_competes)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=sub_brand, numerator_result=num_result,
            denominator_result=den_result, result=self.get_score(num_result, den_result),
            has_children=True, necessary=False, has_parent=True)
        return all_competes

    def calculate_shelf_placement_of_sku(self, product_line, scene_types):
        """
        Gets product (line from template) and checks if it has more facings than targets in the eye level
        :param product_line: series
        :param scene_types:
        :return:
        """
        level = 5
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_SHELF_PLACEMENT_SKU)
        product_fk = self.all_products[
            self.all_products['product_ean_code'] == product_line[Const.PRODUCT_EAN_CODE]]['product_fk'].iloc[0]
        min_shelf_loc = product_line[Const.MIN_SHELF_LOCATION]
        shelf_groups = self.converted_groups[min_shelf_loc]
        min_max_shleves = self.templates[Const.MINIMUM_SHELF_SHEET]
        relevant_groups = min_max_shleves[min_max_shleves[Const.SHELF_NAME].isin(shelf_groups)]
        relevant_products = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'] == product_fk) &
            (self.match_product_in_scene['scene_fk'].isin(scene_types))]
        if relevant_products.empty:
            return None, None, None
        eye_level_product_facings = 0
        for product in relevant_products:
            eye_level_product_facings += self.calculate_specific_product_eye_level(product, relevant_groups)
        all_facings = len(relevant_products)
        result = 100 * (eye_level_product_facings > all_facings * Const.PERCENT_FOR_EYE_LEVEL)
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=product_fk, numerator_result=eye_level_product_facings,
            denominator_result=all_facings, result=result,
            has_children=False, necessary=False, has_parent=True)
        return result / 100, product_fk, self.get_standard_type(product_fk)

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
        level = 1
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_MSRP_TOTAL)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[Const.PRICING_SHEET]
        if self.state in all_products_table[Const.STATE].unique().tolist():
            all_products_table = all_products_table[all_products_table[Const.STATE] == self.state]
        else:
            Log.warning("There are no pricing competitions for state {}".format(self.state))
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
            has_children=True, necessary=False)
        return result, 0, 0

    def calculate_msrp_of_brand(self, brand_fk, brand_products, brand_table, scene_types):
        """

        :param brand_fk:
        :param brand_products: filtered all_products
        :param brand_table: filtered template
        :param scene_types:
        :return:
        """
        level = 2
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_MSRP_BRAND)
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
            has_children=True, necessary=False, has_parent=True)
        return passed_competes, all_competes

    def calculate_msrp_of_sub_brand(self, sub_brand, sub_brand_competitions, scene_types):
        """
        :param sub_brand: fk
        :param sub_brand_competitions: filtered template
        :param scene_types:
        :return:
        """
        level = 3
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_MSRP_SUB_BRAND)
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
            has_children=True, necessary=False, has_parent=True)
        return passed_competes, all_competes

    def calculate_msrp_of_competition(self, competition, relevant_scenes):
        """
        Takes competition between the price of Diageo product and Comp's product.
        The result is the distance between the objected to the observed
        :param competition: line of the template
        :param relevant_scenes:
        :return: 1/0
        """
        level = 4
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_MSRP_COMPETITION)
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
            has_children=True, necessary=False, has_parent=True)
        return (result == 0) * 1

    def calculate_sku_price(self, product_fk, scenes):
        """
        Takes product, checks its price and writes it in the DB.
        :param product_fk:
        :param scenes:
        :return: price
        """
        level = 5
        kpi_fk = self.common.get_kpi_fk_by_kpi_name_new_table(Const.DB_MSRP_PRICE)
        price = self.scif[(self.scif['product_fk'] == product_fk) &
                          (self.scif['scene_fk'].isin(scenes))]['price_recognition']
        if price.empty:
            Log.warning("the product {} doesn't exist in scenes {}".format(product_fk, scenes))
            return None
        result = price.iloc[0]
        self.common.write_to_db_result_new_tables_with_tree(
            fk=kpi_fk, level=level, numerator_id=product_fk, numerator_result=result, result=result,
            necessary=False, has_parent=True)
        return result

# help functions:

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

    def get_standard_type(self, product_fk):
        """
        :param product_fk:
        :return: standard_type of the given product
        """
        if product_fk % 2 == 0:
            return "segment"
        else:
            return "national"

    def get_relevant_scenes(self, scene_types):
        """
        :param scene_types: cell of template
        :return: list of all the scenes contains the cell
        """
        if self.is_exist(scene_types):
            return self.scif[self.scif["template_name"].isin(scene_types.split(", "))][
                "scene_id"].unique().tolist()
        return self.scif["scene_id"].unique().tolist()

    def get_state(self):
        query = "select name from static.state where pk = {};".format(self.data_provider[Data.STORE_INFO]['state_fk'][0])
        state = pd.read_sql_query(query, self.rds_conn.db)
        return state.values[0][0]
