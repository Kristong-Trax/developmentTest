import os
import pandas as pd
import numpy as np
import json
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.DIAGEOUS.Utils.Const import Const
from Projects.DIAGEOUS.Utils.Fetcher import DIAGEOUSDIAGEOUSQueries
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

# from KPIUtils_v2.DIAGEOUSCalculations.AvailabilityDIAGEOUSCalculations import Availability
# from KPIUtils_v2.DIAGEOUSCalculations.NumberOfScenesDIAGEOUSCalculations import NumberOfScenes
# from KPIUtils_v2.DIAGEOUSCalculations.PositionGraphsDIAGEOUSCalculations import PositionGraphs
# from KPIUtils_v2.DIAGEOUSCalculations.SOSDIAGEOUSCalculations import SOS
# from KPIUtils_v2.DIAGEOUSCalculations.SequenceDIAGEOUSCalculations import Sequence
# from KPIUtils_v2.DIAGEOUSCalculations.SurveyDIAGEOUSCalculations import Survey
# from KPIUtils_v2.DIAGEOUSCalculations.DIAGEOUSCalculationsUtils import GENERALToolBoxDIAGEOUSCalculations

__author__ = 'Elyashiv'

OFF_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'off_premise Template.xlsx')
ON_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'on_premise Template.xlsx')


class DIAGEOUSToolBox:
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.assortment = Assortment(self.data_provider, self.output)
        self.fetcher = DIAGEOUSDIAGEOUSQueries
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.state = self.get_state()
        # these 2 functions are temporary
        self.sub_brands = self.get_sub_brands()
        self.refresh_sub_brands()
        #
        self.result_values = self.get_result_values()
        self.products_with_prices = self.get_products_prices()
        self.kpi_static_data = self.common.kpi_static_data
        self.manufacturer_fk = self.all_products[
            self.all_products['manufacturer_name'] == 'DIAGEO']['manufacturer_fk'].iloc[0]
        if self.data_provider[Data.STORE_INFO]['store_type'].iloc[0] in ('Dining', 'Bar/Nightclub'):
            self.on_premise = True
        else:
            self.on_premise = False
        self.templates = {}
        self.get_templates()
        self.kpi_results_queries = []
        if not self.on_premise:
            self.scenes = self.scif_without_emptys['scene_fk'].unique().tolist()
            self.scenes_with_shelves = {}
            for scene in self.scenes:
                shelf = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene][
                    'shelf_number'].max()
                self.scenes_with_shelves[scene] = shelf
            self.converted_groups = self.convert_groups_from_template()
            self.calculated_price, self.calculated_shelf_facings = [], []
            self.targets_table = self.get_targets_table()
        self.assortment_products = self.assortment.get_lvl3_relevant_ass()

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        total_store_score, segment_store_score, national_store_score = 0, 0, 0
        if self.on_premise:
            total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_TOTAL)
            segment_kpi_fk, national_kpi_fk = None, None
        else:
            total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_TOTAL)
            segment_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_SEGMENT)
            national_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NATIONAL)
        for i, kpi_line in self.templates[Const.KPIS_SHEET].iterrows():
            total_weighted_score, segment_weighted_score, national_weighted_score = self.calculate_set(kpi_line)
            if kpi_line[Const.KPI_GROUP]:
                total_store_score += total_weighted_score
                segment_store_score += segment_weighted_score
                national_store_score += national_weighted_score
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, result=total_store_score,
            identifier_result=self.common.get_dictionary(name=Const.TOTAL), score=total_store_score)
        if segment_kpi_fk and national_kpi_fk:
            self.common.write_to_db_result(
                fk=segment_kpi_fk, numerator_id=self.manufacturer_fk, result=segment_store_score,
                identifier_result=self.common.get_dictionary(name=Const.SEGMENT), score=segment_store_score)
            self.common.write_to_db_result(
                fk=national_kpi_fk, numerator_id=self.manufacturer_fk, result=national_store_score,
                identifier_result=self.common.get_dictionary(name=Const.NATIONAL), score=national_store_score)

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
        if not self.does_exist(weight):
            weight = 1
        if kpi_name == Const.SHELF_PLACEMENT:
            total_score, segment_score, national_score = self.calculate_total_shelf_placement(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.SHELF_FACINGS:
            total_score, segment_score, national_score = self.calculate_total_shelf_facings(
                scene_types, kpi_name, weight)
        elif kpi_name == Const.MSRP:
            total_score, segment_score, national_score = self.calculate_total_msrp(scene_types, kpi_name, weight)
        elif kpi_name == Const.DISPLAY_SHARE:
            total_score, segment_score, national_score = self.calculate_total_display_share(scene_types, weight, target)
        elif kpi_name in (Const.POD, Const.DISPLAY_BRAND):
            if self.on_premise:
                total_score, segment_score, national_score = self.calculate_on_assortment(scene_types, kpi_name, weight)
            else:
                total_score, segment_score, national_score = self.calculate_assortment(scene_types, kpi_name, weight)
        elif kpi_name == Const.MENU:
            total_score, segment_score, national_score = self.calculate_menu(scene_types, weight, target)
        else:
            Log.warning("Set {} is not defined".format(kpi_name))
            return 0, 0, 0
        return total_score * weight, segment_score * weight, national_score * weight

    # assortments:

    def calculate_on_assortment(self, scene_types, kpi_name, weight):
        """
        Gets assortment type, and calculates it with the match function
        :param scene_types: string from template
        :param kpi_name:
        :param weight
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        if kpi_name == Const.POD:
            calculate_function = self.calculate_pod_by_sub_brand
        # elif kpi_name == Const.DISPLAY_BRAND:
        #     calculate_function = self.calculate_display_compliance_sku
        #     relevant_scif = relevant_scif[relevant_scif['location_type'] == 'Secondary Shelf']
        else:
            Log.error("Assortment '{}' is not defined in the code".format(kpi_name))
            return 0, 0, 0
        kpi_db_names = Const.DB_ON_NAMES[kpi_name]
        total_level_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.TOTAL])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_level_fk]
        # total_on_trade_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ASSORTMENTS_NAMES[Const.DB_ON])
        # relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_on_trade_fk]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, product_line in relevant_assortment.iterrows():
            additional_attrs = json.loads(product_line['additional_attributes'])
            # if kpi_name == Const.DISPLAY_BRAND and additional_attrs[Const.DISPLAY] == 0:
            #     continue
            standard_type = additional_attrs[Const.NATIONAL_SEGMENT]
            result_line = calculate_function(product_line['product_fk'], relevant_scif, standard_type)
            all_results = all_results.append(result_line, ignore_index=True)
        total_result, segment_result, national_result = self.insert_all_levels_to_db(
            all_results, kpi_db_names, weight, without_subs=True)
        # add extra products to DB:
        # if kpi_name == Const.POD:
        #     self.calculate_extras(relevant_assortment)
        return total_result, segment_result, national_result

    def calculate_assortment(self, scene_types, kpi_name, weight):
        """
        Gets assortment type, and calculates it with the match function
        :param scene_types: string from template
        :param kpi_name:
        :param weight
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[self.scif_without_emptys['scene_id'].isin(relevant_scenes)]
        if kpi_name == Const.POD:
            calculate_function = self.calculate_pod_sku
        elif kpi_name == Const.DISPLAY_BRAND:
            calculate_function = self.calculate_display_compliance_sku
            relevant_scif = relevant_scif[relevant_scif['location_type'] == 'Secondary Shelf']
        else:
            Log.error("Assortment '{}' is not defined in the code".format(kpi_name))
            return 0, 0, 0
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        total_level_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.TOTAL])
        # relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_level_fk]
        total_off_trade_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ASSORTMENTS_NAMES[Const.DB_OFF])
        relevant_assortment = self.assortment_products[self.assortment_products['kpi_fk_lvl2'] == total_off_trade_fk]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT_ASSORTMENT)
        for i, product_line in relevant_assortment.iterrows():
            additional_attrs = json.loads(product_line['additional_attributes'])
            if kpi_name == Const.DISPLAY_BRAND and additional_attrs[Const.DISPLAY] == 0:
                continue
            standard_type = additional_attrs[Const.NATIONAL_SEGMENT]
            result_line = calculate_function(product_line['product_fk'], relevant_scif, standard_type)
            all_results = all_results.append(result_line, ignore_index=True)
        total_result, segment_result, national_result = self.insert_all_levels_to_db(all_results, kpi_db_names, weight,
                                                                                     with_standard_type=True)
        # add extra products to DB:
        if kpi_name == Const.POD:
            self.calculate_extras(relevant_assortment)
        return total_result, segment_result, national_result

    def calculate_extras(self, relevant_assortment):
        """
        add the extra products to DB
        :param relevant_assortment: DF of assortment with all the PODs
        :return:
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.POD][Const.SKU])
        all_diageo_products = self.scif_without_emptys[
            self.scif_without_emptys['manufacturer_fk'] == self.manufacturer_fk]['product_fk'].unique().tolist()
        assortment_products = relevant_assortment['product_fk'].unique().tolist()
        products_not_in_list = set(all_diageo_products) - set(assortment_products)
        result = Const.EXTRA
        for product in products_not_in_list:
            self.common.write_to_db_result(
                fk=sku_kpi_fk, numerator_id=product, result=self.get_pks_of_result(result))

    def calculate_pod_sku(self, product_fk, relevant_scif, standard_type):
        """
        :param standard_type:
        :param product_fk:
        :param relevant_scif:
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.POD][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.POD][Const.TOTAL])
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

    def calculate_pod_by_sub_brand(self, product_fk, relevant_scif, standard_type):
        """
        :param standard_type:
        :param product_fk:
        :param relevant_scif:
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.POD][Const.SUB_BRAND])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.POD][Const.TOTAL])
        brand, sub_brand = self.get_product_details(product_fk)
        sub_brand_name = self.all_products[self.all_products['product_fk'] == product_fk]['sub_brand'].iloc[0]
        facings = relevant_scif[(relevant_scif['brand_fk'] == brand) &
                                (relevant_scif['sub_brand'] == sub_brand_name)]['facings'].sum()
        if facings > 0:
            result, passed = Const.DISTRIBUTED, 1
        else:
            result, passed = Const.OOS, 0
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: passed,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand, Const.STANDARD_TYPE: standard_type}
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=sub_brand,
            result=self.get_pks_of_result(result), identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        return product_result

    def calculate_display_compliance_sku(self, product_fk, relevant_scif, standard_type):
        """
        :param standard_type:
        :param product_fk:
        :param relevant_scif:
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_BRAND][Const.TOTAL])
        facings = self.calculate_passed_display(product_fk, relevant_scif)
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

    def get_pks_of_result(self, result):
        pk = self.result_values[self.result_values['value'] == result]['pk'].iloc[0]
        return pk

    def get_result_values(self):
        query = self.fetcher.get_result_values()
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    # menu

    def calculate_menu(self, scene_types, weight, target):
        """
        calculates the share of all brands and manufacturers in the menu, and
        checks if Diageo result is bigger than target
        :param scene_types:
        :param weight: float
        :param target: float
        :return:
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.TOTAL])
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.MANUFACTURER])
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_ON_NAMES[Const.MENU][Const.BRAND])
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_scif = self.scif_without_emptys[(self.scif_without_emptys['scene_id'].isin(relevant_scenes)) &
                                                 (self.scif_without_emptys['product_type'] == 'POS')]
        all_brands = relevant_scif['brand_fk'].unique().tolist()
        all_manufacturers = relevant_scif['manufacturer_fk'].unique().tolist()
        den_res = relevant_scif['facings'].sum()
        diageo_facings = 0
        for brand_fk in all_brands:
            self.calculate_specific_kind_menu(relevant_scif, den_res, brand_fk, brand_kpi_fk, "brand_fk", total_kpi_fk)
        for manufacturer_fk in all_manufacturers:
            num_res = self.calculate_specific_kind_menu(
                relevant_scif, den_res, manufacturer_fk, manufacturer_kpi_fk, "manufacturer_fk", total_kpi_fk)
            if manufacturer_fk == self.manufacturer_fk:
                diageo_facings = num_res
        diageo_result = self.get_score(diageo_facings, den_res)
        result = 100 if diageo_result / 100 >= target else 0
        score = result * weight
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_facings,
            denominator_result=den_res, result=diageo_result, score=score, weight=weight,
            identifier_result=self.common.get_dictionary(kpi_fk=total_kpi_fk),
            identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
        return score, 0, 0

    def calculate_specific_kind_menu(self, relevant_scif, den_res, kind_fk, kpi_fk, kind_type, total_kpi_fk):
        """
        takes a kind of type and it's value, and calculates its share
        :param relevant_scif: filtered scif
        :param den_res: all the facings
        :param kind_fk: value - manufacturer_fk/brand_fk/...
        :param kpi_fk:
        :param kind_type: str - "manufacturer_fk"/"brand_fk"/...
        :param total_kpi_fk: for parent
        :return:
        """
        num_res = relevant_scif[relevant_scif[kind_type] == kind_fk]['facings'].sum()
        result = self.get_score(num_res, den_res)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=kind_fk, numerator_result=num_res, denominator_result=den_res,
            result=result, identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        return num_res

    # display share:

    def calculate_total_display_share(self, scene_types, weight, target):
        """
        Calculates the products that passed the targets of display, their manufacturer and all of them
        :param scene_types: scenes from template (can be empty)
        :param target: for the score
        :param weight:
        :param target:
        :return: total_result
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.TOTAL])
        manufacturer_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[
                                                                     Const.DISPLAY_SHARE][Const.MANUFACTURER])
        total_dict = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_products = self.scif_without_emptys[(self.scif_without_emptys['scene_fk'].isin(relevant_scenes)) &
                                                     (self.scif_without_emptys['location_type'] == 'Secondary Shelf')]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_DISPLAY)
        for product_fk in relevant_products['product_fk'].unique().tolist():
            product_result = self.calculate_display_share_of_sku(product_fk, relevant_products, manufacturer_kpi_fk)
            all_results = all_results.append(product_result, ignore_index=True)
        den_res = all_results[Const.PASSED].sum()
        diageo_results, diageo_result = 0, 0
        for manufacturer in all_results[Const.MANUFACTURER].unique().tolist():
            num_res = all_results[all_results[Const.MANUFACTURER] == manufacturer][Const.PASSED].sum()
            # if manufacturer == self.manufacturer_fk:
            #     result = target
            # else:
            result = self.get_score(num_res, den_res)
            if manufacturer == self.manufacturer_fk:
                diageo_result = result
                diageo_results = num_res
            result_dict = self.common.get_dictionary(manufacturer_fk=manufacturer, kpi_fk=manufacturer_kpi_fk)
            self.common.write_to_db_result(
                fk=manufacturer_kpi_fk, numerator_id=manufacturer, numerator_result=num_res,
                denominator_result=den_res, result=result, identifier_parent=total_dict, identifier_result=result_dict)
        score = 100 if (diageo_results >= target * den_res) else 0
        # result = 100 if (diageo_results >= target * den_res) else 0
        # score = result * weight
        self.common.write_to_db_result(
            fk=total_kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=diageo_results,
            denominator_result=den_res, result=diageo_result, should_enter=True, weight=weight, score=score,
            identifier_result=total_dict, identifier_parent=self.common.get_dictionary(name=Const.TOTAL))
        return score * weight, 0, 0

    def calculate_display_share_of_sku(self, product_fk, relevant_products, manufacturer_kpi_fk):
        """

        :param product_fk:
        :param relevant_products: DF (scif of the display)
        :param manufacturer_kpi_fk: for write_to_db
        :return: DF of all the results of the manufacturer's products
        """
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.DISPLAY_SHARE][Const.SKU])
        manufacturer = self.get_manufacturer(product_fk)
        sum_scenes_passed = self.calculate_passed_display(product_fk, relevant_products)
        parent_dict = self.common.get_dictionary(kpi_fk=manufacturer_kpi_fk, manufacturer_fk=manufacturer)
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
        :param scene_types:
        :param kpi_name:
        :param weight:
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        relevant_competitions = self.templates[Const.SHELF_FACING_SHEET]
        if self.state in relevant_competitions[Const.STATE].unique().tolist():
            relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == self.state]
        else:
            relevant_competitions = relevant_competitions[relevant_competitions[Const.STATE] == Const.OTHER]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, competition in relevant_competitions.iterrows():
            result_dict = self.calculate_shelf_facings_of_competition(competition, relevant_scenes, i)
            all_results = all_results.append(result_dict, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        total_result, segment_result, national_result = self.insert_all_levels_to_db(all_results, kpi_db_names, weight)
        return total_result, segment_result, national_result

    def calculate_shelf_facings_of_competition(self, competition, relevant_scenes, index):
        """
        Checks the facings of product, creates target (from competitor and template) and compares them.
        :param competition: template's line
        :param relevant_scenes:
        :param index: for hierarchy
        :return: passed, product_fk, standard_type
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.SHELF_FACINGS][Const.COMPETITION])
        our_eans = competition[Const.OUR_EAN_CODE].split(', ')
        our_lines = self.all_products[self.all_products['product_ean_code'].isin(our_eans)]
        if our_lines.empty:
            Log.warning("The products {} in shelf facings don't exist in DB".format(our_eans))
            return None
        our_fks = our_lines['product_fk'].unique().tolist()
        product_fk = our_fks[0]
        result_identifier = self.common.get_dictionary(kpi_fk=kpi_fk, product_fk=product_fk, index=index)
        our_facings = self.calculate_shelf_facings_of_sku(our_fks, relevant_scenes, result_identifier)
        # all_facings = our_facings
        if self.does_exist(competition[Const.COMP_EAN_CODE]):
            comp_eans = competition[Const.COMP_EAN_CODE].split(', ')
            comp_lines = self.all_products[self.all_products['product_ean_code'].isin(comp_eans)]
            if comp_lines.empty:
                Log.warning("The products {} in shelf facings don't exist in DB".format(comp_eans))
                return None
            comp_fks = comp_lines['product_fk'].unique().tolist()
            comp_facings = self.calculate_shelf_facings_of_sku(comp_fks, relevant_scenes, result_identifier)
            # all_facings += comp_facings
            bench_value = competition[Const.BENCH_VALUE]
            # bench_value = float(bench_value.replace("%", ""))/100
            target = comp_facings * bench_value
        else:
            target = competition[Const.BENCH_VALUE]
        comparison = 1 if (our_facings >= target and our_facings > 0) else 0
        # result = self.get_score(our_facings, all_facings)
        brand, sub_brand = self.get_product_details(product_fk)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.SHELF_FACINGS][Const.TOTAL])
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk,
            result=comparison * 100, identifier_result=result_identifier,
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk))
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: comparison,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand}
        return product_result

    def calculate_shelf_facings_of_sku(self, product_fks, relevant_scenes, parent_identifier):
        """
        Gets product(s) and counting its facings.
        :param product_fks: list of FKs
        :param relevant_scenes: list
        :param parent_identifier: for write_to_db
        :return: amount of facings
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.SHELF_FACINGS][Const.SKU])
        amount_of_facings = 0
        for product_fk in product_fks:
            product_facing = self.scif_without_emptys[
                (self.scif_without_emptys['product_fk'] == product_fk) &
                (self.scif_without_emptys['scene_id'].isin(relevant_scenes))]['facings'].sum()
            if product_facing is None or np.isnan(product_facing):
                product_facing = 0
            amount_of_facings += product_facing
            if product_fk in self.calculated_shelf_facings:
                continue
            else:
                self.calculated_shelf_facings.append(product_fk)
                self.common.write_to_db_result(
                    fk=kpi_fk, numerator_id=product_fk, result=product_facing,
                    should_enter=True, identifier_parent=parent_identifier)
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

    def calculate_total_shelf_placement(self, scene_types, kpi_name, weight):
        """
        Takes list of products and their shelf groups, and calculate if the're pass the target.
        :param scene_types:
        :param kpi_name:
        :param weight
        :return:
        """
        relevant_scenes = self.get_relevant_scenes(scene_types)
        all_products_table = self.templates[Const.SHELF_PLACMENTS_SHEET]
        all_results = pd.DataFrame(columns=Const.COLUMNS_FOR_PRODUCT)
        for i, product_line in all_products_table.iterrows():
            result = self.calculate_shelf_placement_of_sku(product_line, relevant_scenes)
            all_results = all_results.append(result, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        total_result, segment_result, national_result = self.insert_all_levels_to_db(all_results, kpi_db_names, weight)
        return total_result, segment_result, national_result

    def calculate_shelf_placement_of_sku(self, product_line, relevant_scenes):
        """
        Gets product (line from template) and checks if it has more facings than targets in the eye level
        :param product_line: series
        :param relevant_scenes: list
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.SHELF_PLACEMENT][Const.SKU])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.SHELF_PLACEMENT][Const.TOTAL])
        product_fk = self.all_products[self.all_products['product_ean_code'] == product_line[
            Const.PRODUCT_EAN_CODE]]['product_fk']
        if product_fk.empty:
            Log.warning("Product_ean '{}' does not exist".format(product_line[Const.PRODUCT_EAN_CODE]))
            return None
        product_fk = product_fk.iloc[0]
        min_shelf_loc = product_line[Const.MIN_SHELF_LOCATION]
        product_fk_with_substs = [product_fk]
        product_fk_with_substs += self.all_products[self.all_products['substitution_product_fk'] == product_fk][
            'product_fk'].tolist()
        relevant_products = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'].isin(product_fk_with_substs)) &
            (self.match_product_in_scene['scene_fk'].isin(relevant_scenes))]
        if relevant_products.empty:
            return None
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
        target = shelf_groups[shelf_groups[Const.NUMBER_GROUP] == min_shelf_loc][Const.SHELF_GROUP].iloc[0]
        target_fk = self.targets_table[self.targets_table['target'] == target]['pk'].iloc[0]
        score = 100 * passed
        brand, sub_brand = self.get_product_details(product_fk)
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=product_fk, score=score, result=self.get_pks_of_result(result),
            identifier_parent=self.common.get_dictionary(kpi_fk=total_kpi_fk), kpi_level_2_target_fk=target_fk)
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: passed,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand}
        return product_result

    def calculate_specific_product_shelf_placement(self, match_product_line, shelf_groups):
        min_max_shleves = self.templates[Const.MINIMUM_SHELF_SHEET]
        shelf_from_bottom = match_product_line['shelf_number_from_bottom']
        scene = match_product_line['scene_fk']
        shelf_groups_for_scene = min_max_shleves[
            (min_max_shleves[Const.NUM_SHLEVES_MIN] <= self.scenes_with_shelves[scene]) &
            (min_max_shleves[Const.NUM_SHLEVES_MAX] >= self.scenes_with_shelves[scene])]
        group_for_product = shelf_groups_for_scene[shelf_groups_for_scene[
            Const.SHELVES_FROM_BOTTOM] == shelf_from_bottom]
        if group_for_product.empty:
            group_names = [Const.OTHER]
        else:
            group_names = group_for_product[Const.SHELF_NAME].tolist()
        if "ALL" in shelf_groups:
            answer_couple = 1, group_names[0]
        else:
            answer_couple = 0, group_names[0]
            common_shelves = set(group_names) & set(shelf_groups)
            if common_shelves:
                answer_couple = 1, common_shelves.pop()

        print answer_couple[1]
        return answer_couple

    # msrp:

    def calculate_total_msrp(self, scene_types, kpi_name, weight):
        """
        Compares the prices of Diageo products to the competitors' (or absolute values).
        :param scene_types:
        :param kpi_name:
        :param weight:
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
            compete_result_dict = self.calculate_msrp_of_competition(competition, relevant_scenes, i)
            all_competes = all_competes.append(compete_result_dict, ignore_index=True)
        kpi_db_names = Const.DB_OFF_NAMES[kpi_name]
        result, segment_result, national_result = self.insert_all_levels_to_db(all_competes, kpi_db_names, weight)
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
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.COMPETITION])
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.TOTAL])
        our_ean = competition[Const.OUR_EAN_CODE]
        our_line = self.all_products[self.all_products['product_ean_code'] == our_ean]
        min_relative, max_relative = competition[Const.MIN_MSRP_RELATIVE], competition[Const.MAX_MSRP_RELATIVE]
        min_absolute, max_absolute = competition[Const.MIN_MSRP_ABSOLUTE], competition[Const.MAX_MSRP_ABSOLUTE]
        if our_line.empty:
            Log.warning("The products {} in MSRP don't exist in DB".format(our_ean))
            return None
        product_fk = our_line['product_fk'].iloc[0]
        result_dict = self.common.get_dictionary(kpi_fk=kpi_fk, product_fk=product_fk, index=index)
        our_price = self.calculate_sku_price(product_fk, relevant_scenes, result_dict)
        if our_price is None:
            return None
        is_competitor = (self.does_exist(competition[Const.COMP_EAN_CODE]) and
                         self.does_exist(min_relative) and self.does_exist(max_relative))
        is_absolute = self.does_exist(min_absolute) and self.does_exist(max_absolute)
        if is_competitor:
            comp_ean = competition[Const.COMP_EAN_CODE]
            comp_line = self.all_products[self.all_products['product_ean_code'] == comp_ean]
            if comp_line.empty:
                Log.warning("The products {} in MSRP don't exist in DB".format(our_ean))
                return None
            comp_fk = comp_line['product_fk'].iloc[0]
            comp_price = self.calculate_sku_price(comp_fk, relevant_scenes, result_dict)
            if comp_price is None:
                return None
            range_price = (round(comp_price + competition[Const.MIN_MSRP_RELATIVE], 2),
                           round(comp_price + competition[Const.MAX_MSRP_RELATIVE], 2))
        elif is_absolute:
            range_price = (competition[Const.MIN_MSRP_ABSOLUTE], competition[Const.MAX_MSRP_ABSOLUTE])
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
        product_result = {Const.PRODUCT_FK: product_fk, Const.PASSED: (result == 0) * 1,
                          Const.BRAND: brand, Const.SUB_BRAND: sub_brand}
        return product_result

    def calculate_sku_price(self, product_fk, scenes, parent_dict):
        """
        Takes product, checks its price and writes it in the DB.
        :param product_fk:
        :param scenes:
        :param parent_dict:
        :return: price
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[Const.MSRP][Const.SKU])
        price = self.products_with_prices[(self.products_with_prices['product_fk'] == product_fk) &
                                          (self.products_with_prices['scene_fk'].isin(scenes))]['price_value']
        if price.empty:
            return None
        result = round(price.iloc[0], 2)
        if product_fk not in self.calculated_price:
            self.calculated_price.append(product_fk)
            self.common.write_to_db_result(
                fk=kpi_fk, numerator_id=product_fk, result=result,
                identifier_parent=parent_dict, should_enter=True)
        return result

    def get_products_prices(self):
        """
        Gets all the session's products and prices from DB
        :return:
        """
        query = self.fetcher.get_prices_dataframe().format(self.session_uid)
        products_with_prices = pd.read_sql_query(query, self.rds_conn.db)
        return products_with_prices[~products_with_prices['price_value'].isnull()]

    # help functions:

    def get_sub_brands(self):
        query = self.fetcher.get_sub_brands()
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def refresh_sub_brands(self):
        all_sub_brands = self.all_products['sub_brand'].unique().tolist()
        current_sub_brand = self.sub_brands['name'].unique().tolist()
        subs_not_in_db = set(all_sub_brands) - set(current_sub_brand)
        if subs_not_in_db:
            self.insert_new_subs(subs_not_in_db)

    def insert_new_subs(self, new_subs):
        queries = []
        for sub_brand in new_subs:
            if sub_brand:
                queries.append(self.fetcher.insert_new_sub_brands().format(sub_brand))
        merge_queries = self.common.merge_insert_queries(queries)
        cur = self.rds_conn.db.cursor()
        for query in merge_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.sub_brands = self.get_sub_brands()

    def calculate_passed_display(self, product_fk, relevant_products):
        """
        Counts how many scenes the given product passed the conditions of the display (defined in Display_target sheet).
        :param product_fk:
        :param relevant_products:
        :return: number of scenes.
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
            sum_scenes_passed += 1 * (facings >= minimum_products)  # if the condition is failed, it will "add" 0.
        return sum_scenes_passed

    def get_templates(self):
        """
        Reads the template (and makes the EANs be Strings)
        """
        if self.on_premise:
            for sheet in Const.ON_SHEETS:
                self.templates[sheet] = pd.read_excel(ON_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                      keep_default_na=False)
        else:
            for sheet in Const.OFF_SHEETS:
                if sheet in ([Const.SHELF_FACING_SHEET, Const.PRICING_SHEET]):
                    converters = {Const.OUR_EAN_CODE: lambda x: str(x).replace(".0", ""),
                                  Const.COMP_EAN_CODE: lambda x: str(x).replace(".0", "")}
                    self.templates[sheet] = pd.read_excel(OFF_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                          converters=converters, keep_default_na=False)
                elif sheet == Const.SHELF_PLACMENTS_SHEET:
                    converters = {Const.PRODUCT_EAN_CODE: lambda x: str(x).replace(".0", "")}
                    self.templates[sheet] = pd.read_excel(OFF_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                          converters=converters, keep_default_na=False)
                else:
                    self.templates[sheet] = pd.read_excel(OFF_TEMPLATE_PATH, sheetname=sheet, skiprows=2,
                                                          keep_default_na=False)

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

    def get_state(self):
        if not self.data_provider[Data.STORE_INFO]['state_fk'][0]:
            Log.error("session '{}' does not have a state".format(self.session_uid))
            return Const.OTHER
        query = self.fetcher.get_state().format(self.data_provider[Data.STORE_INFO]['state_fk'][0])
        state = pd.read_sql_query(query, self.rds_conn.db)
        return state.values[0][0]

    def get_product_details(self, product_fk):
        """
        :param product_fk:
        :return: its details for assortment (brand, sub_brand, standard_type)
        """
        brand = self.all_products[self.all_products['product_fk'] == product_fk]['brand_fk'].iloc[0]
        sub_brand = self.all_products[self.all_products['product_fk'] == product_fk]['sub_brand'].iloc[0]
        if not sub_brand:
            sub_brand_fk = None
        else:
            sub_brand_fk = self.get_sub_brand_fk(sub_brand)
        return brand, sub_brand_fk

    def get_sub_brand_fk(self, sub_brand):
        sub_brand_line = self.sub_brands[self.sub_brands['name'] == sub_brand]
        if sub_brand_line.empty:
            return None
        else:
            return sub_brand_line.iloc[0]['pk']

    def get_manufacturer(self, product_fk):
        return self.all_products[self.all_products['product_fk'] == product_fk]['manufacturer_fk'].iloc[0]

    def get_targets_table(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        """
        query = self.fetcher.get_targets_template()
        groups = pd.read_sql_query(query, self.rds_conn.db)
        return groups

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
        :param num:
        :param den:
        :return: the percent of the num/den
        """
        if den == 0:
            return 0
        return round((float(num) * 100) / den, 2)

    # main insert to DB functions:

    def insert_all_levels_to_db(self, all_results, kpi_db_names, weight, with_standard_type=False, without_subs=False):
        """
        This function gets all the sku results (with details) and puts in DB all the way up (sub_brand, brand, total,
        and segment-national if exist).
        :param all_results: DF with product_fk and its details - passed, sub_brand, brand, standard_type.
        :param kpi_db_names: name as it's shown in the main sheet of the template.
        :param weight:
        :param with_standard_type: in KPIs that include standard_type we need to know for calculation their total
        :param without_subs: when the sub_brand is the lowest level, we already wrote the results in the DB
        :return:
        """
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.TOTAL])
        total_identifier = self.common.get_dictionary(kpi_fk=total_kpi_fk)
        for brand in all_results[Const.BRAND].unique().tolist():
            brand_results = all_results[all_results[Const.BRAND] == brand]
            self.insert_brand_and_subs_to_db(brand_results, kpi_db_names, brand, total_identifier,
                                             without_subs=without_subs)
        all_passed_results = all_results[Const.PASSED]
        total_result = self.insert_totals_to_db(all_passed_results, kpi_db_names, Const.TOTAL, weight, total_identifier)
        segment_result, national_result = 0, 0
        if with_standard_type:
            national_results = all_results[all_results[Const.STANDARD_TYPE] == Const.NATIONAL][Const.PASSED]
            national_result = self.insert_totals_to_db(national_results, kpi_db_names, Const.NATIONAL, weight)
            segment_results = all_results[all_results[Const.STANDARD_TYPE] == Const.SEGMENT][Const.PASSED]
            segment_result = self.insert_totals_to_db(segment_results, kpi_db_names, Const.SEGMENT, weight)
        return total_result, segment_result, national_result

    def insert_brand_and_subs_to_db(self, brand_results, kpi_db_names, brand, total_identifier, without_subs=False):
        """
        Inserting all brand and sub_brand results
        :param brand_results: DF from all_results
        :param kpi_db_names:
        :param brand: fk
        :param total_identifier: for hierarchy
        :param without_subs: when the sub_brand is the lowest level, we already wrote the results in the DB
        :return:
        """
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.BRAND])
        brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
        if not without_subs:
            for sub_brand in brand_results[brand_results[Const.BRAND] == brand][Const.SUB_BRAND].unique().tolist():
                sub_brand_results = brand_results[(brand_results[Const.BRAND] == brand) &
                                                  (brand_results[Const.SUB_BRAND] == sub_brand)]
                self.insert_sub_brands_to_db(sub_brand_results, kpi_db_names, brand, sub_brand, brand_dict)
        results = brand_results[Const.PASSED]
        num_res, den_res = results.sum(), results.count()
        self.common.write_to_db_result(
            fk=brand_kpi_fk, numerator_id=brand, numerator_result=num_res,
            denominator_result=den_res, result=self.get_score(num_res, den_res),
            identifier_parent=total_identifier, identifier_result=brand_dict)

    def insert_sub_brands_to_db(self, sub_brand_results, kpi_db_names, brand, sub_brand, brand_identifier):
        """
        inserting sub_brand results into DB
        :param sub_brand_results: DF from all_products
        :param kpi_db_names:
        :param brand: fk
        :param sub_brand: fk
        :param brand_identifier: for hierarchy
        :return:
        """
        sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[Const.SUB_BRAND])
        sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
        results = sub_brand_results[Const.PASSED]
        num_res, den_res = results.sum(), results.count()
        self.common.write_to_db_result(
            fk=sub_brand_kpi_fk, numerator_id=sub_brand, numerator_result=num_res,
            denominator_result=den_res, result=self.get_score(num_res, den_res),
            identifier_parent=brand_identifier, identifier_result=sub_brand_dict)

    def insert_totals_to_db(self, all_passed_results, kpi_db_names, total_kind, weight, identifier_result=None):
        """
        inserting all total level (includes segment and national) into DB
        :param all_passed_results: 'passed' column from all_results
        :param kpi_db_names:
        :param weight:
        :param total_kind: TOTAL/SEGMENT/NATIONAL
        :param identifier_result: optional, if has children
        :return:
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_db_names[total_kind])
        num_result, den_result = all_passed_results.sum(), all_passed_results.count()
        result = self.get_score(num_result, den_result)
        score = result * weight if kpi_db_names != Const.DB_OFF_NAMES[Const.MSRP] else 0
        self.common.write_to_db_result(
            fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_result,
            denominator_result=den_result, result=result, identifier_result=identifier_result,
            identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight, score=score)
        return score

    # def insert_all_levels_to_db_on(self, all_results, kpi_name, weight, with_standard_type=False):
    #     """
    #     This function gets all the sku results (with details) and puts in DB all the way up (sub_brand, brand, total,
    #     and segment-national if exist).
    #     :param all_results: DF with product_fk and its details - passed, sub_brand, brand, standard_type.
    #     :param kpi_name: name as it's shown in the main sheet of the template.
    #     :param weight:
    #     :param with_standard_type: in KPIs that include standard_type we need to know for calculation their total
    #     :return:
    #     """
    #     total_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[kpi_name][Const.TOTAL])
    #     total_identifier = self.common.get_dictionary(kpi_fk=total_kpi_fk)
    #     for brand in all_results[Const.BRAND].unique().tolist():
    #         brand_results = all_results[all_results[Const.BRAND] == brand]
    #         self.insert_brand_and_subs_to_db(brand_results, kpi_name, brand, total_identifier)
    #     all_passed_results = all_results[Const.PASSED]
    #     total_result = self.insert_totals_to_db(all_passed_results, kpi_name, Const.TOTAL, weight, total_identifier)
    #     segment_result, national_result = 0, 0
    #     if with_standard_type:
    #         national_results = all_results[all_results[Const.STANDARD_TYPE] == Const.NATIONAL][Const.PASSED]
    #         national_result = self.insert_totals_to_db(national_results, kpi_name, Const.NATIONAL, weight)
    #         segment_results = all_results[all_results[Const.STANDARD_TYPE] == Const.SEGMENT][Const.PASSED]
    #         segment_result = self.insert_totals_to_db(segment_results, kpi_name, Const.SEGMENT, weight)
    #     return total_result, segment_result, national_result
    #
    # def insert_brand_and_subs_to_db_on(self, brand_results, kpi_name, brand, total_identifier):
    #     """
    #     Inserting all brand and sub_brand results
    #     :param brand_results: DF from all_results
    #     :param kpi_name:
    #     :param brand: fk
    #     :param total_identifier: for hierarchy
    #     :return:
    #     """
    #     brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[kpi_name][Const.BRAND])
    #     brand_dict = self.common.get_dictionary(kpi_fk=brand_kpi_fk, brand_fk=brand)
    #     for sub_brand in brand_results[brand_results[Const.BRAND] == brand][Const.SUB_BRAND].unique().tolist():
    #         sub_brand_results = brand_results[(brand_results[Const.BRAND] == brand) &
    #                                           (brand_results[Const.SUB_BRAND] == sub_brand)]
    #         self.insert_sub_brands_to_db(sub_brand_results, kpi_name, brand, sub_brand, brand_dict)
    #     results = brand_results[Const.PASSED]
    #     num_res, den_res = results.sum(), results.count()
    #     self.common.write_to_db_result(
    #         fk=brand_kpi_fk, numerator_id=brand, numerator_result=num_res,
    #         denominator_result=den_res, result=self.get_score(num_res, den_res),
    #         identifier_parent=total_identifier, identifier_result=brand_dict)
    #
    # def insert_sub_brands_to_db_on(self, sub_brand_results, kpi_name, brand, sub_brand, brand_identifier):
    #     """
    #     inserting sub_brand results into DB
    #     :param sub_brand_results: DF from all_products
    #     :param kpi_name:
    #     :param brand: fk
    #     :param sub_brand: fk
    #     :param brand_identifier: for hierarchy
    #     :return:
    #     """
    #     sub_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[kpi_name][Const.SUB_BRAND])
    #     sub_brand_dict = self.common.get_dictionary(kpi_fk=sub_brand_kpi_fk, brand_fk=brand, sub_brand_fk=sub_brand)
    #     results = sub_brand_results[Const.PASSED]
    #     num_res, den_res = results.sum(), results.count()
    #     self.common.write_to_db_result(
    #         fk=sub_brand_kpi_fk, numerator_id=sub_brand, numerator_result=num_res,
    #         denominator_result=den_res, result=self.get_score(num_res, den_res),
    #         identifier_parent=brand_identifier, identifier_result=sub_brand_dict)
    #
    # def insert_totals_to_db_on(self, all_passed_results, kpi_name, total_kind, weight, identifier_result=None):
    #     """
    #     inserting all total level (includes segment and national) into DB
    #     :param all_passed_results: 'passed' column from all_results
    #     :param kpi_name:
    #     :param weight:
    #     :param total_kind: TOTAL/SEGMENT/NATIONAL
    #     :param identifier_result: optional, if has children
    #     :return:
    #     """
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.DB_OFF_NAMES[kpi_name][total_kind])
    #     num_result, den_result = all_passed_results.sum(), all_passed_results.count()
    #     result = self.get_score(num_result, den_result)
    #     score = result * weight if kpi_name != Const.MSRP else 0
    #     self.common.write_to_db_result(
    #         fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=num_result,
    #         denominator_result=den_result, result=result, identifier_result=identifier_result,
    #         identifier_parent=self.common.get_dictionary(name=total_kind), weight=weight, score=score)
    #     return score
