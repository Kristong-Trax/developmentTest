import os
import json
from datetime import datetime
import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.INBEVCI.Utils.Fetcher import INBEVCIINBEVCIQueries
from Projects.INBEVCI.Utils.Const import Const
from Projects.INBEVCI.Utils.ParseTemplates import parse_template
from KPIUtils.GeneralToolBox import GENERALToolBox
from KPIUtils.DB.Common import Common
from KPIUtils.Calculations.Assortment import Assortment
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

# ASSORTMENT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Assortment.xlsx')
ASSORTMENT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data/just for local calculation', 'Assortment.xlsx')



def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class INBEVCIINBEVCIToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_id = self.data_provider.session_id
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.attr5 = self.get_attribute5()
        self.match_display_in_scene = self.get_match_display()
        self.tools = GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.template_sheet = {}
        for name in Const.SHEET_NAMES:
            self.template_sheet[name] = parse_template(TEMPLATE_PATH, sheet_name=name,
                                                       lower_headers_row_index=1)
        self.template_sheet[Const.STORE_POLICY] = parse_template(ASSORTMENT_PATH,
                                                                 sheet_name=Const.STORE_POLICY,
                                                                 lower_headers_row_index=0)
        self.common = Common(self.data_provider)
        self.assortment = Assortment(self.data_provider, self.output)
        self.current_date = datetime.now()
        self.new_kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_results_new_tables_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_sos_policies = self.get_store_policies()
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.district_name = self.get_district_name()
        self.groups_fk = self.get_groups_fk()

    def get_store_policies(self):
        query = INBEVCIINBEVCIQueries.get_store_policies()
        store_policies = pd.read_sql_query(query, self.rds_conn.db)
        return store_policies

    def main_calculation(self, set_name):
        """
        This function calculates the KPI results.
        """
        if set_name in (Const.BRAND_FACING_TARGET, Const.BRAND_COMPARISON):
            self.calculate_kpi_level_1(set_name)
        elif set_name == Const.SOS:
            self.main_sos_calculation()
        elif set_name == Const.ASSORTMENT:
            self.main_assortment_calculation()
        elif set_name == Const.TOP_BRAND_BLOCK:
            self.calculate_block_together_sets(set_name)
        else:
            return

    def calculate_kpi_level_1(self, set_name):
        sum_of_total, sum_of_passed = 0, 0
        set_fk = self.get_kpi_fk_by_kpi_name(set_name)
        for i in xrange(len(self.template_sheet[set_name])):
            params = self.template_sheet[set_name].iloc[i]
            if set_name == Const.BRAND_FACING_TARGET:
                if self.attr5 not in params[Const.ATTR5].split(', '):
                    continue
                start_date = datetime.strptime(params["Start date"], '%Y-%m-%d  %H:%M:%S').date()
                end_date = '' if params["End date"] == '' else datetime.strptime(params["End date"], '%Y-%m-%d  %H:%M:%S').date()
                if self.visit_date < start_date or (end_date != '' and self.visit_date > end_date):
                    continue
                result_dict = self.calculate_brand_facing(params)
            elif set_name == Const.BRAND_COMPARISON:
                result_dict = self.calculate_brand_comparison(params)
            sum_of_passed += result_dict['score'] / 100
            sum_of_total += 1
            self.common.write_to_db_result_new_tables(
                fk=result_dict['fk'], result=result_dict['result'], score=result_dict['score'],
                numerator_result=result_dict['numerator_result'], denominator_result=result_dict['denominator_result'],
                numerator_id=result_dict['numerator_id'], denominator_id=0,
                target=result_dict["denominator_result_after_actions"],
                denominator_result_after_actions=result_dict["denominator_result_after_actions"])
        if sum_of_total == 0:
            return 0
        percentage = round(sum_of_passed / float(sum_of_total), 4) * 100
        set_score = (percentage >= 100) * 100
        self.common.write_to_db_result_new_tables(fk=set_fk, result=percentage, score=set_score, numerator_id=0,
                                                  numerator_result=sum_of_passed, denominator_result=sum_of_total)

    def calculate_brand_comparison(self, params):
        """
        getting a template that each line has some Inbev's SKUs and some competitor's SKUs, and comparing them.
        write to DB in 4 lines:
        4 - how many facings the specific SKU has.
        3 - how many facings the specific brand has.
        2 - comparison between the both brands.
        1 - some of all the comparisons.
        """
        level_2_fk = self.get_kpi_fk_by_kpi_name(Const.COMPARISON_LEVEL_2)
        atomic_score = 0
        inbev_skus, inbev_brand_fk = self.get_skus_and_brand_fk("Inbev", params)
        comp_skus, comp_brand_fk = self.get_skus_and_brand_fk("Competitor", params)
        inbev_facings = self.calculate_comparison_level_brand(inbev_skus, inbev_brand_fk, inbev_brand_fk)
        comp_facings = self.calculate_comparison_level_brand(comp_skus, comp_brand_fk, inbev_brand_fk)
        if inbev_facings >= comp_facings:
            atomic_score = 100
        result_dict = {"fk": level_2_fk, "score": atomic_score, "result": inbev_facings,
                       "numerator_result": inbev_facings, "denominator_result": comp_facings,
                       "numerator_id": inbev_brand_fk, "denominator_result_after_actions": comp_brand_fk}
        return result_dict

    def calculate_brand_facing(self, params):
        """
        getting a list of brands and a target for each brand, and:
        for each line, write if it passed the target.
        after that, write how many lines passed.
        """
        atomic_score = 0
        brand = params[Const.BRAND]
        target = int(params[Const.TARGET])
        atomic_fk = self.get_kpi_fk_by_kpi_name(Const.ATOMIC_FACINGS)
        facings = self.tools.calculate_availability(**{"brand_name": brand})
        if facings >= target:
            atomic_score = 100
        brand_fk = self.get_brand_fk(brand)
        result = round(facings / float(target), 4) * 100
        result_dict = {"fk": atomic_fk, "result": result, "score": atomic_score, "numerator_result": facings,
                       "denominator_result": target, "numerator_id": brand_fk,
                       "denominator_result_after_actions": target}
        return result_dict

    def get_skus_and_brand_fk(self, name, params):
        """
        gets a name and a line, and returns list of SKUs and its brand
        :param name: "Inbev" or "Competitor"
        :param params: {"SKUs Inbev": "x, y, z", "SKUs Competitor": "a, b, c"}
        :return: [x, y, z], 36
        """
        skus = params["SKUs " + name].split(', ')
        brand_fk = self.get_brand_fk_from_skus(skus)
        return skus, brand_fk

    def get_brand_fk_from_skus(self, skus):
        """
        gets list of SKUs and returns its brand_fk
        :param skus:
        :return:
        """
        brands = self.all_products[self.all_products['product_ean_code'].isin(skus)]['brand_fk'].unique().tolist()
        brand_fk = -1
        if len(brands) >= 1:
            brand_fk = brands[0]
        return brand_fk

    def calculate_comparison_level_brand(self, skus, brand_fk, inbev_brand_fk):
        """
        :param skus: list
        :param brand_fk: num
        :param inbev_brand_fk: num
        :return: facings of this list
        """
        facings = self.calculate_skus_facings(skus, brand_fk)
        level3_fk = self.get_kpi_fk_by_kpi_name(Const.COMPARISON_LEVEL_3)
        self.common.write_to_db_result_new_tables(fk=level3_fk, result=facings, denominator_id=inbev_brand_fk,
                                                  numerator_result=facings, numerator_id=brand_fk)
        return facings

    def calculate_skus_facings(self, skus, brand_fk):
        """
        :param skus: list
        :param brand_fk: num
        :return: facings of this list
        """
        facings = 0
        level4_fk = self.get_kpi_fk_by_kpi_name(Const.COMPARISON_LEVEL_4)
        for sku in skus:
            level4_filter = {"product_ean_code": sku}
            level4_facings = self.tools.calculate_availability(**level4_filter)
            facings += level4_facings
            product_fk = self.all_products[self.all_products['product_ean_code'] == sku]['product_fk'].iloc[0]
            self.common.write_to_db_result_new_tables(fk=level4_fk, result=level4_facings, denominator_id=brand_fk,
                                                      numerator_result=level4_facings, numerator_id=product_fk)
        return facings

    def get_eye_level_shelves(self, shelves_num):
        """
        :param shelves_num: num of shelves in specific bay
        :return: list of eye shelves
        """
        eye_levels = self.template_sheet[Const.GOLDEN_SHELVES][["num. of shelves min", "num. of shelves max",
                                                                "num. ignored from top",
                                                                "num. ignored from bottom"]].astype(int)
        res_table = eye_levels[(eye_levels["num. of shelves max"] >= shelves_num) &
                               (eye_levels["num. of shelves min"] <= shelves_num)][
            ["num. ignored from top", "num. ignored from bottom"]]
        start_shelf = res_table['num. ignored from top'].iloc[0] + 1
        end_shelf = shelves_num - res_table['num. ignored from bottom'].iloc[0]
        final_shelves = range(start_shelf, end_shelf + 1)
        return final_shelves

    def calculate_eye_level(self, row):
        """
        :param row: "assortment" line
        :return: 1 if this product exists in eye level, 0 otherwise
        """
        scene_recognition_flag = False
        is_eye_level = 0
        scenes_to_check = self.match_product_in_scene['scene_fk'].unique().tolist()
        for scene in scenes_to_check:
            if set(Const.ALL_PALLETS) & set(
                    [display for display in
                     self.match_display_in_scene[self.match_display_in_scene['scene_fk'] == scene][
                         'name'].values]):
                scene_recognition_flag = True
            bays = self.match_product_in_scene[(self.match_product_in_scene['scene_fk'] == scene)][
                'bay_number'].unique().tolist()
            bays_results_dict = {}
            for bay in bays:
                shelves_result_dict = {}
                bay_data = self.match_product_in_scene[
                    (self.match_product_in_scene['scene_fk'] == scene) & (
                        self.match_product_in_scene['bay_number'] == bay)]
                if scene_recognition_flag:
                    bay_match_display_in_scene = self.match_display_in_scene[
                        (self.match_display_in_scene['scene_fk'] == scene) &
                        (self.match_display_in_scene['bay_number'] == bay)]
                    bay_displays = bay_match_display_in_scene['name'].unique().tolist()
                    if bay_displays:
                        if bay_displays[0] in Const.PALLET:
                            factor = Const.PALLET_FACTOR
                        else:
                            factor = Const.HALF_PALLET_FACTOR
                    else:
                        factor = 1
                    number_of_shelves = len(bay_data['shelf_number'].unique()) + (int(factor) - 1)
                else:
                    number_of_shelves = len(bay_data['shelf_number'].unique())
                bay_eye_level_shelves = self.get_eye_level_shelves(number_of_shelves)
                for shelf in bay_eye_level_shelves:
                    filters = {'product_fk': row.product_fk, 'bay_number': bay, 'shelf_number': shelf,
                               'scene_fk': scene}
                    object_facings = self.tools.calculate_assortment(**filters)
                    if object_facings > 0:
                        shelves_result_dict[shelf] = 1
                        break
                    else:
                        shelves_result_dict[shelf] = 0
                if 1 in shelves_result_dict.values():
                    bays_results_dict[bay] = 1
                    break
                else:
                    bays_results_dict[bay] = 0
            if 1 in bays_results_dict.values():
                is_eye_level = 1
                break
        return is_eye_level

    def main_assortment_calculation(self):
        """
        This function calculates the KPI results.
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if lvl3_result.empty:
            return
        eye_level_sku_fk = self.get_kpi_fk_by_kpi_name(Const.EYE_LEVEL_SKU)
        eye_level_fk = self.get_kpi_fk_by_kpi_name(Const.EYE_LEVEL)
        must_have_fk = self.get_kpi_fk_by_kpi_name(Const.MUST_HAVE_SKU)
        total_eye_level, passed_eye_level = 0, 0
        for result in lvl3_result.itertuples():
            if result.kpi_fk_lvl3 == eye_level_sku_fk:
                if result.in_store == 0:
                    continue
                numerator_res = self.calculate_eye_level(result)
                total_eye_level += 1
                passed_eye_level += numerator_res
                score = numerator_res * 100
            else:
                score = result.in_store * 100
                numerator_res = result.in_store
            self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl3, result=score, score=score,
                                                      numerator_id=result.product_fk, denominator_result=1,
                                                      numerator_result=numerator_res,
                                                      denominator_id=result.assortment_group_fk)
        must_have_results = lvl3_result[lvl3_result['kpi_fk_lvl3'] == must_have_fk]
        self.calculate_oos(must_have_results)

        lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
        for result in lvl2_result.itertuples():
            super_group_fk = result.assortment_super_group_fk
            denominator_after_action = None
            if not super_group_fk or np.isnan(super_group_fk):
                super_group_fk = 0
            if result.kpi_fk_lvl2 == eye_level_fk:
                numerator_res = passed_eye_level
                denominator_res = total_eye_level
            else:
                denominator_res = result.total
                numerator_res = result.passes
                if result.target:
                    # if result.group_target_date: #TODO need to check why the targets are not there in the groups
                    #     if result.group_target_date <= self.current_date:
                    #         denominator_after_action = round(np.divide(float(result.target), float(
                    #             result.total)) * 100, 2)
                    # else:
                    #     denominator_after_action = round(np.divide(float(result.target), float(
                    #         result.total)) * 100, 2)
                    denominator_after_action = round(np.divide(float(result.target), float(
                        result.total)) * 100, 2)
            res = round(np.divide(float(numerator_res), float(denominator_res)) * 100, 2)
            score = 100 * (res >= denominator_after_action) if denominator_after_action else 100 * (res >= 100)
            self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl2, result=res, score=score,
                                                      numerator_id=result.assortment_group_fk,
                                                      numerator_result=numerator_res,
                                                      denominator_id=super_group_fk, denominator_result=denominator_res,
                                                      target=denominator_after_action,
                                                      denominator_result_after_actions=denominator_after_action)
        if lvl2_result.empty:
            return
        lvl1_result = self.assortment.calculate_lvl1_assortment(lvl2_result)
        lvl1_result = self.update_targets(lvl1_result)
        for result in lvl1_result.itertuples():
            denominator_res = result.total
            numerator_res = result.passes
            denominator_after_action = None
            if result.super_group_target:
                denominator_after_action = round(np.divide(
                    float(result.super_group_target), float(denominator_res)) * 100, 2)
            res = round(
                np.divide(float(numerator_res), float(denominator_res)) * 100, 2)
            score = 100 * (res >= denominator_after_action) if denominator_after_action else 100 * (res >= 100)
            self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl1, result=res, score=score,
                                                      numerator_result=numerator_res,
                                                      denominator_result=denominator_res,
                                                      numerator_id=result.assortment_super_group_fk,
                                                      target=denominator_after_action,
                                                      denominator_result_after_actions=denominator_after_action)

    def update_targets(self, lvl1_result):
        """
        It takes lvl1 and adds the super group target for it, until the generic code will do that
        :param lvl1_result: DF
        :return: lvl1_result with targets
        """
        super_groups = self.template_sheet[Const.STORE_POLICY]
        super_groups = super_groups[super_groups[Const.GRANULAR_GROUP_NAME] == self.attr5[0]]
        for i in xrange(len(super_groups)):
            line = super_groups.iloc[i]
            super_group_target = line[Const.TARGET]
            assortment_type = line[Const.ASSORTMENT_TYPE]
            assortment_kpi_fk = self.get_kpi_fk_by_kpi_name(assortment_type)
            lvl1_result.loc[lvl1_result.kpi_fk_lvl1 == assortment_kpi_fk, 'super_group_target'] = super_group_target
        return lvl1_result

    def calculate_oos(self, must_have_results):
        """
        gets a dataframe with all the results of must have, and write all the OOS ones
        :param must_have_results: dataframe
        """
        oos_fk = self.get_kpi_fk_by_kpi_name(Const.OOS_KPI)
        oos_sku_fk = self.get_kpi_fk_by_kpi_name(Const.OOS_SKU_KPI)
        for res in must_have_results.itertuples():
            result = 1 if not res.in_store else 0
            self.common.write_to_db_result_new_tables(fk=oos_sku_fk, result=result, score=result * 100,
                                                      denominator_id=oos_fk, denominator_result=1,
                                                      numerator_id=res.product_fk, numerator_result=result)
        oos_numerator = len(must_have_results[must_have_results['in_store'] == 0])
        denominator = len(must_have_results['in_store'])
        oos_res = 0
        if denominator:
            oos_res = np.divide(float(oos_numerator), float(denominator)) * 100
        self.common.write_to_db_result_new_tables(fk=oos_fk, result=oos_res, score=oos_res,
                                                  denominator_result=denominator, numerator_result=oos_numerator,
                                                  numerator_id=oos_fk)

    def main_sos_calculation(self):
        """
        calculates the SOS KPIs
        """
        relevant_stores = pd.DataFrame(columns=self.store_sos_policies.columns)
        for row in self.store_sos_policies.itertuples():
            policies = json.loads(row.store_policy)
            store_info = self.store_info
            for key, value in policies.items():
                store_info = store_info[store_info[key].isin(value)]
            if not store_info.empty:
                visit_date = self.visit_date
                stores = self.store_sos_policies[(self.store_sos_policies['store_policy'] == row.store_policy) &
                                                 (self.store_sos_policies['target_validity_start_date'] <= visit_date) &
                                                 (self.store_sos_policies['target_validity_end_date'] >= visit_date)]

                stores_with_no_end_date = self.store_sos_policies[
                                                 (self.store_sos_policies['store_policy'] == row.store_policy) &
                                                 (self.store_sos_policies['target_validity_start_date'] <= visit_date) &
                                                 (self.store_sos_policies['target_validity_end_date'].isnull())]
                stores = stores.append(stores_with_no_end_date)
                if stores.empty:
                    relevant_stores = stores
                else:
                    relevant_stores = relevant_stores.append(stores, ignore_index=True)
        relevant_stores = relevant_stores.drop_duplicates(subset=['kpi', 'sku_name', 'target', 'sos_policy'],
                                                          keep='last')
        for row in relevant_stores.itertuples():
            sos_policy = json.loads(row.sos_policy)
            numerator_key = sos_policy[Const.NUMERATOR].keys()[0]
            denominator_key = sos_policy[Const.DENOMINATOR].keys()[0]
            numerator_val = sos_policy[Const.NUMERATOR][numerator_key]
            denominator_val = sos_policy[Const.DENOMINATOR][denominator_key]
            if numerator_key == 'manufacturer':
                numerator_key += '_local_name'
            numerator = self.scif[(self.scif[numerator_key].str.upper() == numerator_val.upper()) &
                                  (self.scif[denominator_key].str.upper() == denominator_val.upper())][
                'gross_len_ign_stack'].sum()
            denominator = self.scif[self.scif[denominator_key].str.upper() == denominator_val.upper()][
                'gross_len_ign_stack'].sum()
            if self.all_products[
                        self.all_products[numerator_key].str.upper() == numerator_val.upper()].empty and \
                            numerator_val.upper() == "CCC":
                numerator_val = "ABI Inbev"
            if (self.all_products[
                        self.all_products[numerator_key].str.upper() == numerator_val.upper()].empty) or (
                    self.all_products[self.all_products[
                        denominator_key].str.upper() == denominator_val.upper()].empty):
                Log.error("the DB does not match the template of SOS")
                continue
            numerator_id = self.all_products[self.all_products[numerator_key].str.upper() ==
                                             numerator_val.upper()][numerator_key.split('_')[0] + '_fk'].values[0]
            denominator_id = self.all_products[self.all_products[denominator_key].str.upper() ==
                                               denominator_val.upper()][denominator_key + '_fk'].values[0]
            sos = 0
            if numerator and denominator:
                sos = round(np.divide(float(numerator), float(denominator)) * 100, 2)
            target = row.target * 100
            score = (sos >= target) * 100
            self.common.write_to_db_result_new_tables(fk=row.kpi, result=sos, score=score,
                                                      numerator_result=numerator, numerator_id=numerator_id,
                                                      denominator_id=denominator_id, denominator_result=denominator,
                                                      target=target,
                                                      denominator_result_after_actions=target)

    def validate_groups_exist(self):
        groups_template = self.template_sheet[Const.TOP_BRAND_BLOCK][Const.ATOMIC_NAME].unique().tolist()
        groups_DB = self.groups_fk[Const.GROUP_NAME].unique().tolist()
        groups_to_add = []
        for group in groups_template:
            if group not in groups_DB:
                groups_to_add.append(group)
        if len(groups_to_add) == 0:
            return
        queries = []
        for group in groups_to_add:
            queries.append(INBEVCIINBEVCIQueries.insert_group_to_pservice(group))
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        for query in queries:
            Log.debug("Query '{}' is running".format(query))
            cur.execute(query)
        self.rds_conn.db.commit()
        self.groups_fk = self.get_groups_fk()

    def calculate_block_together_sets(self, set_name):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        self.validate_groups_exist()
        scores = []
        if not self.district_name:
            Log.error("The session has no district, please define a district")
            return
        parameters_df = self.template_sheet[set_name]
        product_groups = parameters_df[Const.ATOMIC_NAME].unique().tolist()
        level1_fk = self.get_kpi_fk_by_kpi_name(Const.TOP_BRAND_BLOCK)
        level2_fk = self.get_kpi_fk_by_kpi_name(Const.BRAND_BLOCK)
        for group in product_groups:
            relevant_df = parameters_df[(parameters_df[Const.ATOMIC_NAME] == group) &
                                        (parameters_df['District'].str.upper() == self.district_name.upper())]
            group_pk = self.groups_fk[self.groups_fk['group_name'] == group]['pk'].iloc[0]
            if not relevant_df.empty:
                products_for_block = relevant_df['EAN Number'].unique().tolist()
                result = self.tools.calculate_block_together(product_ean_code=products_for_block,
                                                             template_group=relevant_df['Scene Type'].values[0])
                score = 1 * result
                scores.append(score)
                self.common.write_to_db_result_new_tables(fk=level2_fk, score=score * 100, result=score,
                                                          numerator_result=score, numerator_id=group_pk)
        if not scores:
            set_score = 0
            target = 0
        else:
            set_score = round((sum(scores) / float(len(scores))) * 100, 2)
            target = len(scores)
        self.common.write_to_db_result_new_tables(fk=level1_fk, score=set_score, result=set_score,
                                                  numerator_result=sum(scores), denominator_result=target,
                                                  numerator_id=0)
        return

    def get_attribute5(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        """
        query = INBEVCIINBEVCIQueries.get_attribute5(self.session_uid)
        attr5 = pd.read_sql_query(query, self.rds_conn.db)
        return attr5.values[0]

    def get_district_name(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        """
        district_fk = self.store_info.get('district_fk')[0]
        if not district_fk:
            return None
        query = INBEVCIINBEVCIQueries.get_district_name(district_fk)
        district_name = pd.read_sql_query(query, self.rds_conn.db)
        return district_name.values[0][0]

    def get_groups_fk(self):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        """
        query = INBEVCIINBEVCIQueries.get_groups_fk()
        groups = pd.read_sql_query(query, self.rds_conn.db)
        return groups

    def get_brand_fk(self, brand_name):
        """
            This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        """
        return self.all_products[
            self.all_products['brand_name'].str.lower() == brand_name.lower()]['brand_fk'].unique().tolist()[0]

    def get_kpi_fk_by_kpi_name(self, kpi_name):
        """
        convert kpi name to kpi_fk
        :param kpi_name: string
        :return: fk
        """
        assert isinstance(kpi_name, (unicode, basestring)), "name is not a string: %r" % kpi_name
        column_key = 'pk'
        column_value = 'client_name'
        try:
            return self.new_kpi_static_data[
                self.new_kpi_static_data[column_value] == kpi_name][column_key].values[0]
        except IndexError:
            Log.info('Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        """
        query = INBEVCIINBEVCIQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display
