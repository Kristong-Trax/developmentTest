__author__ = 'limor'

import pandas as pd
import numpy as np
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.DB.Common import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import Consts
from Trax.Utils.Logging.Logger import Log


class CCUSLiveDemoToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.commonV2 = CommonV2(self.data_provider)
        self.common = Common(self.data_provider)
        # self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)
        self.assortment = Assortment(self.data_provider, self.output)

        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.new_kpi_static_data = self.commonV2.get_new_kpi_static_data()
        self.all_products_include_deleted = self.data_provider[Data.ALL_PRODUCTS_INCLUDING_DELETED]
        self.visit_date = self.data_provider[Data.VISIT_DATE]

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        # Global assortment kpis
        # assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v3()
        assortment_res_dict = self.assortment_calc()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)
        self.commonV2.commit_results_data()

    def assortment_calc(self):
            """
            This function calculates the KPI results.
            """
            dict_list = []
            diageo_fk = self.own_manuf_fk
            store_fk = self.store_info[Consts.STORE_FK].iloc[0]
            lvl3_result = self.assortment.calculate_lvl3_assortment()

            if lvl3_result.empty:
                return dict_list

            # remove live session kpis from regular kpi calculation
            live_session_kpis = list(
                self.new_kpi_static_data[self.new_kpi_static_data['live_session_relevance'] == 1]['pk'])
            lvl3_result = lvl3_result[~lvl3_result.kpi_fk_lvl2.isin(live_session_kpis)]
            if lvl3_result.empty:
                return dict_list

            dist_kpi_fk = \
            self.new_kpi_static_data[(self.new_kpi_static_data['client_name'] == Consts.DISTRIBUTION_KPI) |
                                     (self.new_kpi_static_data['type'] == Consts.DISTRIBUTION_KPI)][
                Consts.PK].values[0]
            kpis = self.new_kpi_static_data[self.new_kpi_static_data['type'].isin(Consts.LMPA_KPIS)][Consts.PK]
            if kpis.empty:
                lvl3_result = pd.merge(
                    lvl3_result,
                    self.all_products_include_deleted[[Consts.PRODUCT_FK, 'category_fk']], on=[Consts.PRODUCT_FK])
            else:
                lvl3_result = pd.merge(lvl3_result[~(lvl3_result['kpi_fk_lvl2'].isin(kpis.values))],
                                       self.all_products_include_deleted[[Consts.PRODUCT_FK, 'category_fk']],
                                       on=[Consts.PRODUCT_FK])

            if not lvl3_result.empty:
                for result in lvl3_result.itertuples():
                    score = 2 if result.in_store == 1 else 1
                    kpi_name = \
                        self.new_kpi_static_data[self.new_kpi_static_data[Consts.PK] == result.kpi_fk_lvl3][
                            'type'].values[
                            0] + Consts.CATEGORY_LEVEL_KPI
                    kpi_fk_by_manu = \
                        self.new_kpi_static_data[self.new_kpi_static_data['type'] == kpi_name][Consts.PK].values[0]

                    identifier_level_2 = self.commonV2.get_dictionary(
                        category_fk=result.category_fk,
                        kpi_fk=result.kpi_fk_lvl2
                    )

                    # by category
                    dict_list.append(
                        self.build_dictionary_for_db_insert_v2(
                            fk=kpi_fk_by_manu, numerator_id=result.product_fk, numerator_result=result.facings,
                            result=score, score=score, denominator_id=result.category_fk, denominator_result=1,
                            identifier_parent=identifier_level_2, should_enter=True))

                dist_results = lvl3_result[lvl3_result['kpi_fk_lvl3'] == dist_kpi_fk]
                for res in dist_results.itertuples():
                    kpi_fk = \
                        self.new_kpi_static_data[
                            self.new_kpi_static_data['type'] == Consts.OOS_SKU_KPI + Consts.CATEGORY_LEVEL_KPI][
                            Consts.PK].values[0]
                    parent_kpi_fk = \
                        self.new_kpi_static_data[
                            self.new_kpi_static_data['type'] == Consts.OOS_KPI + Consts.CATEGORY_LEVEL_KPI][
                            Consts.PK].values[0]
                    num_res = res.facings
                    if not res.in_store:
                        is_oos = 1
                        identifier_level_2 = self.commonV2.get_dictionary(
                            category_fk=res.category_fk,
                            kpi_fk=parent_kpi_fk
                        )
                        dict_list.append(self.build_dictionary_for_db_insert_v2(
                            fk=kpi_fk, numerator_id=res.product_fk, numerator_result=num_res, result=is_oos,
                            score=is_oos,
                            denominator_id=res.category_fk, denominator_result=1, identifier_parent=identifier_level_2))

                lvl2_result = self.assortment.calculate_lvl2_assortment_v2(lvl3_result)
                for result in lvl2_result.itertuples():
                    kpi_name = self.new_kpi_static_data[self.new_kpi_static_data[Consts.PK] ==
                                                        result.kpi_fk_lvl2]['type'].values[
                                   0] + Consts.CATEGORY_LEVEL_KPI
                    identifier_level_2 = self.commonV2.get_dictionary(category_fk=result.category_fk,
                                                                      kpi_fk=result.kpi_fk_lvl2)
                    identifier_level_1 = self.commonV2.get_dictionary(kpi_fk=result.kpi_fk_lvl2)
                    kpi_fk_by_cat = \
                        self.new_kpi_static_data[self.new_kpi_static_data['type'] == kpi_name][Consts.PK].values[0]
                    denominator_res = result.total
                    if not pd.isnull(result.target) and not pd.isnull(
                            result.group_target_date) and result.group_target_date.date() <= self.visit_date:
                        denominator_res = result.target
                    res = np.divide(float(result.passes), float(denominator_res)) * 100
                    if res >= 100:
                        score = 100
                    else:
                        score = 0
                    res = 100 if res > 100 else res

                    # Manufacturer level KPI
                    dict_list.append(self.build_dictionary_for_db_insert_v2(
                        fk=kpi_fk_by_cat, numerator_id=result.category_fk, numerator_result=result.passes,
                        result=res, score=score, denominator_id=diageo_fk,
                        denominator_result=denominator_res, identifier_result=identifier_level_2,
                        identifier_parent=identifier_level_1, should_enter=True))

                dist_fk = self.new_kpi_static_data[self.new_kpi_static_data['type'] ==
                                                   Consts.DISTRIBUTION_LVL2][Consts.PK].values[0]
                dist_results = lvl2_result[lvl2_result['kpi_fk_lvl2'] == dist_fk]
                for res in dist_results.itertuples():
                    kpi_fk = \
                        self.new_kpi_static_data[
                            self.new_kpi_static_data['type'] == Consts.OOS_KPI + Consts.CATEGORY_LEVEL_KPI][
                            Consts.PK].values[0]
                    result = np.divide(float(res.total - res.passes), float(res.total)) * 100
                    identifier_level_1 = self.commonV2.get_dictionary(kpi_fk=kpi_fk)
                    if result >= 100:
                        score = 0
                    else:
                        score = 100
                    identifier_level_2 = self.commonV2.get_dictionary(category_fk=res.category_fk, kpi_fk=kpi_fk)
                    num_res = res.total - res.passes
                    dict_list.append(self.build_dictionary_for_db_insert_v2(
                        fk=kpi_fk, numerator_id=res.category_fk, numerator_result=num_res, result=result, score=score,
                        denominator_id=diageo_fk, denominator_result=res.total, identifier_result=identifier_level_2,
                        identifier_parent=identifier_level_1, should_enter=True))

                if not lvl2_result.empty:
                    lvl1_result = self.assortment.calculate_lvl1_assortment_v2(lvl2_result)
                    for result in lvl1_result.itertuples():
                        kpi_level_1_name = \
                            self.new_kpi_static_data[self.new_kpi_static_data[Consts.PK] == result.kpi_fk_lvl2][
                                'type'].values[
                                0] + Consts.MANUFACTURER_LEVEL_KPI
                        kpi_level_1 = \
                            self.new_kpi_static_data[self.new_kpi_static_data['type'] == kpi_level_1_name][
                                Consts.PK].values[0]
                        identifier_level_1 = self.commonV2.get_dictionary(kpi_fk=result.kpi_fk_lvl2)
                        denominator_res = result.total
                        res = np.divide(float(result.passes), float(denominator_res)) * 100
                        if res >= 100:
                            score = 100
                        else:
                            score = 0
                        dict_list.append(self.build_dictionary_for_db_insert_v2(
                            fk=kpi_level_1, numerator_id=diageo_fk,
                            numerator_result=result.passes, result=res, score=score, denominator_id=store_fk,
                            denominator_result=denominator_res, identifier_result=identifier_level_1,
                            should_enter=False))
                    dist_fk = self.new_kpi_static_data[self.new_kpi_static_data['type'] ==
                                                       Consts.DISTRIBUTION_LVL2][Consts.PK].values[0]
                    dist_results = lvl1_result[lvl1_result['kpi_fk_lvl2'] == dist_fk]
                    for res in dist_results.itertuples():
                        kpi_fk = self.new_kpi_static_data[self.new_kpi_static_data['type'] == Consts.OOS_KPI +
                                                          Consts.MANUFACTURER_LEVEL_KPI][Consts.PK].values[0]
                        result = np.divide(float(res.total - res.passes), float(res.total)) * 100
                        if result >= 100:
                            score = 100
                        else:
                            score = 0
                        kpi_ident = self.new_kpi_static_data[self.new_kpi_static_data['type'] == Consts.OOS_KPI +
                                                             Consts.CATEGORY_LEVEL_KPI][Consts.PK].values[0]
                        identifier_level_1 = self.commonV2.get_dictionary(kpi_fk=kpi_ident)
                        dict_list.append(self.build_dictionary_for_db_insert_v2(
                            fk=kpi_fk, numerator_id=diageo_fk, numerator_result=res.passes, result=result, score=score,
                            denominator_id=store_fk, denominator_result=res.total,
                            identifier_result=identifier_level_1))
            return dict_list

    def build_dictionary_for_db_insert_v2(self, fk=None, kpi_name=None, numerator_id=0, numerator_result=0, result=0,
                                          denominator_id=0, denominator_result=0, score=0,
                                          denominator_result_after_actions=None, context_id=None, target=None,
                                          identifier_parent=None, identifier_result=None, should_enter=None):
        try:
            insert_params = dict()
            if not fk:
                if not kpi_name:
                    return
                else:
                    insert_params['fk'] = self.common.get_kpi_fk_by_kpi_name_new_tables(kpi_name)
            else:
                insert_params['fk'] = fk
            insert_params['numerator_id'] = numerator_id
            insert_params['numerator_result'] = numerator_result
            insert_params['denominator_id'] = denominator_id
            insert_params['denominator_result'] = denominator_result
            insert_params['result'] = result
            insert_params['score'] = score
            if target:
                insert_params['target'] = target
            if denominator_result_after_actions:
                insert_params['denominator_result_after_actions'] = denominator_result_after_actions
            if context_id:
                insert_params['context_id'] = context_id
            if identifier_parent:
                insert_params['identifier_parent'] = identifier_parent
                insert_params['should_enter'] = True
            if identifier_result:
                insert_params['identifier_result'] = identifier_result
            if should_enter:
                insert_params['should_enter'] = should_enter
            return insert_params
        except IndexError:
            Log.error('error in build_dictionary_for_db_insert')
            return None