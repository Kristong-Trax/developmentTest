from datetime import datetime
import json

import numpy as np
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider

from Projects.HEINZCR.Config.Const import Const

__author__ = 'Eli and Hunter'


class HEINZCRToolBox:
    LVL3_HEADERS = ['assortment_group_fk', 'assortment_fk', 'target', 'product_fk',
                    'in_store', 'kpi_fk_lvl1', 'kpi_fk_lvl2', 'kpi_fk_lvl3', 'group_target_date',
                    'assortment_super_group_fk']
    LVL2_HEADERS = ['assortment_group_fk', 'assortment_fk', 'target', 'passes', 'total',
                    'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date']
    LVL1_HEADERS = ['assortment_group_fk', 'target', 'passes', 'total', 'kpi_fk_lvl1']
    ASSORTMENT_FK = 'assortment_fk'
    ASSORTMENT_GROUP_FK = 'assortment_group_fk'
    ASSORTMENT_SUPER_GROUP_FK = 'assortment_super_group_fk'
    BRAND_VARIENT = 'brand_varient'
    NUMERATOR = 'numerator'
    DENOMINATOR = 'denominator'
    DISTRIBUTION_KPI = 'Distribution - SKU'
    OOS_SKU_KPI = 'OOS - SKU'
    OOS_KPI = 'OOS'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = CommonV2  # remove later
        self.common_v2 = CommonV2(self.data_provider)
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_results_queries = []
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.survey = Survey(self.data_provider, output=self.output, ps_data_provider=self.ps_data_provider,
                             common=self.common_v2)
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.country = self.store_info['country'].iloc[0]
        self.current_date = datetime.now()
        self.extra_spaces_template = pd.read_excel(Const.EXTRA_SPACES_RELEVANT_SUB_CATEGORIES_PATH)
        self.store_targets = pd.read_excel(Const.STORE_TARGETS_PATH)
        self.sub_category_weight = pd.read_excel(Const.SUB_CATEGORY_TARGET_PATH, sheetname='category_score')
        self.kpi_weights = pd.read_excel(Const.SUB_CATEGORY_TARGET_PATH, sheetname='max_weight')
        self.store_assortment = PSAssortmentDataProvider(
            self.data_provider).execute(policy_name=None)
        try:
            self.sub_category_assortment = pd.merge(self.store_assortment,
                                                    self.all_products.loc[:, ['product_fk', 'sub_category',
                                                                              'sub_category_fk']],
                                                    how='left', on='product_fk')
            self.sub_category_assortment = \
                self.sub_category_assortment[~self.sub_category_assortment['assortment_name'].str.contains(
                    'ASSORTMENT')]
            self.sub_category_assortment = pd.merge(self.sub_category_assortment, self.sub_category_weight, how='left',
                                                    left_on='sub_category',
                                                    right_on='Category')


        except KeyError:
            self.sub_category_assortment = pd.DataFrame()
        self.update_score_sub_category_weights()
        try:
            self.store_assortment_without_powerskus = \
                self.store_assortment[self.store_assortment['assortment_name'].str.contains('ASSORTMENT')]
        except KeyError:
            self.store_assortment_without_powerskus = pd.DataFrame()

        self.adherence_results = pd.DataFrame(columns=['product_fk', 'trax_average',
                                                       'suggested_price', 'into_interval', 'min_target', 'max_target'])
        self.extra_spaces_results = pd.DataFrame(
            columns=['sub_category_fk', 'template_fk', 'count'])

        self.powersku_scores = {}
        self.powersku_empty = {}

        self.powersku_bonus = {}
        self.powersku_price = {}
        self.powersku_sos = {}

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        if self.scif.empty:
            return
        # these function must run first
        self.adherence_results = self.heinz_global_price_adherence(pd.read_excel(Const.PRICE_ADHERENCE_TEMPLATE_PATH,
                                                                                 sheetname="Price Adherence"))
        self.extra_spaces_results = self.heinz_global_extra_spaces()
        self.set_relevant_sub_categories()

        # this isn't relevant to the 'Perfect Score' calculation
        self.heinz_global_distribution_per_category()
        self.calculate_assortment()

        perfect_store_score = 0
        self.calculate_powersku_assortment()
        self.main_sos_calculation()
        self.calculate_powersku_price_adherence()
        self.calculate_perfect_store_extra_spaces()
        self.check_bonus_question()

        self.calculate_perfect_sub_category()

        # relevant_target_df = \
        #     self.store_targets[self.store_targets['Country'].str.encode('utf-8') == self.country.encode('utf-8')]
        # if not relevant_target_df.empty:
        #     target = relevant_target_df['Store Execution Score'].iloc[0]
        # else:
        #     target = 0
        #
        # result = 1 if perfect_store_score >= target else 0
        #
        # perfect_store_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.PERFECT_STORE)
        # self.common_v2.write_to_db_result(perfect_store_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
        #                                   denominator_id=self.store_id,
        #                                   result=result, target=target,
        #                                   score=perfect_store_score, identifier_result=Const.PERFECT_STORE)
        # return

    def calculate_assortment(self):
        if self.store_assortment_without_powerskus.empty:
            return

        products_in_store = self.scif[self.scif['facings'] > 0]['product_fk'].unique().tolist()
        pass_count = 0

        total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Distribution')
        identifier_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)

        oos_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('OOS')
        oos_identifier_dict = self.common_v2.get_dictionary(kpi_fk=oos_kpi_fk)

        for row in self.store_assortment_without_powerskus.itertuples():
            result = 0
            if row.product_fk in products_in_store:
                result = 1
                pass_count += 1

            sku_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Distribution - SKU')
            self.common_v2.write_to_db_result(sku_kpi_fk, numerator_id=row.product_fk, denominator_id=row.assortment_fk,
                                              result=result, identifier_parent=identifier_dict, should_enter=True)

            oos_result = 0 if result else 1
            oos_sku_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('OOS - SKU')
            self.common_v2.write_to_db_result(oos_sku_kpi_fk, numerator_id=row.product_fk,
                                              denominator_id=row.assortment_fk,
                                              result=oos_result, identifier_parent=oos_identifier_dict,
                                              should_enter=True)

        number_of_products_in_assortment = len(self.store_assortment_without_powerskus)
        if number_of_products_in_assortment:
            total_result = (pass_count / float(number_of_products_in_assortment)) * 100
            oos_products = number_of_products_in_assortment - pass_count
            oos_result = (oos_products / float(number_of_products_in_assortment)) * 100
        else:
            total_result = 0
            oos_products = number_of_products_in_assortment
            oos_result = number_of_products_in_assortment
        self.common_v2.write_to_db_result(total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          numerator_result=pass_count,
                                          denominator_result=number_of_products_in_assortment,
                                          result=total_result, identifier_result=identifier_dict)
        self.common_v2.write_to_db_result(oos_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          numerator_result=oos_products,
                                          denominator_result=number_of_products_in_assortment,
                                          result=oos_result, identifier_result=oos_identifier_dict)

    def calculate_powersku_assortment(self):
        if self.sub_category_assortment.empty:
            return 0

        total_presense_sku = 0
        parent_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.PERFECT_STORE_SUB_CATEGORY)
        total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_TOTAL)
        sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_SUB_CATEGORY)
        sku_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU)
        target_kpi_weight = float(
            self.kpi_weights['Score'][self.kpi_weights['KPIs'] == Const.KPI_WEIGHTS['POWERSKU']].iloc[
                0])

        kpi_weight = self.get_kpi_weight('POWERSKU')

        products_in_session = self.scif[self.scif['facings'] > 0]['product_fk'].unique().tolist()
        self.sub_category_assortment['in_session'] = \
            self.sub_category_assortment.loc[:, 'product_fk'].isin(products_in_session)

        # save PowerSKU results at SKU level
        for sku in self.sub_category_assortment[
            ['product_fk', 'sub_category_fk', 'in_session', 'sub_category']].itertuples():
            parent_dict = self.common_v2.get_dictionary(
                kpi_fk=sub_category_kpi_fk, sub_category_fk=sku.sub_category_fk)
            relevant_sub_category_df = self.sub_category_assortment[
                self.sub_category_assortment['sub_category'] == sku.sub_category]
            if relevant_sub_category_df.empty:
                sub_category_count = 0
            else:
                sub_category_count = len(relevant_sub_category_df)

            result = 1 if sku.in_session else 0

            score = result * (target_kpi_weight / float(sub_category_count))
            self.common_v2.write_to_db_result(sku_kpi_fk, numerator_id=sku.product_fk,
                                              denominator_id=sku.sub_category_fk, score=score,
                                              result=result, identifier_parent=parent_dict, should_enter=True)
        # save PowerSKU results at sub_category level

        aggregated_results = self.sub_category_assortment.groupby('sub_category_fk').agg(
            {'in_session': 'sum', 'product_fk': 'count'}).reset_index().rename(
            columns={'product_fk': 'product_count'})
        aggregated_results['percent_complete'] = \
            aggregated_results.loc[:, 'in_session'] / aggregated_results.loc[:, 'product_count']
        aggregated_results['result'] = aggregated_results['percent_complete']
        for sub_category in aggregated_results.itertuples():
            # parent_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)
            identifier_dict = self.common_v2.get_dictionary(kpi_fk=sub_category_kpi_fk,
                                                            sub_category_fk=sub_category.sub_category_fk)
            weight_percent = 0
            if self.country in self.sub_category_assortment.columns.to_list():
                weight_value = self.sub_category_assortment[self.country][
                    (self.sub_category_assortment.sub_category_fk == sub_category.sub_category_fk)].iloc[0]
                if not pd.isna(weight_value):
                    weight_percent = weight_value * .01

            result = sub_category.result
            score = result * kpi_weight

            self.powersku_scores[sub_category.sub_category_fk] = score
            self.common_v2.write_to_db_result(sub_category_kpi_fk, numerator_id=sub_category.sub_category_fk,
                                              denominator_id=self.store_id,
                                              identifier_parent=sub_category.sub_category_fk,
                                              identifier_result=identifier_dict, result=result * 100, score=score,
                                              weight=target_kpi_weight, target=target_kpi_weight,
                                              should_enter=True)

    def heinz_global_distribution_per_category(self):
        relevant_stores = pd.DataFrame(columns=self.store_sos_policies.columns)
        for row in self.store_sos_policies.itertuples():
            policies = json.loads(row.store_policy)
            df = self.store_info
            for key, value in policies.items():
                try:
                    df_1 = df[df[key].isin(value)]
                except KeyError:
                    continue
            if not df_1.empty:
                stores = self.store_sos_policies[(self.store_sos_policies['store_policy'] == row.store_policy)
                                                 & (
                                                         self.store_sos_policies[
                                                             'target_validity_start_date'] <= datetime.date(
                                                     self.current_date))]
                if stores.empty:
                    relevant_stores = stores
                else:
                    relevant_stores = relevant_stores.append(stores, ignore_index=True)
        relevant_stores = relevant_stores.drop_duplicates(subset=['kpi', 'sku_name', 'target', 'sos_policy'],
                                                          keep='last')
        for row in relevant_stores.itertuples():
            sos_policy = json.loads(row.sos_policy)
            numerator_key = sos_policy[self.NUMERATOR].keys()[0]
            denominator_key = sos_policy[self.DENOMINATOR].keys()[0]
            numerator_val = sos_policy[self.NUMERATOR][numerator_key]
            denominator_val = sos_policy[self.DENOMINATOR][denominator_key]
            kpi_fk = row.kpi
            target = row.target * 100
            if numerator_key == 'manufacturer':
                numerator_key = numerator_key + '_name'

            if denominator_key == 'sub_category' \
                    and denominator_val.lower() != 'all' \
                    and json.loads(row.store_policy).get('store_type') \
                    and len(json.loads(row.store_policy).get('store_type')) == 1:
                try:
                    denominator_id = self.all_products[self.all_products[denominator_key] == denominator_val][
                        denominator_key + '_fk'].values[0]
                    numerator_id = self.all_products[self.all_products[numerator_key] == numerator_val][
                        numerator_key.split('_')[0] + '_fk'].values[0]

                    # self.common.write_to_db_result_new_tables(fk=12, numerator_id=numerator_id,
                    #                                           numerator_result=None,
                    #                                           denominator_id=denominator_id,
                    #                                           denominator_result=None,
                    #                                           result=target)
                    self.common_v2.write_to_db_result(fk=12, numerator_id=numerator_id, numerator_result=None,
                                                      denominator_id=denominator_id, denominator_result=None,
                                                      result=target)
                except Exception as e:
                    Log.warning(denominator_key + ' - - ' + denominator_val)

    def calculate_perfect_store(self):
        pass

    def calculate_perfect_sub_category(self):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.PERFECT_STORE_SUB_CATEGORY)
        parent_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Const.PERFECT_STORE)

        total_score = 0
        sub_category_fk_list = []
        kpi_type_dict_scores = [self.powersku_scores, self.powersku_empty, self.powersku_price,
                                self.powersku_sos]

        for kpi_dict in kpi_type_dict_scores:
            sub_category_fk_list.extend(kpi_dict.keys())

        kpi_weight_perfect_store = 0
        if self.country in self.sub_category_assortment.columns.to_list():
            kpi_weight_perfect_store = self.sub_category_weight[self.country][
                self.sub_category_weight['Category'] == Const.PERFECT_STORE_KPI_WEIGHT]

            if not kpi_weight_perfect_store.empty:
                kpi_weight_perfect_store = kpi_weight_perfect_store.iloc[0]

        unique_sub_cat_fks = list(dict.fromkeys(sub_category_fk_list))

        sub_category_fks = self.sub_category_weight.sub_category_fk.unique().tolist()
        relevant_sub_cat_list = [x for x in sub_category_fks if str(x) != 'nan']

        # relevant_sub_cat_list = self.sub_category_assortment['sub_category_fk'][
        #     self.sub_category_assortment['Category'] != pd.np.nan].unique().tolist()
        for sub_cat_fk in unique_sub_cat_fks:
            if sub_cat_fk in relevant_sub_cat_list:
                bonus_score = 0
                try:
                    bonus_score = self.powersku_bonus[sub_cat_fk]
                except:
                    pass

                sub_cat_weight = self.get_weight(sub_cat_fk)
                sub_cat_score = self.calculate_sub_category_sum(kpi_type_dict_scores, sub_cat_fk)

                result = sub_cat_score

                score = (result * sub_cat_weight) + bonus_score
                total_score += score

                self.common_v2.write_to_db_result(kpi_fk, numerator_id=sub_cat_fk,
                                                  denominator_id=self.store_id,
                                                  result=result, score=score,
                                                  identifier_parent=parent_kpi,
                                                  identifier_result=sub_cat_fk,
                                                  weight=sub_cat_weight * 100,
                                                  should_enter=True)

        self.common_v2.write_to_db_result(parent_kpi, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          result=total_score, score=total_score,
                                          identifier_result=parent_kpi,
                                          target=kpi_weight_perfect_store,
                                          should_enter=True)

    def main_sos_calculation(self):
        number_of_passing_sub_categories = 0
        relevant_stores = pd.DataFrame(columns=self.store_sos_policies.columns)
        for row in self.store_sos_policies.itertuples():
            policies = json.loads(row.store_policy)
            df = self.store_info
            for key, value in policies.items():
                try:
                    if key != 'additional_attribute_3':
                        df1 = df[df[key].isin(value)]
                except KeyError:
                    continue
            if not df1.empty:
                stores = self.store_sos_policies[(self.store_sos_policies['store_policy'] == row.store_policy)
                                                 & (
                                                         self.store_sos_policies[
                                                             'target_validity_start_date'] <= datetime.date(
                                                     self.current_date))]
                if stores.empty:
                    relevant_stores = stores
                else:
                    relevant_stores = relevant_stores.append(stores, ignore_index=True)

        relevant_stores = relevant_stores.drop_duplicates(subset=['kpi', 'sku_name', 'target', 'sos_policy'],
                                                          keep='last')

        results_df = pd.DataFrame(columns=['sub_category', 'sub_category_fk', 'score'])

        sos_sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_SUB_CATEGORY)
        total_sos_sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(
            Const.SOS_SUB_CATEGORY_TOTAL)

        for row in relevant_stores.itertuples():
            sos_policy = json.loads(row.sos_policy)
            numerator_key = sos_policy[self.NUMERATOR].keys()[0]
            denominator_key = sos_policy[self.DENOMINATOR].keys()[0]
            numerator_val = sos_policy[self.NUMERATOR][numerator_key]
            denominator_val = sos_policy[self.DENOMINATOR][denominator_key]
            json_policy = json.loads(row.store_policy)
            kpi_fk = row.kpi

            # This is to assign the KPI to SOS_manufacturer_category_GLOBAL
            if json_policy.get('store_type') and len(json_policy.get('store_type')) > 1:
                kpi_fk = 8

            if numerator_key == 'manufacturer':
                numerator_key = numerator_key + '_name'
                # we need to include 'Philadelphia' as a manufacturer for all countries EXCEPT Chile
                if self.country == 'Chile':
                    numerator_values = [numerator_val]
                else:
                    numerator_values = [numerator_val, 'Philadelphia']
            else:
                # if the numerator isn't 'manufacturer', we just need to convert the value to a list
                numerator_values = [numerator_val]

            if denominator_key == 'sub_category':
                include_stacking_list = ['Nuts', 'DRY CHEESE', 'IWSN', 'Shredded', 'SNACK']
                if denominator_val in include_stacking_list:
                    facings_field = 'facings'
                else:
                    facings_field = 'facings_ign_stack'
            else:
                facings_field = 'facings_ign_stack'

            if denominator_key == 'sub_category' and denominator_val.lower() == 'all':
                # Here we are talkin on a KPI when the target have no denominator,
                # the calculation should be done on Numerator only
                numerator = self.scif[(self.scif[numerator_key] == numerator_val) &
                                      (self.scif['location_type'] == 'Primary Shelf')
                                      ][facings_field].sum()
                kpi_fk = 9
                denominator = None
                denominator_id = None
            else:
                numerator = self.scif[(self.scif[numerator_key].isin(numerator_values)) &
                                      (self.scif[denominator_key] == denominator_val) &
                                      (self.scif['location_type'] == 'Primary Shelf')][facings_field].sum()
                denominator = self.scif[(self.scif[denominator_key] == denominator_val) &
                                        (self.scif['location_type'] == 'Primary Shelf')][facings_field].sum()

            try:
                if denominator is not None:
                    denominator_id = self.all_products[self.all_products[denominator_key] == denominator_val][
                        denominator_key + '_fk'].values[0]
                if numerator is not None:
                    numerator_id = self.all_products[self.all_products[numerator_key] == numerator_val][
                        numerator_key.split('_')[0] + '_fk'].values[0]

                sos = 0
                if numerator and denominator:
                    sos = np.divide(float(numerator), float(denominator)) * 100
                score = 0
                target = row.target * 100
                if sos >= target:
                    score = 100

                identifier_parent = None
                should_enter = False
                if denominator_key == 'sub_category' and kpi_fk == row.kpi:
                    # if this a sub_category result, save it to the results_df for 'Perfect Store' store
                    results_df.loc[len(results_df)] = [denominator_val, denominator_id, score / 100]
                    identifier_parent = self.common_v2.get_dictionary(kpi_fk=sos_sub_category_kpi_fk,
                                                                      sub_category_fk=denominator_id)
                    should_enter = True

                manufacturer = None
                # self.common.write_to_db_result_new_tables(fk=kpi_fk, numerator_id=numerator_id,
                #                                           numerator_result=numerator,
                #                                           denominator_id=denominator_id,
                #                                           denominator_result=denominator,
                #                                           result=target, score=sos,
                #                                           score_after_actions=manufacturer)
                self.common_v2.write_to_db_result(kpi_fk, numerator_id=numerator_id, numerator_result=numerator,
                                                  denominator_id=denominator_id, denominator_result=denominator,
                                                  result=target, score=sos, target=target,
                                                  score_after_actions=manufacturer, identifier_parent=identifier_parent,
                                                  should_enter=should_enter)
            except Exception as e:
                Log.warning(denominator_key + ' - - ' + denominator_val)

        # if there are no sub_category sos results, there's no perfect store information to be saved
        if len(results_df) == 0:
            return 0

        # save aggregated results for each sub category
        total_dict = self.common_v2.get_dictionary(kpi_fk=total_sos_sub_category_kpi_fk)
        kpi_weight = self.get_kpi_weight('SOS')
        for row in results_df.itertuples():
            identifier_result = \
                self.common_v2.get_dictionary(kpi_fk=sos_sub_category_kpi_fk,
                                              sub_category_fk=row.sub_category_fk)

            # sub_cat_weight = self.get_weight(row.sub_category_fk)
            result = row.score
            score = result * kpi_weight

            self.powersku_sos[row.sub_category_fk] = score
            # limit results so that aggregated results can only add up to 3
            self.common_v2.write_to_db_result(sos_sub_category_kpi_fk,
                                              numerator_id=row.sub_category_fk,
                                              denominator_id=self.store_id,
                                              result=row.score, score=score,
                                              identifier_parent=row.sub_category_fk,
                                              identifier_result=identifier_result,
                                              weight=kpi_weight,
                                              target=kpi_weight,
                                              should_enter=True)
        #
        # # save total score for sos sub_category
        # number_of_passing_sub_categories = results_df['score'].sum()
        # number_of_possible_sub_categories = len(results_df)
        #
        # # this ensures that this KPI doesn't return more than 3 possible points max for stores that have
        # # multiple sub category policies
        # store_result = (number_of_passing_sub_categories / float(number_of_possible_sub_categories)) * 3
        #
        # self.common_v2.write_to_db_result(total_sos_sub_category_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
        #                                   denominator_id=self.store_id,
        #                                   numerator_result=number_of_passing_sub_categories,
        #                                   denominator_result=number_of_possible_sub_categories,
        #                                   result=store_result,
        #                                   score=store_result,
        #                                   identifier_result=total_dict, identifier_parent=Const.PERFECT_STORE,
        #                                   should_enter=True)
        #
        # return store_result

    def calculate_powersku_price_adherence(self):
        adherence_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_PRICE_ADHERENCE)
        adherence_sub_category_kpi_fk = \
            self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_PRICE_ADHERENCE_SUB_CATEGORY)
        adherence_total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(
            Const.POWER_SKU_PRICE_ADHERENCE_TOTAL)
        total_dict = self.common_v2.get_dictionary(kpi_fk=adherence_total_kpi_fk)

        if self.sub_category_assortment.empty:
            return False

        results = pd.merge(self.sub_category_assortment,
                           self.adherence_results, how='left', on='product_fk')
        results['into_interval'].fillna(0, inplace=True)

        for row in results.itertuples():
            parent_dict = self.common_v2.get_dictionary(kpi_fk=adherence_sub_category_kpi_fk,
                                                        sub_category_fk=row.sub_category_fk)
            score = 1 if row.into_interval else 0
            self.common_v2.write_to_db_result(adherence_kpi_fk, numerator_id=row.product_fk,
                                              denominator_id=row.sub_category_fk, result=row.trax_average,
                                              score=score, target=row.suggested_price, numerator_result=row.min_target,
                                              denominator_result=row.max_target,
                                              identifier_parent=parent_dict, should_enter=True)

        aggregated_results = results.groupby('sub_category_fk').agg(
            {'into_interval': 'sum', 'product_fk': 'count'}).reset_index().rename(
            columns={'product_fk': 'product_count'})
        aggregated_results['percent_complete'] = \
            aggregated_results.loc[:, 'into_interval'] / aggregated_results.loc[:, 'product_count']

        for row in aggregated_results.itertuples():
            identifier_result = self.common_v2.get_dictionary(kpi_fk=adherence_sub_category_kpi_fk,
                                                              sub_category_fk=row.sub_category_fk)
            kpi_weight = self.get_kpi_weight('PRICE')
            sub_cat_weight = self.get_weight(row.sub_category_fk)
            result = row.percent_complete
            score = result * kpi_weight

            self.powersku_price[row.sub_category_fk] = score

            self.common_v2.write_to_db_result(adherence_sub_category_kpi_fk, numerator_id=row.sub_category_fk,
                                              denominator_id=self.store_id, result=result, score=score,
                                              numerator_result=row.into_interval, denominator_result=row.product_count,
                                              identifier_parent=row.sub_category_fk,
                                              identifier_result=identifier_result,
                                              weight=kpi_weight, target=kpi_weight,
                                              should_enter=True)

    def heinz_global_price_adherence(self, config_df):
        # =============== remove after updating logic to support promotional pricing ===============
        self.match_product_in_scene.loc[self.match_product_in_scene['price'].isna(), 'price'] = \
            self.match_product_in_scene.loc[self.match_product_in_scene['price'].isna(), 'promotion_price']
        # =============== remove after updating logic to support promotional pricing ===============
        results_df = self.adherence_results
        my_config_df = \
            config_df[config_df['STORETYPE'].str.encode('utf-8') == self.store_info.store_type[0].encode('utf-8')]
        products_in_session = self.scif.drop_duplicates(subset=['product_ean_code'], keep='last')[
            'product_ean_code'].tolist()
        for product_in_session in products_in_session:
            if product_in_session:
                row = my_config_df[my_config_df['EAN CODE'] == int(product_in_session)]
                if not row.empty:
                    # ean_code = row['EAN CODE'].values[0]
                    # product_pk = self.labels[self.labels['ean_code'] == product_in_session]['pk'].values[0]
                    product_pk = \
                        self.all_products[self.all_products['product_ean_code']
                                          == product_in_session]['product_fk'].iloc[0]
                    # product_in_session_df = self.scif[self.scif['product_ean_code'] == ean_code]
                    mpisc_df_price = \
                        self.match_product_in_scene[(self.match_product_in_scene['product_fk'] == product_pk) |
                                                    (self.match_product_in_scene[
                                                         'substitution_product_fk'] == product_pk)]['price']
                    try:
                        suggested_price = row['SUGGESTED_PRICE'].values[0]
                    except Exception as e:
                        Log.error("Product with ean_code {} is not in the configuration file for customer type {}"
                                  .format(product_in_session, self.store_info.store_type[0].encode('utf-8')))
                        break
                    upper_percentage = (100 + row['PERCENTAGE'].values[0]) / float(100)
                    lower_percentage = (100 - row['PERCENTAGE'].values[0]) / float(100)
                    min_price = suggested_price * lower_percentage
                    max_price = suggested_price * upper_percentage
                    into_interval = 0
                    prices_sum = 0
                    count = 0
                    trax_average = None
                    for price in mpisc_df_price:
                        if price and pd.notna(price):
                            prices_sum += price
                            count += 1

                    if prices_sum > 0:
                        trax_average = prices_sum / count
                        into_interval = 0

                    if not np.isnan(suggested_price):
                        if min_price <= trax_average <= max_price:
                            into_interval = 100

                    results_df.loc[len(results_df)] = [product_pk, trax_average,
                                                       suggested_price, into_interval / 100, min_price, max_price]

                    # self.common.write_to_db_result_new_tables(fk=10,
                    #                                           numerator_id=product_pk,
                    #                                           numerator_result=suggested_price,
                    #                                           denominator_id=product_pk,
                    #                                           denominator_result=trax_average,
                    #                                           result=row['PERCENTAGE'].values[0],
                    #                                           score=into_interval)
                    self.common_v2.write_to_db_result(10, numerator_id=product_pk,
                                                      numerator_result=suggested_price,
                                                      denominator_id=product_pk,
                                                      denominator_result=trax_average,
                                                      result=row['PERCENTAGE'].values[0],
                                                      score=into_interval)
                    if trax_average:
                        mark_up = (np.divide(np.divide(float(trax_average), float(1.13)),
                                             float(suggested_price)) - 1) * 100
                        # self.common.write_to_db_result_new_tables(fk=11,
                        #                                           numerator_id=product_pk,
                        #                                           numerator_result=suggested_price,
                        #                                           denominator_id=product_pk,
                        #                                           denominator_result=trax_average,
                        #                                           score=mark_up,
                        #                                           result=mark_up)
                        self.common_v2.write_to_db_result(11, numerator_id=product_pk,
                                                          numerator_result=suggested_price,
                                                          denominator_id=product_pk,
                                                          denominator_result=trax_average,
                                                          score=mark_up,
                                                          result=mark_up)
                else:
                    continue
                    # Log.warning("Product with ean_code {} is not in the configuration file for customer type {}"
                    #             .format(product_in_session, self.store_info.store_type[0].encode('utf-8')))
        return results_df

    def calculate_perfect_store_extra_spaces(self):
        extra_spaces_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(
            Const.PERFECT_STORE_EXTRA_SPACES_SUB_CATEGORY)
        extra_spaces_total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(
            Const.PERFECT_STORE_EXTRA_SPACES_TOTAL)
        total_dict = self.common_v2.get_dictionary(kpi_fk=extra_spaces_total_kpi_fk)

        sub_cats_for_store = self.relevant_sub_categories

        if self.extra_spaces_results.empty:
            pass

        try:
            relevant_sub_categories = [x.strip() for x in self.extra_spaces_template[
                self.extra_spaces_template['country'].str.encode('utf-8') == self.country.encode('utf-8')][
                'sub_category'].iloc[0].split(',')]
        except IndexError:
            Log.warning(
                'No relevant sub_categories for the Extra Spaces KPI found for the following country: {}'.format(
                    self.country))

        self.extra_spaces_results = pd.merge(self.extra_spaces_results,
                                             self.all_products.loc[:, [
                                                                          'sub_category_fk',
                                                                          'sub_category']].dropna().drop_duplicates(),
                                             how='left', on='sub_category_fk')

        relevant_extra_spaces = \
            self.extra_spaces_results[self.extra_spaces_results['sub_category'].isin(
                relevant_sub_categories)]
        kpi_weight = self.get_kpi_weight('EXTRA')
        for row in relevant_extra_spaces.itertuples():
            weight = self.get_weight(row.sub_category_fk)
            self.powersku_empty[row.sub_category_fk] = 1 * kpi_weight
            score = result = 1

            sub_cats_for_store.remove(row.sub_category_fk)

            self.common_v2.write_to_db_result(extra_spaces_kpi_fk, numerator_id=row.sub_category_fk,
                                              denominator_id=row.template_fk, result=result, score=score,
                                              identifier_parent=row.sub_category_fk,
                                              target=1, should_enter=True)

        for sub_cat_fk in sub_cats_for_store:
            result = score = 0
            self.powersku_empty[sub_cat_fk] = 0
            self.common_v2.write_to_db_result(extra_spaces_kpi_fk, numerator_id=sub_cat_fk,
                                              denominator_id=0, result=result, score=score,
                                              identifier_parent=sub_cat_fk,
                                              target=1, should_enter=True)

    def heinz_global_extra_spaces(self):
        try:
            supervisor = self.store_info['additional_attribute_3'][0]
            store_target = -1
            for row in self.store_sos_policies.itertuples():
                policies = json.loads(row.store_policy)
                for key, value in policies.items():
                    try:
                        if key == 'additional_attribute_3' and value[0] == supervisor:
                            store_target = row.target
                            break
                    except KeyError:
                        continue
        except Exception as e:
            Log.error("Supervisor target is not configured for the extra spaces report ")
            raise e

        results_df = self.extra_spaces_results

        # limit to only secondary scenes
        relevant_scif = self.scif[(self.scif['location_type_fk'] == float(2)) &
                                  (self.scif['facings'] > 0)]
        if relevant_scif.empty:
            return results_df
        # aggregate facings for every scene/sub_category combination in the visit
        relevant_scif = \
            relevant_scif.groupby(['scene_fk', 'template_fk', 'sub_category_fk'], as_index=False)['facings'].sum()
        # sort sub_categories by number of facings, largest first
        relevant_scif = relevant_scif.sort_values(['facings'], ascending=False)
        # drop all but the sub_category with the largest number of facings for each scene
        relevant_scif = relevant_scif.drop_duplicates(subset=['scene_fk'], keep='first')

        for row in relevant_scif.itertuples():
            results_df.loc[len(results_df)] = [row.sub_category_fk, row.template_fk, row.facings]
            self.common_v2.write_to_db_result(13, numerator_id=row.template_fk,
                                              numerator_result=row.facings,
                                              denominator_id=row.sub_category_fk,
                                              denominator_result=row.facings,
                                              context_id=row.scene_fk,
                                              result=store_target)

        return results_df

    def check_bonus_question(self):
        bonus_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.BONUS_QUESTION_SUB_CATEGORY)
        bonus_weight = self.kpi_weights['Score'][self.kpi_weights['KPIs'] == Const.KPI_WEIGHTS['Bonus']].iloc[0]

        sub_category_fks = self.sub_category_weight.sub_category_fk.unique().tolist()
        sub_category_fks = [x for x in sub_category_fks if str(x) != 'nan']
        if self.survey.check_survey_answer(('question_fk', Const.BONUS_QUESTION_FK), 'Yes,yes,si,Si'):
            result = 1
        else:
            result = 0

        for sub_cat_fk in sub_category_fks:
            # sub_cat = self.sub_category_assortment['CATEGORY'][ self.sub_category_assortment['sub_category_fk']== sub_cat_fk].iloc[0]
            sub_cat_weight = self.get_weight(sub_cat_fk)

            score = result * sub_cat_weight
            target_weight = bonus_weight * sub_cat_weight
            self.powersku_bonus[sub_cat_fk] = score

            self.common_v2.write_to_db_result(bonus_kpi_fk, numerator_id=sub_cat_fk,
                                              denominator_id=self.store_id,
                                              result=result, score=score, identifier_parent=sub_cat_fk,
                                              weight=target_weight, target=target_weight,
                                              should_enter=True)

    def commit_results_data(self):
        self.common_v2.commit_results_data()

    def update_score_sub_category_weights(self):
        all_sub_category_fks = self.all_products[['sub_category', 'sub_category_fk']].drop_duplicates()
        self.sub_category_weight = pd.merge(self.sub_category_weight, all_sub_category_fks, left_on='Category',
                                            right_on='sub_category',
                                            how='left')

    def get_weight(self, sub_category_fk):
        weight_value = 0

        if self.country in self.sub_category_weight.columns.to_list():
            weight_df = self.sub_category_weight[self.country][
                (self.sub_category_weight.sub_category_fk == sub_category_fk)]
            if weight_df.empty:
                return 0

            weight_value = weight_df.iloc[0]

            if pd.isna(weight_value):
                weight_value = 0

        weight = weight_value * 0.01
        return weight

    def get_kpi_weight(self, kpi_name):
        weight = self.kpi_weights['Score'][self.kpi_weights['KPIs'] == Const.KPI_WEIGHTS[kpi_name]].iloc[0]
        return weight

    def calculate_sub_category_sum(self, dict_list, sub_cat_fk):
        total_score = 0
        for item in dict_list:
            try:
                total_score += item[sub_cat_fk]
            except:
                pass

        return total_score

    def set_relevant_sub_categories(self):
        df = self.sub_category_weight[['Category', 'sub_category_fk', self.country]].dropna()
        self.relevant_sub_categories = df.sub_category_fk.to_list()
