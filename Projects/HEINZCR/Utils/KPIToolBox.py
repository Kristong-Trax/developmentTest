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
        except KeyError:
            self.sub_category_assortment = pd.DataFrame()

        try:
            self.store_assortment_without_powerskus = \
                self.store_assortment[self.store_assortment['assortment_name'].str.contains('ASSORTMENT')]
        except KeyError:
            self.store_assortment_without_powerskus = pd.DataFrame()

        self.adherence_results = pd.DataFrame(columns=['product_fk', 'trax_average',
                                                       'suggested_price', 'into_interval', 'min_target', 'max_target'])
        self.extra_spaces_results = pd.DataFrame(
            columns=['sub_category_fk', 'template_fk', 'count'])

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

        # this isn't relevant to the 'Perfect Score' calculation
        self.heinz_global_distribution_per_category()
        self.calculate_assortment()

        perfect_store_score = 0
        perfect_store_score += self.calculate_powersku_assortment()
        perfect_store_score += self.main_sos_calculation()
        perfect_store_score += self.calculate_powersku_price_adherence()
        perfect_store_score += self.calculate_perfect_store_extra_spaces()
        perfect_store_score += self.check_bonus_question()

        relevant_target_df = \
            self.store_targets[self.store_targets['Country'].str.encode('utf-8') == self.country.encode('utf-8')]
        if not relevant_target_df.empty:
            target = relevant_target_df['Store Execution Score'].iloc[0]
        else:
            target = 0

        result = 1 if perfect_store_score > target else 0

        perfect_store_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.PERFECT_STORE)
        self.common_v2.write_to_db_result(perfect_store_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          result=result, target=target,
                                          score=perfect_store_score, identifier_result=Const.PERFECT_STORE)
        return

    def calculate_assortment(self):
        if self.store_assortment_without_powerskus.empty:
            return

        products_in_store = self.scif[self.scif['facings'] > 0]['product_fk'].unique().tolist()
        pass_count = 0

        total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Distribution')
        identifier_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)

        for row in self.store_assortment_without_powerskus.itertuples():
            result = 0
            if row.product_fk in products_in_store:
                result = 1
                pass_count += 1

            sku_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Distribution - SKU')
            self.common_v2.write_to_db_result(sku_kpi_fk, numerator_id=row.product_fk, denominator_id=row.assortment_fk,
                                              result=result, identifier_parent=identifier_dict, should_enter=True)

        number_of_products_in_assortment = len(self.store_assortment_without_powerskus)
        if number_of_products_in_assortment:
            total_result = (pass_count / float(number_of_products_in_assortment)) * 100
        else:
            total_result = 0
        self.common_v2.write_to_db_result(total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          numerator_result=pass_count,
                                          denominator_result=number_of_products_in_assortment,
                                          result=total_result, identifier_result=identifier_dict)

    def calculate_powersku_assortment(self):
        if self.sub_category_assortment.empty:
            return 0

        total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_TOTAL)
        sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_SUB_CATEGORY)
        sku_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU)

        products_in_session = self.scif[self.scif['facings'] > 0]['product_fk'].unique().tolist()
        self.sub_category_assortment['in_session'] = \
            self.sub_category_assortment.loc[:, 'product_fk'].isin(products_in_session)
        # save PowerSKU results at SKU level
        for sku in self.sub_category_assortment[['product_fk', 'sub_category_fk', 'in_session']].itertuples():
            parent_dict = self.common_v2.get_dictionary(
                kpi_fk=sub_category_kpi_fk, sub_category_fk=sku.sub_category_fk)
            result = 1 if sku.in_session else 0
            self.common_v2.write_to_db_result(sku_kpi_fk, numerator_id=sku.product_fk,
                                              denominator_id=sku.sub_category_fk,
                                              result=result, identifier_parent=parent_dict, should_enter=True)
        # save PowerSKU results at sub_category level
        aggregated_results = self.sub_category_assortment.groupby('sub_category_fk').agg(
            {'in_session': 'sum', 'product_fk': 'count'}).reset_index().rename(
            columns={'product_fk': 'product_count'})
        aggregated_results['percent_complete'] = \
            aggregated_results.loc[:, 'in_session'] / aggregated_results.loc[:, 'product_count']
        for sub_category in aggregated_results.itertuples():
            parent_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)
            identifier_dict = self.common_v2.get_dictionary(kpi_fk=sub_category_kpi_fk,
                                                            sub_category_fk=sub_category.sub_category_fk)
            result = sub_category.percent_complete
            self.common_v2.write_to_db_result(sub_category_kpi_fk, numerator_id=sub_category.sub_category_fk,
                                              denominator_id=self.store_id, identifier_parent=parent_dict,
                                              identifier_result=identifier_dict, result=result,
                                              should_enter=True)
        # save PowerSKU total score
        total_score = aggregated_results['percent_complete'].sum()
        total_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)
        self.common_v2.write_to_db_result(total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          result=total_score, score=total_score, identifier_parent=Const.PERFECT_STORE,
                                          identifier_result=total_dict, should_enter=True)
        return total_score

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

            if denominator_key == 'sub_category' and denominator_val.lower() == 'all':
                # Here we are talkin on a KPI when the target have no denominator,
                # the calculation should be done on Numerator only
                numerator = self.scif[(self.scif[numerator_key] == numerator_val) &
                                      (self.scif['location_type'] == 'Primary Shelf')
                                      ]['facings_ign_stack'].sum()
                kpi_fk = 9
                denominator = None
                denominator_id = None
            else:
                numerator = self.scif[(self.scif[numerator_key] == numerator_val) &
                                      (self.scif[denominator_key] == denominator_val) &
                                      (self.scif['location_type'] == 'Primary Shelf')]['facings_ign_stack'].sum()
                denominator = self.scif[(self.scif[denominator_key] == denominator_val) &
                                        (self.scif['location_type'] == 'Primary Shelf')]['facings_ign_stack'].sum()

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
        for row in results_df.itertuples():
            identifier_result = self.common_v2.get_dictionary(kpi_fk=sos_sub_category_kpi_fk,
                                                              sub_category_fk=row.sub_category_fk)
            self.common_v2.write_to_db_result(sos_sub_category_kpi_fk, numerator_id=row.sub_category_fk,
                                              denominator_id=self.store_id, result=row.score,
                                              identifier_parent=total_dict, identifier_result=identifier_result,
                                              should_enter=True)

        # save total score for sos sub_category
        number_of_passing_sub_categories = results_df['score'].sum()
        number_of_possible_sub_categories = len(results_df)
        self.common_v2.write_to_db_result(total_sos_sub_category_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          numerator_result=number_of_passing_sub_categories,
                                          denominator_result=number_of_possible_sub_categories,
                                          result=number_of_passing_sub_categories,
                                          score=number_of_passing_sub_categories,
                                          identifier_result=total_dict, identifier_parent=Const.PERFECT_STORE,
                                          should_enter=True)

        return number_of_passing_sub_categories

    def calculate_powersku_price_adherence(self):
        adherence_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_PRICE_ADHERENCE)
        adherence_sub_category_kpi_fk = \
            self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_PRICE_ADHERENCE_SUB_CATEGORY)
        adherence_total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(
            Const.POWER_SKU_PRICE_ADHERENCE_TOTAL)
        total_dict = self.common_v2.get_dictionary(kpi_fk=adherence_total_kpi_fk)

        if self.sub_category_assortment.empty:
            self.common_v2.write_to_db_result(adherence_total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                              denominator_id=self.store_id,
                                              result=0, identifier_result=total_dict,
                                              identifier_parent=Const.PERFECT_STORE,
                                              should_enter=True)
            return 0

        results = pd.merge(self.sub_category_assortment,
                           self.adherence_results, how='left', on='product_fk')
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
            result = row.percent_complete
            self.common_v2.write_to_db_result(adherence_sub_category_kpi_fk, numerator_id=row.sub_category_fk,
                                              denominator_id=self.store_id, result=result, score=result,
                                              identifier_parent=total_dict, identifier_result=identifier_result,
                                              should_enter=True)
        # this value is not necessarily a whole number
        number_of_categories_meeting_price_adherence = aggregated_results['percent_complete'].sum()
        number_of_possible_categories = len(aggregated_results)
        self.common_v2.write_to_db_result(adherence_total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          numerator_result=number_of_categories_meeting_price_adherence,
                                          denominator_result=number_of_possible_categories,
                                          result=number_of_categories_meeting_price_adherence,
                                          score=number_of_categories_meeting_price_adherence,
                                          identifier_result=total_dict, identifier_parent=Const.PERFECT_STORE,
                                          should_enter=True)
        return number_of_categories_meeting_price_adherence

    def heinz_global_price_adherence(self, config_df):
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
                    mpisc_df_price = self.match_product_in_scene[self.match_product_in_scene['product_fk'] == product_pk][
                        'price']
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
                        if price:
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

        if self.extra_spaces_results.empty:
            self.common_v2.write_to_db_result(extra_spaces_total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                              denominator_id=self.store_id, result=0, score=0,
                                              identifier_parent=Const.PERFECT_STORE,
                                              identifier_result=total_dict, should_enter=True)
            return 0

        relevant_sub_categories = [x.strip() for x in self.extra_spaces_template[
            self.extra_spaces_template['country'].str.encode('utf-8') == self.country.encode('utf-8')][
            'sub_category'].iloc[0].split(',')]

        self.extra_spaces_results = pd.merge(self.extra_spaces_results,
                                             self.all_products.loc[:, [
                                                 'sub_category_fk', 'sub_category']].dropna().drop_duplicates(),
                                             how='left', on='sub_category_fk')

        relevant_extra_spaces = \
            self.extra_spaces_results[self.extra_spaces_results['sub_category'].isin(
                relevant_sub_categories)]

        for row in relevant_extra_spaces.itertuples():
            self.common_v2.write_to_db_result(extra_spaces_kpi_fk, numerator_id=row.sub_category_fk,
                                              denominator_id=row.template_fk, result=1, identifier_parent=total_dict,
                                              should_enter=True)

        number_of_extra_spaces = relevant_extra_spaces['sub_category_fk'].nunique()
        score = 1 if number_of_extra_spaces > 2 else 0

        if score == 0:
            if self.survey.check_survey_answer(('question_fk', Const.EXTRA_SPACES_SURVEY_QUESTION_FK), 'Yes,yes,si,Si'):
                score = 1

        self.common_v2.write_to_db_result(extra_spaces_total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          result=number_of_extra_spaces, score=score,
                                          identifier_parent=Const.PERFECT_STORE,
                                          identifier_result=total_dict, should_enter=True)

        return score

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
        if self.survey.check_survey_answer(('question_fk', Const.BONUS_QUESTION_FK), 'Yes,yes,si,Si'):
            result = 1
        else:
            result = 0

        bonus_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(
            Const.BONUS_QUESTION)
        self.common_v2.write_to_db_result(bonus_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                          denominator_id=self.store_id,
                                          result=result, score=result, identifier_parent=Const.PERFECT_STORE,
                                          should_enter=True)
        return result

    def commit_results_data(self):
        self.common_v2.commit_results_data()
