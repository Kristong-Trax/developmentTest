from datetime import datetime
import json

import numpy as np
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
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
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.current_date = datetime.now()
        self.store_assortment = PSAssortmentDataProvider(self.data_provider).execute(policy_name=None)
        self.sub_category_assortment = pd.merge(self.store_assortment,
                                                self.all_products.loc[:, ['product_fk', 'sub_category',
                                                                          'sub_category_fk']],
                                                how='left', on='product_fk')


    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # self.heinz_global_distribution_per_category()  # this isn't relevant to the 'Perfect Score' calculation
        score = 0
        self.calculate_powersku_assortment()
        return

    def calculate_powersku_assortment(self):
        if self.store_assortment.empty:
            return 0

        total_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_TOTAL)
        sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU_SUB_CATEGORY)
        sku_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.POWER_SKU)

        products_in_session = self.scif['product_fk'].unique().tolist()
        self.sub_category_assortment['in_session'] = \
            self.sub_category_assortment.loc[:, 'product_fk'].isin(products_in_session)
        # save PowerSKU results at SKU level
        for sku in self.sub_category_assortment[['product_fk', 'sub_category_fk', 'in_session']].itertuples():
            parent_dict = self.common_v2.get_dictionary(kpi_fk=sub_category_kpi_fk, sub_category_fk=sku.sub_category_fk)
            self.common_v2.write_to_db_result(sku_kpi_fk, numerator_id=sku.product_fk,
                                              denominator_id=sku.sub_category_fk,
                                              result=sku.in_session, identifier_parent=parent_dict, should_enter=True)
        # save PowerSKU results at sub_category level
        aggregated_results = self.sub_category_assortment.groupby('sub_category_fk').agg(
            {'in_session': 'sum', 'product_fk': 'count'}).reset_index().rename(
            columns={'product_fk': 'product_count'})
        aggregated_results['complete'] = \
            aggregated_results.loc[:, 'in_session'] == aggregated_results.loc[:, 'product_count']
        for sub_category in aggregated_results.itertuples():
            parent_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)
            identifier_dict = self.common_v2.get_dictionary(kpi_fk=sub_category_kpi_fk,
                                                            sub_category_fk=sub_category.sub_category_fk)
            self.common_v2.write_to_db_result(sub_category_kpi_fk, numerator_id=sub_category.sub_category_fk,
                                              denominator_id=self.store_id, identifier_parent=parent_dict,
                                              identifier_result=identifier_dict, result=sub_category.complete,
                                              should_enter=True)
        # save PowerSKU total score
        total_score = aggregated_results['complete'].sum()
        total_dict = self.common_v2.get_dictionary(kpi_fk=total_kpi_fk)
        self.common_v2.write_to_db_result(total_kpi_fk, numerator_id=1, denominator_id=self.store_id,
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

        results_df = pd.DataFrame['sub_category', 'sub_category_fk', 'score']

        sos_sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_SUB_CATEGORY)
        total_sos_sub_category_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_SUB_CATEGORY_TOTAL)

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
                numerator = self.scif[(self.scif[numerator_key] == numerator_val)]['facings_ign_stack'].sum()
                kpi_fk = 9
                denominator = None
                denominator_id = None
            else:
                numerator = self.scif[
                    (self.scif[numerator_key] == numerator_val) & (self.scif[denominator_key] == denominator_val)][
                    'facings_ign_stack'].sum()
                denominator = self.scif[self.scif[denominator_key] == denominator_val]['facings_ign_stack'].sum()

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
                if denominator_key == 'sub_category':
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
                                                  result=target, score=sos,
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
            self.common_v2.write_to_db_result(total_sos_sub_category_kpi_fk, numerator_id=1,
                                              denominator_id=self.store_id,
                                              numerator_result=number_of_passing_sub_categories,
                                              denominator_result=number_of_possible_sub_categories,
                                              identifier_result=total_dict, identifier_parent=Const.PERFECT_STORE,
                                              should_enter=True)

        return number_of_passing_sub_categories

    def heinz_global_price_adherence(self, config_df):
        my_config_df = config_df[config_df['STORETYPE'] == self.store_info.store_type[0]]
        products_in_session = self.scif.drop_duplicates(subset=['product_ean_code'], keep='last')['product_ean_code'].tolist()
        for product_in_session in products_in_session:
            if product_in_session:
                row = my_config_df[my_config_df['EAN CODE'] == int(product_in_session)]
                if not row.empty:
                    # ean_code = row['EAN CODE'].values[0]
                    product_pk = self.labels[self.labels['ean_code'] == product_in_session]['pk'].values[0]
                    # product_in_session_df = self.scif[self.scif['product_ean_code'] == ean_code]
                    mpisc_df_price = self.match_product_in_scene[self.match_product_in_scene['product_fk'] == product_pk][
                        'price']
                    try:
                        suggested_price = row['SUGGESTED_PRICE'].values[0]
                    except Exception as e:
                        Log.error("Product with ean_code {} is not in the configuration file for customer type {}"
                                  .format(product_in_session, self.store_info.store_type[0]))
                        break
                    upper_percentage = (100 + row['PERCENTAGE'].values[0]) / 100
                    lower_percentage = (100 - row['PERCENTAGE'].values[0]) / 100
                    min_price = suggested_price * lower_percentage
                    max_price = suggested_price * upper_percentage
                    into_interval = None
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

                    if min_price <= trax_average <= max_price:
                        into_interval = 100

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
                        mark_up = (np.divide(np.divide(float(trax_average), float(1.13)), float(suggested_price)) -1) * 100
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
                    Log.warning("Product with ean_code {} is not in the configuration file for customer type {}"
                              .format(product_in_session, self.store_info.store_type[0]))

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

        # kpi_fk = row.kpi
        scene_types = self.scif.drop_duplicates(subset=['template_fk'], keep='first')
        for index, row in scene_types.iterrows():
            template_fk = row['template_fk']
            location_type = row.get('location_type_fk')
            if template_fk >= 0 and location_type == float(2):
                scene_data = self.scif[(self.scif['template_fk'] == template_fk) & (self.scif['sub_category_fk'])]
                categories_in_scene = scene_data.drop_duplicates(subset=['sub_category_fk'], keep='last')
                winner = []
                max_count = -1
                for index1, category_row in categories_in_scene.iterrows():
                    category = category_row['sub_category_fk']
                    if not pd.isnull(category):
                        df = scene_data[scene_data['sub_category_fk'] == category]
                        item_count =len(df)
                        if item_count > max_count:
                            max_count = item_count
                            winner = [{'sub_category_fk': category,
                                       'count': item_count}]
                        elif item_count > max_count:
                            winner.append({'sub_category_fk': category,
                                           'count': item_count})

                for i in winner:
                    # self.common.write_to_db_result_new_tables(fk=13,
                    #                                           numerator_id=template_fk,
                    #                                           numerator_result=i.get('count'),
                    #                                           denominator_id=i.get('sub_category_fk'),
                    #                                           denominator_result=i.get('count'),
                    #                                           result=store_target)
                    self.common_v2.write_to_db_result(13, numerator_id=template_fk,
                                                      numerator_result=i.get('count'),
                                                      denominator_id=i.get('sub_category_fk'),
                                                      denominator_result=i.get('count'),
                                                      result=store_target)

    def commit_results_data(self):
        pass
