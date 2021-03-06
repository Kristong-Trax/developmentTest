# coding=utf-8
import pandas as pd
from collections import Counter
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from Projects.TNUVAILV2_SAND.Utils.Consts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.TNUVAILV2_SAND.Utils.DataBaseHandler import DBHandler
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ProductsConsts, ScifConsts
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Data.ProfessionalServices.PsConsts.DB import SessionResultsConsts
from Trax.Data.ProfessionalServices.PsConsts.Consts import BasicConsts, HelperConsts

__author__ = 'idanr'


class TNUVAILSANDToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common_v2 = Common(self.data_provider)
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.assortment = Assortment(self.data_provider, self.output)
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.db_handler = DBHandler(self.data_provider.project_name, self.data_provider.session_uid)
        self.previous_oos_results = self.db_handler.get_last_session_oos_results()
        self.kpi_result_types = self.db_handler.get_kpi_result_value()
        self.oos_store_results = list()
        self.initial_scif = self.scif.copy()

    def get_relevant_assortment_instance(self, assortment):
        if self.data_provider.session_info.status.values[0] == Consts.COMPLETED_STATUS:
            self.update_scif_for_assortment()
            assortment = Assortment(self.data_provider, self.output)
        return assortment

    def update_scif_for_assortment(self):
        oos_reasons = self.db_handler.get_oos_reasons_for_session(self.data_provider.session_uid)
        oos_reason_products = oos_reasons[ProductsConsts.PRODUCT_FK].values
        selected_products_df = self.all_products[self.all_products[ProductsConsts.PRODUCT_FK].isin(oos_reason_products) &
                                                 self.all_products[ProductsConsts.SUBSTITUTION_PRODUCT_FK].isnull()]
        if not selected_products_df.empty:
            extra_col = list(set(self.all_products.columns.values).difference(set(self.scif.columns.values)))
            selected_products_df.drop(extra_col, axis=1, inplace=True)
            dummy_scif = self.create_dummy_scif_lines_excluding_product_columns()
            add_scif_df = selected_products_df.merge(dummy_scif, left_on=Consts.PRODUCT_POLICY_ATTR,
                                                     right_on=ScifConsts.TEMPLATE_NAME, how='inner')
            new_scif = self.scif.append(add_scif_df, ignore_index=True)
            self.scif = new_scif.copy()
            self.data_provider._set_scene_item_facts(new_scif)

    def create_dummy_scif_lines_excluding_product_columns(self):
        policies = [Consts.MILKY_POLICY, Consts.TIRAT_TSVI_POLICY]
        unique_scenes_df = self.scif[(self.scif[ScifConsts.TEMPLATE_NAME].isin(policies)) &
                                     (~self.scif[ScifConsts.FACINGS].isnull())].drop_duplicates(
                            subset=[ScifConsts.TEMPLATE_NAME]).copy()
        unique_scenes_df[ScifConsts.FACINGS] = 1.0
        unique_scenes_df[ScifConsts.FACINGS_IGN_STACK] = 1.0
        non_product_columns = list(set(self.scif.columns.values).difference(set(self.all_products.columns.values)))
        unique_scenes_df = unique_scenes_df[non_product_columns]
        return unique_scenes_df

    def main_calculation(self):
        """ This function calculates all of the KPIs' results."""
        self._calculate_facings_sos()
        self.assortment = self.get_relevant_assortment_instance(self.assortment)
        self._calculate_assortment()
        self.scif = self.initial_scif
        self.data_provider._set_scene_item_facts(self.scif)
        self.common_v2.commit_results_data()

    def _calculate_facings_sos(self):
        """
        This kpi calculates SOS in 3 levels: Manufacturer out of store, Manufacturer Out of Category and
        Manufacturer out of Category.
        """
        self._calculate_sos_by_policy(Consts.MILKY_POLICY)
        self._calculate_sos_by_policy(Consts.TIRAT_TSVI_POLICY)

    def _calculate_assortment(self):
        """
        This is the main function for assortment calculation. It prepares the data and calculating all of the relevant
        policies. In this project there is abuse of notion of "Assortment" KPI - Distribution and OOS are been
        calculated on different data. Distribution is been calculated for everything and OOS only for
        "Obligatory" policy.
        """
        lvl3_result = self._prepare_data_for_assortment_calculation()
        if lvl3_result.empty:
            Log.warning(Consts.EMPTY_ASSORTMENT_DATA)
            return
        self._calculate_assortment_results_per_policy(lvl3_result, policy=Consts.MILKY_POLICY)
        self._calculate_assortment_results_per_policy(lvl3_result, policy=Consts.TIRAT_TSVI_POLICY)
        self._calculate_total_oos_results()  # New addition: In order to support NCC report and OOS reasons

    def _calculate_total_oos_results(self):
        """
        This KPI uses the previous OOS results that were calculated and saves the results in the store level
        (without considering "Dairy" or "Tirat Tsvi" policies).
        """
        if not self.oos_store_results:
            return
        store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_LEVEL)
        total_res = Counter()
        for result in self.oos_store_results:
            total_res.update(result)
        total_res[SessionResultsConsts.DENOMINATOR_ID] = self.store_id
        total_res[ProductsConsts.MANUFACTURER_FK] = self.own_manufacturer_fk
        total_res = [dict(total_res)]
        self._save_results_for_assortment(ProductsConsts.MANUFACTURER_FK, total_res, store_level_fk, None, True)

    def _prepare_data_for_assortment_calculation(self):
        """ This method gets the level 3 assortment results (SKU level), adding category_fk and returns the DataFrame"""
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        category_per_product = self.all_products[[ProductsConsts.PRODUCT_FK, ScifConsts.CATEGORY_FK]]
        lvl3_result = pd.merge(lvl3_result, category_per_product, how='left')
        return lvl3_result

    def _filter_data_for_oos_calculation(self, lvl3_data):
        """
        In the project's logic they treating differently OOS and Distribution. We need to treat only "Obligatory"
        products from the assortment which defines as a KPI (this is a bit of abuse of notion of the regular assortment
        calculation be design so this is why I'm filtering by KPI.
        """
        obligatory_assortment_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OBLIGATORY_ASSORTMENT)
        lvl3_data = lvl3_data.loc[lvl3_data.kpi_fk_lvl2 == obligatory_assortment_kpi_fk]
        return lvl3_data

    def _calculate_assortment_results_per_policy(self, lvl3_result, policy):
        """
        This method triggering the Distribution and OOS calculations for the relevant policy.
        :param lvl3_result: Assortment SKU level results + category_fk column.
        :param policy:  חלבי או טירת צבי - this policy is matching for scene types and products as well
        """
        if policy not in self.scif.template_name.unique().tolist():
            return
        lvl3_data = self._get_relevant_assortment_data(lvl3_result, policy)
        self._calculate_distribution_and_oos(lvl3_data, policy, is_dist=True)  # Distribution
        lvl3_data = self._filter_data_for_oos_calculation(lvl3_data)
        self._calculate_distribution_and_oos(lvl3_data, policy, is_dist=False)  # OOS

    def _get_filtered_scif_for_sos_calculations(self, policy):
        """ This method filters scene item facts by policy and removes redundant row for SOS calculation"""
        filtered_scif = self._get_filtered_scif_per_scene_type(policy)
        filtered_scif = filtered_scif[~filtered_scif[ProductsConsts.PRODUCT_TYPE].isin(Consts.TYPES_TO_IGNORE_IN_SOS)]
        filtered_scif = filtered_scif.loc[filtered_scif[ScifConsts.FACINGS_IGN_STACK] > 0]
        return filtered_scif

    def _get_filtered_scif_per_scene_type(self, scene_type):
        """
        This method filters scene item facts by relevant scene type.
        :param scene_type:  חלבי או טירת צבי scene types.
        :return: filtered scif and match product in scene DataFrames.
        """
        filtered_scif = self.scif.loc[self.scif.template_name.str.encode(HelperConsts.UTF8)
                                      == scene_type.encode(HelperConsts.UTF8)]
        filtered_scif = filtered_scif.loc[filtered_scif.facings > 0]
        return filtered_scif

    def _get_relevant_assortment_data(self, lvl3_assortment, policy):
        """
        This method filters the relevant products according to their attributes.
        Plus, in case there are products that can be found but not in the relevant scene, the in_store gets 0.
        :param policy:  חלבי או טירת צבי - this policy is matching for scene types and products as well
        :return: Relevant data for the current policy calculation.
        """
        # Products with the relevant policy attribute
        product_with_policy_attr = self.all_products.loc[
            self.all_products[Consts.PRODUCT_POLICY_ATTR].str.encode(HelperConsts.UTF8) == policy.encode(
                HelperConsts.UTF8)]
        product_with_policy_attr = product_with_policy_attr.product_fk.unique().tolist()
        # Products that appear in scenes with the relevant policy
        filtered_scif = self._get_filtered_scif_per_scene_type(policy)
        relevant_products = filtered_scif.product_fk.unique().tolist()
        # Filtering the relevant products
        relevant_lvl3_assortment_data = lvl3_assortment.loc[lvl3_assortment.product_fk.isin(product_with_policy_attr)]
        # In store = 0 if the products don't appear in the relevant scenes
        relevant_lvl3_assortment_data.loc[~lvl3_assortment.product_fk.isin(relevant_products), Consts.IN_STORE] = 0
        return relevant_lvl3_assortment_data

    def _get_assortment_kpi_fks(self, policy, is_distribution):
        """
        This project have 12 different kpis for distribution and oos. So this method returns the relevant ones
        according to the scene_type, level and distribution / oos.
        :param is_distribution: True if the KPI is distribution, else (KPI is OOS) False.
        :return: A tuple with the three relevant Assortment KPI names according to the attributes.
        """
        if is_distribution:
                if policy == Consts.MILKY_POLICY:
                    store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_STORE_LEVEL_DAIRY)
                    category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_CATEGORY_LEVEL_DAIRY)
                    sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_SKU_LEVEL_DAIRY)
                else:
                    store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_STORE_LEVEL_TIRAT_TSVI)
                    category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_CATEGORY_LEVEL_TIRAT_TSVI)
                    sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_SKU_LEVEL_TIRAT_TSVI)
        else:
            if policy == Consts.MILKY_POLICY:
                store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_LEVEL_DAIRY)
                category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_CATEGORY_LEVEL_DAIRY)
                sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LEVEL_DAIRY)
            else:
                store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_LEVEL_TIRAT_TSVI)
                category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_CATEGORY_LEVEL_TIRAT_TSVI)
                sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LEVEL_TIRAT_TSVI)
        return store_level_fk, category_level_fk, sku_level_fk

    def _calculate_store_level_assortment(self, lvl3_data, is_distribution=True):
        """
        This method filters the relevant data to save (Distribution or OOS) and transform that DataFrame into a
        convenient dictionary with product_fk, numerator_result and denominator_result
        :param lvl3_data: Assortment SKU level results
        :param is_distribution: True if the KPI is distribution, else (KPI is OOS) False.
        :return: A dictionary which contains the following keys: manufacturer_fk, denominator_id, numerator_result
        and denominator_result.
        """
        store_result_dict = dict()
        store_result_dict[SessionResultsConsts.NUMERATOR_RESULT] = lvl3_data.in_store.sum()
        store_result_dict[SessionResultsConsts.DENOMINATOR_RESULT] = lvl3_data.in_store.count()
        store_result_dict[ProductsConsts.MANUFACTURER_FK] = self.own_manufacturer_fk
        store_result_dict[SessionResultsConsts.DENOMINATOR_ID] = self.store_id
        if not is_distribution:  # In OOS the numerator is total - distribution
            store_result_dict[SessionResultsConsts.NUMERATOR_RESULT] = \
                store_result_dict[SessionResultsConsts.DENOMINATOR_RESULT] - store_result_dict[
                    SessionResultsConsts.NUMERATOR_RESULT]
            self.oos_store_results.append(store_result_dict)
        return [store_result_dict]

    def _calculate_category_level_assortment(self, lvl3_data, is_distribution):
        """
        This method grouping by the assortment SKUs per category and returns the amount of in_store SKUs per product.
        :param lvl3_data: The SKU level assortment data
        :param is_distribution: True if we calculating Distribution, False if we calculating OOS
        :return: A dictionary which contains the following keys: category_fk, denominator_id, numerator_result and
        denominator_result.
        """
        in_store_per_category = lvl3_data[[ScifConsts.CATEGORY_FK, Consts.IN_STORE]].fillna(0)
        in_store_per_category = in_store_per_category.groupby(ScifConsts.CATEGORY_FK, as_index=False).agg(
            ['sum', 'count'])
        in_store_per_category.columns = in_store_per_category.columns.droplevel(0)
        in_store_per_category.reset_index(inplace=True)
        in_store_per_category = in_store_per_category.assign(denominator_id=self.own_manufacturer_fk)
        in_store_per_category.rename(Consts.AGGREGATION_COLUMNS_RENAMING, inplace=True, axis=1)
        if not is_distribution:  # In OOS the numerator is total - distribution
            in_store_per_category[SessionResultsConsts.NUMERATOR_RESULT] = \
                in_store_per_category[SessionResultsConsts.DENOMINATOR_RESULT] - in_store_per_category[
                    SessionResultsConsts.NUMERATOR_RESULT]
        in_store_per_category = in_store_per_category.to_dict('records')
        return in_store_per_category

    @staticmethod
    def _calculate_sku_level_assortment(lvl3_data, is_distribution):
        """
        This method filters the relevant data to save (Distribution or OOS) and transform that DataFrame into a
        convenient dictionary with product_fk, numerator_result and denominator_result
        :param lvl3_data: Assortment SKU level results
        :param is_distribution: True if the KPI is distribution, else (KPI is OOS) False.
        :return: A dictionary - which contains the following keys: product_fk, denominator_id, numerator_result and
        denominator_result.
        """
        sku_level_res = lvl3_data[
            [ProductsConsts.PRODUCT_FK, Consts.IN_STORE, ScifConsts.CATEGORY_FK, ScifConsts.FACINGS]]
        sku_level_res.rename(Consts.SOS_SKU_LVL_RENAME, axis=1, inplace=True)
        if not is_distribution:
            sku_level_res = sku_level_res.assign(denominator_result=1)
        sku_level_res = sku_level_res.to_dict('records')
        return sku_level_res

    def _calculate_distribution_and_oos(self, lvl3_data, policy, is_dist):
        """
        This method calculates the 3 levels of the assortment.
        :param lvl3_data: Assortment SKU level results + category_fk column.
        :param policy:  חלבי או טירת צבי - this policy is matching for scene types and products as well
        """
        if lvl3_data.empty:
            Log.warning(Consts.LOG_EMPTY_ASSORTMENT_DATA_PER_POLICY.format(policy.encode(HelperConsts.UTF8)))
            return
        store_level_kpi_fk, cat_lvl_fk, sku_level_fk = self._get_assortment_kpi_fks(policy, is_distribution=is_dist)
        store_res = self._calculate_store_level_assortment(lvl3_data, is_distribution=is_dist)
        category_res = self._calculate_category_level_assortment(lvl3_data, is_distribution=is_dist)
        sku_level_res = self._calculate_sku_level_assortment(lvl3_data, is_distribution=is_dist)
        self._save_results_for_assortment(ProductsConsts.MANUFACTURER_FK, store_res, store_level_kpi_fk)
        self._save_results_for_assortment(ScifConsts.CATEGORY_FK, category_res, cat_lvl_fk, store_level_kpi_fk)
        self._save_results_for_assortment(ProductsConsts.PRODUCT_FK, sku_level_res, sku_level_fk, cat_lvl_fk)
        if not is_dist:  # New addition in order to support OOS reasons and NCC report
            sku_no_policy_kpi, policy_level_kpi, sku_level_kpi = self._get_oos_reason_and_ncc_kpis(policy)
            self._save_results_for_assortment(ProductsConsts.PRODUCT_FK, sku_level_res, sku_no_policy_kpi, None, True)
            self._save_results_for_assortment(ProductsConsts.PRODUCT_FK, sku_level_res, sku_level_kpi, None, True)
            self._save_results_for_assortment(ProductsConsts.MANUFACTURER_FK, store_res, policy_level_kpi, None, True)

    def _get_oos_reason_and_ncc_kpis(self, policy):
        """
        This fetcher gets the relevant policy and returns the relevant OOS KPIs.
        :param policy:  חלבי או טירת צבי - this policy is matching for scene types and products as well
        :return: A tuple with 3 KPIs: (store_level, policy_level, sku_level)
        """
        if policy == Consts.MILKY_POLICY:
            sku_no_policy_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_IN_STORE_LEVEL)
            category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_DAIRY_PREV_RES)
            sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_DAIRY_PREV_RES)
        else:
            sku_no_policy_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_IN_STORE_LEVEL)
            category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_TIRAT_TSVI_PREV_RES)
            sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_TIRAT_TSVI_PREV_RES)
        return sku_no_policy_kpi, category_level_fk, sku_level_fk

    def _save_results_for_assortment(self, numerator_entity, results_list, kpi_fk, parent_kpi_fk=None, prev_res=False):
        """
        This method saves the assortments results for all of the levels.
        For OOS the last session's results per product from this store is being saved as "score"!
        :param numerator_entity: The key of the numerator id that can be found in the results dictionary.
        :param results_list: List of dictionaries with the assortment results.
        :param kpi_fk: Relevant fk to save.
        :param prev_res: It will be True in cases
        """
        should_enter = True if parent_kpi_fk is not None else False
        for result in results_list:
            numerator_id, denominator_id = result[numerator_entity], result[SessionResultsConsts.DENOMINATOR_ID]
            num_res = result[SessionResultsConsts.NUMERATOR_RESULT]
            denominator_res = result[SessionResultsConsts.DENOMINATOR_RESULT]
            score, result = self._calculate_assortment_score_and_result(numerator_entity, num_res, denominator_res)
            score = self._get_previous_oos_score(kpi_fk, numerator_id) if prev_res else score   # Only for OOS-SKU!
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, numerator_result=num_res,
                                              denominator_id=self.store_id, denominator_result=denominator_res,
                                              score=score, result=result, should_enter=should_enter,
                                              identifier_result=(kpi_fk, numerator_id),
                                              identifier_parent=(parent_kpi_fk, denominator_id))

    def _calculate_assortment_score_and_result(self, numerator_entity, numerator_result, denominator_result):
        """
        This method calculates the relevant assortment score. We need to handle SKU level differently in order
        to support the kpi_result_type.
        :param numerator_entity: The relevant assortment entity. E.g: "category_fk" or "product_fk"
        :return: A tuple of score and result. The result represent the kpi_type_result_fk.
        """
        total_score = round((numerator_result / float(denominator_result)) * 100, 2) if denominator_result else 0
        if numerator_entity == ProductsConsts.PRODUCT_FK:
            total_score = 100 if total_score > 0 else 0
            result_type = Consts.DISTRIBUTION_TYPE if total_score else Consts.OOS_TYPE
            result = self.kpi_result_types.loc[self.kpi_result_types.value == result_type, BasicConsts.PK].values[0]
        else:
            result = total_score
        return total_score, result

    def _get_previous_oos_score(self, kpi_fk, numerator_id):
        """
        In order to support special NCC report, this method matches the relevant OOS result from the previous
        session in the SKU level.
        :param kpi_fk: kpi_level_2_fk.
        :param numerator_id: The relevant product fk for the oos results.
        :return: The previous OOS results per kpi_fk and product_fk.
        """
        if self.previous_oos_results is None:
            return 0
        prev_result = self.previous_oos_results.loc[(self.previous_oos_results.kpi_level_2_fk == kpi_fk) & (
                self.previous_oos_results.numerator_id == numerator_id)]
        return prev_result.result.values[0] if not prev_result.empty else 0

    def _get_sos_kpi_fks(self, policy):
        """
        The SOS has 3 level for each policy. So this method gets the relevant policy (חלבי או טירת צבי) and returns A
        tuple with the relevant KPIs (store_level, own manufacturer_out_of_category, all_manufacturers_out_of_category)
        """
        if policy == Consts.MILKY_POLICY:
            store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_MANU_OUT_OF_STORE_KPI_DAIRY)
            own_manu_category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_OWN_MANU_OUT_OF_CAT_KPI_DAIRY)
            all_manu_category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_ALL_MANU_OUT_OF_CAT_KPI_DAIRY)
        else:
            store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_MANU_OUT_OF_STORE_KPI_TIRAT_TSVI)
            own_manu_category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_OWN_MANU_OUT_OF_CAT_KPI_TSVI)
            all_manu_category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_ALL_MANU_OUT_OF_CAT_KPI_TSVI)
        return store_level_fk, own_manu_category_level_fk, all_manu_category_level_fk

    def _calculate_sos_by_policy(self, policy):
        """
        This method calculating the SOS for the relevant policy it gets. It gets the relevant KPIs FKs, does the
        calculation and saves the results.
        """
        filtered_scif_by_policy = self._get_filtered_scif_for_sos_calculations(policy)
        if filtered_scif_by_policy.empty:
            return
        store_level_kpi, own_manu_out_of_cat_kpi, all_manu_out_of_cat_kpi = self._get_sos_kpi_fks(policy)
        # Store level calculation
        num_result, denominator_result = self._calculate_own_manufacturer_sos(filtered_scif_by_policy)
        sos_score = round(num_result / float(denominator_result)*100, 2) if denominator_result else 0
        self.common_v2.write_to_db_result(fk=store_level_kpi, numerator_id=self.own_manufacturer_fk, score=sos_score,
                                          numerator_result=num_result, denominator_id=self.store_id, result=sos_score,
                                          denominator_result=denominator_result, identifier_result=store_level_kpi)
        # Category level calculations
        category_results = self._calculate_category_levels_sos(filtered_scif_by_policy)
        self._save_results_for_sos(category_results, store_level_kpi, own_manu_out_of_cat_kpi, all_manu_out_of_cat_kpi)

    def _calculate_category_levels_sos(self, filtered_scif):
        """
        This function calculates SOS results per every category.
        :param filtered_scif: Scene item facts that filtered by the relevant policy.
        :return: A list of dictionaries with the results.
        """
        category_results = []
        categories_list = filtered_scif.category_fk.unique().tolist()
        for category_fk in categories_list:
            category_results.extend(self._calculate_manufacturer_of_out_category_sos(filtered_scif, category_fk))
        return category_results

    @staticmethod
    def _general_sos_calculation(df_to_filter, **sos_filters):
        """
        This method calculate sos according to the relevant filters and return numerator result and denominator result.
        :param df_to_filter: The DataFrame to filter. The sum of it's facing will be the denominator result.
        :param sos_filters: Dictionary that will be send to ParseInputKPI module.
        :return: A tuple of numerator result (sum of facing of the new DataFrame) and denominator_result (sum of facings
        before the filtering)
        """
        population_filters = {ParseInputKPI.POPULATION: {ParseInputKPI.INCLUDE: [sos_filters]}}
        filtered_df = ParseInputKPI.filter_df(population_filters, df_to_filter)
        numerator_result = filtered_df[ScifConsts.FACINGS_IGN_STACK].sum()
        denominator_result = df_to_filter[ScifConsts.FACINGS_IGN_STACK].sum()
        return numerator_result, denominator_result

    def _calculate_own_manufacturer_sos(self, df_to_filter):
        """ The method calculates the SOS of Tnuva Manufacturer out of the relevant DataFrame and returns a tuple:
        numerator result (sum of facing of the new DataFrame) and denominator_result
        (sum of facings before the filtering)"""
        filters = {ProductsConsts.MANUFACTURER_FK: self.own_manufacturer_fk}
        numerator_result, denominator_result = self._general_sos_calculation(df_to_filter, **filters)
        return numerator_result, denominator_result

    def _calculate_manufacturer_of_out_category_sos(self, df_to_filter, category_fk):
        """ The method calculates the SOS of Tnuva Manufacturer out of the relevant DataFrame and returns a tuple:
        numerator result (sum of facing of the new DataFrame) and denominator_result
        (sum of facings before the filtering)"""
        results_list = list()
        filtered_scif_by_category = df_to_filter.loc[df_to_filter.category_fk == category_fk]
        manufacturers_list = filtered_scif_by_category.manufacturer_fk.unique().tolist()
        if self.own_manufacturer_fk not in manufacturers_list:
            manufacturers_list.append(self.own_manufacturer_fk)
        for manufacturer in manufacturers_list:
            sos_result = {key: 0 for key in Consts.ENTITIES_FOR_DB}
            filters = {ProductsConsts.MANUFACTURER_FK: int(manufacturer)}
            numerator_result, denominator_result = self._general_sos_calculation(filtered_scif_by_category, **filters)
            sos_result[SessionResultsConsts.NUMERATOR_RESULT] = numerator_result
            sos_result[SessionResultsConsts.DENOMINATOR_RESULT] = denominator_result
            sos_result[ProductsConsts.MANUFACTURER_FK], sos_result[ScifConsts.CATEGORY_FK] = manufacturer, category_fk
            results_list.append(sos_result)
        return results_list

    def _save_results_for_sos(self, results, store_lvl_fk, own_manu_out_of_category_fk, all_manu_out_of_category_fk):
        """ This method saves results for category level SOS KPIs. The hierarchy is manufacturer-store,
        own manufacturer-all category and all manufacturer-in-category. So in order to avoid double calculations we
        are checking if current manufacturer is own manufacturer and saves it in both level.
        """
        for res_dict in results:
            manufacturer_id, category_id = res_dict[ProductsConsts.MANUFACTURER_FK], res_dict[ScifConsts.CATEGORY_FK]
            numerator_res = res_dict[SessionResultsConsts.NUMERATOR_RESULT]
            denominator_res = res_dict[SessionResultsConsts.DENOMINATOR_RESULT]
            sos_score = round(numerator_res / float(denominator_res)*100, 2) if denominator_res else 0
            if res_dict[ProductsConsts.MANUFACTURER_FK] == self.own_manufacturer_fk:
                self.common_v2.write_to_db_result(fk=own_manu_out_of_category_fk, numerator_id=category_id,
                                                  numerator_result=numerator_res, denominator_id=manufacturer_id,
                                                  denominator_result=denominator_res, score=sos_score, result=sos_score,
                                                  identifier_result=(own_manu_out_of_category_fk, category_id),
                                                  identifier_parent=store_lvl_fk, should_enter=True)
            if not sos_score:
                continue  # Tnuva should be represented in the second level (even if 0) but not in the third!
            self.common_v2.write_to_db_result(fk=all_manu_out_of_category_fk, numerator_id=manufacturer_id,
                                              numerator_result=numerator_res, denominator_id=category_id,
                                              denominator_result=denominator_res, score=sos_score, result=sos_score,
                                              identifier_result=all_manu_out_of_category_fk,  should_enter=True,
                                              identifier_parent=(own_manu_out_of_category_fk, category_id))