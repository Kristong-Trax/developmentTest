# coding=utf-8
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from Projects.TNUVAIL_SAND.Utils.Consts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment


__author__ = 'idanr'


class TNUVAILSANDToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common_v2 = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.assortment = Assortment(self.data_provider, self.output)
        self.own_manufacturer_fk = self.data_provider.own_manufacturer.param_value.values[0]

    def main_calculation(self):
        """ This function calculates all of the KPIs' results """
        self._calculate_facings_sos()
        self._calculate_assortment()
        self.common_v2.commit_results_data()

    def _calculate_assortment(self):
        """
        This the main function for assortment calculation. It prepares the data and calculating all of the relevant
        policies. In this project there is abuse of notion of "Assortment" KPI - Distribution and OOS are been
        calculated on different data. Distribution is been calculated for everything and OOS only for
        "Obligatory" policy
        """
        lvl3_result = self._prepare_data_for_assortment_calculation()
        if lvl3_result.empty:
            Log.warning(Consts.EMPTY_ASSORTMENT_DATA)
            return
        self._calculate_assortment_results_per_policy(lvl3_result, policy=Consts.MILKY_POLICY)
        self._calculate_assortment_results_per_policy(lvl3_result, policy=Consts.TIRAT_TSVI_POLICY)

    def _prepare_data_for_assortment_calculation(self):
        """ This method gets the level 3 assortment results (SKU level), adding category_fk and returns the DataFrame"""
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        category_per_product = self.all_products[[Consts.PRODUCT_FK, Consts.CATEGORY_FK]]
        lvl3_result = pd.merge(lvl3_result, category_per_product, how='left')
        return lvl3_result

    def _calculate_assortment_results_per_policy(self, lvl3_result, policy):
        """
        This method triggering the Distribution and OOS calculations for the relevant policy.
        :param lvl3_result: Assortment SKU level results + category_fk column.
        :param policy: חלבי או טירת צבי
        """
        lvl3_data = self._get_relevant_assortment_data(lvl3_result, policy)
        if lvl3_data.empty:
            Log.warning(Consts.LOG_EMPTY_ASSORTMENT_DATA_PER_POLICY.format(policy))     # todo!! check hebrew !!!
        self._calculate_distribution_and_oos(lvl3_data, policy, is_dist=True)
        self._calculate_distribution_and_oos(lvl3_data, policy, is_dist=False)

    def _get_filtered_scif_for_sos_calculations(self):
        """ This method filters the relevant attribute from scene item facts for sos calculation"""
        filtered_scif = self.scif[~self.scif[Consts.PRODUCT_TYPE].isin(Consts.TYPES_TO_IGNORE_IN_SOS)]
        filtered_scif = filtered_scif.loc[filtered_scif[Consts.FACINGS_FOR_SOS] > 0]
        return filtered_scif

    def _get_filtered_scif_per_scene_type(self, scene_type):
        """
        This method filters scene item facts by relevant scene type.
        :param scene_type:  חלבי או טירת צבי scene types.
        :return: filtered scif and match product in scene DataFrames.
        """
        filtered_scif = self.scif.loc[self.scif.template_name.str.encode('utf-8') == scene_type.encode('utf-8')]
        filtered_scif = filtered_scif.loc[filtered_scif.facings > 0]
        return filtered_scif

    def _get_relevant_assortment_data(self, lvl3_assortment, policy):
        """
        This method filters only the relevant assortment data according to the relevant scene type to calculate.
        :param policy:  חלבי או טירת צבי - this policy is matching for scene types and products as well
        :return:
        """
        # Products with the relevant policy attribute
        product_with_relevant_policy = self.all_products.loc[
            self.all_products[Consts.PRODUCT_POLICY_ATTR].str.encode('utf-8') == policy.encode('utf-8')]
        product_with_relevant_policy = product_with_relevant_policy.product_fk.unique().tolist()
        # Products that appear in scenes with the relevant policy
        filtered_scif = self._get_filtered_scif_per_scene_type(policy)
        relevant_products_in_scene = filtered_scif.product_fk.unique().tolist()
        # Filtering by their intersection
        relevant_products = list(set(product_with_relevant_policy) & set(relevant_products_in_scene))
        relevant_lvl3_assortment_data = lvl3_assortment.loc[lvl3_assortment.product_fk.isin(relevant_products)]
        return relevant_lvl3_assortment_data

    def _get_assortment_kpi_fks(self, policy, is_distribution):
        """
        This project have 12 different kpis for distribution and oos. So this method returns the relevant ones
        according to the scene_type, level and distribution / oos.
        :param is_distribution: True if the KPI is distribution, else (KPI is OOS) False.
        :return: The relevant Assortment KPI name by attributes.
        """
        if is_distribution:
                if policy == Consts.DIST_STORE_LEVEL_MILKY:
                    store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_STORE_LEVEL_MILKY)
                    category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_CATEGORY_LEVEL_MILKY)
                    sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_SKU_LEVEL_MILKY)
                else:
                    store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_STORE_LEVEL_TIRAT_TSVI)
                    category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_CATEGORY_LEVEL_TIRAT_TSVI)
                    sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DIST_SKU_LEVEL_TIRAT_TSVI)
        else:
            if policy == Consts.DIST_STORE_LEVEL_MILKY:
                store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_LEVEL_MILKY)
                category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_CATEGORY_LEVEL_MILKY)
                sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LEVEL_MILKY)
            else:
                store_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_LEVEL_TIRAT_TSVI)
                category_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_CATEGORY_LEVEL_TIRAT_TSVI)
                sku_level_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LEVEL_TIRAT_TSVI)
        return store_level_fk, category_level_fk, sku_level_fk

    @staticmethod
    def _calculate_store_level_assortment(lvl3_data, is_distribution=True):
        """
        This method filters the relevant data to save (Distribution or OOS) and transform that DataFrame into a
        convenient dictionary with product_fk, numerator_result and denominator_result
        :param lvl3_data: Assortment SKU level results
        :param is_distribution: True if the KPI is distribution, else (KPI is OOS) False.
        :return:
        """
        store_result_dict = dict()       # todo ???
        store_result_dict[Consts.NUMERATOR_RESULT] = lvl3_data.in_store.sum()
        store_result_dict[Consts.DENOMINATOR_RESULT] = lvl3_data.in_store.count()
        store_result_dict['TODO TODO TODO CHOOSE ENTITY'] = '?????????'     # TODO TODO TODO
        if not is_distribution:  # In OOS the numerator is total - distribution
            store_result_dict[Consts.NUMERATOR_RESULT] = store_result_dict[Consts.DENOMINATOR_RESULT] - \
                                                         store_result_dict[Consts.NUMERATOR_RESULT]
        return [store_result_dict]

    @staticmethod
    def _calculate_category_level_assortment(lvl3_data, is_distribution):
        """
        This method grouping by the assortment SKUs per category and returns the amount of in_store skus per product.
        :param lvl3_data: The SKU level assortment data
        :param is_distribution: True if we calculating Distribution, False if we calculating OOS
        :return: A dictionary - the key is the category_fk and the value is the amount of SKUs in store from that
        category. E.g: {1: 10, 2:0, 3:22 ..}
        """
        in_store_per_category = lvl3_data[[Consts.CATEGORY_FK, 'in_store']].fillna(0)  # todo Replace with CONST
        in_store_per_category = in_store_per_category.groupby(Consts.CATEGORY_FK, as_index=False).agg(['sum', 'count'])
        in_store_per_category.columns = in_store_per_category.columns.droplevel(0)
        in_store_per_category.reset_index(inplace=True)
        in_store_per_category.rename(Consts.AGGREGATION_COLUMNS_RENAMING, inplace=True, axis=1)
        if not is_distribution:  # In OOS the numerator is total - distribution
            in_store_per_category[Consts.NUMERATOR_RESULT] = in_store_per_category[Consts.DENOMINATOR_RESULT] - \
                                                             in_store_per_category[Consts.NUMERATOR_RESULT]
        in_store_per_category = in_store_per_category.to_dict('records')
        return in_store_per_category

    @staticmethod
    def _calculate_sku_level_assortment(lvl3_data, is_distribution):
        """
        This method filters the relevant data to save (Distribution or OOS) and transform that DataFrame into a
        convenient dictionary with product_fk, numerator_result and denominator_result
        :param lvl3_data: Assortment SKU level results
        :param is_distribution: True if the KPI is distribution, else (KPI is OOS) False.
        :return:
        """
        in_store_relevant_attribute = 1 if is_distribution else 0
        sku_level_res = lvl3_data.loc[lvl3_data.in_store == in_store_relevant_attribute]
        sku_level_res = sku_level_res[[Consts.PRODUCT_FK, 'in_store']]  # todo Consts.IN_STORE
        sku_level_res = sku_level_res.assign(denominator_result=1).rename({'in_store': 'numerator_result'}, axis=1)
        sku_level_res = sku_level_res.to_dict('records')
        return sku_level_res

    def _calculate_distribution_and_oos(self, lvl3_data, policy, is_dist):
        """
        This method calculates the 3 levels of the assortment.
        :param lvl3_data: Assortment SKU level results + category_fk column.
        :param policy: חלבי או טירת צבי
        """
        store_level_kpi_fk, cat_lvl_fk, sku_level_fk = self._get_assortment_kpi_fks(policy, is_distribution=is_dist)
        store_results = self._calculate_store_level_assortment(lvl3_data, is_distribution=is_dist)
        category_results = self._calculate_category_level_assortment(lvl3_data, is_distribution=is_dist)
        sku_level_results = self._calculate_sku_level_assortment(lvl3_data, is_distribution=is_dist)
        self._save_results_for_assortment_('TODO!!!', store_results, store_level_kpi_fk)
        self._save_results_for_assortment_(Consts.CATEGORY_FK, category_results, cat_lvl_fk, store_level_kpi_fk)
        self._save_results_for_assortment_(Consts.PRODUCT_FK, sku_level_results, sku_level_fk, cat_lvl_fk)

    def _save_results_for_assortment_(self, numerator_entity, results_list, kpi_fk, parent_kpi_fk=None):
        """
        This method saves the assortments results for all of the levels.
        :param numerator_entity: The key of the numerator id that can be found in the results dictionary.
        :param results_list: List of dictionaries with the assortment results.
        :param kpi_fk: Relevant fk to save.
        :param parent_kpi_fk.
        """
        should_enter = True if parent_kpi_fk is not None else False
        for result in results_list:
            numerator_id = result[numerator_entity]
            num_res, denominator_res = result[Consts.NUMERATOR_RESULT], result[Consts.DENOMINATOR_RESULT]
            total_score = round((num_res / float(denominator_res))*100, 2) if denominator_res else 0
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, numerator_result=num_res,
                                              denominator_id=self.store_id, denominator_result=denominator_res,
                                              score=total_score, result=total_score, should_enter=should_enter,
                                              identifier_result=kpi_fk, identifier_parent=parent_kpi_fk)

    def _calculate_facings_sos(self):
        """
        This kpi calculates SOS in 3 levels: Manufacturer out of store, Manufacturer Out of Category and
        Manufacturer out of Category. """
        filtered_scif = self._get_filtered_scif_for_sos_calculations()
        # Calculate own manufacturer out of store
        manufacturer_out_of_store_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_MANUFACTURER_OUT_OF_STORE_KPI)
        num_result, denominator_result = self._calculate_own_manufacturer_sos(filtered_scif)
        sos_score = round(num_result / float(denominator_result)*100, 2) if denominator_result else 0
        self.common_v2.write_to_db_result(fk=manufacturer_out_of_store_fk, numerator_id=self.own_manufacturer_fk,
                                          numerator_result=num_result, denominator_id=self.store_id,
                                          denominator_result=denominator_result, score=sos_score, result=sos_score)
        # Calculate manufacturers out of Categories
        category_results = []
        categories_list = filtered_scif.category_fk.unique().tolist()
        for category_fk in categories_list:
            category_results.extend(self._calculate_manufacturer_of_out_category_sos(filtered_scif, category_fk))
        self._save_results_for_sos(category_results, parent_identifier=manufacturer_out_of_store_fk)

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
        numerator_result = filtered_df[Consts.FACINGS_FOR_SOS].sum()
        denominator_result = df_to_filter[Consts.FACINGS_FOR_SOS].sum()
        return numerator_result, denominator_result

    def _calculate_own_manufacturer_sos(self, df_to_filter):
        """ The method calculates the SOS of Tnuva Manufacturer out of the relevant DataFrame and returns a tuple:
        numerator result (sum of facing of the new DataFrame) and denominator_result
        (sum of facings before the filtering)"""
        filters = {Consts.MANUFACTURER_FK: int(self.own_manufacturer_fk)}
        numerator_result, denominator_result = self._general_sos_calculation(df_to_filter, **filters)
        return numerator_result, denominator_result

    def _calculate_manufacturer_of_out_category_sos(self, df_to_filter, category_fk):
        """ The method calculates the SOS of Tnuva Manufacturer out of the relevant DataFrame and returns a tuple:
        numerator result (sum of facing of the new DataFrame) and denominator_result
        (sum of facings before the filtering)"""
        results_list = list()
        filtered_scif_by_category = df_to_filter.loc[df_to_filter.category_fk == category_fk]
        manufacturers_list = filtered_scif_by_category.manufacturer_fk.unique().tolist()
        for manufacturer in manufacturers_list:
            sos_result = {key: 0 for key in Consts.ENTITIES_FOR_DB}
            filters = {Consts.MANUFACTURER_FK: int(manufacturer)}
            numerator_result, denominator_result = self._general_sos_calculation(filtered_scif_by_category, **filters)
            sos_result[Consts.NUMERATOR_RESULT] = numerator_result
            sos_result[Consts.DENOMINATOR_RESULT] = denominator_result
            sos_result[Consts.MANUFACTURER_FK], sos_result[Consts.CATEGORY_FK] = manufacturer, category_fk
            results_list.append(sos_result)
        return results_list

    def _save_results_for_sos(self, results, parent_identifier):
        """ This method saves results for SOS KPI. The hierarchy is manufacturer-store, own manufacturer-all category
        and all manufacturer-in-category. This is why there is a condition that checking if the current manufacturer is
        the own manufacturer.
        """
        own_manu_out_of_category_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_OWN_MANUFACTURER_OUT_OF_CAT_KPI)
        all_manu_out_of_category_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_ALL_MANUFACTURER_OUT_OF_CAT_KPI)
        for res_dict in results:
            manufacturer_id, category_id = res_dict[Consts.MANUFACTURER_FK], res_dict[Consts.CATEGORY_FK]
            numerator_res, denominator_res = res_dict[Consts.NUMERATOR_RESULT], res_dict[Consts.DENOMINATOR_RESULT]
            sos_score = round(numerator_res / float(denominator_res)*100, 2) if denominator_res else 0
            if res_dict[Consts.MANUFACTURER_FK] == self.own_manufacturer_fk:
                self.common_v2.write_to_db_result(fk=own_manu_out_of_category_fk, numerator_id=category_id,
                                                  numerator_result=numerator_res, denominator_id=manufacturer_id,
                                                  denominator_result=denominator_res, score=sos_score, result=sos_score,
                                                  identifier_result=own_manu_out_of_category_fk,
                                                  identifier_parent=parent_identifier, should_enter=True)
            self.common_v2.write_to_db_result(fk=all_manu_out_of_category_fk, numerator_id=manufacturer_id,
                                              numerator_result=numerator_res, denominator_id=category_id,
                                              denominator_result=denominator_res, score=sos_score, result=sos_score,
                                              identifier_result=all_manu_out_of_category_fk,
                                              identifier_parent=own_manu_out_of_category_fk, should_enter=True)
