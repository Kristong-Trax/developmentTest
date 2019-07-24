# coding=utf-8
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from Projects.TNUVAIL_SAND.Utils.Consts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'idanr'


class TNUVAILToolBox:

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

    def _get_filtered_scif_for_sos_calculations(self):
        """ This method filters the relevant attribute from scene item facts for sos calculation"""
        filtered_scif = self.scif[~self.scif[Consts.PRODUCT_TYPE].isin(Consts.TYPES_TO_IGNORE_IN_SOS)]
        filtered_scif = filtered_scif.loc[filtered_scif[Consts.FACINGS_FOR_SOS] > 0]
        return filtered_scif

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
        filters = {Consts.MANUFACTURER_FK: self.own_manufacturer_fk}
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
            filters = {Consts.MANUFACTURER_FK: manufacturer}
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
            sos_score = numerator_res / float(denominator_res) if denominator_res else 0
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

    def _get_filtered_scif_per_scene_type(self, scene_type):
        """
        This method filters scene item facts by relevant scene type.
        :param scene_type:  חלבי או טירת צבי scene types.
        :return: filtered scif and match product in scene DataFrames.
        """
        filtered_scif = self.scif.loc[self.scif.template_name == scene_type]
        filtered_scif = filtered_scif.loc[filtered_scif.facings > 0]
        return filtered_scif

    def _get_relevant_assortment_data(self, lvl3_assortment, scene_type_to_filter):
        """
        This method filters only the relevant assortment data according to the relevant scene type to calculate.
        :param scene_type_to_filter:  חלבי או טירת צבי scene types.
        :return:
        """
        filtered_scif = self._get_filtered_scif_per_scene_type(scene_type_to_filter)
        relevant_products = filtered_scif.product_fk.unique().tolist()
        relevant_lvl3_assortment_data = lvl3_assortment.loc[lvl3_assortment.product_fk.isin(relevant_products)]
        return relevant_lvl3_assortment_data, filtered_scif

    def _get_assortment_kpi_fks(self, policy, is_distribution):
        """
        This project have 12 different kpis for distribution and oos. So this method returns the relevant one according
        to the scene_type, level and distribution / oos.
        :param is_distribution: True if the KPI is distribution. False ==> OOS.
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
    def _calculate_category_level_distribution(lvl3_data):
        """
        This method grouping by the assortment SKUs per category and returns the amount of in_store skus per product.
        :param lvl3_data: The SKU level assortment data
        :return: A dictionary - the key is the category_fk and the value is the amount of SKUs in store from that
        category. E.g: {1: 10, 2:0, 3:22 ..}
        """
        in_store_per_category = lvl3_data[[Consts.CATEGORY_FK, Consts.IN_STORE]].fillna(0)
        in_store_per_category = in_store_per_category.groupby(Consts.CATEGORY_FK, as_index=False).agg(['sum', 'count'])
        in_store_per_category.columns = in_store_per_category.columns.droplevel(0)
        in_store_per_category = in_store_per_category[Consts.IN_STORE].to_dict()
        return in_store_per_category

    def _calculate_distribution(self, lvl3_data, policy):
        """

        :param lvl3_data:
        :param policy:
        :return:
        """
        store_level_kpi_fk, category_lvl_fk, sku_level_fk = self._get_assortment_kpi_fks(policy, is_distribution=True)
        category_per_product = self.all_products[[Consts.PRODUCT_FK, Consts.CATEGORY_FK]]
        lvl3_data = pd.merge(lvl3_data, category_per_product, how='left')
        store_num_res, store_denom_res = lvl3_data.in_store.sum(), lvl3_data.in_store.count()
        assortment_ratio = store_num_res / float(store_denom_res) if store_denom_res else 0
        self.common_v2.write_to_db_result(fk=store_level_kpi_fk, numerator_id=999, numerator_result=store_num_res,
                                          denominator_id=self.store_id, denominator_result=store_denom_res,
                                          score=assortment_ratio, result=assortment_ratio,
                                          identifier_result=store_level_kpi_fk)
        results_per_category = self._calculate_category_level_distribution(lvl3_data)

    def _save_results_for_assortment_category_level(self, results_per_category, kpi_fk, parent_kpi_fk):
        """

        :param category_lvl_kpi_fk:
        :return:
        """
        for category_fk, result in results_per_category.results_per_category.iteritems():
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=999, numerator_result=store_num_res,
                                              denominator_id=self.store_id, denominator_result=store_denom_res,
                                              score=result, result=result,
                                              identifier_result=kpi_fk, identifier_parent=parent_kpi_fk)

    def _calculate_assortment(self):
        """

        :return:
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        milky_data, filtered_scif = self._get_relevant_assortment_data(lvl3_result, Consts.MILKY_POLICY)
        tirat_tsvi_data, filtered_scif = self._get_relevant_assortment_data(lvl3_result, Consts.TIRAT_TSVI_POLICY)


    def _calculate_facings_sos(self):
        """
        This kpi calculates SOS in 3 levels: Manufacturer out of store, Manufacturer Out of Category and
        Manufacturer out of Category. """
        filtered_scif = self._get_filtered_scif_for_sos_calculations()
        # Calculate own manufacturer out of store
        num_result, denominator_result = self._calculate_own_manufacturer_sos(filtered_scif)
        sos_score = num_result / float(denominator_result) if denominator_result else 0
        manufacturer_out_of_store_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.SOS_MANUFACTURER_OUT_OF_STORE_KPI)
        self.common_v2.write_to_db_result(fk=manufacturer_out_of_store_fk, numerator_id=self.own_manufacturer_fk,
                                          numerator_result=num_result, denominator_id=self.store_id,
                                          denominator_result=denominator_result, score=sos_score, result=sos_score)
        # Calculate manufacturers out of Categories
        results_list = self._calculate_manufacturer_of_out_category_sos(filtered_scif)
        self._save_results_for_sos(results_list)

    def main_calculation(self):
        """ This function calculates the KPI results. """
        self._calculate_facings_sos()
        self._calculate_assortment()
        self.common_v2.commit_results_data()
