import numpy as np
import pandas as pd
import json

from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.NESTLEIL.Utils.Consts import Consts
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.Utils.Consts.DB import StaticKpis, SessionResultsConsts
from Projects.NESTLEIL_SAND.Utils.Fetcher import NestleilQueries
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from KPIUtils_v2.Utils.Consts.PS import ExternalTargetsConsts


__author__ = 'idanr'


class NESTLEILToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.ps_data_provider = PsDataProvider(data_provider)
        self.kpi_result_values = self.ps_data_provider.get_result_values()
        self.common_v2 = Common(self.data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.assortment = Assortment(self.data_provider)
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.kpi_static_data = self.common_v2.kpi_static_data[['pk', StaticKpis.TYPE]]

        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.project_name = self.data_provider.project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.external_targets = self._retrieve_completeness_external_targets()
        self.products_trax_cat = self._get_products_with_trax_categories()
        self.ass_groups_present = {Consts.DISTR_SNACKS: 0, Consts.DISTR_SABRA: 0}

    def _get_products_with_trax_categories(self):
        products_categories = self._get_trax_category_for_products()
        prod_group_trax_cat_map = self.external_targets[[Consts.PRODUCT_GROUP_FK, Consts.TRAX_CATEGORY_FK]]
        products_categories = products_categories.merge(prod_group_trax_cat_map, on=Consts.TRAX_CATEGORY_FK, how='left')
        products_categories[Consts.IS_INCLUDED] = 0
        return products_categories

    def _get_trax_category_for_products(self):
        query = NestleilQueries.get_trax_category_for_products_query()
        products_categories = pd.read_sql_query(query, self.rds_conn.db)
        return products_categories

    def _retrieve_completeness_external_targets(self):
        external_targets = self._get_kpi_external_targets()
        external_targets = external_targets.drop_duplicates(subset=[SessionResultsConsts.KPI_LEVEL_2_FK], keep='last')
        external_targets = self._unpack_kpi_targets_from_db(external_targets, 'key_json')
        return external_targets

    def _get_kpi_external_targets(self):
        query = NestleilQueries.kpi_external_targets_query(Consts.COMPLETENESS_CHECK, self.visit_date)
        external_targets = pd.read_sql_query(query, self.rds_conn.db)
        return external_targets

    def _unpack_kpi_targets_from_db(self, input_df, field_name):
        input_df['json_dict_with_pk'] = input_df.apply(self._add_pk_to_json, args=(field_name,), axis=1)
        json_dict_list = input_df['json_dict_with_pk'].values.tolist()
        output_df = pd.DataFrame(json_dict_list)
        input_df = input_df.merge(output_df, on='pk', how='left')
        return input_df

    @staticmethod
    def _add_pk_to_json(row, field_name):
        json_value = row[field_name]
        json_to_dict = json.loads(json_value)
        json_to_dict.update({'pk': row['pk']})
        return json_to_dict

    def main_calculation(self):
        """ This function calculates the KPI results."""
        self._determine_collection_completeness_for_product_groups()
        self._calculate_assortment()
        self.common_v2.commit_results_data()

    def _determine_collection_completeness_for_product_groups(self):
        self._define_snacks_completeness()
        self._define_sabra_completeness()

    def get_external_targets_row(self, kpi_name):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(kpi_name)
        external_targets = self.external_targets[self.external_targets[ExternalTargetsConsts.KPI_LEVEL_2_FK] == kpi_fk]
        et_row = pd.Series()
        if not external_targets.empty:
            et_row = external_targets.iloc[0]
        else:
            Log.error('No kpi targets set for kpi {}'.format(kpi_name))
        return et_row

    def _define_snacks_completeness(self):
        et_row = self.get_external_targets_row(Consts.COLLECTION_COMPLETENESS_SNACKS)
        brands_list = et_row['allocation_value']
        brands_facings = self.scif.groupby([ScifConsts.BRAND_FK], as_index=False).agg({ScifConsts.FACINGS: np.sum})
        brands_facings = brands_facings[brands_facings[ScifConsts.BRAND_FK].isin(brands_list)]
        brands_in_session = brands_facings[ScifConsts.BRAND_FK].values.tolist()
        all_brands_exist = all([brand in brands_in_session for brand in brands_list])
        score = 0
        if all_brands_exist:
            brands_below_threshold = brands_facings[brands_facings[ScifConsts.FACINGS] < 3]
            if brands_below_threshold.empty:
                score = 1
        self.ass_groups_present[Consts.DISTR_SNACKS] = score
        product_group_fk = et_row[Consts.PRODUCT_GROUP_FK]
        self.common_v2.write_to_db_result(fk=et_row[ExternalTargetsConsts.KPI_LEVEL_2_FK],
                                          numerator_id=product_group_fk, denominator_id=self.store_id, score=score,
                                          result=score)

    def _define_sabra_completeness(self):
        et_row = self.get_external_targets_row(Consts.COLLECTION_COMPLETENESS_SABRA)
        score = 0
        cat_list = et_row['allocation_value']
        cat_in_session = self.scif[ScifConsts.CATEGORY_FK].unique().tolist()
        all_categories_exist = all([cat in cat_in_session for cat in cat_list])
        if all_categories_exist:
            score = 1
        product_group_fk = et_row[Consts.PRODUCT_GROUP_FK]
        self.ass_groups_present[Consts.DISTR_SABRA] = score
        self.common_v2.write_to_db_result(fk=et_row[ExternalTargetsConsts.KPI_LEVEL_2_FK],
                                          numerator_id=product_group_fk, denominator_id=self.store_id, score=score,
                                          result=score)

    @kpi_runtime()
    def _calculate_assortment(self):
        """
        This method calculates and saves results into the DB.
        First, it calculate the results per store and sku level
        and than saves results both for Assortment and SKU.
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if lvl3_result.empty:
            Log.warning(Consts.EMPTY_ASSORTMENT_DATA)
            return
        lvl3_result = self._add_kpi_types_to_assortment_result(lvl3_result)
        lvl3_result = lvl3_result.merge(self.products_trax_cat, on=ScifConsts.PRODUCT_FK, how='left')
        # Getting KPI fks
        for ass_group, presence_score in self.ass_groups_present.items():
            lvl3_result.loc[lvl3_result[Consts.STORE_ASS_KPI_TYPE] == ass_group, Consts.IS_INCLUDED] = presence_score
        lvl3_result = lvl3_result[lvl3_result[Consts.IS_INCLUDED] == 1]
        store_lvl_kpi_list = lvl3_result[Consts.STORE_ASS_KPI_TYPE].unique().tolist()
        for store_lvl_kpi in store_lvl_kpi_list:
            relevant_lvl3_res = lvl3_result[lvl3_result[Consts.STORE_ASS_KPI_TYPE] == store_lvl_kpi]
            sku_lvl_kpi = relevant_lvl3_res[Consts.SKU_ASS_KPI_TYPE].values[0]
            dist_store_kpi_fk, oos_store_kpi_fk = self._get_ass_kpis(store_lvl_kpi)
            dist_sku_kpi_fk, oos_sku_kpi_fk = self._get_ass_kpis(sku_lvl_kpi)
            # Calculating the assortment results
            store_level_res = self._calculated_store_level_results(relevant_lvl3_res)
            sku_level_res = self._calculated_sku_level_results(relevant_lvl3_res)
            # Saving to DB
            self._save_results_to_db(store_level_res, dist_store_kpi_fk)
            self._save_results_to_db(sku_level_res, dist_sku_kpi_fk, dist_store_kpi_fk)
            self._save_results_to_db(store_level_res, oos_store_kpi_fk, is_complementary=True)
            self._save_results_to_db(sku_level_res, oos_sku_kpi_fk, oos_store_kpi_fk)

        # sku lvl assortment result for canvas
        self.calculate_aggregate_sku_lvl_assortment(lvl3_result)

    def calculate_aggregate_sku_lvl_assortment(self, lvl3_result):
        distr_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DISTRIBUTION_SKU_LEVEL)
        oos_kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LEVEL)
        for i, row in lvl3_result.iterrows():
            distr_res = row[Consts.IN_STORE]
            oos_res = 1 - distr_res
            self.common_v2.write_to_db_result(fk=distr_kpi_fk, numerator_id=row[Consts.PRODUCT_FK],
                                              denominator_id=self.own_manufacturer_fk, numerator_result=distr_res,
                                              denominator_result=1, context_id=row[Consts.PRODUCT_GROUP_FK],
                                              result=distr_res, score=distr_res)
            self.common_v2.write_to_db_result(fk=oos_kpi_fk, numerator_id=row[Consts.PRODUCT_FK],
                                              denominator_id=self.own_manufacturer_fk, numerator_result=oos_res,
                                              denominator_result=1, context_id=row[Consts.PRODUCT_GROUP_FK],
                                              result=oos_res, score=oos_res)

    def _add_kpi_types_to_assortment_result(self, lvl3_result):
        lvl3_result = lvl3_result.merge(self.kpi_static_data, left_on=Consts.KPI_FK_LVL2, right_on='pk', how='left')
        lvl3_result = lvl3_result.drop(['pk'], axis=1)
        lvl3_result.rename(columns={StaticKpis.TYPE: Consts.STORE_ASS_KPI_TYPE}, inplace=True)
        lvl3_result = lvl3_result.merge(self.kpi_static_data, left_on=Consts.KPI_FK_LVL3, right_on='pk', how='left')
        lvl3_result = lvl3_result.drop(['pk'], axis=1)
        lvl3_result.rename(columns={StaticKpis.TYPE: Consts.SKU_ASS_KPI_TYPE}, inplace=True)
        return lvl3_result

    def _get_ass_kpis(self, distr_kpi_type):
        """
            This method fetches the assortment KPIs in the store level
            :return: A tuple: (Distribution store KPI fk, OOS store KPI fk)
        """
        distribution_kpi = self.common_v2.get_kpi_fk_by_kpi_type(distr_kpi_type)
        oos_kpi_type = Consts.DIST_OOS_KPIS_MAP[distr_kpi_type]
        oos_kpi = self.common_v2.get_kpi_fk_by_kpi_type(oos_kpi_type)
        return distribution_kpi, oos_kpi

    # def _get_store_level_kpis(self):
    #     """
    #     This method fetches the assortment KPIs in the store level
    #     :return: A tuple: (Distribution store KPI fk, OOS store KPI fk)
    #     """
    #     distribution_store_level_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DISTRIBUTION_STORE_LEVEL)
    #     oos_store_level_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_STORE_LEVEL)
    #     return distribution_store_level_kpi, oos_store_level_kpi
    #
    # def _get_sku_level_kpis(self):
    #     """
    #     This method fetches the assortment KPIs in the SKU level
    #     :return: A tuple: (Distribution SKU KPI fk, OOS SKU KPI fk)
    #     """
    #     distribution_sku_level_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Consts.DISTRIBUTION_SKU_LEVEL)
    #     oos_sku_level_kpi = self.common_v2.get_kpi_fk_by_kpi_type(Consts.OOS_SKU_LEVEL)
    #     return distribution_sku_level_kpi, oos_sku_level_kpi

    def _calculated_store_level_results(self, lvl3_result):
        """
        This method calculates the assortment results in the store level
        :return: A list with a dictionary that includes the relevant entities for the DB
        """
        result = {key: 0 for key in Consts.ENTITIES_FOR_DB}
        result[Consts.NUMERATOR_RESULT] = lvl3_result.in_store.sum()
        result[Consts.DENOMINATOR_RESULT] = lvl3_result.in_store.count()
        result[Consts.NUMERATOR_ID] = self.own_manufacturer_fk
        result[Consts.DENOMINATOR_ID] = self.store_id
        return [result]

    def _calculated_sku_level_results(self, lvl3_result):
        """
        This method calculates the assortment results in the sku level
        :return: A dictionary with the relevant entities for the DB
        """
        sku_level_res = lvl3_result[[Consts.PRODUCT_FK, Consts.IN_STORE]]
        sku_level_res.rename(Consts.SOS_SKU_LVL_RENAME, axis=1, inplace=True)
        sku_level_res = sku_level_res.assign(denominator_id=self.own_manufacturer_fk)
        sku_level_res = sku_level_res.assign(denominator_result=1)
        sku_level_res = sku_level_res.to_dict('records')
        return sku_level_res

    def _calculate_assortment_score_and_result(self, num_result, den_result, save_result_values=False):
        """
        The method calculates the score & result per kpi. In order to support MR Icons results, in case of
        SKU level KPI, the method fetches the relevant pk from kpi_result_value entities.
        :return: A tuple of the score and result.
        """
        score = result = round((num_result / float(den_result)) * 100, 2) if den_result else 0
        if save_result_values:
            result_type = Consts.DISTRIBUTED_VALUE if score else Consts.OOS_VALUE
            result = self.kpi_result_values.loc[self.kpi_result_values.value == result_type, 'pk'].values[0]
        return score, result

    def _save_results_to_db(self, results_list, kpi_fk, parent_kpi_fk=None, is_complementary=False):
        """
        This method saves result into the DB. The only change between Distribution
        and OOS is the numerator result so it is taking this into consideration.
        :param results_list: A list of dictionary with the results.
        :param is_complementary: There are complementary KPIs (like distribution and OOS) that the only
        difference is in the numerator result (and the score that is being affected from it).
        So instead of calculating twice we are just changing the numerator result.
        """
        for result in results_list:
            numerator_id, denominator_id = result[Consts.NUMERATOR_ID], result[Consts.DENOMINATOR_ID]
            num_res, den_res = result[Consts.NUMERATOR_RESULT], result[Consts.DENOMINATOR_RESULT]
            num_res = den_res - num_res if is_complementary else num_res
            should_enter = save_result_values = True if parent_kpi_fk is not None else False
            score, result = self._calculate_assortment_score_and_result(num_res, den_res, save_result_values)
            identifier_parent = (parent_kpi_fk, denominator_id) if should_enter else None
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=numerator_id, numerator_result=num_res,
                                              denominator_id=denominator_id, denominator_result=den_res,
                                              score=score, result=result, identifier_result=(kpi_fk, numerator_id),
                                              should_enter=should_enter, identifier_parent=identifier_parent)
