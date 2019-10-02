from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.NESTLEIL_SAND.Utils.Consts import Consts
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Consts.DB import StaticKpis

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

    def main_calculation(self):
        """ This function calculates the KPI results."""
        self._calculate_assortment()
        self.common_v2.commit_results_data()

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
        # Getting KPI fks
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
        distribution_store_level_kpi = self.common_v2.get_kpi_fk_by_kpi_type(distr_kpi_type)
        oos_kpi_type = Consts.DIST_OOS_KPIS_MAP[distr_kpi_type]
        oos_store_level_kpi = self.common_v2.get_kpi_fk_by_kpi_type(oos_kpi_type)
        return distribution_store_level_kpi, oos_store_level_kpi

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
        sku_level_res = sku_level_res.assign(denominator_id=self.own_manufacturer_fk, denominator_result=1)
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
                                              denominator_id=self.store_id, denominator_result=den_res,
                                              score=score, result=result, identifier_result=(kpi_fk, numerator_id),
                                              should_enter=should_enter, identifier_parent=identifier_parent)
