from datetime import datetime
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass

from KPIUtils.Calculations.LiveAssortment import LiveAssortmentCalculation
from KPIUtils_v2.DB.LiveCommon import LiveCommon
from Trax.Utils.Logging.Logger import Log


class CalculateKpi(LiveSessionBaseClass):
    LVL3_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score']
    LVL2_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score']
    SKU_LEVEL = 3
    GROUPS_LEVEL = 2
    LIVE_OOS = 'Live OOS'
    LIVE_OOS_SKU = 'Live OOS - SKU'
    LIVE_DIST = 'Live Distribution'
    LIVE_DIST_SKU = 'Live Distribution - SKU'
    DIST = 'Distribution'

    def __init__(self, data_provider, output):
        LiveSessionBaseClass.__init__(self, data_provider, output)
        self._data_provider = data_provider
        self.products = self._data_provider.products
        self.common = LiveCommon(data_provider)
        self.store_fk = data_provider.store_fk
        self.current_date = datetime.now()
        self.assortment = LiveAssortmentCalculation(data_provider, False, False)
        self._own_manufacturer = self._get_own_manufacturer()
        self.result_value = self.common.get_result_values()

    def calculate_session_live_kpi(self):
        """
        Main function of live project
        """
        self.availability_by_assortment_calc()
        self.common.commit_live_queries_session()

    @staticmethod
    def manipulate_result_row(df):
        """
        This function manipulate the result in group level to be in the same structure
        :param df: df with column 'result'
        """
        df.loc[df.result > 1] = 1
        df.loc[:, 'result'] = round(df['result'], 2) * 100

    def availability_by_assortment_calc(self):
        """
        This function calculate availability kpis based assortment.
        Kpis of : distribution and oos .
        In addition its apply "excluded products from Actions" on the assortment store results.
        :return:
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment(False)
        distribution_live = self.get_kpi_fk(self.DIST)
        lvl3_result = lvl3_result[lvl3_result.kpi_fk_lvl2 == distribution_live]
        if lvl3_result.empty:
            Log.warning('Assortment is Empty for this session')
            return
        self.assortment_result_base_actions(lvl3_result)  # this function will have the actions affect on lvl3_result
        lvl_2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
        lvl_2_result.rename(columns={'passes': 'numerator_result', 'total': 'denominator_result', 'kpi_fk_lvl2': 'kpi_level_2_fk'},
                            inplace=True)

        dist_result_lvl1, dist_results_lvl2 = self.distribution_calc(lvl3_result, lvl_2_result)
        self.write_results_to_db(dist_result_lvl1)
        self.write_results_to_db(dist_results_lvl2)

        oos_sku_res, oos_group_res = self.oos_calc(dist_result_lvl1, lvl_2_result)
        self.write_results_to_db(oos_sku_res)
        self.write_results_to_db(oos_group_res)

    def distribution_calc(self, lvl3_result, lvl2_result):
        """
        This function calculate distribution kpi based on assortment results.
        :param lvl3_result: assortment results on sku level
        :param lvl2_result: assortment results on group
        :return: dist_result_lvl1 -  results on sku level , dist_results_lvl2- results in group level
        """
        dist_result_lvl1 = self.distribution_sku_level(lvl3_result.copy())
        dist_results_lvl2 = self.distribution_group_level(lvl2_result)
        return dist_result_lvl1, dist_results_lvl2

    def oos_calc(self, lvl3_result, lvl2_result):
        """
       This function calculate oos kpi based on assortment results.
       :param lvl3_result: assortment results on sku level
       :param lvl2_result: assortment results on group
       :return: oos_results_lvl1 -  results on sku level , oos_results_lvl2- results in group level
       """
        oos_results_lvl1 = self.oos_sku_level(lvl3_result)
        oos_results_lvl2 = self.oos_group_level(lvl2_result)
        return oos_results_lvl1, oos_results_lvl2

    def distribution_group_level(self, lvl_2_result):
        """
           This function create df sql results, results of distribution on group level based assortment
           :param lvl_2_result: df of assortment results in group level
           :return: df of sql results for oos assortment group level
        """
        lvl_2_result = lvl_2_result.copy()

        live_kpi_dist = self.get_kpi_fk(self.LIVE_DIST)
        lvl_2_result.loc[:, 'kpi_level_2_fk'] = live_kpi_dist

        lvl_2_result.loc[lvl_2_result['target'] == -1, 'target'] = None
        lvl_2_result.loc[:, 'denominator_result'] = \
            lvl_2_result.apply(lambda row: row['target'] if (row['target'] >= 0 and row['group_target_date'] >
                                                             self.current_date) else row['denominator_result'], axis=1)
        lvl_2_result.loc[:, 'result'] = lvl_2_result.numerator_result / lvl_2_result.denominator_result
        self.manipulate_result_row(lvl_2_result)
        self._add_visit_summary_kpi_entities(lvl_2_result)
        lvl_2_result = lvl_2_result[self.LVL2_SESSION_RESULTS_COL]
        Log.info('Distribution group level is done ')
        return lvl_2_result

    def distribution_sku_level(self, lvl_3_result):
        """ This function receive df = lvl_3_result assortment with data regarding the assortment products
            This function turn the sku_assortment_results to be in a shape of db result.
            return distribution_db_results df
        """
        lvl_3_result.rename(columns={'product_fk': 'numerator_id', 'assortment_group_fk': 'denominator_id',
                                     'in_store': 'result', 'kpi_fk_lvl3': 'kpi_level_2_fk'}, inplace=True)
        live_kpi_dist = self.get_kpi_fk(self.LIVE_DIST_SKU)
        lvl_3_result.loc[:, 'kpi_level_2_fk'] = live_kpi_dist
        lvl_3_result.loc[:, 'result'] = lvl_3_result.apply(lambda row: self.kpi_result_value(row.result), axis=1)
        lvl_3_result = lvl_3_result.assign(numerator_result=lvl_3_result['result'],
                                           denominator_result=lvl_3_result['result'],
                                           score=lvl_3_result['result'])
        lvl_3_result = self.filter_df_by_col(lvl_3_result, self.SKU_LEVEL)
        Log.info('Distribution sku level is done ')
        return lvl_3_result

    def oos_sku_level(self, lvl_3_result):
        """
        This function create df sql results, results of oos on sku level based assortment
        :param lvl_3_result:  df of assortment results in sku level
        :return: df of sql results for oos assortment sku level
        """
        oos_results = lvl_3_result.copy()
        if oos_results.empty:
            return oos_results
        oos_result = self.kpi_result_value(0)
        oos_results = oos_results.loc[oos_results['result'] == oos_result]
        if oos_results.empty:
            return oos_results
        oos_sku_kpi = self.get_kpi_fk(self.LIVE_OOS_SKU)
        oos_results.loc[:, 'kpi_level_2_fk'] = oos_sku_kpi
        oos_results = self.filter_df_by_col(oos_results, self.SKU_LEVEL)
        Log.info('oos_results_sku level Done')
        return oos_results

    def oos_group_level(self, lvl_2_result):
        """
        This function create df sql results, results of oos on group level based assortment
        :param lvl_2_result: df of assortment results in group level
        :return: df of sql results for oos assortment group level
        """
        lvl_2_result = lvl_2_result.copy()
        lvl_2_result.loc[:, 'numerator_result'] = lvl_2_result['denominator_result'] - lvl_2_result['numerator_result']
        lvl_2_result.loc[:, 'result'] = lvl_2_result.numerator_result / lvl_2_result.denominator_result
        self.manipulate_result_row(lvl_2_result)
        oos_group_kpi = self.get_kpi_fk(self.LIVE_OOS)
        lvl_2_result.loc[:, 'kpi_level_2_fk'] = oos_group_kpi
        lvl_2_result.loc[lvl_2_result['target'] == -1, 'target'] = None
        self._add_visit_summary_kpi_entities(lvl_2_result)
        lvl_2_result = self.filter_df_by_col(lvl_2_result, self.GROUPS_LEVEL)
        Log.info('oos_results group level Done')
        return lvl_2_result

    def assortment_result_base_actions(self, lvl3_res):
        """This Function will handle assortment results in sku level based on the actions made in the mobile
           all products which excluded from oos will be removed from oos list  and will be added to distribution
          :param lvl3_res : assortment results in sku level
        """
        excluded_from_oos = self.common.get_oos_exclude_values()
        if excluded_from_oos.empty:
            return
        products_excluded = excluded_from_oos.product_fk.unique()
        lvl3_res.loc[((lvl3_res['in_store'] == 0) & (lvl3_res['product_fk'].isin(products_excluded))), 'in_store'] = 1

    def filter_df_by_col(self, df, level):
        """

        :param df: df results lvl2 / lvl3 assortment results
        :param level: sku /  group level
        :return:filtered df
        """
        if level == self.SKU_LEVEL:
            return df[self.LVL3_SESSION_RESULTS_COL]

        if level == self.GROUPS_LEVEL:
            return df[self.LVL2_SESSION_RESULTS_COL]

    def get_kpi_fk(self, kpi_type):
        """
        :param kpi_type: kpi type
        getting kpi pk from static.kpi_level_2 based on kpi 'type' name
        :return: kpi fk
        """
        kpi_by_name = self.common.get_kpi_fk_by_kpi_type(kpi_type)
        return kpi_by_name

    def _add_visit_summary_kpi_entities(self, df):
        """
        :param df :  kpi results df
        Adding attributes to kpi df for result in visit summary level .
        To db result will be visible in visit summary only when denominator will have the store pk
        and numerator will have own_manufacturer fk
        """
        df.loc[:, 'score'] = df['result']
        df.loc[:, 'denominator_id'] = self.store_fk
        df.loc[:, 'numerator_id'] = self._own_manufacturer

    def write_results_to_db(self, res_df):
        """Function receive dataframe in result structure - meaning df has all desired columns for db result.
          this function turn the df to list of dict and save it in data_provider session_results.
          """
        if res_df.empty:
            return
        self.common.save_to_new_tables_sessions(res_df)

    def kpi_result_value(self, value):
        """
        :param value:  availability kpi result 0 for oos and 1 for distrbution
         Function retrieve the kpi_result_value needed for Availability KPIS
        (kpi result value match to mobile report signs) , according to the kpi result.
        :return pk of kpi_result_value
         """
        value = 'OOS' if value == 0 else 'DISTRIBUTED'
        value_info = self.result_value[self.result_value['value'] == value]
        if value_info.empty:
            return
        return value_info.pk.iloc[0]

    def _get_own_manufacturer(self):
        """ Gets own_manufacturer fk according to the assortment product list """
        if self._data_provider.own_manufacturer.empty:
            Log.warning('This project doesnt have own manufacturer ')
        return int(self._data_provider.own_manufacturer['param_value'].iloc[0])