import pandas as pd
from datetime import datetime
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.PSAssortmentProvider import LiveAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.Assortment import LiveAssortmentCalculation
from KPIUtils_v2.DB.LiveCommon import LiveCommon


class CalculateKpi(LiveSessionBaseClass):
    LVL3_SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score']
    LVL2_SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score']
    SKU_LEVEL = 3
    GROUPS_LEVEL = 2

    def __init__(self, data_provider):
        LiveSessionBaseClass.__init__(self, data_provider)
        self._data_provider = data_provider
        self.products = self._data_provider.products
        self.common = LiveCommon(data_provider)
        self.store_fk = data_provider.store_fk
        self.current_date = datetime.now()
        self.assortment = LiveAssortmentCalculation(data_provider)
        self.manufacturer_fk = 0

    def calculate_session_live_kpi(self):

        # getting results
        lvl3_result = self.assortment.calculate_lvl3_assortment(False)
        if lvl3_result.empty:
            return
        lvl_2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
        lvl_2_result.rename(columns={'passes': 'numerator_result', 'total': 'denominator_result', 'kpi_fk_lvl2': 'fk'},
                            inplace=True)
        # todo remove this assigment , get info from data provider
        self.manufacturer_fk = self.get_manufacturer_fk(lvl3_result)
        dist_result_lvl1, dist_results_lvl2 = self.availability(lvl3_result, lvl_2_result)
        self.write_results_to_db(dist_result_lvl1)
        self.write_results_to_db(dist_results_lvl2)

        oos_sku_res = self.oos_sku(dist_result_lvl1)
        self.write_results_to_db(oos_sku_res)

        oos_group_res = self.oos_group_level(lvl_2_result)
        self.write_results_to_db(oos_group_res)

        self.common.commit_live_queries_session()

    def assortment_group_level(self, lvl_2_result):
        lvl_2_result = lvl_2_result.copy()
        lvl_2_result.loc[lvl_2_result['target'] == -1, 'target'] = None
        lvl_2_result.loc[:, 'denominator_result'] = \
            lvl_2_result.apply(lambda row: row['target'] if (row['target'] >= 0 and row['group_target_date'] >
                                                             self.current_date) else row['denominator_result'], axis=1)
        lvl_2_result.loc[:, 'result'] = lvl_2_result.numerator_result / lvl_2_result.denominator_result
        self.manipulate_result_row(lvl_2_result)
        self.add_visit_summary_kpi_entities(lvl_2_result)
        lvl_2_result = lvl_2_result[self.LVL2_SESSION_RESULTS_COL]

        return lvl_2_result

    def availability(self, lvl3_result, lvl2_result):
        dist_result_lvl1 = self.assortment_sku_level(lvl3_result.copy())
        dist_results_lvl2 = self.assortment_group_level(lvl2_result)
        return dist_result_lvl1, dist_results_lvl2

    def write_results_to_db(self, res_df):
        if res_df.empty:
            return
        dict_results = res_df.to_dict('records')
        self.common.save_json_to_new_tables_sessions(dict_results)

    def assortment_sku_level(self, lvl_3_result):

        lvl_3_result.rename(columns={'product_fk': 'numerator_id', 'assortment_group_fk': 'denominator_id',
                                     'in_store': 'result', 'kpi_fk_lvl3': 'fk'}, inplace=True)
        lvl_3_result.loc[:, 'result'] *= 100
        lvl_3_result = lvl_3_result.assign(numerator_result=lvl_3_result['result'],
                                           denominator_result=lvl_3_result['result'],
                                           score=lvl_3_result['result'])
        lvl_3_result = self.filter_df_by_col(lvl_3_result, self.SKU_LEVEL)

        return lvl_3_result

    # todo complete it
    def oos_sku(self, lvl_3_result):

        # filter distrubution kpis

        oos_results = lvl_3_result[lvl_3_result['result'] == 0]
        if oos_results.empty:
            return oos_results
        oos_results.loc[:, 'fk'] = self.get_kpi_fk('OOS - SKU')
        oos_results = self.filter_df_by_col(oos_results, self.SKU_LEVEL)
        return oos_results

    def oos_group_level(self, lvl_2_result):
        lvl_2_result = lvl_2_result.copy()
        lvl_2_result.loc[:, 'numerator_result'] = lvl_2_result['denominator_result'] - lvl_2_result['numerator_result']
        lvl_2_result.loc[:, 'result'] = lvl_2_result.numerator_result / lvl_2_result.denominator_result
        self.manipulate_result_row(lvl_2_result)
        lvl_2_result.loc[:, 'fk'] = self.get_kpi_fk('OOS')
        self.add_visit_summary_kpi_entities(lvl_2_result)
        lvl_2_result = self.filter_df_by_col(lvl_2_result, self.GROUPS_LEVEL)
        return lvl_2_result

    @staticmethod
    def manipulate_result_row(df):
        df[df.result > 100] = 100
        df.loc[:, 'result'] = round(df['result'], 2) * 100

    def filter_df_by_col(self, df, level):

        if level == self.SKU_LEVEL:
            return df[self.LVL3_SESSION_RESULTS_COL]

        if level == self.GROUPS_LEVEL:
            return df[self.LVL2_SESSION_RESULTS_COL]

    def get_kpi_fk(self, kpi_type):
        # todo remove this function should reterive this info from data provider
        return self.common.get_kpi_fk_by_kpi_type(kpi_type)

    def add_visit_summary_kpi_entities(self, df):
        df.loc[:, 'score'] = df['result']
        df.loc[:, 'denominator_id'] = self.store_fk
        df.loc[:, 'numerator_id'] = self.manufacturer_fk

    def get_manufacturer_fk(self, assortment_df):
        manufacturer_df = assortment_df[['product_fk']].merge(self.products[['product_fk', 'manufacturer_fk']],
                                                              how='left', on='product_fk')
        return manufacturer_df['manufacturer_fk'].loc[0]
