import pandas as pd
from datetime import datetime
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.PSAssortmentProvider import LiveAssortmentDataProvider
from Projects.CCUSLIVEDEMO.LiveSessionKpis.Assortment import LiveAssortmentCalculation
from KPIUtils_v2.DB.LiveCommon import LiveCommon


class CalculateKpi(LiveSessionBaseClass):

    SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                 'denominator_result', 'result', 'score']
    LVL2_SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score']

    def __init__(self, data_provider):
        LiveSessionBaseClass.__init__(self, data_provider)
        self.common = LiveCommon(data_provider)
        self.store_fk = data_provider.store_fk
        self.current_date = datetime.now()
        self.assortment = LiveAssortmentCalculation(data_provider)

    def calculate_session_live_kpi(self):

        # getting results
        lvl3_result = self.assortment.calculate_lvl3_assortment(False)
        lvl_2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
        lvl_2_result.rename(columns={'passes': 'numerator_result', 'total': 'denominator_result', 'kpi_fk_lvl2': 'fk'},
                            inplace=True)

        # dist_result = self.assortment_sku_level(lvl3_result.copy())
        # dist_results_sku = dist_result.to_dict('records')
        # self.common.save_json_to_new_tables_sessions(dist_results_sku)

        oos_sku_res = self.oos_sku(dist_result)
        self.common.save_json_to_new_tables_sessions(oos_sku_res)
        oos_group_res = self.oos_group_level(lvl_2_result)
        self.common.save_json_to_new_tables_sessions(oos_group_res)

        # dist_results_lvl2 = self.assortment_group_level(lvl_2_result)
        # dist_results_lvl2 = dist_results_lvl2.to_dict('records')
        # self.common.save_json_to_new_tables_sessions(dist_results_lvl2)


        self.common.commit_live_queries()

    def assortment_group_level(self, lvl_2_result):
        lvl_2_result = lvl_2_result.copy()
        lvl_2_result.loc[lvl_2_result['target'] == -1, 'target'] = None
        lvl_2_result.loc[:, 'denominator_result'] = \
            lvl_2_result.apply(lambda row: row['target'] if (row['target'] >= 0 and row['group_target_date'] >
                                                             self.current_date) else row['denominator_result'], axis=1)
        lvl_2_result.loc[:, 'result'] = lvl_2_result.numerator_result / lvl_2_result.denominator_result
        self.manipulate_result_row(lvl_2_result)
        lvl_2_result.loc[:, 'score'] = lvl_2_result['result']
        manufacturer_fk = 1
        lvl_2_result.loc[:, 'denominator_id'] = self.store_fk
        lvl_2_result.loc[:, 'numerator_id'] = manufacturer_fk
        lvl_2_result = lvl_2_result[self.LVL2_SESSION_RESULTS_COL]

        return lvl_2_result

    def availability(self, lvl3_result, lvl2_result):
        dist_result = self.assortment_sku_level(lvl3_result.copy())
        dist_results_sku = dist_result.to_dict('records')
        self.common.save_json_to_new_tables_sessions(dist_results_sku)
        dist_results_lvl2 = self.assortment_group_level(lvl2_result)
        dist_results_lvl2 = dist_results_lvl2.to_dict('records')
        self.common.save_json_to_new_tables_sessions(dist_results_lvl2)

    def assortment_sku_level(self, lvl_3_result):

        lvl_3_result.rename(columns={'product_fk': 'numerator_id', 'assortment_group_fk': 'denominator_id',
                                     'in_store': 'result', 'kpi_fk_lvl3': 'fk'}, inplace=True)
        lvl_3_result.loc[:, 'result'] *= 100
        lvl_3_result = lvl_3_result.assign(numerator_result=lvl_3_result['result'],
                                           denominator_result=lvl_3_result['result'],
                                           score=lvl_3_result['result'])
        lvl_3_result = lvl_3_result[self.SESSION_RESULTS_COL]

        return lvl_3_result

    # todo complete it
    def oos_sku(self, lvl_3_result):

        # filter distrubution kpis

        oos_results = lvl_3_result.loc[lvl_3_result['result'] == 0]
        oos_results.loc[:, 'fk'] = self.get_kpi_fk('OOS - SKU')

    def oos_group_level(self, lvl_2_result):
        lvl_2_result = lvl_2_result.copy()
        lvl_2_result.loc[:, 'numerator_result'] = lvl_2_result['denominator_result'] - lvl_2_result['numerator_result']
        lvl_2_result.loc[:, 'result'] = lvl_2_result.numerator_result / lvl_2_result.denominator_result
        self.manipulate_result_row(lvl_2_result)
        lvl_2_result.loc[:, 'fk'] = self.get_kpi_fk('OOS')

        return lvl_2_result

    @staticmethod
    def manipulate_result_row(df):
        df[df.result > 100] = 100
        df.loc[:, 'result'] = round(df['result'], 2) * 100

    def get_kpi_fk(self, kpi_type):
        return self.common.get_kpi_fk_by_kpi_type(kpi_type)




