from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class HeroAvailabilityKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroAvailabilityKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def calculate(self):
        lvl3_ass_res_df = self.dependencies_data[self.dependencies_data['kpi_type'] \
                                      == self.util.HERO_SKU_AVAILABILITY_SKU]
        distribution_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_AVAILABILITY)
        if lvl3_ass_res_df.empty:
            total_skus_in_ass = len(lvl3_ass_res_df)
            in_store_skus = len(self.util.get_available_hero_sku_list(self.dependencies_data))
            res = np.divide(float(in_store_skus), float(total_skus_in_ass)) * 100
            score = 100 if res >= 100 else 0
            self.write_to_db_result(fk=distribution_kpi_fk, numerator_id=self.util.own_manuf_fk,
                                    numerator_result=in_store_skus, result=res,
                                    denominator_id=self.util.store_id, denominator_result=total_skus_in_ass,
                                    score=score)
            self.util.add_kpi_result_to_kpi_results_df(
                [distribution_kpi_fk, self.util.own_manuf_fk, self.util.store_id, res, score])

    # def calculate(self):
    #     distribution_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_AVAILABILITY)
    #     # identifier_parent = self.util.common.get_dictionary(kpi_fk=distribution_kpi_fk)
    #     if not self.util.lvl3_ass_result.empty:
    #         lvl2_result = self.util.assortment.calculate_lvl2_assortment(self.util.lvl3_ass_result)
    #         for result in lvl2_result.itertuples():
    #             denominator_res = result.total
    #             if result.target and not np.math.isnan(result.target):
    #                 if result.group_target_date <= self.util.visit_date:
    #                     denominator_res = result.target
    #             res = np.divide(float(result.passes), float(denominator_res)) * 100
    #             score = 100 if res >= 100 else 0
    #             # self.write_to_db_result(fk=result.kpi_fk_lvl2, numerator_id=self.util.own_manuf_fk,
    #             #                                numerator_result=result.passes, result=res,
    #             #                                denominator_id=self.util.store_id, denominator_result=denominator_res,
    #             #                                score=score,
    #             #                                identifier_result=identifier_parent, should_enter=True)
    #             self.write_to_db_result(fk=result.kpi_fk_lvl2, numerator_id=self.util.own_manuf_fk,
    #                                     numerator_result=result.passes, result=res,
    #                                     denominator_id=self.util.store_id, denominator_result=denominator_res,
    #                                     score=score)
    #             self.util.add_kpi_result_to_kpi_results_df(
    #                 [result.kpi_fk_lvl2, self.util.own_manuf_fk, self.util.store_id, res, score])

    def kpi_type(self):
        pass