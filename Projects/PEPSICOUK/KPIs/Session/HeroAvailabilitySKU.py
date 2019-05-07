from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class HeroAvailabilitySkuKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroAvailabilitySkuKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def calculate(self):
        # distribution_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_AVAILABILITY)
        # identifier_parent = self.util.common.get_dictionary(kpi_fk=distribution_kpi_fk)
        for i, result in self.util.lvl3_ass_result.iterrows():
            score = result.in_store * 100
            custom_res = self.util.commontools.get_yes_no_result(score)
            # self.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
            #                         numerator_result=result.in_store, result=custom_res,
            #                         denominator_id=self.util.store_id, should_enter=True,
            #                         denominator_result=1, score=score, identifier_parent=identifier_parent)
            self.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                    numerator_result=result.in_store, result=custom_res,
                                    denominator_id=self.util.store_id, denominator_result=1, score=score)
            self.util.add_kpi_result_to_kpi_results_df(
                [result['kpi_fk_lvl3'], result['product_fk'], self.util.store_id, custom_res,
                 score])

    def kpi_type(self):
        pass