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
        lvl3_ass_res_df = self.dependencies_data
        distribution_kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.HERO_SKU_AVAILABILITY)
        if not lvl3_ass_res_df.empty:
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

    def kpi_type(self):
        pass
