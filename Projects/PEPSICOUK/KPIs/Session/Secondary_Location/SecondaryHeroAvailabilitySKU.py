from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import numpy as np
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class HeroAvailabilitySkuKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroAvailabilitySkuKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']
        self.location = self._config_params['location']

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.kpi_name)
        self.calculate_kpi_for_secondary_shelf()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def kpi_type(self):
        pass

    def calculate_kpi_for_secondary_shelf(self):
        # change to the appropriate template attribute
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == self.location] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]

        for i, result in self.util.lvl3_ass_result.iterrows():
            score = result.in_store * 100
            custom_res = self.util.commontools.get_yes_no_result(score)
            self.write_to_db_result(fk=self.kpi_name, numerator_id=result.product_fk,
                                    numerator_result=result.in_store, result=custom_res,
                                    denominator_id=self.util.own_manuf_fk, denominator_result=1, score=score,
                                    context_id=location_type_fk)
            self.util.add_kpi_result_to_kpi_results_df(
                [result['kpi_fk_lvl3'], result['product_fk'], self.util.store_id, custom_res,
                 score, None])
