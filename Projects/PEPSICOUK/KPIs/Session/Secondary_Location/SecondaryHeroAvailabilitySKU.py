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
        # scif for secondary should have display and store_area breakdown
        ass_list = self.util.lvl3_ass_result[ScifConsts.PRODUCT_FK].values.tolist()
        filtered_scif = self.util.filtered_scif[self.util.filtered_scif[ScifConsts.PRODUCT_FK].isin(ass_list)]
        assortment_scif = filtered_scif.drop_duplicates(subset=[ScifConsts.TEMPLATE_FK, 'store_area',
                                                                ScifConsts.PRODUCT_FK])
        for i, result in assortment_scif.iterrows():
            score = 100
            custom_res = self.util.commontools.get_yes_no_result(score)
            self.write_to_db_result(fk=self.kpi_name, numerator_id=result.product_fk,
                                    numerator_result=1, result=custom_res,
                                    denominator_id=result.template_fk, denominator_result=1, score=score,
                                    context_id=result.store_area)
