from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts
import numpy as np


class SecondaryAvailabilityPerProductKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryAvailabilityPerProductKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                 self.util.filtered_matches_secondary,
                                                                                 self.kpi_name)
        if not self.util.filtered_matches_secondary.empty:
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
            filtered_matches = self.util.filtered_matches_secondary.copy()
            store_area = filtered_matches['store_area_fk'].values[0]
            product_display = filtered_matches.drop_duplicates(subset=[MatchesConsts.PRODUCT_FK, 'display_id'])
            for i, row in product_display.iterrows():
                self.write_to_db_result(fk=kpi_fk, result=1,  numerator_id=row[MatchesConsts.PRODUCT_FK],
                                        denominator_id=row[MatchesConsts.PRODUCT_FK],
                                        denominator_result=row['display_id'],
                                        context_id=store_area)
        self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()