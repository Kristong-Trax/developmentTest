from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts
import numpy as np


class SecondaryLinearSpacePerProductKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryLinearSpacePerProductKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
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
            store_area = self.util.filtered_scif_secondary['store_area_fk'].values[0]

            result_df = filtered_matches.groupby([MatchesConsts.PRODUCT_FK, 'display_id'],
                                                 as_index=False).agg({MatchesConsts.WIDTH_MM_ADVANCE: np.sum})

            for i, row in result_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_result=row[MatchesConsts.WIDTH_MM_ADVANCE],
                                        result=row[MatchesConsts.WIDTH_MM_ADVANCE],
                                        numerator_id=row[MatchesConsts.PRODUCT_FK],
                                        denominator_id=row[MatchesConsts.PRODUCT_FK],
                                        denominator_result=row['display_id'], context_id=store_area)
        self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()