from Projects.PNGJP_SAND2.KPIS.Util import PNGJP_SAND2Util
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts, ScifConsts, ProductsConsts
import numpy as np


class POSFacingsProductKpi(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(POSFacingsProductKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2Util(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.scene_info.empty:
            template_fk = self.util.scif[ScifConsts.TEMPLATE_FK].values[0]
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.POSM_FCOUNT_SCENE)
            display_matches = self.util.match_display_in_scene
            display_matches['count'] = 1
            result_df = display_matches.groupby(['display_fk'], as_index=False).agg({'count': np.sum})
            for i, row in result_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=row[MatchesConsts.PRODUCT_FK],
                                        denominator_id=template_fk,
                                        result=row['count'], by_scene=True)