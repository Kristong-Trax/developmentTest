from Projects.PNGJP_SAND2.KPIS.Util import PNGJP_SAND2Util
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts, ScifConsts
import numpy as np


class SKUFacingsbySceneKpi(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SKUFacingsbySceneKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2Util(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        ext_targets = self.util.get_target_by_kpi_type(self.kpi_name)
        if not ext_targets.empty:
            target_prameters = ext_targets.iloc[0]
            matches = self.util.filter_scif_for_scene_kpis(target_prameters)

            matches['result'] = matches[ScifConsts.GROSS_LEN_ADD_STACK] if target_prameters['Include Stacking'] else \
                                matches[ScifConsts.GROSS_LEN_IGN_STACK]
            for i, row in matches.iterrows():
                pass
        pass