from Projects.PNGJP_SAND2.KPIS.Util import PNGJP_SAND2Util
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ScifConsts
import numpy as np
import pandas as pd


class BayCountbySceneKpi(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(BayCountbySceneKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2Util(None, data_provider)
        # self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.scif.empty:
            ext_targets = self.util.get_target_by_kpi_type('PGJAPAN_BAY_COUNT_BY_SCENE')
            if not ext_targets.empty:
                template_fk = self.util.scif[ScifConsts.TEMPLATE_FK].values[0]
                kpi_fk = self.util.common.get_kpi_fk_by_kpi_type('PGJAPAN_BAY_COUNT_BY_SCENE')
                target_prameters = ext_targets.iloc[0]
                matches = self.util.filter_matches_for_scene_kpis(target_prameters)
                matches = matches[~(matches[ScifConsts.PRODUCT_TYPE] == 'POS')]
                matches = matches[~(matches[MatchesConsts.BAY_NUMBER] == -1)]
                if not target_prameters['Include Stacking']:
                    matches = matches[matches[MatchesConsts.STACKING_LAYER] == 1]
                bay_count = matches[MatchesConsts.BAY_NUMBER].max()
                bay_count = 0 if pd.isnull(bay_count) else bay_count
                self.write_to_db_result(fk=kpi_fk,
                                        numerator_id=self.util.store_fk,
                                        denominator_id=template_fk,
                                        result=bay_count,
                                        by_scene=True)
