from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np


class SosVsTargetParentKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetParentKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        # in DB the config parameters should have 'child_kpi' and kpi_type
        # check what I will get as a dependency (all results or only relevant results?)
        child_kpi_results = self.dependencies_data

        if not child_kpi_results.empty:
            score = len(child_kpi_results)
            self.write_to_db_result(score=score, numerator_id=self.util.own_manuf_fk, denominator_id=self.util.store_id)
