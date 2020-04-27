from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class HeroAvailabilityKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(HeroAvailabilityKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):
        kpi_result = len(self.dependencies_data['numerator_id'].unique())
        self.write_to_db_result(fk=self.kpi_name, numerator_id=self.util.own_manuf_fk,
                                result=kpi_result, score=kpi_result,
                                denominator_id=self.util.store_id)

    def kpi_type(self):
        pass
