from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
import numpy as np
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd
from Trax.Utils.Logging.Logger import Log


class SecondaryNumberofUniqueHeroSKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondaryNumberofUniqueHeroSKUKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def calculate(self):
        total_skus_in_ass = len(self.util.lvl3_ass_result)
        if not total_skus_in_ass:
            return
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        lvl3_ass_result_sku = self.dependencies_data
        if lvl3_ass_result_sku.empty and total_skus_in_ass:
            self.write_to_db_result(fk=kpi_fk, numerator_id=self.util.own_manuf_fk,
                                    numerator_result=0, result=0,
                                    denominator_id=self.util.store_id, denominator_result=total_skus_in_ass,
                                    score=0)
            return
        kpi_result = len(lvl3_ass_result_sku['numerator_id'].unique())
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.util.own_manuf_fk,
                                result=kpi_result, score=kpi_result,
                                denominator_id=self.util.store_id)

    def kpi_type(self):
        pass
