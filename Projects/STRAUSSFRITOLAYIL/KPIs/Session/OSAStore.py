from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class OSAStoreKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(OSAStoreKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.OSA_STORE_KPI)
        sku_results = self.dependencies_data
        denominator = len(sku_results)
        numerator = len(sku_results[sku_results['score'] == Consts.PASS]) if denominator != 0 else 0
        result = self.utils.calculate_sos_result(numerator, denominator)
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk,
                                denominator_id=self.utils.store_id, result=result,
                                numerator_result=numerator, denominator_result=denominator)

    def kpi_type(self):
        pass
