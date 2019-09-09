from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoAssortment(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoAssortment, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return 'Assortment'  # todo

    def calculate(self):
        assortment_res_v2 = self.util.diageo_generator.diageo_global_assortment_function_v2()
        assortment_res_v3 = self.util.diageo_generator.diageo_global_assortment_function_v3()
        for result in assortment_res_v2+assortment_res_v3:
            self.write_to_db_result(**result)
