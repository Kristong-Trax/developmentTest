from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
# from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoSOS(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoSOS, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        pass
