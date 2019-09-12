from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoSecondaryDisplays(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoSecondaryDisplays, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        pass
