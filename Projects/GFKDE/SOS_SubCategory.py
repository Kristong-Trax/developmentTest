from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.DesignPatterns.Decorators import classproperty


class SOSSubCategory_KPI(UnifiedCalculationsScript):

    @classproperty
    def kpi_type(self):
        return "SOS_SUB_CATEGORY"

    def calculate(self):
        pass
