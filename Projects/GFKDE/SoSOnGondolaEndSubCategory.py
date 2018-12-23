from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.DesignPatterns.Decorators import classproperty


class SosOnGondolaEndSubCategory_KPI(UnifiedCalculationsScript):

    @classproperty
    def kpi_type(self):
        return "SOS_ON_GONDOLA_END_SUB_CATEGORY"

    def calculate(self):
        pass
