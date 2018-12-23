from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.DesignPatterns.Decorators import classproperty


class ShareOfBrandedZonesCategory_KPI(UnifiedCalculationsScript):

    @classproperty
    def kpi_type(self):
        return "SHARE_OF_BRANDED_ZONES_CATEGORY"

    def calculate(self):
        pass
