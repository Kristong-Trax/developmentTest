from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.DesignPatterns.Decorators import classproperty


class ShareOfBrandedZonesSubCategory_KPI(UnifiedCalculationsScript):

    @classproperty
    def kpi_type(self):
        return "SHARE_OF_BRANDED_ZONES_SUB_CATEGORY"

    def calculate(self):
        pass
