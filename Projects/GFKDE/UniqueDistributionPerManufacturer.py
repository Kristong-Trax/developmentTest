from KPIUtils.GlobalProjects.GFK.Base.DistributionBaseCalculation import GfkDistributionBaseCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class UniqueDistributionPerManufacturer_KPI(GfkDistributionBaseCalculationScript):

    @classproperty
    def kpi_type(self):
        return "UNIQUE_DISTRIBUTION_PER_MANUFACTURER"

    def kpi_policy(self):
        return {
            "population": {
                "include": {
                    "category_local_name": ["Washing Machines"],
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Fields.MANUFACTURER_FK,
        }
