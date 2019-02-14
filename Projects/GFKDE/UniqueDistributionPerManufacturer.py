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
                    "category": ["Washing Machines"],
                    "additional_attribute_2": ["Y"]
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Fields.MANUFACTURER_FK,
            "denominator": Keys.CATEGORY_FK,
            "kpi_additional_params": {
                "fill_not_found_elements": True
            }
        }
