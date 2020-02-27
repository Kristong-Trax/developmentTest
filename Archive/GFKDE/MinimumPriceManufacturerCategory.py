from KPIUtils.GlobalProjects.GFK.Base.MinimumPriceCalculationScript import GfkMinimumPriceCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class MinimumPriceDistributionManufacturerCategory_KPI(GfkMinimumPriceCalculationScript):

    @classproperty
    def kpi_type(self):
        return "MINIMUM_PRICE_MANUFACTURER_CATEGORY"

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
        }
