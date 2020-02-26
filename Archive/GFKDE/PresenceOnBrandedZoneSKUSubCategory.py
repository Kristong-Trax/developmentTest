from KPIUtils.GlobalProjects.GFK.Base.PrecenseBaseCalculation import GfkPrecenseBaseCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class PresenceonBrandedZonesSKUSubCategory_KPI(GfkPrecenseBaseCalculationScript):

    @classproperty
    def kpi_type(self):
        return "PRESENCE_ON_BRANDED_ZONES_SKU_SUB_CATEGORY"

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
            "numerator": Fields.PRODUCT_FK,
            "denominator": Keys.CATEGORY_FK,
            "kpi_additional_params": {
                "filter_branded_zones": True
            }
        }
