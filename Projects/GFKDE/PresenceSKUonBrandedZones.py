from KPIUtils.GlobalProjects.GFK.Base.PrecenseBaseCalculation import GfkPrecenseBaseCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class PresenceSKUonBrandedZones_KPI(GfkPrecenseBaseCalculationScript):

    @classproperty
    def kpi_type(self):
        return "PRESENCE_SKU_ON_BRANDED_ZONES"

    def kpi_policy(self):
        return {
            "population": {
                "include": {
                    "category_local_name": ["Washing Machines"],
                    "additional_attribute_2": ["Y"]
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Fields.PRODUCT_FK,
            "kpi_additional_params": {
                "filter_branded_zones": True
            }
        }
