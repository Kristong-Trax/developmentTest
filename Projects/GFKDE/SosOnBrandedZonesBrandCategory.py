from KPIUtils.GlobalProjects.GFK.Base.BaseCalculationScript import GfkBaseCalculation
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class SosOnBrandedZonesBrandCategory_KPI(GfkBaseCalculation):

    @classproperty
    def kpi_type(self):
        return "SOS_ON_BRANDED_ZONES_BRAND_CATEGORY"

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
            "numerator": Fields.BRAND_FK,
            "denominator": Keys.CATEGORY_FK,
            "kpi_additional_params": {
                "filter_branded_zones": True
            }
        }
