from KPIUtils.GlobalProjects.GFK.Base.BaseCalculationScript import GfkBaseCalculation
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class SosOnBrandedZonesSkuSubCategory_KPI(GfkBaseCalculation):

    @classproperty
    def kpi_type(self):
        return "SOS_ON_BRANDED_ZONES_SKU_SUB_CATEGORY"

    def kpi_policy(self):
        return {
            "location": {
                "template_name": ["Washing Machines - Gondola End - Price tag",
                                  "Washing Machines - Secondary Shelf - Tag image"]
            },
            "population": {
                "include": {
                    "category": ["Washing Machines"],
                    "additional_attribute_2": ["Y"]
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Fields.PRODUCT_FK,
            "denominator": Keys.SUB_CATEGORY_FK,
            "kpi_additional_params": {
                "filter_branded_zones": True
            }
        }
