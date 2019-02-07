from KPIUtils.GlobalProjects.GFK.Base.ShareOfBrandedZonesCalculationScript import \
    GfkShareOfBrandedZonesCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class ShareOfBrandedZonesBrandSubCategory_KPI(GfkShareOfBrandedZonesCalculationScript):

    @classproperty
    def kpi_type(self):
        return "SHARE_OF_BRANDED_ZONES_BRAND_SUB_CATEGORY"

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
            "denominator": Keys.SUB_CATEGORY_FK,
            "kpi_additional_params": {
                "filter_branded_zones": True
            }
        }
