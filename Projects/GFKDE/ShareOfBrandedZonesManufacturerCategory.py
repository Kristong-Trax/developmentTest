from KPIUtils.GlobalProjects.GFK.Base.ShareOfBrandedZonesCalculationScript import \
    GfkShareOfBrandedZonesCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class ShareOfBrandedZonesManufacturerCategory_KPI(GfkShareOfBrandedZonesCalculationScript):

    @classproperty
    def kpi_type(self):
        return "SHARE_OF_BRANDED_ZONES_MANUFACTURER_CATEGORY"

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
            "denominator": Keys.CATEGORY_FK,
            "kpi_additional_params": {
                "filter_branded_zones": True
            }
        }
