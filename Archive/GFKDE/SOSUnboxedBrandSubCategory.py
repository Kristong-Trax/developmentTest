from KPIUtils.GlobalProjects.GFK.Base.BaseCalculationScript import GfkBaseCalculation
from Trax.Utils.DesignPatterns.Decorators import classproperty
from Trax.Algo.Calculations.Core.Constants import Keys, Fields


class SOSUnboxedBrandSubCategory_KPI(GfkBaseCalculation):

    @classproperty
    def kpi_type(self):
        return "SOS_UNBOXED_BRAND_SUB_CATEGORY"

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
            "denominator": Keys.SUB_CATEGORY_FK
        }
