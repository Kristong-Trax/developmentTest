from KPIUtils.GlobalProjects.GFK.Base.BaseCalculationScript import GfkBaseCalculation
from Trax.Utils.DesignPatterns.Decorators import classproperty
from Trax.Algo.Calculations.Core.Constants import Keys, Fields


class SOSUnboxedSKUSubCategory_KPI(GfkBaseCalculation):

    @classproperty
    def kpi_type(self):
        return "SOS_UNBOXED_SKU_SUB_CATEGORY"

    def kpi_policy(self):
        return {
            "population": {
                "include": {
                    "category_local_name": ["Washing Machines"],
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Fields.PRODUCT_FK,
            "denominator": Keys.SUB_CATEGORY_FK
        }
