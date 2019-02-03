from KPIUtils.GlobalProjects.GFK.Base.PrecenseBaseCalculation import GfkPrecenseBaseCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class PresenceonGondolaEndManufacturerSubCategory_KPI(GfkPrecenseBaseCalculationScript):

    @classproperty
    def kpi_type(self):
        return "PRESENCE_ON_GONDOLA_END_MANUFACTURER_SUB_CATEGORY"

    def kpi_policy(self):
        return {
            "location": {
                "template_name": ["Washing Machines - Gondola End"]
            },
            "population": {
                "include": {
                    "category_local_name": ["Washing Machines"],
                    "additional_attribute_2": ["Y"]
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Fields.MANUFACTURER_FK,
            "denominator": Keys.SUB_CATEGORY_FK
        }
