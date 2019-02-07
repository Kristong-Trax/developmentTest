from KPIUtils.GlobalProjects.GFK.Base.BaseCalculationScript import GfkBaseCalculation
from Trax.Algo.Calculations.Core.Constants import Keys
from Trax.Utils.DesignPatterns.Decorators import classproperty


class SosOnGondolaEndManufacturerCategory_KPI(GfkBaseCalculation):

    @classproperty
    def kpi_type(self):
        return "SOS_UNBOXED_GONDOLA_MANUFACTURER_CATEGORY"

    def kpi_policy(self):
        return {
            "location": {
                "template_name": ["Washing Machines - Gondola End"]
            },
            "population": {
                "include": {
                    "category": ["Washing Machines"],
                    "additional_attribute_2": ["Y"]
                },
                "exclude": {},
                "include_operator": "and"
            },
            "numerator": Keys.MANUFACTURER_FK,
            "denominator": Keys.CATEGORY_FK
        }
