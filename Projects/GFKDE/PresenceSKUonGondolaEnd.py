from KPIUtils.GlobalProjects.GFK.Base.PrecenseBaseCalculation import GfkPrecenseBaseCalculationScript
from Trax.Algo.Calculations.Core.Constants import Keys, Fields
from Trax.Utils.DesignPatterns.Decorators import classproperty


class PresenceSKUonGondolaEnd_KPI(GfkPrecenseBaseCalculationScript):

    @classproperty
    def kpi_type(self):
        return "PRESENCE_SKU_ON_GONDOLA_END"

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
            "numerator": Fields.PRODUCT_FK,
        }
