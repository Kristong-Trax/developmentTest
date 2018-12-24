from KPIUtils.GlobalProjects.GFK.Base.BaseCalculationScript import GfkBaseCalculation
from Trax.Algo.Calculations.Core.Constants import ProductTypes
from Trax.Utils.DesignPatterns.Decorators import classproperty


class ShareOfBrandedZonesCategory_KPI(GfkBaseCalculation):

    @classproperty
    def kpi_type(self):
        return "SHARE_OF_BRANDED_ZONES_CATEGORY"

    def kpi_policy(self):
        return {'scenes': {
            'included': [{'name': 'Washing Machines - Secondary Shelf'},
                         {'name': 'Washing Machines - Gondola End'}],
            'excluded': []
        },
            'product_type': [ProductTypes.P_TYPE_SKU, ProductTypes.P_TYPE_OTHER],
            'numerator': '',
            'denominator': 'Category',
            'include_stacking': True

        }

    def calculate(self):
        pass
