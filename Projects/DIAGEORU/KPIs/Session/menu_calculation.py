from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
# from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoMenu(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoMenu, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return 'menu'  # todo

    def calculate(self):
        results_list = self.util.diageo_generator.diageo_global_share_of_menu_cocktail_function(
            cocktail_product_level=True)
        for result in results_list:
            self.write_to_db_result(**result)
