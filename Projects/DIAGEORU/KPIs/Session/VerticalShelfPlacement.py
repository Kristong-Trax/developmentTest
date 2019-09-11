from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoShelfPlacement(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoShelfPlacement, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return DiageoKpiNames.VERTICAL_SHELF_PLACEMENT

    def calculate(self):
        template_data = self.util.get_template_data(DiageoKpiNames.VERTICAL_SHELF_PLACEMENT)
        res_list = self.util.diageo_generator.diageo_global_vertical_placement(template_data)
        for res in res_list:
            self.write_to_db_result(**res)
