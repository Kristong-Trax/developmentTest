from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoRelativePosition(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoRelativePosition, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return DiageoKpiNames.RELATIVE_POSITION

    def calculate(self):
        template_data = self.util.get_template_data(DiageoKpiNames.RELATIVE_POSITION)
        results_list = self.util.diageo_generator.diageo_global_relative_position_function(
            template_data, location_type=ScifConsts.TEMPLATE_NAME)
        for result in results_list:
            self.write_to_db_result(**result)
