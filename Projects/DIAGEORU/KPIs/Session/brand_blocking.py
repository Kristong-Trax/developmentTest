from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil


class DiageoBrandBlock(UnifiedCalculationsScript):

    def __init__(self, data_provider):
        super(DiageoBrandBlock, self).__init__(data_provider)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return DiageoKpiNames.BRAND_BLOCKING

    def calculate(self):
        template_data = self.util.get_template_data(DiageoKpiNames.BRAND_BLOCKING)
        results_list = self.util.diageo_generator.diageo_global_block_together(
            kpi_name=DiageoKpiNames.BRAND_BLOCKING,
            set_templates_data=template_data)
        for result in results_list:
            self.write_to_db_result(**result)
