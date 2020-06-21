
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.PNGJP_SAND2.KPIGenerator import PNGJP_SAND2Generator


class OldTableCalculationsKpis(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(OldTableCalculationsKpis, self).__init__(data_provider, config_params=config_params, **kwargs)

    def kpi_type(self):
        pass

    def calculate(self):
        self.timer.start()
        PNGJP_SAND2Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')