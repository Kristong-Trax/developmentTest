from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
# from Projects.PNGJP_SAND2.KPIGenerator import PNGJP_SAND2Generator
from Projects.PNGJP_SAND2.KPISceneGenerator import SceneGenerator


class OldTablesSceneCalculationsKpis(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(OldTablesSceneCalculationsKpis, self).__init__(data_provider, config_params=config_params, **kwargs)

    def kpi_type(self):
        pass

    def calculate(self):
        self.timer.start()
        SceneGenerator(self.data_provider, self.output).scene_main_calculation()
        self.timer.stop('KPIGenerator.run_project_calculations')