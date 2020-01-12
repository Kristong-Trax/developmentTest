from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.PS1_SAND.KPIGenerator import PS1Generator


class PS1SandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PS1Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
