from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.CCUS.KPIGenerator import CCUSGenerator


__author__ = 'ortal_shivi_Jasmine'


class CCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
