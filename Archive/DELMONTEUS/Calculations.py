from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.DELMONTEUS.KPIGenerator import Generator

__author__ = 'Sam'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIDELMONTEUSGenerator.run_project_calculations')
