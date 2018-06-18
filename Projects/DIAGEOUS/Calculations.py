
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEOUS.KPIGenerator import Generator

__author__ = 'Elyashiv'


class DIAGEOUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIDIAGEOUSGenerator.run_project_calculations')



