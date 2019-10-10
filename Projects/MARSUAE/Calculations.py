
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.MARSUAE.KPIGenerator import MARSUAEGenerator

__author__ = 'natalyak'


class MarsuaeCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MARSUAEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')



