
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.MARSUAE_SAND.KPIGenerator import MARSUAE_SANDGenerator

__author__ = 'natalyak'


class MarsuaeSandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MARSUAE_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')



