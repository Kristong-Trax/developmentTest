
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.HBCDE_SAND.KPIGenerator import Generator

__author__ = 'ilays'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
