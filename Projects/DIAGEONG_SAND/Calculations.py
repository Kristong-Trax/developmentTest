
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEONG_SAND.KPIGenerator import DIAGEONGSANDGenerator

__author__ = 'michaela'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEONGSANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
