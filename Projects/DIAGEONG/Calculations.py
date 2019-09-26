
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEONG.KPIGenerator import DIAGEONGGenerator

__author__ = 'michaela'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEONGGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
