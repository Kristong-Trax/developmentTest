from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MOLSONCOORSCA.KPIGenerator import Generator

__author__ = 'Sam'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIMOLSONCOORSCAGenerator.run_project_calculations')
