from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GOOGLESG.KPIGenerator import Generator

__author__ = 'Eli_Shivi_Sam'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGOOGLESGGenerator.run_project_calculations')