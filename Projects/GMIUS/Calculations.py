from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GMIUS.KPIGenerator import Generator

__author__ = 'Sam'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        # pass
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGMIUSGenerator.run_project_calculations')
