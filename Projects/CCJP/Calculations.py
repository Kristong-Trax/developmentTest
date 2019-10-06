
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.CCJP.KPIGenerator import Generator

__author__ = 'satya'


class Calculations(BaseCalculationsScript):

    def run_project_calculations(self):
        self.timer.start()
        set_up_file = 'setup.xlsx'
        Generator(self.data_provider, self.output, set_up_file).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
