
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.INTEG41.KPIGenerator import Generator

__author__ = 'Elyashiv'


class INTEG41Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIDIAGEOUSINTEG41Generator.run_project_calculations')



