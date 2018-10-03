
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCBOTTLERSUS_SAND.KPIGenerator import CCBOTTLERSUS_SANDGenerator

__author__ = 'Elyashiv'


class CCBOTTLERSUS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBOTTLERSUS_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPICCBOTTLERSUS_SANDGenerator.run_project_calculations')



