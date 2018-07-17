
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEOUS_SAND.KPIGenerator import DIAGEOUS_SANDGenerator

__author__ = 'Elyashiv'


class DIAGEOUS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOUS_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIDIAGEOUSDIAGEOUS_SANDGenerator.run_project_calculations')



