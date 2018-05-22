
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.INBEVTRADMX.KPIGenerator import INBEVTRADMXGenerator

__author__ = 'yoava'


class INBEVTRADMXCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVTRADMXGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIINBEVTRADMXGenerator.run_project_calculations')



