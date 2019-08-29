
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCLIBERTYUS.KPIGenerator import CCLIBERTYUSGenerator

__author__ = 'Hunter'


class CCLIBERTYUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCLIBERTYUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPICCLIBERTYUSGenerator.run_project_calculations')



