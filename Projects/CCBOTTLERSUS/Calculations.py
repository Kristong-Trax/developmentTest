from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCBOTTLERSUS.KPIGenerator import CCBOTTLERSUSGenerator

__author__ = 'Elyashiv'


class CCBOTTLERSUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBOTTLERSUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPICCBOTTLERSUSGenerator.run_project_calculations')
