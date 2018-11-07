from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GOOGLEKR_SAND.KPIGenerator import GOOGLEGenerator

__author__ = 'Eli_Shivi_Sam'


class GOOGLEKR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        GOOGLEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGOOGLEKR_SANDGenerator.run_project_calculations')
