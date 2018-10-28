from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GOOGLEKR.KPIGenerator import GOOGLEKRGOOGLEGenerator

__author__ = 'Eli_Shivi_Sam'


class GOOGLEKRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        GOOGLEKRGOOGLEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGOOGLEKR_SANDGenerator.run_project_calculations')
