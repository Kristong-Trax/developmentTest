from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GOOGLEKR_SAND.KPIGenerator import GOOGLEGenerator
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from Trax.Utils.Logging.Logger import Log
import pandas as pd

__author__ = 'Eli_Shivi_Sam'


class GOOGLEKR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        GOOGLEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGOOGLEKR_SANDGenerator.run_project_calculations')
