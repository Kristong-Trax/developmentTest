from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEOCO_SAND.KPIGenerator import DIAGEOCO_SANDGenerator



__author__ = 'huntery'


class DIAGEOCO_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        DIAGEOCO_SANDGenerator(self.data_provider, self.output).main_function()

        self.timer.stop('KPIGenerator.run_project_calculations')
