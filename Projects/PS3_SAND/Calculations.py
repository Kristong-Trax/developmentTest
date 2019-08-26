from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEOCO.KPIGenerator import DIAGEOCOGenerator



__author__ = 'huntery'


class DIAGEOCOCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()

        DIAGEOCOGenerator(self.data_provider, self.output).main_function()

        self.timer.stop('KPIGenerator.run_project_calculations')
