from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator

__author__ = 'Shivi'


class Calculations(BaseCalculationsScript):

    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))), 'SANOFIML', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
