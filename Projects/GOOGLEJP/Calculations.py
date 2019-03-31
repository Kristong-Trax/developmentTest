from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.GOOGLE.KPIGenerator import Generator
import os

__author__ = 'Eli_Shivi_Sam'

FIXTURE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                     'Google Fixture Targets 201901_updated_April Visists.xlsx')


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output, FIXTURE_TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGOOGLEJPGenerator.run_project_calculations')
