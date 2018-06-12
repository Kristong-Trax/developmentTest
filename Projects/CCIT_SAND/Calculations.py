
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Apps.Services.KEngine.Handlers.SceneHandler import SceneAnalyzedHandler
from Projects.CCIT_SAND.KPIGenerator import Generator

__author__ = 'nissand'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


