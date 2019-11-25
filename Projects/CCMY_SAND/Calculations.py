
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCMY_SAND.KPIGenerator import CCMY_SANDGenerator

__author__ = 'Nimrod'


class CCMY_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCMY_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('ccmy-sand calculations')
    Config.init()
    project_name = 'ccmy-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['d3c791a2-c34b-4be1-8995-59dd4e89cd77']

    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        CCMY_SANDCalculations(data_provider, output).run_project_calculations()