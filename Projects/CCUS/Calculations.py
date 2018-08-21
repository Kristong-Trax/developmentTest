
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from mock import MagicMock
from Projects.CCUS.KPIGenerator import CCUSGenerator

__author__ = 'ortal'


class CCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')



if __name__ == '__main__':
    LoggerInitializer.init('ccus calculations')
    Config.init()
    project_name = 'ccus'
    data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
    session_uids = ['fffec80f-3cfd-44e6-b105-8510e39f27ff']
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCUSCalculations(data_provider, output).run_project_calculations()
