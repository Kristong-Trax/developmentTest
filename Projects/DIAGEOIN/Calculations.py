
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIN.KPIGenerator import DIAGEOINGenerator


__author__ = 'nidhin'


class DIAGEOINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#    LoggerInitializer.init('DIAGEOIN calculations')
#    Config.init()
#    project_name = 'DIAGEOIN'
#    data_provider = KEngineDataProvider(project_name)
#    session = '37ae1878-bc8b-4d1b-8b26-1798f4c562da'
#    data_provider.load_session_data(session)
#    output = Output()
#    DIAGEOINCalculations(data_provider, output).run_project_calculations()
