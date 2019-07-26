
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PS1_SAND.KPIGenerator import DIAGEOITGenerator


__author__ = 'Nimrod'


class DIAGEOITCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOITGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#    LoggerInitializer.init('diageoit calculations')
#    Config.init()
#    project_name = 'ps1-sand'
#    data_provider = KEngineDataProvider(project_name)
#    session = '17E91F59-1812-4B7A-B944-1BE4DEABD182'
#    data_provider.load_session_data(session)
#    output = Output()
#    DIAGEOITCalculations(data_provider, output).run_project_calculations()
