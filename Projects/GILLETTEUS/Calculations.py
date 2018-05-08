
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.GILLETTEUS.KPIGenerator import GILLETTEUSGenerator

__author__ = 'Nimrod'


class GILLETTEUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        GILLETTEUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('gilletteus calculations')
#     Config.init()
#     project_name = 'gilletteus'
#     data_provider = ACEDataProvider(project_name)
#     session = '9B1AF9BA-8F9E-43FD-98FF-95DFE89EAD5E'
#     data_provider.load_session_data(session)
#     output = Output()
#     GILLETTEUSCalculations(data_provider, output).run_project_calculations()
