
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INTEG16.KPIGenerator import INTEG16Generator

__author__ = 'Nimrod'


class INTEG16Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INTEG16Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('cckh calculations')
#     Config.init()
#     project_name = 'integ16'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     INTEG16Calculations(data_provider, output).run_project_calculations()
