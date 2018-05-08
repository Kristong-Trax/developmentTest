
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCMY.KPIGenerator import CCMYGenerator

__author__ = 'Nimrod'


class CCMYCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCMYGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccmy calculations')
#     Config.init()
#     project_name = 'ccmy'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     CCMYCalculations(data_provider, output).run_project_calculations()
