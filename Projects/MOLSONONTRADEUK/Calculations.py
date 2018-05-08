
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MOLSONONTRADEUK.KPIGenerator import MOLSONONTRADEUKGenerator

__author__ = 'nissand'


class MOLSONONTRADEUKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MOLSONONTRADEUKGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('molsonontradeuk calculations')
#     Config.init()
#     project_name = 'molsonontradeuk-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'e191645a-ec74-4e61-8844-276b2ac42b7d'
#     data_provider.load_session_data(session)
#     output = Output()
#     MOLSONONTRADEUKCalculations(data_provider, output).run_project_calculations()
