
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVCO.KPIGenerator import INBEVCOINBEVCOGenerator

__author__ = 'Israel'


class INBEVCOINBEVCOCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVCOINBEVCOGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevco calculations')
#     Config.init()
#     project_name = 'inbevco'
#     data_provider = KEngineDataProvider(project_name)
#     session = '6bf00e17-e9fb-4c07-ba87-9f44bc6455a3'
#     # session = '16ccdfc9-65eb-4a9d-bff1-766b44432c50'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVCOINBEVCOCalculations(data_provider, output).run_project_calculations()
