
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOKE.KPIGenerator import DIAGEOKEGenerator

__author__ = 'Nimrod'


class DIAGEOKECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOKEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageoke'
#     data_provider = KEngineDataProvider(project_name)
#     session = '41dd9f2c-4a92-4a70-bb64-a4d24e349fbd'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOKECalculations(data_provider, output).run_project_calculations()
