
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVEC_SAND.KPIGenerator import INBEVEC_SANDGenerator

__author__ = 'Israel'


class INBEVEC_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVEC_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevec calculations')
#     Config.init()
#     project_name = 'inbevec_sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'CB56C77F-CD97-422B-95FB-28D774BA9EC5'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVEC_SANDCalculations(data_provider, output).run_project_calculations()
