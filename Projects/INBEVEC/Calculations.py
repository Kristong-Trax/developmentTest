
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVEC.KPIGenerator import INBEVECGenerator

__author__ = 'Israel'


class INBEVECCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVECGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevec calculations')
#     Config.init()
#     project_name = 'inbevec'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'CB56C77F-CD97-422B-95FB-28D774BA9EC5'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVECCalculations(data_provider, output).run_project_calculations()
