
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEAPI2_SAND.KPIGenerator import NESTLEAPI2_SANDNESTLEAPIGenerator

__author__ = 'Ortal'


class NESTLEAPI2_SANDNESTLEAPICalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        NESTLEAPI2_SANDNESTLEAPIGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')



# if __name__ == '__main__':
#     LoggerInitializer.init('nestleapi-sand calculations')
#     Config.init()
#     project_name = 'nestleapi2-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'be62a4a3-72e8-46f4-8d3e-216442207bc6'
#     data_provider.load_session_data(session)
#     output = Output()
#     NESTLEAPI2_SANDNESTLEAPICalculations(data_provider, output).run_project_calculations()
