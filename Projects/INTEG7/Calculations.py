
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INTEG7.KPIGenerator import INTEG7Generator

__author__ = 'Nimrod'


class INTEG7Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INTEG7Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageoau-sand'
#     data_provider = ACEDataProvider(project_name)
#     session = 'ebb18d88-0935-11e7-b3e4-12fd878568f8'
#     data_provider.load_session_data(session)
#     output = Output()
#     INTEG7Calculations(data_provider, output).run_project_calculations()
