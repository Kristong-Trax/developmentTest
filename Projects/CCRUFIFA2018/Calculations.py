
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCRUFIFA2018.KPIGenerator import CCRUFIFA2018Generator

__author__ = 'uri'


class CCRUFIFACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCRUFIFA2018Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccrufifa2018 calculations')
#     Config.init()
#     project_name = 'ccrufifa2018'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'D3E3EAAD-B7AD-4466-B341-31390FAA3A79' # AA795EDE-9327-4DDA-93E5-14D180578405, B284DB41-84E1-4D72-9338-A12396D41B1A
#     data_provider.load_session_data(session)
#     output = Output()
#     CCRUFIFACalculations(data_provider, output).run_project_calculations()
