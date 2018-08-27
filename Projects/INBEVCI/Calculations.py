
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVCI.KPIGenerator import INBEVCIINBEVCIGenerator

__author__ = 'Elyashiv'


class INBEVCIINBEVCICalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVCIINBEVCIGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevci calculations')
#     Config.init()
#     project_name = 'inbevci'
#     sessions = [
#
#         'f1b8686f-26e8-486c-99f0-83123bdec91a'
#         # "fcc6e8e0-7a62-466e-987b-c1dbe8e7a077",
#         # "f8f0379b-3675-479f-aae4-4cb4bb326cbf",
#         # "ec1a483a-6a9d-410e-8991-84c956cfcfe1",
#         # "1aee69d4-04ca-43f1-8dab-3e20ab771618",
#         # "39ce333d-8da8-4c07-b66f-28680d547eed",
#         # "52eb3813-7110-4698-ac2b-4ada360366ad",
#     ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVCIINBEVCICalculations(data_provider, output).run_project_calculations()
