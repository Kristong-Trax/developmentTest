
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCUS_TEST.KPIGenerator import CCUSGenerator

__author__ = 'Nimrod'


class CCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccus-test calculations')
#     Config.init()
#     project_name = 'ccus-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['fa661a4f-1c3a-456e-b8fb-d95be638ce87',
#      'f273ef55-1dd5-4d1f-b5a1-b663986fcd82',
#      'e09f2ac8-fad3-4c6c-ad52-808e8002538a',
#      'f8bc1adf-b5d5-4b50-836e-1dbf2bb19202',
#      '8211ff91-6a7a-4507-87c6-975bfdf37b89',
#      '78d2420b-06e1-432c-b20e-1dc2dab40e56',
#      '0651d38d-ac68-4149-a1de-dbf305b6ee52',
#      'b03a1352-12c6-457c-9042-48fa653447f1',
#      '8b7d1125-df52-4683-92bd-d941fdb26467',
#      'f60189d3-8c68-4e30-9ca3-f758d7a1ab95']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCUSCalculations(data_provider, output).run_project_calculations()
