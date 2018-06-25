
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEUK_SAND.KPIGenerator import NESTLEUK_SANDGenerator

__author__ = 'uri'


class NESTLEUK_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        NESTLEUK_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('nestleuk calculations')
#     Config.init()
#     project_name = 'nestleuk-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#             # 'b49172fa-6218-48b4-a71c-18c9b77805d5',
#             # '6fd29e1f-b3ed-440d-9ee4-930baf2397ee',
#             # '0be4cec3-1347-4862-bbdf-da79a74a4d4d',
#             # '6b2cfaf7-4340-4199-bbea-adba649dcd62',
#             # 'f73da15b-9282-432d-a917-c8bc4395c05d',
#             # '4c460dde-60fd-4673-8890-669ee9a64dba',
#             # 'b7e9d169-9da8-4fd2-b3a3-4a30edb81897',
#             # '2f6fade3-c497-4ff1-afe4-528066a9d3fc'
#         'f73da15b-9282-432d-a917-c8bc4395c05d'
#                 ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         NESTLEUK_SANDCalculations(data_provider, output).run_project_calculations()
