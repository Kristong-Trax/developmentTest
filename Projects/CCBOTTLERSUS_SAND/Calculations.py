
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Utils.Conf.Configuration import Config
# #from Trax.Utils.Conf.Keys import DbUsers
# #from Trax.Utils.Conf.Keys import DbUsers
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock, patch

from Projects.CCBOTTLERSUS_SAND.KPIGenerator import CCBOTTLERSUS_SANDCcbottlersGenerator

__author__ = 'ortal'


class CCBOTTLERSUS_SANDCCBOTTLERSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBOTTLERSUS_SANDCcbottlersGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('bci calculations')
#     Config.init()
#     docker_user = DbUsers.Docker
#     dbusers_class_path = 'Trax.Cloud.Services.Connector.Keys'
#     dbusers_patcher = patch('{0}.DbUser'.format(dbusers_class_path))
#     dbusers_mock = dbusers_patcher.start()
#     dbusers_mock.return_value = docker_user
#     project_name = 'ccbottlers'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#
#     # supermarket
#     session_uids = [
#                      '85897dc0-17f8-4c02-9cc6-596729e1ec11'
#                       ,
#                      # 'EA23942B-9BFC-4D8E-9C0B-3E55859EFFF8',
#                      # 'eb29618e-fbc6-478e-8013-5011d1ce3aba',
#                      # 'cfd074aa-b938-48ef-9407-40da5364c544',
#                      # 'b81911ce-6531-4c19-8ed0-108f3de3efb0',
#                      # '88a3b709-5c93-4ebb-b9d1-b55fe2e6180b',
#                      # '1a559be0-1a32-4b42-9868-183b51b88542',
#                      # '80801140-c092-4ce7-8013-ff1ea6fc8a95',
#                      # '3f90c68b-1f5c-403c-b43c-5abcf4cd2943',
#                      # 'dd0b592e-38e2-4287-8d08-1ddcf7fc5f29',
#                      # 'cb535df4-2a2f-4848-b139-985469e13ffc',
#                      # '4e45ab82-6bc2-465a-bd57-536b27636ecf',
#                      # 'ba042f12-9332-4627-9be9-d5e9d170aae2'
#                     ]
#
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCBOTTLERSUS_SANDCCBOTTLERSCalculations(data_provider, output).run_project_calculations()


