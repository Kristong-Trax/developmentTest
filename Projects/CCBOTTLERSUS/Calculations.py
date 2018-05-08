
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Utils.Conf.Configuration import Config
# #from Trax.Utils.Conf.Keys import DbUsers
# #from Trax.Utils.Conf.Keys import DbUsers
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock, patch

from Projects.CCBOTTLERSUS.KPIGenerator import CcbottlersGenerator

__author__ = 'ortal'


class CCBOTTLERSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CcbottlersGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('bci calculations')
#     Config.init()
#     # docker_user = DbUsers.Docker
#     # dbusers_class_path = 'Trax.Cloud.Services.Connector.Keys'
#     # dbusers_patcher = patch('{0}.DbUser'.format(dbusers_class_path))
#     # dbusers_mock = dbusers_patcher.start()
#     # dbusers_mock.return_value = docker_user
#     project_name = 'ccbottlersus'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#
#     # supermarket
#     session_uids = [
#                     # '8F94E751-17B3-4A0C-AB58-99EAD1022381',
#                      # '85897dc0-17f8-4c02-9cc6-596729e1ec11',
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
#                      # 'ba042f12-9332-4627-9be9-d5e9d170aae2',
#                      # '505BABF2-C361-4B8F-ACC2-AF41052EFDEF'
#
#                     # 20_sessions
#                     # 1-5
#                     # '8F94E751-17B3-4A0C-AB58-99EAD1022381',
#                     # '8D1195F6-939A-4A72-8E38-511990F816DD',
#                     # '603E8667-B994-4D71-85F6-0477EB505746',
#                     # 'dbb66a15-b7c9-4478-be2a-c30ff2a8c7df',
#                     # '584058a4-fd5a-4e2b-99e8-885d6abe76e4',
#                     # # 6-10
#                     # '4aee357a-f2bb-4bf4-8712-1490578486cb',
#                     # 'd48fa01e-8fcf-4fbd-8a74-b7823e270a17',
#                     # '7b69c6cf-8ce4-4c23-aee0-a4bc09f51b11',
#                     # '3853a312-8e88-4f4e-8e38-815a7edac631',
#                     # '9d0684b1-0ab0-4e95-bbb4-c8253b7b4f5b',
#                     # # 11-15
#                     # '95c25464-8ee5-4b36-a8a1-d670876bbaaf',
#                     # '1abaf193-b4e9-43ac-b540-112191e215aa',
#                     # 'c7482b7c-ba8f-4bd5-8935-8cabd631539a',
#                     # '0565aac4-3cd2-41bb-8bcc-86154dc96839',
#                     # '023194e5-ad94-440e-926a-c0c02efa3424',
#                     # # 16-20
#                     'A9645825-68FF-4F19-8CE3-989B7284CB6A',
#                     '8B28A8B8-8238-4FD5-BEB2-DDFE9422D60E',
#                     '23007E5B-B167-4808-8EBF-D549260974B7',
#                     '59b74a48-82a3-43dc-b2b5-6ed0d2a211da',
#                     '9309f6ef-f75b-4d93-998e-cd0e5ddd025e',
#
#     ]
#
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCBOTTLERSCalculations(data_provider, output).run_project_calculations()


