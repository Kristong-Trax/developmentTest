
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.SOLARBR.KPIGenerator import Generator

__author__ = 'nicolaske'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')




# if __name__ == '__main__':
#     LoggerInitializer.init('SolarBr calculations')
#     Config.init()
#     project_name = 'solarbr'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#
#     # second report
#     sessions = [
#         'dbb78a08-b0b3-41a0-b49c-80d313e6c9d5',
#         '92427d47-eeb5-4f09-8155-9ab17bdb8140',
#         'd1150fca-3f19-4def-a357-b4c3a7f190ca',
#         '80ae8056-813c-405a-94ff-8bc2cde346d3',
#         'c121a357-1dbc-4bbe-82bb-e1a57beec01a',
#         'a6c90c7a-cacf-4dd5-bc73-179f01fc581b',
#         'd801a54b-ea71-4fcc-b2f7-e366d9159d06',
#         'fdd8ac37-b000-47c7-9fa1-1002282ca468',
#         'e40e29b2-1fa5-4317-854f-572b6a67ca0d',
#         'ffccb811-9a49-46ba-8fda-583afa07208a',
#         'faff65f5-34aa-49d8-b00e-f31df7015182',
#         'edc898c0-7d28-488e-a549-8b4e732043de',
#         'fa19ab8c-e982-46df-8907-36ee453f41be',
#         'fc2962fb-8246-4577-927b-79aa32554ccb',
#         'fcc266c5-06b0-4ef8-9c73-b7543f4d3afe',
#         'e49c56ed-1c40-40e5-ba58-f2bda900ae27',
#         'e572e48f-7d9a-4850-84b7-2e34847ba885',
#         'f8041e65-2a2e-4879-83fe-ab4cac4e12a8',
#         'fbd8f697-c2b6-4676-9905-0b0343291e69',
#         'ffab9285-e0f3-4f8a-9e72-8b3f127f042f',
#         '84e82b89-7f1c-4e2f-8ddb-13304388552f',
#         'fa82d8d6-592d-41d1-95ef-5fb019e769b3']
#
#     sessions = [
#         'ee4ba026-8613-426a-8b20-124b4491a6c5',
#         'f39329c9-db7e-4338-af40-821c8c9e8cf7',
#         'f48ac56a-8ccc-4236-ae23-97d91ab4410f',
#         'f8070411-bdf7-4d0c-a0e5-209887ce613e',
#         'ff794762-4a7f-4815-b9c5-17d3db3b7acc',
#         '126640a4-b79a-4a4e-8437-4438e4a82746',
#         'fe89abb5-be3b-4a5e-9695-23663fce7634',
#         'f811eb3b-d596-43d2-8b82-ee419a5cffec',
#         'fde26d16-5b8c-4aa4-a191-995937262eb2',
#         '8054f193-ee05-43eb-898b-82522df6a1b3'
#     ]
#
#
#     for session in sessions:
#         print('================================={}========================================='.format(session))
#         data_provider.load_session_data(session)
#         Calculations(data_provider, output).run_project_calculations()
