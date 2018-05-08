
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.RNBDE_SAND.KPIGenerator import RNBDE_SANDGenerator

__author__ = 'uri'


class RNBDECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RNBDE_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('rnbde-sand calculations')
#     Config.init()
#     project_name = 'rnbde-sand'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'f928af7c-6c2b-427e-b290-d709eb93008c'
#     sessions = [
#         '1be2f8ab-1f7a-485a-86e4-45e93ced283b',
#         '1b19451e-64c2-43e7-a053-9223b0b49455',
#         '0fe564ec-f4a0-40f6-a5ae-92e4a2ae7e06',
#         '0ef48e11-046a-4842-bf74-047b362eb074',
#         'cea8637d-433b-4f76-9082-d271a58aac1d',
#         'beed07dd-7250-45e3-a8bf-f8dda8c0ec51',
#         'dd0e5c7e-6b78-461d-ad08-1e4d1648f1c1',
#         '32610714-e0c0-4098-98e0-4527f8ddcf01',
#         'b13f8cb5-74c0-4237-9e4a-afdce205f711',
#         '059314d7-57b0-4691-99f3-bb69f413dcdb',
#         '369df165-dbcd-48f7-9ed6-d461c5a6bdd4',
#         'ba19aab2-fe72-41f0-ae60-bfe8f6a6dbe4',
#         '80d7c790-9406-4619-809b-c42877b4c3b8',
#         '9972fa09-1c48-4d95-b3d7-3f1e62e5e16f',
#         '5074f82b-748c-439a-845e-946450232180',
#         '73da9318-da42-4d76-bdd1-008bfbdbed3d',
#         'd3668e25-6a97-4db3-b620-dac8627dcde7',
#         '951e5f4f-85a8-447e-8f35-4ef2406ab7ed',
#
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         RNBDECalculations(data_provider, output).run_project_calculations()
