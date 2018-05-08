
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGAMERICA.KPIGenerator import PNGAMERICAGenerator

__author__ = 'Ortal'


class PNGAMERICACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGAMERICAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngamerica calculations')
#     Config.init()
#     project_name = 'pngamerica'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#                 #BABY CARE
#                 # '58132271-c80a-40f4-b371-a053334cf535',
#                 # '0479246e-de46-4004-a329-5c0c04fb70c2',
#                 # '9225d536-3cfc-4d61-88cd-2ad14db86b35',
#                 # 'f99bd29f-e900-4e28-80ba-1f38ae9d9333',
#                 # '04a0746e-6780-4ac7-97c2-fbc9a57a7ed3',
#                 #
#                 # # '5bed62fe-a4f5-4579-a03f-5abae561473d',
#                 #
#                 # '33932d04-ecc2-46f1-b21a-4299c4904df5',
#                 # 'b6e7db7a-6f02-4c69-92cd-e3fbdc161ddf',
#                 # 'f436e5b8-ff0c-4583-b3fb-6934575f4d5e'
#                 # '4475aae7-6dc3-4202-ae6b-1ebc55b6fbea'
#         #FABRICARE
#         # '3026a58a-6c46-4ccd-9187-840b7f7f8c3e'
#         # 'f95b18c7-ed1c-4837-9d69-36b55cb0da7e',
#         # '3e6b3e3d-3bb6-48e2-b443-1eeedf06ffe7'
#         # 'bdebfbe3-7a25-43b9-9729-4a2de6eb4b12'
#
#         # 'b89db930-643e-4ecd-ba7d-b25a9781a7d2',
#         # 'F1105ACE-8317-4913-9764-ACB7A22E3825',
#         # '2a14a77e-5345-4c74-91f8-4e38e4da1ac2',
#         # 'a1c05623-4aac-42d9-9a02-eba9dbe58374',
#         # 'b01b9495-689e-4328-b93e-e69598a45748',
#         # '0479246e-de46-4004-a329-5c0c04fb70c2'
#
#         # 'f0fb2a15-291a-468d-becd-f30e0e0b16da'
#         # 'cfedebdb-1932-48a7-a48a-0eb248f29af4',
#         '590c9025-0671-4465-826d-c322b53d1b87',
#         # 'ae12c3d6-ec33-419e-88c2-2066445d0a2c'
#
#         # '67b43e77-81bd-40e2-b198-61b71c0f7939',
#         # 'a917b87a-f8f7-4f02-ab40-ce7a028e0b01'
#
#
#         ## '2610120e-5f7e-4ca2-a05c-32a89c79caa1',
#         # '8dfae72e-49a9-4c51-9bc0-583528e65dc1',
#         # '9139e12f-85b5-4f3b-81c6-5e07ad86dd9a',
#         # '051ad88c-27ee-429f-b637-808f4530e22b',
#         # '28022ea8-5f77-41c0-a45e-853b7568ef32',
#         # '0c5ef05b-e029-4baa-8069-dc57695a30bd',
#         # '6bc8c339-b535-4d5e-afa2-4da0a14427a9',
#         # '776ad4f2-e663-41b2-92fe-4f2e2f9792ca',
#         # '54be589b-90b2-4fe3-aced-b90486f6ec49',
#         # 'd560da0a-6560-4f1d-93d0-3b832c4eeadd'
#         # '31c25822-2bd4-4e49-918f-d895b1ac788c'
#
#
#         # '378b6ad0-9883-430a-b7d0-8c6eaee2592b',
#         # '5407a9fa-a5d0-4919-aaaa-56edd29dcd20',
#         # '307de4b5-e7b2-4703-8212-75f732a836c1',
#         # 'bd56eaa2-db2b-4cd0-a88e-80c4783a3178',
#         # '2bec9e7e-a37c-403f-9920-0062f3040550',
#         # '7eed2ace-2041-45e1-b988-76ec5521f0a9',
#         # '931e2e4c-89ce-46c3-b786-86b54dbace3c',
#         # 'ffe888d4-c781-4a06-b2a7-3dff92f3d28d',
#
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGAMERICACalculations(data_provider, output).run_project_calculations()

