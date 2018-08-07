
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.INBEVBR.KPIGenerator import INBEVBRGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'ilays'


class INBEVBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbr calculations')
#     Config.init()
#     project_name = 'inbevbr'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#
#     # second report
#     list_sessions = [
#                     'ee5ed5c4-47dd-487c-af41-4bebfa4b7031',
#                     'ee6e5b25-7fad-46bd-b56b-3bc10aab3848',
#                      'b85ef7f1-49a0-4ca3-b1d9-6617d66d6674',
#                     'ebfb699e-c8c5-4553-b586-f422c45e173c',
#                      '41896a7b-4d9e-4920-9aba-2b1adcbb334c',
#                     '6db4c530-747c-472e-a2b2-da5fcdca3aef',
#                     'f03fb2ad-52a0-4f32-8dcd-30ad94970f80',
#                     'f89ffb04-f041-4182-9be3-9afb51728bcc',
#                     'f026f90a-7c9b-4291-bce8-19833cd8ab84',
#                     'ee525a29-4b43-4820-b444-3b849f01a0ce',
#                     'e3527b80-4cd5-425e-b500-6a737e8750b2',
#                     'e086c8a0-8a91-4d37-b116-f9ddde98dab4',
#                     'dbdbfa41-3d4a-45d3-a360-6a1dffb36c51',
#                     'd24f8584-abec-4f71-87e0-99703aff0177',
#                     'd14b041f-0b6d-490a-8689-17b66c242a1d',
#                     'cd459913-ab3b-4e07-878a-4c51bac75c28',
#                     'bd188ede-349f-43cb-91f5-54681150ea62',
#                     'bb5594b8-77d6-48f6-a935-7007cac66e74',
#                     'b4f80d82-f914-46f7-9309-ab23515872be',
#                     'a9af2d52-5165-4847-9c51-a44176969900',
#                     'a7e2c8ac-d066-47fc-a7ee-16a623409266',
#                     'a78c3a96-966b-498e-b77d-b4e12db2f4cd',
#                     'a72d2ec4-e2d7-46e5-b0a9-3f791377ab77',
#                     '9c2563bb-af7a-47ef-9483-40f2eb5d377d',
#                     '9c061a70-4346-4d4b-a94d-7415d9e23410',
#                     '97d8f1e5-bb3d-4742-93d3-45492a5b48f4',
#                     '92d4a418-ad3c-414b-8d51-0e31cf928c63',
#                     '8d99703d-72ab-45a6-ac93-329eecff2091',
#                     '8cd13f71-8676-4de0-871e-9673e5b2dcc2',
#                     '8a2ccc66-35d0-4a59-aec8-cb9f9bac8191',
#                     '87de9c8f-1877-4523-ab98-b4c3db96a771',
#                     '7ba7de14-b636-4a8c-8457-88cd5654dd2a',
#                     '77c21ba6-b0d3-429c-97cf-7cd81a9edd76',
#                     '769040a1-c3fe-4333-9b51-2f9e173fab2e',
#                     '717e0503-7e0a-4927-aa5e-7431481256fa',
#                     '6a6e7e06-ea91-4ee2-9ae6-02a9a8632b7f',
#                     '68a2819c-8c14-4422-8a32-933c831a5415',
#                     '61d0b530-8352-4546-aa78-9b87438acfe5',
#                     '5b81ea81-e32f-4a62-8bc5-0f0446d5fa0c',
#                     '56919033-8c9e-4823-a019-2add065ce405',
#                     '53bbb1cb-8099-41ab-8873-6542cb239656',
#                     '5179703b-3ff0-42a4-b9ed-8fa2edaae30b',
#                     '4b465e7b-8c22-473e-80d1-46d4b3ef2eef',
#                     '4590a677-fab8-49e3-be23-bfb04e33d1f3']
#
#
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         INBEVBRCalculations(data_provider, output).run_project_calculations()
