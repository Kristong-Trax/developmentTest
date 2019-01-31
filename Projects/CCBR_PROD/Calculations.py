
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.CCBR_PROD.KPIGenerator import CCBRGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'ilays'


class CCBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('ccbr-prod calculations')
#     Config.init()
#     project_name = 'ccbr-prod'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     list_sessions = [
#     #                   '8cc8c098-fa97-47dd-83d6-00f109a78ada',
#     #                 '1b4d4795-90b9-4161-b321-cfba53ba5fc5',
#     #                 '233f5fca-8ce7-4398-8e8e-bb248de06b1a',
#     #                 '6c66c955-f3af-4688-9463-5441c8cc4c90',
#     #                 'f92ebe9b-fa0d-49ec-b4a3-53928e7f886c',
#     #                 'f92ebe9b-fa0d-49ec-b4a3-53928e7f886c',
#     #                 'dfedd3ef-9367-4bc3-b808-87460966c01d',
#     #                 'c02cbdf9-2555-4734-99e3-11623c1abddf'
#                     'a94a6ab1-dc69-4500-bee6-f42c8580434b',
#         '1f8e4a1c-613b-44f5-9bab-f96f75898b9b',
#         'ca062d2a-4c04-4c31-a0dc-dd2f01463765',
#         'aa9065bf-9674-460a-bdc0-20f060ca1fd9',
#         'a94a6ab1-dc69-4500-bee6-f42c8580434b',
#         '3650a71f-9de4-4744-86a4-a50b02bd5c84',
#         'edfd2411-4cc8-41a9-80f9-0e6e97f86934',
#         'd81977df-52a9-4f05-994f-e7ac6727d2ec',
#         'ed081fb7-d996-4798-a906-066895fafb1d',
#         '44494e7a-1c23-4018-8c8d-a091b8bb1619',
#         'fda0c96a-bb85-40ba-88e1-0ec954ef0234',
#         '9a6dbd0a-76f0-4b58-8646-04cce376673a',
#         '28704993-fed9-414e-b4be-7f3272fd8d95',
#         'b3509766-3b72-4a58-9f7f-054e0fb044f9',
#         '9284effa-be9c-4937-8c3d-3025f12e07ed',
#         '1c38c57f-7a25-4f31-bb12-b9fac20c41e6',
#         '519144f6-563b-441f-9df0-29a1e0772d06',
#         '179ff94e-5e88-4541-8cc6-9b5f5d33d8bd',
#         'e44dddae-e029-4642-a68d-bf96f8b4df6a',
#         '6f91d5ed-6258-41de-9897-1604493048bc',
#         'e9952974-4f3b-46e4-b3c0-bcb6790c5fae',
#         '229f6db3-d9eb-4913-8716-7ff63c30d367',
#         '9a5a0268-74de-4aac-852e-cece24bfb713',
#         '4c328a77-6da0-4698-a909-d1bbc5ee2bea',
#         'b8ba41ed-e571-4366-8878-c0118209ebd1',
#         'b612ed0d-1958-4fc7-b849-7358144143b6',
#         '0232d9a1-b48d-404c-bce7-85ddf3a31a3c',
#         'f47b23ef-34e9-4524-9fa3-b8488c7884a6',
#         'b02021a9-7b1f-4fd2-9012-dd57965dc49c',
#         'd3a0c229-f60b-44c1-8337-3297e6fff582',
#         'a49b05fe-13e0-45a5-894d-22b0230b50e3',
#         '783c841b-38b0-47c5-8a0b-b98ce4e88889',
#         'd0a76a8b-aed9-46d1-8f66-13213290eb4e',
#         'fc5c81be-5b2a-49b3-8b6d-c8de1a980406',
#         'ce5249a1-a19c-4cc6-a38b-1d722f94edaa',
#         '524365a0-baf2-4808-aa2e-cd36186e52ac',
#         '89e3a33e-54a2-4694-865d-64377d484051',
#         '64efa726-0d83-4ded-afd7-71eebf54d972',
#         '30427a37-46db-48dd-ade3-ffc2293c928b',
#         'fc3e584c-bd0e-49d7-8c28-3d3c5a6ab9cb',
#         'd0d1b540-401d-4a3b-923a-035001f52b24',
#         'e4cb3158-3811-4edd-bb3b-37dd9107fe24',
#         '64ed16b9-183e-4453-9521-8f01eea886ea',
#         '23b7718a-7911-4e77-8f4a-8942dc27283e',
#         '6836e32a-6d2c-4f86-b0fc-8bae14866562',
#         'f24b69d0-d2eb-4071-80ce-e62307050626',
#         '9475bbd5-66ca-4276-b7d7-deb10d46dfcd',
#         'ea2214dd-0dbe-4487-a9ee-8a2540be737d',
#         '579a79fd-9b09-43bc-ad95-5f5e6abd8684',
#         '12e14a68-50fd-444c-8ade-4afc817b73d2',
#         '296da7b0-af9b-4263-b9fd-44f9fc20795e',
#         '93c30eeb-8c11-45e4-a373-ca8751c5643c',
#         '6e91ce1b-d12b-4088-9459-cdab711c5ee8',
#         'e538e318-7adc-46a0-ae0e-0260cd0954cd',
#         'c7949f39-d8ef-4679-b88d-71a9e8aa011b',
#         'b08c0406-1b6d-4692-a6c8-61e7c6b0bb8f',
#         '2bbacee4-74bc-4248-bfa9-6bb2cf661035',
#         '40127a16-5a43-45a7-819f-41dad045297f',
#         '290deb5c-3261-4ab1-af2b-2129ad05a8e8',
#         'a273f5fa-040f-4992-97c1-7229ccafd148',
#         '5c8f73f5-f9dc-4354-8839-4bae46ff3aad',
#         'a6c674c5-1d14-4460-b0a7-71d6c839e0e8',
#         '8e7893db-2615-4fab-ac9b-0f8205c37c21',
#         'd9cf2cc3-35ae-491e-a8bd-84a96e997c7c',
#         '121ffe92-f302-4d65-8a11-6fe401411879',
#         '0db94995-f8a4-4127-855f-71fa588ae32b',
#         'eed0dbc9-1c11-4907-ac22-9fd321c6a576',
#         'c5cbfa21-9c48-416e-a721-167383226398',
#         '15451b51-fcbb-4227-ac52-b163a1a8342a',
#         'd396b5cf-89cb-45b4-b51a-a227799c45dd',
#         '6766b29f-569b-4efc-8274-27869af57e42',
#         '3291375a-9629-48fc-bf12-f2996282e304',
#         '6c230ebe-551c-4ffe-b8ff-d8d0f35971c3',
#         '833776c0-5a52-4a43-961a-1792e0c341c3',
#         '65cda116-554e-4a72-9a44-bed2b9f271ec',
#         '23cf34e2-bd80-4004-afeb-9d46889a5506',
#         'bd1d4515-bfa1-4382-995c-fb3d8d627b44',
#         'd3b5a5de-fcf0-4376-8aad-b80baa83db5f',
#         '91ca5cbe-17c5-4797-bf1a-67d5bef9893e',
#         'e19e9bdb-bd62-434c-aee1-3af6c38dae5f',
#         '53334219-cbed-401b-95e1-bf3a6455a0fe',
#         '1e099f4c-f093-4b66-b6f6-230cb07b536a',
#         'f52fbc5c-14b8-4eab-9b37-2a4ed157989c',
#         '4f810684-88e9-45a3-a01a-5b9facf1d4eb',
#         'f7743205-e1e1-48fd-b7ef-f10c1145d3d9',
#         '26b58481-9ab7-4b55-a9be-4956df9a41e6',
#         '0af039de-e30c-4422-816f-e49065d36920',
#         '5e3cfc09-a578-4bcc-b0e1-0a1fd357aee0',
#         '0d919742-7020-49ec-8ca1-cb619b148237',
#         'c0febf9f-87da-43ec-ac4e-efb940443f76',
#         'cb8ac6f4-52b9-48d0-93a0-dede6c9286e6',
#         '293e00cc-66b0-4d33-b436-2f0d2c80727c',
#         'aebd11e6-a871-4e52-a85f-0b207f51a611',
#         'b3265a01-4fa5-4ea4-8339-8c3a79331f6f',
#         'c4540c55-31c5-4c15-aad7-442d589732b5',
#         '8fdf389d-32a6-437f-81ac-1dc96cea24e2',
#         'bf3212b6-4cc9-4810-80bb-9a375e5bfb80',
#         '3eb75f9f-daae-42a0-9c0f-a75718c28dfa',
#         '184587ce-b8ed-4bb0-8dc0-f4a629c40990',
#         'ffaa3faf-ef7f-44fb-b562-517c5e80a2f2',
#         '4a5d00e6-093a-41cc-a817-ad9f1675866e',
#
#     ]
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         CCBRCalculations(data_provider, output).run_project_calculations()
