import os
from Projects.GMIUS.Helpers.Result_Uploader import ResultUploader
from Projects.GMIUS.Helpers.Entity_Uploader import EntityUploader
from Projects.GMIUS.Helpers.Atomic_Farse import AtomicFarse
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GMIUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gmius calculations')
    Config.init()
    project_name = 'gmius'
    # ru = ResultUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.2.xlsx')
    # eu = EntityUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.2.xlsx')
    # af = AtomicFarse(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.2.xlsx')
    # asdfas

    sessions = ['508e23d5-f7c4-41a3-a5fd-9c0b123f82a3']

    sessions = [
        '36536ea2-a6c1-4c52-b9fc-8168fc0c385d',
        '2fe10dac-8bda-40ba-91eb-ea86016a6c6b',
        '10314089-10b2-416c-8db4-6c255f12492e',
        # '3ee63d70-0696-4513-8307-957131460c3d',
        # '50c99053-4504-4d05-af28-34a464706633',
        # '26175b23-f786-4564-a9a4-e8810ce231d9',
        # '508e23d5-f7c4-41a3-a5fd-9c0b123f82a3',
        # 'cb4fd38c-435c-4427-b23f-24c65b2e9e4e',
        # '7bcb09ed-12d7-4fd9-8f81-42e96489f750',
        # '5022a207-62a8-4597-8414-13563ef9bdee',
        # 'e9ece002-8977-4b19-a42b-c5f06c82009b',
        # '9d7ff6b2-97b0-4ca4-9184-03e35cbc7f65',
        # '38a4d253-820f-484f-bfaa-26d4f1871bf6',
        # '8ce297d9-8f02-418a-b56f-015a147078e4',
        # '873a2a34-662c-4d7b-b7e8-fbe37a3b4402',
        # '4645d645-46b8-40da-8743-b60eea969f02',
        # 'c698e18e-0e2d-4bd1-8128-a652f5cf8c23',
        # 'd561f90e-a90b-46b7-a5c3-09e799a4ead5',
        # '7b1c4b26-6fef-4156-93cb-56a9d1dd6630',
        # 'b87c2bac-5c14-48b2-ab5c-99c641519753',
        # '92885501-d3ed-48ea-80de-34621d49771e',
        # '335c4f69-7d42-4764-8a2b-b918d0c784f7',
        # '8ca98892-2df8-460b-833a-7d5f81aa4733',
        # 'cc0430b6-4704-4f1c-94fa-2e4290020e31',
        # '2a598db5-538d-4ef3-a96c-e8d81185a0f1',
        # '6da48919-801f-4116-80bb-edf19ac873f2',
        # '8932230c-cb06-4b24-8244-7ce2aaf9152a',
        # 'd777feca-8feb-4d77-bf8b-82e655575097',
        # '80c2e40b-d8a6-470c-b075-372921e038e6',
        # '94a54e9c-b6e6-43a3-a30a-32d44355c849',
        # 'bb040704-7d0f-40b2-8667-9442fdefb88b',
        # 'd53ceb5e-0b19-4b16-893b-bd7e4e120475',
        # 'f6fa1602-0635-4f0d-a2c6-e236ec98b035',
        # 'a1474a21-1dcf-4db0-b142-2646fe4ad196',
        # 'd3b4df2c-104c-43ca-9b51-4ebb8e8e5236',
        # 'fa633a88-752f-49fe-a6a7-b895253ccc5d',
        # 'e2e82e3c-470c-494b-8dfd-76287694dfa7',
        # '21a2fe06-6e6f-478a-a45b-059e09c96de2',
        # '5e9da09f-a853-4827-bafc-e9397bf3825c',
        # '2ba64880-f489-41c4-925b-3e025b5307c7',
        # '8e7972b1-94e0-4b85-99d4-ec378c52c023',
        # '7876f841-dffb-4758-b598-594c2fc30b4b',
        # '1714c1cb-12ab-48f0-9e75-72080cfef10c',
        # 'ffbd5ac2-250c-49d3-9280-42329fa2957d',
        # 'ac0ef838-752a-409d-8fdd-3568afc58544',
        # 'bdf432f4-09e5-47db-bad2-5eab92e575a7',
        # 'c88403da-6f76-47f9-b637-539954df0dab',
        # '642a9ace-382b-4bda-97f6-c6cefab4c760',
        # '649aca2a-814d-4362-b4f3-7d47fdbbb598',
        # 'd8bb1b54-d361-420d-b275-8b64418b519d',
        # '18014c0a-113d-4aa9-b8c6-5b8fadba536c',
        # 'bf149001-47f5-41bf-8991-82e40f41faac',
        # '201530e8-2c36-437b-9b7a-5018933e5b14',
        # 'd01af37e-130f-4986-954e-d0c90831057c',
        # 'c5afabbc-0823-419a-b69c-6b44dc49c584',
        # '3822bdb9-f32f-4c13-9567-8851aa1dde22',
        # '47a6062c-0a63-4a36-99e1-b7599eeb2902',
        # '7678269d-e3a3-41e3-b80b-6ca5a91a7f77',
        # '34936d7f-b852-41be-beb0-ddb5191e2c3b',
        # '76721a7c-3ea3-4f9c-8432-1e91e9c78253',
        # '4640e7f1-a5de-4f5f-8a4a-2310370a0311',
        # 'a51c1af8-4752-436f-94ff-900ab09b579d',
        # '76fbab6f-6684-44b2-a644-6d55a1953574',
        # '8c73560b-2a59-475a-a2c2-1be932354769',
        # '15914ed7-eef4-4d85-991e-b4184ee9203f',
        # '4a3b4531-b51b-4de1-8f44-550f77823712',
        # '417966e9-bb33-4ad8-80c3-7c04ed1ea54e',
        # '4c57b75c-0656-4d42-94b4-9bb54abcb811',
        # 'b432d821-fe51-40a6-822c-edc871356c09',
        # '7ee359ce-b0d6-4537-bd9c-a12280861ed8',
        # '70739e27-d651-40d7-af66-22cdfee09e89',
        # 'd9ea8e5d-adc6-4b41-83c4-7d5cbbbe33a1',
        # 'aa54510e-0a71-4abc-9e8e-43b149582739',
        # '92a7a7b4-8bc4-4fe2-ab9a-f0f48873b223',
        # '70ec27f5-457c-4513-8927-697450c4cbe5',
        # '92cbcdba-da07-4aa6-81fb-719c082d5053',
        # 'bd4df300-59cf-4e48-a2f3-054ae36cbc78',
        # '21ba287f-0c44-4626-bb0f-8f60c85d9b6d',
        # '7756dc25-9951-4a2a-bc4f-7abf26c55c31',
        # '5aaca77d-977c-4bad-835d-b9096c316423',
        # '5f98d236-2da1-4868-b60f-00ff1b6a7c5e',
        # 'a3d4d7c2-7b16-473d-90ce-2e0a9f285b30',
        # '483b45bb-1281-450e-ab08-bd8ba981e7be',
        # 'b570b8a8-9fd0-45d4-807a-d3537e5a6c96',
        # 'cb479b64-7d3d-4a5c-b1ca-4fe33d0d434c',
        # '163f3ebb-1484-40f4-a338-c4feb3bf7980',
        # '42f38708-fdf9-4d0a-9d95-00c350000ebc',
        # '6bb714a2-a88c-4bde-887f-bb09613b5334',
        # '9f07860c-d03c-4cec-85ca-3a2850451948',
        # '3932a2a4-32d0-40fc-ad6e-6f7727371ae0',
        # 'e2d8c75f-3f7c-41f7-9365-9f40a185f81f',
        # '3e709cbc-68ab-4bb7-bc22-4432e72af4a9',
        # '50874a80-ca9e-4f1d-a24b-91262d6e9227',
        # '5e990914-17ae-46f8-ab25-8f15565e0392',
        # '686c9297-4746-4b41-b6ff-bf824be43a09',
        # 'd01194f1-3c44-4484-98ef-55eb032b3412',
        # 'b6c99b84-8bff-4109-8d81-fc893ff988df',
        # '14a698e7-db2c-4b64-b15d-cf787853c351',
        # 'e4eaf146-4b4b-4190-908d-5d9d65e72fa5',
        # '850a8fce-2158-47de-9e74-bc95d05f6eb8',
        # 'b21be9f6-4622-41ec-ae4e-e3e88395f2db',
        # '2cab1a5e-511f-439e-9c81-498e9928a2c4',
        # '5cd23b4c-5736-4377-a275-2954829c17e8',
        # 'ce70515b-7cf4-4279-ae55-a6b1b3ca6783',
        # 'fcee8444-2421-4097-af47-a51a35d21406',
        # '104ed183-b46f-4750-b3d0-fd26f031016a',
        # 'e544ac0c-2956-43e6-a73f-6146b63d31de',
        # '7b247649-5b78-43fc-a260-d3b199085087'
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

