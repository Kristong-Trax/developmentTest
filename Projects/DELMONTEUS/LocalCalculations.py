from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DELMONTEUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('delmonteus calculations')
    Config.init()
    project_name = 'delmonteus'

    sessions = [
                '7622a28d-faa0-4332-8daf-e94147fe961c',
                'e54a25c4-00f9-4287-8b1f-07af15a23dd4',
                '9f3a2329-6cb1-4279-8520-d23c2fd6d75f',
                'b1319c6e-2bfb-414f-82a3-1bb8a66428d8',
                'da67f51f-27f3-404c-a00b-2bb1fbe881ec',
                '794c638e-6f87-4f7a-bb2b-30affd83b910',
                'e0c62f6f-428b-4a9c-a08a-f4e2c5f77404',
                '37750ad0-a454-4e28-98a0-1a8753528782',
                'b6e51b3a-30ff-4081-89ff-10a6e0bd8a90',
                '80d936ca-f7be-45cf-a75d-bb32bb2dd0c2',
                ]

    sessions = ['e54a25c4-00f9-4287-8b1f-07af15a23dd4']

    sessions = [
        '9f3a2329-6cb1-4279-8520-d23c2fd6d75f',
        '7622a28d-faa0-4332-8daf-e94147fe961c',
        'e54a25c4-00f9-4287-8b1f-07af15a23dd4',
        '794c638e-6f87-4f7a-bb2b-30affd83b910',
        'da67f51f-27f3-404c-a00b-2bb1fbe881ec',
        'b1319c6e-2bfb-414f-82a3-1bb8a66428d8',
        'e0c62f6f-428b-4a9c-a08a-f4e2c5f77404',
        'b6e51b3a-30ff-4081-89ff-10a6e0bd8a90',
        '37750ad0-a454-4e28-98a0-1a8753528782',
        '07d3bc44-8361-474e-aead-ddd53fc753de',
        '9392d9f1-2492-48c7-87e4-082ff73735d5',
        'a84e60b1-c66e-4c7d-91ee-5c89bf19da5d',
        '91e5ec33-4f06-465e-a73a-928d15b1f3b0',
        '80d936ca-f7be-45cf-a75d-bb32bb2dd0c2',
        'f91e81c7-f7fa-467a-81f0-cd4026ffba51',
        '779b2c19-d180-4cfd-af56-724b4fdafce1',
        '3c45c10a-5b4e-4214-a0c3-513557a338e7',
        'ff98d7fc-bb17-4ced-aff9-92961fb2c75e',
        'db39ff3c-22d1-4903-94e2-cbcc26275510',
        '67c66a2f-c105-4aba-9da0-f2311e6ac5ec',
        '3be75e18-6bf5-42f9-8f9e-54262bc11739',
        'd513fe00-4289-4997-8e81-d92d30df4cc4',
        '8fd27be3-8b41-4597-b1d9-caf034fe4c96',
        'd4e5328c-38ee-4148-a92c-65824e31d358',
        '7db125a5-e29c-40fb-ae7c-f5f5865a0c7b',
        '01c4f07d-8083-49ef-93da-1529f50927d9',
        'fb989cd3-3af2-451b-92cd-9a78f35fdcbf',
        '556fc174-68c5-45d5-80b5-8aef29911bee',
        '062556f5-49dc-4614-9f6b-f8f7295f17a2',
        'fe6fc2dc-9c76-43ae-a4b6-736cf2f58ca6',
        '429a2b0c-c684-477d-a1a8-08a5fd5de1ef',
        'f54b7fda-9b44-424b-a871-e3c26ba4ee9d',
        '9ad039ef-5b49-44dd-b8f5-5bf6bc8d24dd',
        '041550ae-cee3-4029-ac65-a59e15efa8b7',
        '96bf076d-13ce-4003-85cd-582249ca5708',
        'c4f693af-8a9d-4149-b7d6-fe21ece9156e',
        'f2c7a473-9eaa-45a6-81e8-d9212e6ca236',
        '0aa272ff-cad7-48b1-abf3-8bd9b247fb20',
        '4d7d78ff-c3b6-4487-b14d-80a551eeb3f8',
        '73eacc94-832a-40af-a878-4d093b17d53a',
        '6ba314aa-232a-4e19-82f5-ba74d424f7d1',
        '7678ee8b-6d22-41eb-9d08-82b995e6609c',
        'a04e452d-97b7-45a7-8f24-f1e2e1af32ee',
        'ed29df6f-26db-4c37-b59d-f817e7a1283f',
        '5fe3b601-8d8f-4776-b09e-1227375958cf',
        'c53edf79-7063-4312-a273-29c21ab000ad',
        '1f603aef-b3a3-46a5-84e9-cf633a94983c',
        '19f84235-6617-4132-afec-8376bd16790b',
        '67da0fc8-5efc-4be1-9b4d-1f04c8cd6a56',
        'a29417e6-4a80-4690-9a16-bf41332d111a',
        '0bbf7cd4-db17-4f2e-84f3-0827b965d204',
        'eead3f98-a668-4a1d-b50d-ed19604c887e',
        'af2dcf43-bf62-4e8a-aac4-ec0994b8e33b',
        '18a71860-5990-4a8c-9aeb-5b87ceb2e556',
        'd95d5048-93de-4b7c-9c2d-8dca1e8dc23d',
        'dd66c0a2-a38a-4754-aa5c-272ce68e07cf',
        '83e3c30b-65d8-4a9e-b819-df302ed0c20a',
        'cf6c4ee7-7292-4ec8-b2f0-0ca59d460401',
        '98b8dca9-4375-4302-a865-e3c9742e8e75',
        '5043edca-bfb2-4935-b1de-dd4aa2b58b6e',
        '58eed174-2129-421d-9bc6-450665edca39',
        'ad004314-eb47-4d8a-b267-514b0ac9a767',
        '7b70621b-a331-454b-a5fb-dff89b3dcd8a',
        'b52c9068-8d14-4a47-a37d-30d2303d293f',
        'ce759488-9d72-4d9f-8843-64696aab6dbc',
        'e96ab608-0721-463f-9fcd-87c78b4d9533',
        '52be6f5b-d649-4816-afd5-a354429dbf96',
        '8f9629b8-6eef-4964-a94e-1ddd987bf1f6',
        'b29c4492-98b7-443f-bbc7-44bf8ba91bf2',
        '23e9bf0a-e4ba-44d5-8fd0-502c57e218ef',
        'c46bcc4a-141f-44b5-965b-35c2a477642d',
        '6bbae2ab-6179-499b-bc4d-35eb6effaded',
        'a3249cc1-6f05-465e-bc3a-7821e68c6da9',
        '5cae80a1-26c1-4735-8c98-6124a745d069',
        'c72f7208-afbc-43d5-bf09-41f59c192d10',
        'ee48a404-66d2-4af7-b2bb-e834fdb29e2e',
        '24ccd2b8-f249-4b01-9ec5-607ebc02b7a2',
        'cb6e4513-2a1f-4594-b72a-9f489962e408',
        'c60fd4dd-fa03-415c-bfd9-4e1ac93e47a1',
        '4ca3bf21-4e86-423d-86ff-c8817bb4577f',
        '8d4ab20a-39ac-4884-b262-743cea2cf820',
        '46afb5a2-15ab-4632-b3b1-8bc24e26ded2',
        'c2d37cea-9a59-4bd1-b8d0-cbf19b3ad8e3',
        '4c916eaf-d188-49f0-96f5-17020f9f763e',
        '0652d2d9-9490-449e-aef3-0040cdfd6958',
        '22604b8b-543f-492f-a16f-dca6385f38a7',
        '81598bcf-b37c-47eb-8e02-f13e140da965',
        '9a797ac4-3c4a-454b-903a-5465b8b33610',
        'd7ff4502-3135-4232-af04-a30f9c413074',
        'dee26244-5658-433d-ad3a-4cb1c0142e8e',
        'a4652c20-3d4a-48f2-a8d7-9bf53a0ddbf3',
        'b5838e38-ce7b-481a-9b29-91754e798a0a',
        '6ac58ee9-3e2e-49ff-8d48-5f219b9086ac',
        '804e8665-079a-4fee-b6dc-faaa0564d21e',
        'e34554c0-0674-4924-a685-5744b95c8b19',
        'a5548333-43bf-4069-9454-b57bcb0738bd',
        '05901a9c-237f-47e2-a600-57a833258fe8',
        '0cd3bc24-2f4f-4f93-a073-431fff1bc91e',
        '475c7a06-ab42-41a1-9359-8c0b0c5ce2b2',
        '921260ed-01ea-4284-8172-1d8d2e166a02',
    ]


    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

