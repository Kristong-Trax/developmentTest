
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'
    # sessions = [
    #     "55d5c959-cb08-477b-9f9e-0af4fa9f3795",
    #     "7c5284d4-93e0-46e2-a31c-75075d2323e0",
    #     "714f5168-b9d9-4f9b-8f3e-3a0723c68253",
    #     "D8AC45CA-252D-4490-805C-37FF601AC7EC",
    #     "a8e23604-4ef0-48c8-bd89-563d8d687441",
    #     "b0aabe46-724b-466f-be0d-56caf82d6667",
    #     "546AAB1A-259D-4EDA-909E-D8AC9E89D4AD",
    #     "D3D3E61E-F595-4D9D-9B5F-7188321420E1",
    #     "bc8a1244-a53d-4b2f-9591-fcf57f9d3054",
    #
    #     "fb96c8dd-7c4a-4b53-b32a-4d1e5e5bec33",
    #     "f7015dbf-1bd8-4574-b75d-0fb25cc561b9",
    #     "ee2b0cf4-2153-4e71-937e-27552a057058",
    #     "eb67dd5e-dc40-44df-bfd2-f0fe301fa178",
    #     "e860b343-4e10-4ca7-9c09-1b416ff5699f",
    #
    #     "171ebeab-60f0-4df3-8403-f83bf519f6bd",
    #     "FE678C7C-DAF5-4906-9279-2F4490EDD5F9",
    # ]

    sessions = [
                '6f963459-f5f1-4fc4-a77e-2a804a885f6b',
                # '6fc73672-db6a-4ace-9dc1-e0f07ea57c03',
                # '9DD11D67-61EA-49BC-AEEB-90F005DD0AB1',
                # '6A165E9D-8F6B-499D-9AB8-98E4C3F27EB0',
                # 'B3B3954A-61E2-4DF7-BCFF-2DFA7A2AD58E',
                # 'F1F8AF30-01B4-4AFC-89A2-4238BD94B153',
                # '9223B9F1-CB25-466C-9E54-2D163D68EA2C',
                # 'a3256296-e4b0-45d6-ac48-b6a451dc6cee',
                # '934599EB-952E-49C1-9505-7864A537C06D',
                # '1ECD1661-27E1-4324-AEDB-BC3CBF44CACB',
                # 'c2b54ac8-f018-468f-8d2b-f00ab246c985',
                # '50820afd-91e4-4367-af3d-f82d37d76a7c',
                # '51a9fbe3-b28a-4107-9c32-b9ce5163404c',
                # '035858a8-f8d8-49f9-80ed-f5e9619e0133',
                # 'cd059d85-46a6-4bf8-9e46-d2e96fbeac82',
                # 'f33c783c-3f20-409a-8804-57b113571773',
                # '8D7A0575-6C8B-4C87-8B01-3E27DFD1A91A',
                # '2fe5398b-9990-4428-b7f8-9d7289982188',
                # 'c4f60940-1fb5-4d15-9188-5ba56b19b659',
                # '900f9fdb-0f3c-4da1-855b-99afd1b6e006',
                # '442F2F63-EA9F-4246-BB5A-24C733D71C02',
                # 'a7c9b74a-2373-447d-bd5a-6f1118b80a55',
                # 'f7cc3295-04a4-4951-8d69-c7e4c0939e6c',
                # '3fe7c5a6-9d5d-4914-8e8d-9aee7f07c40e',
                # '4c060e04-bde4-40ba-a820-26d93734f327',
                # '48C34109-8501-4A80-9A2B-154F14B32A10',
                # '8BC25372-BBBF-4EA7-A8D9-086038B6B4D4',
                # '82044472-9404-4B65-B5CD-71B42CD6EC0C',
                # '03780670-eaa6-47dd-a159-1aa55f46019a',
                # 'bb6029f3-f5ff-4b06-83f7-025dc813a454',
                # '8a5bf2b4-d5ce-4057-903f-b97b357c5932',
                # '2DF63371-C086-4378-89B6-8F52F5146094',
                # '5cb6986b-a7f7-40d2-bd35-d1e5731ef7a1',
                # '24d5a7f5-ac8b-45c2-9af6-73b48d702711',
                # '54dee50f-8750-4768-8d58-5a9b18ddf4e8',
                # 'f14b216c-3684-4166-ae8d-aa8bbd956865',
                # '66F41897-887A-429F-BB4F-6A1C12F1F3E7',
                # 'd30529f6-21c0-459c-b027-5642f82d0cac',
            ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
