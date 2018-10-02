
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data


if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'
    sessions = [
        # "12dfda9a-fce4-45b7-8b9f-140390f55e74",
        # "fe63af52-6b87-4c94-82aa-9aee63116d45",
        # "b93d6582-cee9-47e2-8c23-19a5e87683b1",
        # "bc8a1244-a53d-4b2f-9591-fcf57f9d3054",
        # "D3D3E61E-F595-4D9D-9B5F-7188321420E1",
        # "546AAB1A-259D-4EDA-909E-D8AC9E89D4AD",
        # "11044558-fc7f-4882-8243-e301528aa5e8",
        # "402bb0f7-7e58-4532-94a5-21ed2538d2e6",
        # "15283f33-65f7-4abf-91e2-084801ec4c61",
        # "c2b4723f-ea1b-456d-9647-48ef779cfcb8",
        # "9e0cd962-74b6-48ac-ba13-6e674c198ea3",
        # "86997b82-e7e4-4155-91f5-9cd30de7b55c",
        # "7c5284d4-93e0-46e2-a31c-75075d2323e0",
        # "55d5c959-cb08-477b-9f9e-0af4fa9f3795",
        # "714f5168-b9d9-4f9b-8f3e-3a0723c68253",
        # "95260A0C-5C37-4675-9718-6144D31A040D"
    ]

    sessions = [
        '048198E5-4D8D-47CC-817F-BBDD05D854AA',
        # '07292af8-313d-4e55-b371-64c685f95e70',
        # '0adce9c1-dd65-496f-8eb6-e80fa0823cdc',
        # '0b190960-7088-4048-8d0d-8572d0f077fc',
        # '12035249-fb97-44b8-aa72-59c5b15114c9',
        # '4d6984c7-dc29-44b1-8c2e-44cc95b792f7',
        # '9e6d65f2-ec14-4cc7-aa8e-4cbfc6609376',
    ]
    sessions = [
        '76D3ED09-1B30-4D59-8EEE-F05D8478F607',
        '4A102E93-FF88-4A2B-8AA5-F62DA53D0AC3',
        '3e2938e1-3dab-486f-9ffb-2b18b4566b48',
        'da55402d-96f1-4307-8ff2-798a59886889',
        'd909a67f-faba-40c7-abee-dff2f23f2aa7',
        '282ec6cf-3a7b-41e8-9c4d-c2a5b4369411',
        '3e48b094-2aab-4a86-9936-cdf66800ffbd',
        'bce79446-3a01-4a50-9f08-fae988f10000',
        'e16ce5d8-8336-4fff-9235-79bba21cee30',
        '1fe0540b-56f6-4783-a8f2-d3955d26386d',
        '1d83a58e-822a-44c0-8b46-48b42da2e8a7',
        'd7ebd787-124a-410f-a6a4-07f064fccdd1',
        'ad339fa9-f63f-4cc2-9456-3b959a296033',
        '40a1d7b4-e2b2-4d18-a71e-ff0a2d63e1da',
        '68f4713b-9b3b-4d92-890e-4e38caf4a174',
        '760529e3-6562-40e4-af71-3d2946d22f50',
        'cf2a5fce-39d6-438a-9fc4-fa9f91dc1c5e',
        '2dce3112-5b05-4dc1-a6b9-078a05debe6c',
        '3941a140-294e-4111-9382-07134c7588eb',
        '6dec370e-f7be-49f6-9684-aad58a5dffed',
        'da602725-f7d5-4b89-b60a-c0c85abbec54',
        'df36d2de-9030-498f-8ea3-d5d7c370b22e',
        'f5a86694-0ae8-48a4-a2ff-244a9f950fb9',
        'e038dc5b-d07e-4c5e-8de8-a10e1d2a9154',
        '4d6984c7-dc29-44b1-8c2e-44cc95b792f7',
        'e55818f2-5a0d-49f4-9ba1-b68a25f9b4b0',
        '7e1f3771-8e1c-4ce7-9b43-1b4ec8df5e85',
        '3fca41ab-32b3-4b46-8ee8-34af370297ce',
        'a7703f42-6918-4116-a84a-e049995a76ed',
        '231dfc0a-0439-4703-a972-8f3b45297b88',
        'b411b007-7fe3-408a-a0e0-a63410fbe9b2',
        '23199c3c-aac6-4cb9-b280-6a1864a0a9f1',
        '831cb676-bea4-4894-a3fe-72c3af98b9dd',
        'a880cf41-62b5-482d-b342-7ed473570767',
        '80d9ec36-a262-45f3-a434-9833b3da8522',
        '1b4a1ba0-6d4d-4e5e-a735-d064bdbca5e3',
        '1d2b5d1e-be4a-4459-a40a-5fa67d5b4704',
        'e93a0a0a-b030-426c-b5a1-17a8b97476b0',
        'bf0b3a70-8a63-4761-a64d-ca2819c6d478',
        '3d104119-2055-41d7-b4c6-ee8ef0f04e73',
        'ca0e97e7-db29-451c-958a-6823bb4a3b45',
        'a262443c-0767-4862-a177-64fd9cbab8b2',
        '37424efb-8f2f-44cf-9d6a-94dc0ab2cf69',
        '866a6f12-6879-4104-9a72-575e1e5bd2cc',
        'e9c90fb6-0517-4c55-80e4-6a0ae36aa9d1',
        '6eb1d227-4ce6-40f2-a2cd-87e059517c91',
        'ec1eaf1a-bdef-4241-8bfa-48a5ec9dd262',
        '12035249-fb97-44b8-aa72-59c5b15114c9',
        'cb7491c6-8226-4f03-b800-aab0a439cd6a',
        'beffe143-83bd-414a-9943-d5470f34a936',
        'a1207d25-3141-412e-9a35-49f64551c3a9',
        'ad1fb9e2-f356-4dcd-a196-2b880aebaebe',
        '8fecddc0-b2ce-408f-9114-898b75100ccb',
        '853F296F-1602-46CD-81F3-53DE966835D3',
        '5ec864f7-1ba4-4697-bfdc-638e62ab5d8f',
        'b71448eb-6a47-438b-a1c1-48e8cb2442a6',
        '21eaf1a9-f6db-4baf-88f7-ec089c4d1d0b',
        'c103ab77-3de0-4ad2-8976-3080f6d73b11',
        '7e73cc92-d378-452a-b27b-b7681989220e',
        'a59db63d-07a9-46ac-9292-6b0db4cbb2e6',
        '125dc4e8-ca32-460e-84f5-b00d25a46adc',
        '483e8c8e-aae0-49c5-8032-d183bfdd37f7',
        'f5c64b26-821c-4ffd-a1bc-23bfffe5766c',
        '4dd1daf1-4b60-4616-932d-00c2ffc57249',
        '2bb53e34-8c47-49f8-9f94-dccd37738287',
        '2df7e113-b41c-49ba-8f10-4752bd14e2ff',
        '7d6f8ae4-79e1-4dee-83a5-917eff5ba733',
        'c8e41237-978b-48fa-807f-52794566cb24',
        '9e6d65f2-ec14-4cc7-aa8e-4cbfc6609376',
        '80ac64a3-d56b-4e67-86bb-f47b41bf7b5e',
        '3b2969f0-54fc-47e4-b963-ccd51ea272a7',
        '7b6b1e13-1a32-4633-b5d0-1734e3be2b18',
        '32d3a251-8784-41ad-885c-8d11c29c6467',
        '49e1a629-7b4e-4866-bd1a-b3ac16cf3f4c',
        '5ced9281-ae56-483b-b136-e9f3908f6401',
        '7434c769-5016-4fe5-8153-b171da407adb',
        'C3928204-90EA-4072-8F52-572657C895A2',
        '3db0fd79-2aa8-4ee5-86cb-861be1a7a4a6',
        '5c79918c-0a6a-472d-bcff-7ee5099fd041',
        '5649def6-eee1-4f72-b497-09db59ee1139',
        'bbaacaf5-c132-4a1f-ae53-5c5f3da9de97',
        '2fa2fa5b-1af9-4ee8-9cfc-fc1db69e5d65',
        '26dc110d-5849-42d7-9664-9401c16c971d',
        '6a6d556e-6d51-4908-bf25-7960bba44001',
        '3b89b3d6-0492-4ef4-bb62-c521736fd989',
        '8ea30dae-22d9-417a-aa11-31af7620085a',
        '172438d9-3c28-433c-806a-f8ed4f9ffdbf',
        '3F3CA3C0-66E1-4A0A-8CF9-4C49096CA5B4',
        '7950e426-40ff-46ad-8520-62ff811a5e7d',
        '6f429b01-f5a9-47fa-b576-8348dabe8155',
        '55034e05-6c75-4fa8-ad38-eed5a5e03d39',
        '7ab5a9af-effb-4fd9-8f4c-28148e0658a6',
        'a486e3f8-b178-4505-af87-6ae2d810dabf',
        '0b190960-7088-4048-8d0d-8572d0f077fc',
        '07292af8-313d-4e55-b371-64c685f95e70',
        '7afaf1c6-c360-4b7f-86b4-b35b6e41fa68',
        '55bb2bf6-2077-48be-ab00-2e2a60510199',
        '1c96dee0-62d2-42f6-8775-fe1b3def310a',
        '922BDF4F-5C5F-4B5E-A6C5-AA48A6DAA957',
        '4b3d3915-4445-44d2-9cf7-f86126952feb',
        'cd1a4b09-98d9-4ff3-991a-474e8f51b521',
        'b53e4c7a-9a72-41de-b18e-0cf8f94ba26f',
        '71f36d19-9a6c-4903-89f7-f8cb1df84ab6',
        '0adce9c1-dd65-496f-8eb6-e80fa0823cdc',
        'd521d548-4cff-41f6-a11b-b0360f855a02',
        '4c8d0f0b-ddd7-4f7b-a7f6-04482b3bd32f',
        '31f82cc3-fefb-4473-b06a-0ad7339e2d3a',
        'e84d8820-fe65-4bc7-97c4-f7c78e0f4d2d',
        '2f70e180-601e-4c63-8b5b-023190fcbb10',
        '29abf0c7-2fd4-4759-9bcd-6112e64a7067',
        '2ea083a3-41c2-428f-b46f-69e702f0866e',
        'f9ffb060-8e47-4224-bb7d-7b3e0826b04b',
        '407f417b-d861-4df7-be35-643ceefa9eb7',
        'bfdf3b84-5e5b-44ec-8c58-fed021a8b91c',
        'c83cda46-1ab6-4d17-84e8-7fad7b4a3677',
        '4f11992d-2580-4b27-9a00-50ef3a4e207f',
        'b5911917-45a5-4be1-b559-cd6ee7b1689c',
        '1c619877-832a-4117-9393-34d1df5f8f43',
        'edaef0b1-2210-4c17-9642-fa1d337ea0ac',
        '5ebdcf54-29eb-4b24-88b7-b95014434833',
        '98d9cd33-819c-48ea-acc5-b0fbce182594',
        '8c58c366-ec42-4b6b-ad8f-1bfc5ed841b2',
        'dc4ab748-66f0-4e3d-aafe-7927408d72e9',
        '9d3db25b-72ad-4500-85e7-36b094e98845',
        '048198E5-4D8D-47CC-817F-BBDD05D854AA',
        '70cdd2e6-9c4d-42f7-8412-b36f2bd0471a',
        'd44c2155-0c2e-4f8f-b35d-f9f9fc4d5929',
        '54812eb9-7a14-4a20-86e0-1ae764704f0f',
        'b0fa2af0-e453-42c5-9947-ed938f8bc738',
        '862a8a76-72d0-4080-a450-299a681d0773',
        'bd31bce2-e682-4f8a-8384-bd72b66516df',
        '931872EC-9305-48EC-843C-718A9015BDC7',
        'FF7EB0D2-DF3C-4971-BFA9-033E3144A194',
        'c102776a-f1e8-43a4-9dce-cd693b1abf42',
        'f4aeaf9b-fecb-46f7-9e97-cc77046171bf',
        '3c916f06-de36-4406-b7a7-83165af9f098',
        '3e490987-0d06-452a-a5fa-c4dd776eb609',
        '436fc165-670a-4cad-a491-e425ffd89f6a',
        'da1091b1-9b31-4029-af02-13c6fba70cba',
        'FD7A2626-78B1-47F9-BC30-573B4409E31C',

    ]
    df = pd.DataFrame()
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()

    #     scif = data_provider[Data.SCENE_ITEM_FACTS]
    #     scif = scif[scif['product_type'] != "Irrelevant"]
    #     # scif = scif[cols]
    #
    #     df = pd.concat([df, scif])
    #
    # with pd.ExcelWriter('/home/samk/Downloads/scifs-cma.xlsx') as writer:
    #     df.to_excel(writer)

        CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
