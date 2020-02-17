from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('MARSRU calculations')
    project_name = 'marsru-prod'
    session_uids = [
        'ff481ab3-8fb0-47ea-8497-780acf5ad39b',
        
        # '20c275a9-afe2-4004-a727-618dee0e5599',
        # '0c5cf562-6209-49af-9a7f-730952c99788',
        # '8ad39b48-3ebe-4d1c-b8a2-2cce205fc2a4',
        # '9bc95fa1-f680-40d5-aace-c9c93f1eeba8',
        # 'f77d3fac-88dd-4a18-9fae-fb77405d4dca',
        # 'd6830d7b-b768-43e2-b633-f7e8cd4b91f2',
        # '5a590f6c-0e60-4a50-aa15-4228722da9bb',
        # '0eff8329-8f98-49f5-8680-46ed7373d0d7',
        # 'f9a74bca-07c6-400b-af2d-87a44049ff69',
        # 'd08d932e-fdfb-4302-8c86-bb8bfc29fdda',
        # 'bd95df06-6e39-4345-8f8d-4d377260478d',
        # 'd62f4612-3db5-469a-8dd3-844b64e439c5',
        # '98353329-b618-4e4b-91a8-e81bab2e5aa9',
        # '2d36ab44-8df5-426b-9ba8-9800613d4a16',
        # '7181e8b0-185e-4e34-91eb-788e91600623',
        # '31eddcae-d6b8-450e-a1cc-e4c4441821a8',
        # 'ae8e4a6f-1a1a-4029-bde7-9b572b1b4ac9',
        # '3beda0d7-fbdf-4435-abc9-8faa941ae34a',
        # '9e27480d-1fae-40c2-a1bc-4c45c4560ebf',
        # '0af126f7-37dd-4fb9-85ac-7ab237ef77e7',
        # 'e9a1d49a-1481-4dd7-b732-c8abe6a4d235',
        # '37364789-0801-436d-9c70-9f5044908f94',
        # 'ff72b0ff-5f9e-4020-a955-c069924edeae',
        # '515f0001-0c2d-436e-acec-9404521767ef',
        # '2f3d13cb-5369-4c9d-a35b-0d50d1633fd2',
        # 'ffddea4c-5259-4a9d-b23f-0e3bc2cd1092',
        # '1c93823f-fa7b-44a7-891b-b766e84b2455',
        # '1be6b522-9a35-4b67-bff4-5ee0155aa6ce',
        #
        # '9e94b72b-8a6f-4b39-9c8e-75b7f9a28152',
        # 'c9969a00-d7c0-48cf-9495-d0d009bdf40b',
        # '77210b72-fe35-43fc-b62e-fdbbf2608975',
        # '738ef110-6d62-494c-9958-d1a971d3e94a',
        # '3c192efc-01c7-4d31-89cd-37cceade0ab2',
        # 'ee74d0a0-b355-4a5c-b392-28202e24978a',
        # '476949a7-1dc5-4662-821c-68eb1717746f',
        # '69646768-b06d-40c0-94f1-4c5d4cc010f4',
        # 'f219c78f-1b30-4897-835f-096f06caf68a',
        # '9e501b61-bad0-410f-b4a3-10040b6d77ee',
        # '5e118ff6-b964-47d7-af1e-f2ac853a69d6',
        # 'f38d26a9-0b69-4487-88c1-5020cdfdcede',
        # 'c1365b31-353f-4e9d-8263-fc9063ad8811',
        # '8ee45965-36b6-4800-a569-900096a6e880',
        # '57ef382f-edff-4fd9-a7ec-d604efcf657f',
        # '84df285c-e78e-4a01-9f87-999b56155feb',
        # 'e312089c-0771-4189-bd7f-9da9ffaf6e9a',
        # '00b88dd9-a8f6-4393-978e-2ae197c520ee',
        # '7a1f0a11-227e-4860-9b9d-c7977c132326',
        # 'a215c2a7-92fb-4c58-badd-8fd00ad9e70f',
        # '99c69d7c-bad2-4062-b0eb-b8af33a9e1ba',
        # '278e9b45-b8f1-468c-b2da-65ac6466fff1',
        # '42e4ffa5-8073-4e6f-9688-0a43ee2bebd0',
        # 'ba6748fb-2584-4033-81ca-f24ae8e9ffd0',
        # 'ce1b2831-f3ea-4022-90d8-dddeeaf9c1d7',
        # 'c70d343b-2216-4fba-8c8b-e5dedbc967f5',
        # 'dd6ad61a-2a84-4ba8-843a-e5614a71d6a3',

        #
        # '3a82729b-d83f-400f-bb4a-8a24388cba4d',
        # '6d3507e0-72bc-44db-85ec-3a399654bd18',
        # 'cb513922-26c0-44b0-9fd5-4c706765434c',
        # 'e18bc259-8af9-4c30-bbf3-20d473b1c562',
        # '16809e5b-7a7a-4075-aeed-800bd6f7dd2d',
        # '63c18613-e9f2-4662-a814-961255e5786c',
        # '5c70c682-a37a-4305-be85-34a02d7dfc99',
        # '9699ec8f-a83f-4895-8cc8-275c2e17dbe0',
        # 'e245fbab-aa99-40a0-b800-16568391d6ef',
        # '46f5fb87-c2fe-4034-b1da-5910f3b98e72',
        # 'baf4624d-c632-425c-bc55-e7a18eb5fc4d',
        # 'd5733b72-56a5-4ba4-a060-cf2cefe02ecf',
        # 'd9a83c04-9d53-4d54-942f-44c012ba576a',
        # 'e4062cb8-97f3-40c9-840d-b2fed4b17279',
        # '7bf61a44-506d-4d6b-99e8-3203b073a5c6',
        # '43e6d5db-fff8-4a66-b8d9-7f8fbace6786',
        # '9acb749a-c64c-40ad-b19e-56198b421597',
        # '4a6f9856-5618-4313-8a13-9672240ee76f',
        # '7660379c-e2d0-468e-acda-9e1a8dc6a391',
        # '806a7a92-bc16-43e4-b75f-7157ae86b2ae',
        # '99355c21-89cb-4d71-bdd4-b7da81f6bbda',
        # 'dd4a97cb-d0a2-455c-9576-00ac4e5b9351',
        # '148e4cdc-c580-4a93-9353-dfdb4361b28c',
        # '8c57f79f-cacf-4882-83f1-9f32e9a62f3b',
        # '43fbfb1d-73bb-45ce-8425-59ca4f3f07f1',
        # '0c062a5d-216f-4c76-beab-8e92ef462dc9',
        # 'ef961d9c-3256-45fb-ad91-3630dd5d48ca',
        # '05f080d8-4f68-4e9c-9688-472ee784497d',
        #
        # '655386fe-edb8-4376-9503-0798b40f1ef4',
        # '69668e3c-f39d-4524-bdbc-7b148ecc9663',
        # '9e87a45e-0712-4636-a517-898c38ba7f50',
        # 'd7c34892-cb55-4c29-8625-c8094f939ac4',
        # '06c2e700-1c85-409c-8f8c-d1ac68d3566d',
        # '83728cdf-cd29-402a-938a-ac58aae6a545',
        # 'ea953f4d-77d1-4fc6-8715-3e1b6394e387',
        # 'd5c2d6bc-dace-4397-951b-7659fbfba83d',
        # 'a0f50d36-aad8-4110-a360-fbe485fdd8cf',
        # 'b59b4798-a75b-4c79-92f2-b7b10a42e6ef',
        # '506b005d-46f3-4de7-825b-50b547e13d56',
        # 'c611e281-00ea-4458-9137-56035ee698b9',
        # '51132161-8c99-4cb8-af10-d115fdc6d5ab',
        # '8a9ca4e8-9c8f-4f7e-bf6f-2ee21d391b7b',
        # 'b4a14b6e-b158-4fde-ae42-159b37631f29',
        # 'c759022c-503f-47d0-bfcc-fd62640edbfe',
        # 'f7eb6c63-018b-4efa-861f-2eee1e546be9',
        # '9b713f37-283a-44e4-8ecd-53ad9e63e39f',
        # '9e27480d-1fae-40c2-a1bc-4c45c4560ebf',
        # '5ba2515e-36d3-4550-acef-332df3821355',
        # 'af8878ef-3d88-4331-b877-6d920a39cdf2',
        # '8fc2e240-982d-4b61-8735-e6a0d255bf08',
        # 'ee67486e-2d78-482b-b1de-0cf5c71fe963',
        # 'e4fbf609-0e7c-49e9-8acf-ce2f09d231ef',
        # '0278e610-18dc-46df-9eed-b33b8b39760c',
        # 'fe2894fb-2c36-4ef1-aaaa-fdc6e1983c17',
        # 'db4df66c-e502-4911-bd71-62924450a96b',
        # 'b16c3782-8f75-4d20-a9cf-88790a6dac15',
        #
        # 'e90c43ad-e61f-459a-aced-ca2858db88a2',
        # 'f388a119-2000-4315-915f-03cf76cc97d7',
        # '350829b1-307b-434e-b900-d7e5da2843ee',
        # '430187a9-3c15-49c5-9417-342139d60a6f',
        # 'bb5b5b34-9fd8-41b3-a72c-a94851947a9c',
        # '99fcff63-a6eb-42fb-ba6b-885235b3d408',
        #
        # '9c72dbc6-f841-47ce-9771-79e626b122f4',
        # 'cb42f901-f75e-41ef-9ccb-946f6aac0c03',
        # '53493644-d6c0-42d5-a8ae-07e502deed12',
        # '0c4a9ad4-dccf-45df-ac4a-d98efccae367',
        # 'c1669fc4-9f64-440e-80d3-234c29674262',
        # 'fa52ef00-7a0a-4a55-ae60-15b85b2b1b69',
        #
        # 'c670cb1a-2890-4b10-9e66-a7aaac3e5a73',
        # 'f339a2e4-fff0-4159-b281-b935e316883c',
        # '6e34d37f-9637-4006-af40-7b409bb46161',
        # 'dd892e99-aceb-4b46-834b-338dd68153e4',
        # '9a0805c3-0ef6-4bab-9b8e-4c618362cd2d',
        # 'e31159b7-9b2e-4610-880c-bcc126310780',
        #
        # 'f649d500-574a-4de8-943c-7d05ca574738',
        #
        # '6e587617-520c-457b-adcb-bb74eba5caf2',
        # '8506b811-de1d-4135-b094-85d62f62ac0d',
        # 'fe6a45da-1877-4771-86cc-53333676c655',
        # '9bf6f2d6-1df7-4faf-8bba-0ceec262206e',
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        print session
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()
    #
    # data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    # data_provider.load_session_data('4647a9e5-84c6-4c77-9a2f-11e62000f70a', [])
    # output = Output()  # calling live calculation (live data provider)
    # CalculateKpi(data_provider, output).calculate_session_live_kpi()
