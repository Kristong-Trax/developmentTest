from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations
from Projects.CCBOTTLERSUS.MSC.KPIToolBox import MSCToolBox
from KPIUtils_v2.DB.CommonV2 import Common

if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'

    # # MSC
    # sessions = [
    #     'FC51FAC6-4EBB-4C9B-AC1B-F72052442DDE',
    #     'E79B5B80-BAA2-4FA0-8C1F-594269B39457',
    #     'E86F80DE-62C2-44AB-9949-80E520BCB3B2',
    #     'F05079E5-11C4-4289-B5AE-5B8205594E15',
    #     'dc322cc1-bfb7-4f2b-a6c3-c4c33a12b077'
    # ]
    # # Liberty
    # sessions = [
    #     'fe6e86a5-e96c-4ed1-b285-689ee8da393c',
    #     'FAB57A4E-4814-4B74-A521-53A003864D06',
    #     'BE9F0199-17B6-4A11-BA97-97751FE6EE0E',
    #     'f6c0247d-64b4-4d11-8e0b-f7616316c08f'
    # ]
    sessions = ['019B32A0-FE74-43A3-B375-796CCBD797CB']

    sessions = [
        'FDC55FB2-C731-4535-B132-2ABE3678E8D0',
        'F661BEB9-6B29-4640-B8FB-BA8C9F3C0209',
        'FAB121A6-72BC-4079-A8B0-C17BA244C2BE',
        'F51B7530-6A86-411E-A23F-9B831DB3DCC7',
        'FF146A9B-5444-45F4-AC07-C10B18B9F0C7'
    ]

    sessions = [
        # 'ce279084-9c17-4719-bc14-3777728a0183'
        # , '50FBB7A3-12E5-4B02-8EF9-89D92F8ED188'
        # , '7248681e-5b6a-4a79-bcef-2be712569501'
        # , 'ce806488-783e-4df7-b5af-fb6f802a8f8d'
        # , 'b2c4da9e-b4e2-4b6e-87bb-8137913bffb7'
        # , '3a3cc10c-be72-41d7-95aa-3ab9d4bfc4da'
        # , '2a21df06-8eee-46e8-b3b3-746f4050f7ae'
        # , '2C00C628-C89D-45A8-AF39-B6D5055DFC72'
        # , '30003B8E-25C0-4326-9E93-D8C417FF1AB2'
        # , 'E4FAA01B-36BA-4CC6-B384-119C34958074'
        # , '3fbabe3e-01b6-4229-8e9c-a674dd3e9a8d'
        # , 'dde7bb25-d32d-44fe-b541-243e22b36bcf'
        # , 'b524e8de-8b9e-49eb-96f2-bf6689e64e5d'
        # , 'ffdd7097-6081-4a50-9cea-2739d647341d'
        # , '37EE89EB-C1CD-4661-8307-D9DC053A6FC4',
        #  'C20CEC79-5CDD-48CB-BFCE-3BAFF4245A62'
        # , 'C16C261B-E23C-4623-B70A-C279C76EA2AD'
        # , '180e9f55-573c-4010-b5c4-2d9beb61c0cc'
        # , 'c20d8368-539b-440f-b479-25835d9cd5a5'
        # , '13AD76E0-7C6C-4AB9-BB3F-B0B2DEE1AC94'
        # , '0bf270d3-c95d-4512-8d07-fb9100127264'
        # , 'C8820CC0-2C98-4435-9F77-9D29AD88F398'
        # , '1b79514a-4217-4a14-bde7-5f104f61f461'
        # , '6ccd2ba8-c425-43b8-898c-9095ddd89057'
        # , '9b7673bc-9d78-4c5e-8673-f6be501a9c1d'
        # , '400E00D4-EB04-4EC2-9B69-7A89C2863AA2'
        # , '987d854e-4bef-433f-8cb8-375ae1884bfa'
        # , 'bc509507-9698-45a5-af21-3f83d023d524'
        # , 'b4e617f2-4005-4df9-a39c-06f320c280a6'
        # , 'ea4978ad-7640-4ded-979d-9bd5801bd1ff'
        # , 'EECAB01A-5ACC-4733-A2BD-975CA5796B14'
        # , '2cbe3225-6393-4613-ae27-70bccb635d1e'
        # , '487f3571-965e-47c8-84b4-f4ededbd3cf7'
        # , 'AF3F5E10-FE74-417E-8754-9B2656F73502'
        # , 'efd0b0ee-98a7-4022-be36-726c9628ee2c'
        # , 'A55D012B-C351-4D0F-A54B-3C9D93C3276D'
        # , 'd54945e1-be4d-4fd7-a363-d0f78432c322'
        # , '3525bd7e-230f-4e88-aed8-62eca51c1c70'
        # , 'f0fa96c0-1870-4d48-ba7b-76a8ececcd6a'
        # , '5BC3D8AD-A37A-40C7-823B-63D0F2277335'
        # , '3f1730cc-305c-4b78-8082-6cee22d4a795'
        # , '4cd3429d-8b64-45ab-b679-124dddfb4d36'
        # , '56E2FAE9-68C6-463D-891F-4D19DFB72694'
        # , 'E55E99FA-B062-424F-BA0A-08F379A56DF3'
        # , '55d0a8ed-0ce6-4913-8bd4-517346d5dcc1'
        # , '0340922c-3adb-4e86-91db-f8302ded47ec'
        # , 'C4B02C93-88C7-460E-A5FA-42A4EF1C50F8'
        # , 'e0685baf-87ec-4239-b88e-0e0b47a785a1'
        # , 'b74be68f-590e-466c-be4e-f45878c59d49'
        # , '17BA2E78-0B87-4B66-99F6-CB885D02BEA3'
        # , '1C1D970D-D0C2-4670-A045-5F508F5A8A2E'
        # , '2d3c18f6-b2b1-4194-ac08-b4f3e06f30ac'
        # , 'A9074BBA-FD1C-4909-A546-5AC6CE9EF644'
        # , '20f2194f-ddd6-4fc1-87d2-db91e7bddac9'
        # , '181b3b5e-9f05-44f6-9f87-1796f6929f61'
        # , 'd6958228-2d45-4831-95d4-97325e90cf30'
        # , '8957c90b-b8fc-43ec-92fe-3aa86c5c7e81'
        # , 'e104c604-a5ca-4415-8e8b-30b9fa18251d'
        # , '3e7aebf0-561c-4e46-9be9-cf31a2d61a4f'
        # , '4FA6E221-EB29-4134-9C6A-9B5CF6359D56'
        # , 'FFCA0EC2-6987-463B-A88E-98B51C785B25'
        # , 'a39c3e98-a17b-4ce6-b547-362c03d5eb47'
        # , 'e828c8a0-2b3b-4ecd-8c30-635a770247ef'
        # , 'ee578225-9ab2-498d-9000-c622e0b94750'
        # , '63796b7e-9e87-42d5-b9a8-81c44cbab0d0'
        # , 'C8D84030-4C0F-4B41-998F-2A804F4DAC5D'
        # , '59a66dd0-bd16-4568-b922-08639012b904'
        # , 'fa2c9d2d-10f5-42e0-92e8-737b2ff5dd44'
        # , 'e7f00a00-c77c-4d36-ac1b-b2d3a59bbc01'
        # , '35ce1e0b-0230-4b56-9cdc-8106c7645dc6'
        # , '52f925a3-6846-49ec-a1ec-55c1da267e5c'
        # , '827C7AFC-BDC6-4808-A242-301D02F4FF99'
        # , '29ea4d1e-0601-450f-8265-22e631924ad6'
        # , '3c7ed12f-630c-43b6-8972-9f7434d59a63'
        # , 'f3e0c4f0-7392-4d7c-8c41-e06c36777e64'
        # , 'CB64271A-5890-480B-9963-E8AF53E3DF1B'
        # , 'd9c6d0c5-8b55-49c9-a664-1a0db627077b'
        # , 'e291dc79-ad67-466a-841e-b9909974007c'
        # , '54c56fc8-8826-4d0d-aa7f-e9c8d5edc9f8'
        # , '8618e8e6-d1b1-45f9-8f11-62bb7beb3009'
        # , '52039b00-66b9-4a98-b48a-291c7e5561dc'
        # , 'b1080363-d252-4308-b27b-36661595bfc3'
        # , '7a503d7b-7318-4f67-97e1-1442d4771cbb'
        # , 'c0922dcc-bc8e-4b04-8530-750e30a04bc3'
        # , '6fc33ed5-4b7d-403c-8b03-91f389bb0a36'
        # , '3c5cbc89-af3f-498b-bc01-e79560792b82'
        # , '9DEE9790-E6CF-4B45-B5C5-BC8AF6ED01FC',
        '608bb823-02bc-4bc2-bc44-7eff177579d9'
        , 'ab0c1aa6-9479-452e-a0bf-9cfbde535d36'
        , '3ED67EDB-45B3-4E27-A1E7-CDBE2AE04879'
        , '723a42e5-3119-449b-bb88-2038234e6873'
        , '10856d9e-9ec7-40a2-af06-a59586236522'
        , 'C1F53388-B04B-4DBD-A290-A92F1308D226'
        , 'C75E8FCA-3412-4AD2-9ECD-31728DD2BA54'
        , 'e52a999c-abc1-4426-8a4a-951a895c5f1c'
        , 'C2351584-122B-4181-9CB2-F8F928DE319E'
        , '8d103aee-7fb1-4ae8-9081-8fa37ce48b63'
        , '9bcab424-8cb3-4110-8d1f-5703d1b9e6c4'
        , '93c04600-3734-4c02-beba-0ace019e2efe'
        , '34743EEB-F3D5-4040-8BF4-EC71C9A825C3'
        , '3c359054-301d-4bc6-8820-037258a118cd'
        , 'ac35736a-9947-4cc0-a54a-0afc3b4b1b95'
        , '71728e9c-f58a-49cc-9d44-5935d8040dcf'
        , 'ecf2084f-3f01-4edd-ae3c-2a78de33ea60'
        , 'fcea7644-07ec-4d3d-9b2b-ac1efc0ac4e7'
        , '63ee7243-30cf-4a93-a199-a1de8856059b'
        , 'fc620f3c-2928-4c34-b19b-b959e1299184'
        , 'd3025ed7-9508-4540-9ad8-9898faa57c64'
        , '825d54fe-0ca8-4227-a035-32d0847af41b'
        , 'e0334fa1-6d98-4f77-a063-f6c312b67f60'
        , '3c50bc90-63b8-448d-8a13-815cb335e01b'
        , '847CDC82-A5C1-47F5-9959-760690F80D78'
        , 'da2528be-86b4-4c48-8a63-9682c0173db5'
        , '0FB406F6-3342-4710-A61B-23584C2FC8B8'
        , '5bfc116b-b6f0-47a4-bb48-3c94cd8ec258'
        , '060EA4D9-3A2D-4DCA-B4C8-8486A23FDBA0'
        , '0f13f55c-0af9-486c-ad31-0b0a7cbe1488'
        , 'BA415E26-32A8-4946-8B1E-0C8381EDAD12'
        , 'd948bf6e-012d-41ea-bcf4-7b9c9b1fdf85'
        , '99fe1a10-33f7-4c6f-80e3-dd270fed983c'
        , '372997e3-f17c-4e57-b170-a963bd7f0373'
        , '414f5275-724b-4304-8307-eaf03c9edff4'
        , '6d275a8a-efb1-4e43-9a05-a9fd629ae18f'
        , '033c4bb3-3580-4f40-a4f2-a2f3e1204895'
        , 'ae895ca2-1207-41e1-acee-7e9f4109f9c0'
        , '3fd169d3-8225-4d1b-8785-91b398328732'
        , '0b75ce2f-0cab-4cb0-8154-4ab23274edd0'
        , 'ea3a019e-37b4-4e04-9d68-4b6f6940cd4a'
        , 'F6D3E976-CF8E-45B4-A103-6C7AB4AF6A2F'
        , '519710e2-c61e-42f3-a3e3-c8b7b52180ba'
        , 'f6015648-f5fe-41e5-9083-1c6b691c42df'
        , '3fa36193-672a-4079-816a-85c7bf955352'
        , 'c7a1d63e-3587-40bd-9999-4416fa19c58b'
        , '1829d74f-0ef3-40d2-bcff-2b8cb6187f9c'
        , 'ced7fdbc-13da-4a88-a0e0-e94e24b40eba'
        , '0b09a731-f7d0-407b-b318-6b434e25e620'
        , '39F2D0F9-008F-4000-BB51-89FF2CC4DB85'
        , '7EAEBA46-D16D-4F30-887D-2930D916BAB1'
        , 'CBAC54CA-101D-446C-8FDD-53D3D622471B'
        , 'c68c4d68-c3f1-4f06-9411-930cef902e5c'
        , '3ED46AFC-786A-469B-A336-1171FDC9D7D1'
        , '50806abd-87d9-4b32-b77b-05f46fbdaa60'
        , '4cd9175f-806b-4947-a53a-31ec78869982'
        , 'E5602CED-7C7A-4569-BC30-902002727807'
        , '5BAA74DD-6D9B-4A2E-8A40-9D9550608BEB'
        , 'C227180F-A4E2-45B5-B8E6-55DD496CBB9B'
        , '0B8C6ADC-70A5-43BB-94FD-A25500B7A1FD'
        , '8E32ACC3-8ADF-49F0-B10C-1682FBDFE697'
        , 'bf6b0406-bc48-42a6-a1e9-dd3fef28659e'
        , '3C1448DC-9891-4619-A974-B9CF682FC503'
        , '42504deb-68a3-4c32-9d40-9cda6c06d479'
        , '4668423a-434a-4b81-b4c3-78cd1e0ca19d'
        , 'aba481de-de2c-4304-bedf-aa80bc85f828'
        , '65c88587-86c7-400f-99db-7433af21efb6'
        , '2ba49b02-34d6-4bbb-bfdb-f817233678f5'
        , '0c4c0052-1f7e-41c4-80b4-c350398d7c1d'
        , 'a758dbf3-01bc-4178-b59c-d23412d0ec47'
        , '97726a34-846b-4c28-a6e3-4ae4883e83b4'
        , '62247414-7389-4aab-866b-bf3581f8ac70'
        , 'e518f727-9cf8-4989-85e9-f312cd1f0f1b'
        , '3df69e03-46a0-4133-a968-53f4cc25215d'
        , 'B4059E97-5149-49AA-8824-FE9EDAA401BA'
        , '6996c485-c00a-4830-bd22-250bcb2c1386'
        , 'd024c98e-a453-45e0-827d-590feb673f53'
        , '1bd3ee4a-6636-48bf-9960-daa321c7d51e'
        , 'a44d2dd8-d119-4cd5-a1b0-2e0e1459f5f3'
        , 'D90419A1-E84A-4350-A47E-5F15C7B7E5CA'
        , '00dbc7a8-ccd3-41b9-9bb7-2df59b105ad0'
        , '406bfc27-7bb6-40fe-804a-5ba5ab179609'
        , '8E43C998-85E3-4040-921E-62A042FF888E'
        , '1cc45039-1d3c-4c3c-b0f4-a5411e2da226'
        , '02459C35-93D0-49F2-92D3-49669270330C'
        , '6032ED4B-1537-438F-87A8-AD1E721A4F91'
        , '52523b40-e019-4444-a8a5-ffdd3adb9444'
        , 'b93eace0-3e26-4d59-ac4a-4d15965ac5ab'
        , '752E80AC-9688-4A69-9DAD-9A7833DB79BF'
        , 'b3e5decf-5753-4b58-86f1-d2d4bac0e1c8'
        , '28089795-5D36-4780-817F-3308BC3C646E'
        , 'e614356f-46d4-44c0-abfd-af2077d0b65b'
        , '1806bb48-eada-4434-9680-6726701bfd4b'
        , '49c5c4fa-38be-44a9-9183-62259cb3d695'
        , '9b7052f4-dbfa-4c76-ba7a-eff3bd9da6da'
        , '29418730-83f2-4028-a8f7-ab037ec4f4c1'
        , '053e33e9-6891-406a-8248-e5a099360646'
        , '1A23328D-B0D5-4129-845D-DBAF22CBA8F7'
        , '0F8164DF-5270-49C5-8057-7AFF0CFE2B95'
        , '72c36c43-270e-40e2-a858-f1194cc37511'
        , 'c2b8aed8-8fd1-4de7-9f78-5739a2f43c82'
        , 'e196c197-fe04-4ba4-8479-fc7491e1219a'
        , '3260ca71-963e-4418-98e9-a9596b40a7c3'
        , 'a25f0513-179e-4fa3-8dbe-8a3d251cc926'
        , '39a3195f-4a9a-4640-82a6-22305677477f'
        , '4d69ef14-ebfe-4146-af3e-2c0326a8e8d7'
        , 'd990cde0-15d0-407c-b436-0c4d51c7efaa'
        , 'DD274D5A-6CE8-4357-8B19-5DED4C4E2C12'
        , '6ecc0991-8113-47ff-aacf-e99386487ef3'
        , 'CF66B1E0-01E6-413A-BEB8-21A9697D5941'
        , '743f6a92-655a-4173-b0dc-dc76390b1d48'
        , '0B2FF16E-E743-47DB-A3EA-E5809C2EBE06'
        , '054e0a87-7b14-401a-84a2-7da49c8a9096'
        , '9285bbe6-458e-48e2-936e-26c76686f51a'
        , '1EFFC14B-CD92-4235-B2B3-743858ACAB08'
        , '5657c8f8-b6d2-4b17-abb6-9cbb288a5594'
        , '830f4210-8119-45df-b8b7-56714fbfa999'
        , 'c6516767-392b-489b-bd70-f840cf2f0351'
        , '7455ef5b-1340-4ae1-b6ba-96eeda80885d'
        , '89906555-8B13-4EFD-B88E-61F0A34FCD3C'
        , '730E8282-B3F9-4277-B6D5-E9A20FDC49E2'
        , 'ebf746d5-728d-486b-baa6-06c73c8e2695'
        , '19ab4085-3790-4c83-8f69-f1bd287ea0f0'
        , '05920A34-1D37-4A25-A296-B02B5E3038B5'
        , 'f7601a95-6012-4fe5-a576-80cec807ef70'
        , '4CF53BBC-C981-4E16-9C45-D7DD6FEC0B2B'
        , '5561348a-db17-4a62-9e8e-d8e179ded04e'
        , '5c3545aa-2907-43b5-bec0-d5152083aaaf'
        , '4975d60a-543c-4709-bfc3-8f496d878622'
        , 'a65e88a9-50db-41f5-bcd7-bbe84bd837a6'
        , '5654f8af-954c-49af-a513-8e7416002451'
        , 'B3598151-2512-49DA-BED9-5EB84485FA29'
        , 'eff134ea-f97c-44aa-97a4-1e428b84bfa7'
        , 'b7f69838-267a-4c4d-bf47-4429856f56fc'
        , '2E7DA7AE-0F46-4832-86E5-BC610D87AACB'
        , '3c72e694-2268-45d4-a880-f88eb2ca9b11'
        , '37d993c4-e999-4e48-9957-8e2db33b94d2'
        , '766c1d42-b518-4afe-b85d-54926fc7a6e4'
        , '939076ee-11e5-4817-a1fa-22b9e68db187'
        , '572A0A0C-E891-4C9B-B685-E9C2CEC1ABF6'
        , '1E9CF145-7EF4-473F-BC10-18DCA297ECCE'
        , 'e781a14b-3f55-4355-af81-acd41372a94c'
        , '5476939a-55a4-40da-9bcb-cfffc94e9de1'
        , 'c0ff4baa-3302-4ddd-ac1d-fb68618b7d66'
        , '2440522c-fad0-4598-b0cd-1cdcb3f30e67'
        , '7a5b9b12-edae-473e-ab74-1529d615eda5'
        , '1478a233-1436-471c-a3f6-6a16d3501d46'
        , 'fc2d2dcb-4d04-4f6e-a7d1-bf36feefc38a'
        , '55781a09-472e-435c-866a-da1dfba0af65'
        , '71b37adc-144a-479a-9b69-75775e301cea'
        , 'bdfdfca3-537d-4e8b-9bec-e0bdbdf5f137'
        , '2a6bbefb-ff14-448d-ae64-7eb9c11b9a08'
        , 'C972A1C5-E0B8-423A-9835-78F2ED12F7A7'
        , 'FA2D60D3-79FE-46F0-B92D-CAD3FFD5F36D'
        , '35929c67-c7d9-4444-a9cd-21da7eeb3199'
        , '84A6315F-961C-4B7B-B30E-0CF8DF4BD582'
        , 'CCB3F305-779D-4691-ACA3-0DA9D9CD1846'
        , '5a147c34-3ae8-4b2e-99c6-ca968c82a456'
        , 'f12b05e5-2a09-4d4c-a408-7f72c441e4a7'
        , '476597FE-4F99-41A2-A875-240D5A440589'
        , '6DC57AA3-9ED4-4FCE-8734-D9D007157291'
        , '995a1861-42e6-4e21-a539-935e2de0af91'
        , 'b401d636-3fff-4cf2-981b-aedc785e4b96'
        , '49cb14a4-6a38-4946-a4a0-3f9864c63ba8'
        , 'ff66398c-bf90-4a2e-82ce-386537cebaf6'
        , '06ec5eb7-598b-4d5d-888b-9d5738ad22a3'
        , '2469C528-D5ED-4C6C-BF23-EA78461616EC'
        , '2338dd86-4cf7-4992-bd8f-dce5aec18abc'
        , '278D4D3D-1345-48D1-8D39-45CE35F57365'
        , 'c57b3cfc-bcc6-4a8b-ac50-8b3f30345d10'
        , '8bc72c68-c52a-40a9-a135-6b99ec04c34f'
        , 'EB1E6ED5-75AC-41D9-B265-256B0BC552DA'
        , 'e0e86349-ee09-46b4-a12f-6b6c9359c25b'
        , '8f0a6787-2db5-4a05-bfdc-9286434e9c57'
        , '45a96319-a073-442a-ad67-a3280ab43e93'
        , '5A378D29-0B8B-42D1-9777-58E4E5A7A95E'
        , '127072b6-dbb4-4d6d-bead-f0f32f8fb2d5'
        , '936CF02B-0A8E-4DAE-B226-C711C2BA8D7D'
        , '24ba10fb-55f8-4034-ac32-0e2ac20115b5'
        , '31EA47CA-C52E-4A05-B8DA-C5F0AF005557'
        , '61AE1F1C-B74D-4431-A6EB-0D977501ABAB'
        , '314bab57-de4b-407c-905b-ebddc3b0b27d'
        , 'A0949D8A-1D9E-49DB-996D-776A8082AF94'
        , 'a9d5b08d-900f-416d-a6ca-5656ce1d0078'
        , 'E9808FB3-8F61-4099-9BE6-4A305B84A07E'
        , '5D73BE5B-28B7-4EB0-B3B0-F4B8025C82FF'
        , '771bbe68-7c75-44b7-baf6-cea41ef9c2af'
        , 'f71a39e6-e0d0-4d2f-913d-67715d3a34f1'
        , '16ca3b69-c0ef-4f53-9712-fd3180ed69ea'
        , '6de5d805-9363-4a0f-afc9-36c7301f6ce1'
        , '8BE20462-2799-4619-8D88-DF69D652844B'
        , '12d646c2-71af-4bd0-a029-757166f9e0f8'
        , '4d929ff8-bd82-4f1a-aa91-ca14597bb752'
        , 'C9DA3C42-C421-4589-B612-6146D0D39F8E'
        , 'f2efbf4a-ff63-4dba-a0a0-0292c3497679'
        , 'dadfc310-f3c0-4a16-86c2-eda39ac10a04'
        , 'f624fd40-1276-4a56-92b9-c36c8361ae8f'
        , '23671e6a-685d-494d-99ce-c0cfeacaa56b'
        , '69dcf033-be51-42ed-b503-80f52a26bd2b'
        , '26848d40-62d9-4896-adcd-e4f822640285'
        , '6d4ee9f5-15f1-4a64-bf4d-4d65d8c5c9fa'
        , 'f60bcbbc-2fc7-473a-9676-e634227ecf89'
        , '8ac8c8d7-7438-46a0-a971-dbf3673de49f'
        , '4915e4bf-fbe5-44cb-8adb-b46a589a5df6'
        , '77018803-1e70-47a0-992e-015e8e5e769b'
        , '12cefd4b-37ad-41c1-aae4-2ccde73a941c'
        , '3B3C8C3B-A3E7-404B-BADE-383529C11EDD'
        , '76c2877e-df03-4662-9c77-7aa00062f41b'
        , 'da34b970-8df9-4b36-adf9-67e16ee1082d'
        , '09ea9b83-5c74-40f4-ab18-b6c2b5da74e9'
        , '85fa6cf4-c3a0-4578-a01a-38dc044f2da3'
        , '3deec0b4-3179-4822-8148-6197aa144ef5'
        , '938c04dc-1fad-47d3-aea6-ef2103e82bb6'
        , '0fdcb37d-d7d4-453d-b3da-a8dc7a3f11ef'
        , '1daedaf6-4da0-4ab9-bfdc-c2c4f13440f7'
        , '157086fd-1be9-46df-9d75-6c2990e290ec'
        , '11006a86-31ed-4579-a48d-d91964e35b6a'
        , '1abb8af6-d222-492b-95cc-8c9f625f383a'
        , 'b9086969-5324-4902-9b1d-f892073a8701'
        , '62351393-b023-464d-b40a-a50fe9002256'
        , '9ca11327-930f-4cee-92cd-d027974afaf3'
        , '46096d1d-0235-4bff-a157-70c20bb403e8'
        , '5176F89A-625B-4D92-B229-41C23322E18F'
        , 'bfa783f4-44ed-4eed-a9a7-c367e1b946cf'
        , '006df1a6-aa6a-4bbc-b405-166dbcb99b61'
        , 'b624f1c3-e399-44fe-9e1d-83818d74bba3'
        , '630d2fe9-6d53-4b36-bfe7-923df4751c5f'
        , '2f33a79b-0e6a-41c0-99ca-5d524f5e1456'
        , '37d90c1e-9a82-45c9-aa2c-b0c7fe3a8932'
        , 'f30bde42-8968-4da5-a13e-260fc7a9c100'
        , '530B2371-296F-4EAA-A5A9-D3512834E283'
        , '9fb634ad-e394-4c48-b64d-c660aea2e389'
        , '1CD44E3D-D93E-4878-9A68-729B5E9E900A'
        , 'E885A4CA-0EA3-4627-AC17-5BEAB1338D52'
        , '7eb19ba5-495a-4ecb-a16b-caaca2bbc6d7'
        , 'd72571e6-1b6c-45d1-a005-8fa9779c38e6'
        , '781FBE3F-3B92-4C70-96B2-A79105C2268F'
        , '79478936-d46f-427c-8c24-7236f4ccbba0'
        , 'fb8c7f77-52f6-4b18-a4a9-abf748da300f'
        , '9345ea46-fdff-4876-a544-352eaa2b1caf'
        , 'dc40a264-1097-4ba4-bdd4-f0fcdf5a844f'
        , '11521c8b-3ee1-41a2-b9cd-83ce090bf0c2'
        , '4d2b400e-c1d6-459f-89a4-0d88ddc0629d'
        , '6edc9901-5490-4f3e-b0b8-a1b1b57c596f'
        , 'a42d07dd-f9fb-4423-8432-60fb137b0b12'
        , 'ceca81e6-db22-4c1a-afc2-1e390c3948e2'
        , '7dbda3bf-7ec4-4476-bdd7-b99bfa3044f7'
        , 'ca744bf0-f8e9-460d-ab3d-a4c3e3eab218'
        , '73d93041-3efd-42e6-aaba-1ccb923dff7e'
        , 'ba3a8832-32ee-40d8-bc46-561e5d718512'
        , 'e586c9ed-5491-4570-b6b4-c499b9e2a2c3'
        , '7bec9cdc-3d1b-4132-87aa-4981fb040b41'
        , 'dc0074ea-75d6-41c2-a664-70d608633c1f'
        , 'b27f758b-9e93-4614-a9a2-76440beb5bd7'
        , '6fe4f496-6c5b-4086-857f-6764068bf0ef'
        , '71fad580-1494-4d67-ae1f-1387db8d429e'
        , 'f1ee1273-9017-4a76-8443-56ca1cd11854'
        , 'b00704d4-a673-4630-98ef-fca2fd193158'
        , 'b8411b5a-bc65-4d2d-93cb-0b6c4dd7ce47'
        , 'b2d443c6-96d8-4b0b-b50b-18c5b102651c'
        , '7e1dee56-e5a4-419a-885a-d0045d003ba7'
        , '456b4eea-0901-468f-a0af-4817dcf56ef5'
        , '7736d379-21f4-43ba-bec9-8317d98aac45'
        , 'ec3e524b-d4cd-42b9-afd7-eb90a38a32bf'
        , '95b6b6ec-4c12-4e10-a6ec-67fb6a82833a'
        , '708AEECB-8445-4AF5-97C2-29185A68FE99'
        , 'aeecc7bc-81b1-48a3-a041-096b69ae5066'
        , '9b461030-59f9-4021-a52f-3f2d5b7c9c1e'
        , 'fe98dcfa-b31f-48c5-acb5-5b47301700a3'
        , '45fd47d3-b426-4c06-a43c-463bca475f16'
        , '0E5C715B-3098-408D-9D81-BBB9CCC43141'
        , 'BC1ADF50-1F7D-4434-BCF4-9895D5B9AB0D'
        , 'B2709B3D-DDDB-493A-92AA-FEE3FA100B92'
        , 'ED1B6D8B-44BD-464B-90C0-A98D2AC2ADCE'
        , 'CFCC0397-CF57-434B-9D5C-3EFAD96FC288'
        , 'DA3DA12C-E847-496A-944A-61D30D4E1C0A'
        , '6A6B5F12-17C7-40CC-B714-62C08EB7FF68'
        , 'C36ED48E-0BD9-4223-8473-52D0A11B4035'
        , 'BD766091-427B-4802-A1E2-8D407DE0A6DB'
        , '90646961-59D4-4B26-BEB9-EA08029A015C'
        , '30DC26F5-9A68-467E-8E0B-129FD72ED5F7'
        , 'B58BD794-43A9-45A5-A405-2CFAEF2CCFB1'
        , '2ED7876F-C24A-4E09-8F14-40BD38F3B7D3'
        , '7EEB56AF-9D30-4763-B3BC-EC7A143E2AAB'
        , '4C2AE483-AA01-410E-88F0-C30640C48B2C'
        , '9E5C88D7-58B3-4694-A98E-27523FA9144F'
        , '6E14B46C-D7BF-447D-AECD-07D1BC98F2B2'
        , '473A01CD-CAF5-4317-BE88-6E67013E05C9'
        , '4F39ACA5-7FA2-4D6D-92E3-45C721C224E3'
        , 'DF297F87-BA35-452C-BBF3-6759EE8CFE87'
        , '2F8B1A4D-944F-441C-BE9B-C4C21069C761'
        , '36C9F159-AA61-4F06-B925-8F7D3C7BCE3A'
        , '8A60E8A8-9219-4931-9835-10B1962CF9AB'
        , 'B564EA2A-2290-4FA6-92D8-FD982AE804BA'
        , '1B591398-DB5A-498B-B45E-0F0D7BB9948A'
        , 'ADA7D2DE-AB72-4A77-AF24-2800086047C2'
        , '5F5052AC-0AF9-465A-801C-A7D82ACBBBC5'
        , 'E7AF6279-24AE-4655-8B65-17534D728BBD'
        , '7BE55691-4319-45E5-84BB-0D306256BE77'
        , '042B0AC4-1752-4BE4-8E50-C069657B5E12'
        , '8E38037A-E9AD-4230-8847-B4D45ED31551'
        , '6B7DBE43-499C-414B-A296-DC7365AE7E01'
        , '9509C3B7-3BFF-4281-A1E6-A95B2AF9C1AB'
        , 'DEB0913A-EEC4-4577-8A90-2B53D8A5F75D'
        , '34B1E060-EA07-4CEB-98A0-4CD4A0D867C7'
        , '2638EF44-6075-485E-B74A-8D2F9CCEFCE7'
        , 'b3ee4f31-667b-43c7-996d-42e73085f7cc'
        , 'FE9557E2-0DFF-4E95-BDEE-EDFC1E875333'
        , 'B2E613B7-290B-4E89-B54F-233825C5ABE0'
        , '20DF8AF6-549F-4C2A-A69E-F67F8274ACFD'
        , '6F26C431-3535-40FB-BA38-17CA673D9889'
        , 'D10F4056-A3EF-400D-8F99-E3948CA055FD'
        , 'B0C2B324-B5F3-46C2-A86F-7EF33BB43247'
        , '71740d9f-dbc6-4e76-88f7-2d224dca534a'
        , 'f5c93d46-cab9-4295-8bda-a6b86d7e095b'
        , 'aca3dc10-c2a0-4fa6-9051-591bc6f74ac9'
        , 'ca186a4e-6378-4269-b333-a47d3fc9d462'
        , '2e3ae4c7-beba-4c65-9b49-5d22fed3f2c3'
        , '9b27735f-cd29-42b1-87d2-09c1323da21a'
        , '57E81C0A-A711-4D6D-A583-4E0AB1045AFD'
        , '0bffc114-aca6-4b58-8df4-049cca72a0ce'
        , '69830ad3-7207-44fc-bb9c-668756e83a80'
        , 'c743b9c6-2462-41a5-98a5-f4cd717af82a'
        , '88C7EAD8-04A0-4C93-9C6A-19C84868AA36'
        , 'AF78D948-9927-4B3E-AF31-58E4C7498935'
        , '27c90d75-0df6-457a-8e50-c32f82404f06'
        , '15840440-a54d-4184-a42e-3e66a3099be4'
        , '9b3b8e40-4ab4-4942-b06c-506426b2c4e3'
        , 'baf6a3b1-a76e-44e9-8e9b-7cb8eccb663e'
        , 'ef6b5e09-8bef-4db0-b308-6045f327290d'
        , 'f9676f16-fd21-42e9-a304-81446c1c1993'
        , 'ea0fde80-2eb6-4ca7-874e-1ee13f927678'
        , '778172F3-6A6C-4D3F-A47B-278B45B8A42C'
        , '4a05774e-64d2-44cd-95b7-f96e11f8cb0d'
        , '4e1efa9c-9405-4025-850c-41ce93b1fd06'
        , '83404816-3DDE-40EC-8034-49FC34D956E6'
        , '7c0a2a18-b80d-4a19-b04d-e63a8240f314'
        , '00e59ae3-bd8c-4ca9-92d9-f30a4cc7270a'
        , 'e34afee7-6077-4eeb-bfcc-5379cb375135'
        , '52e68250-5780-42d7-95c6-d2204c7c739b'
        , '1fe23c65-7d26-45f0-9cc3-55b62a026516'
        , 'BCAC1C0F-CB22-4ABB-B8C5-6497DB65416A'
        , '756497b8-2acc-47ce-a101-697a5de0fee3'
        , '8134ba11-da7a-43a6-80da-2836d3bf0ef8'
        , '02dc1b37-8a17-48bd-b287-c9afc983aa63'
        , 'f792eb41-6140-4dc2-a036-d9db5e7bb66e'
        , '223e0b5d-d219-47cc-aa50-9fdd2b9318ce'
        , 'dac13ae7-00de-4647-9e9b-596872270c98'
        , '0c9d293b-21d8-40d4-8f0b-7a01a5624d86'
        , '967486b5-fc71-4f50-b9fd-51778cdd6435'
        , 'e5160897-69cd-454e-b004-ba7d6d04e938'
        , 'cecc5ccc-250c-487c-bc94-6cfa41b898f6'
        , '676618a1-f834-40c1-8f40-7e77dcb9e4e2'
        , '563E4A84-1221-4FD2-B03E-B61F6627BB6E'
        , '18da5b42-0826-4be1-ac2a-96a708344352'
        , 'f5554d45-1305-4062-8d10-da2279820e56'
        , 'fbec8261-4a12-48c9-867e-fb6175b575b6'
        , 'ae2e20be-37f6-46e1-b3ed-360b7ee86bb8'
        , 'f74ab7ed-96de-4e72-89c9-89d8630c51ae'
        , '26072ac9-9bc5-4918-ae20-728ca3e88d91'
        , 'CA6230BD-C1C8-4180-8B85-749ECBE6F880'
        , '46763720-c0bb-4924-9aca-5680ccb76103'
        , '821f9d59-3f91-4467-8029-cd6d60543ad8'
        , '96bf5126-542b-44f9-aa4c-adaf65849cbe'
        , 'FC3083E8-6D4C-45D5-9981-BC92A64460D4'
        , 'e20ff04d-9dcc-46a0-b2b7-702392008443'
        , 'b9018afe-3a2d-4200-8d6c-4a617b5f8209'
        , '719b7481-d594-4a14-9ed8-b4cf93e25051'
        , '83CF0271-754B-4240-8229-088D0668B93D'
        , '7aaadd8f-acf1-4f48-b879-c652e4f32215'
        , 'ADF1402F-EF21-4EF7-8728-09FF882673F9'
        , '4c362d7f-2446-4529-8bb9-9c1fcfbb5c2a'
        , '587ad275-efde-4650-8556-3aba8a831b51'
        , '8A8EAF90-276D-49F7-AF0B-649765532ED9'
        , '8be560f3-2573-4659-a690-0bceeb9a43b9'
        , '1d68e0a1-5f6b-4a74-b76f-725e149241ab'
        , '28e18407-727f-42a2-aa0e-b684b3d32b7d'
        , 'DC506EAA-C4DF-46A3-AE8F-F81F27619650'
        , '0e211726-fb52-42ca-8bfd-924716859dd6'
        , '7d5d81f9-b6d5-451f-bfe8-faf98b16f742'
        , '1e504dfe-f3d1-4ccf-951a-d813013e8c41'
        , 'b9c9f684-10e5-46ce-adfe-6caa09152b3c'
        , '61f4f85a-9446-4680-867a-9ca562cb3263'
        , 'f458e870-ed06-41a7-9943-0e44fe9d282b'
        , 'F66C08DD-6F8B-4650-B4E8-AC5CF7FCBA27'
        , '25157fc9-d1a8-4adc-9584-8b1f8d7b349d'
        , 'e8cbdf32-dfc0-4b63-80da-2c4abacb1b9d'
        , 'b79748bd-4525-4735-9aec-dd5f7080a333'
        , '15a0dd46-eaa3-41b7-a349-b3ebb93854dc'
        , '1041d675-65d7-444d-9e32-70e22d29076f'
        , '8964186f-b985-4dd9-9b23-17461c9ec876'
        , 'addfceed-d964-4f43-a75a-b7c3877ab7a6'
        , 'e9a30a79-ecee-4c73-9dfe-5981c9cd33e4'
        , '4cd1d562-899a-40e8-bc8d-db9cfa48fe37'
        , 'ca950d26-aaf0-4e79-a325-3a1ebd1ca6d4'
        , '9ab799de-3a5b-4f09-830b-4bf72980319e'
        , '6a5c36c8-e978-4003-8f28-d92cdab69e54'
        , '732b8fd9-4fc3-439d-b605-fec29a376064'
        , 'B6477977-AA42-4E8B-A93B-D8507F31BE78'
        , '408f1e30-f053-492a-8cdb-c0eedf65ef24'
        , 'bcb04847-090a-4bcf-8418-adf29c8f597c'
        , '068ED909-0482-4B6F-8FF3-4C677F88EBCD'
        , '05e5f64e-ed07-43b4-8947-5d009395b89d'
        , 'f325eaf3-000a-442e-a643-09c472bf70c0'
        , 'b84e720a-d21c-4eea-a87c-e4b1ff4c4c67'
        , 'a2149bd8-320f-47df-a1eb-c9c9c981a966'
        , '6af43958-c6fa-407f-aa0b-b8c51ac4f71c'
        , '34bff8d9-fea3-4635-9a37-84a21f11bead'
        , '400d0066-2b17-4f8e-860d-f12d0feb7f9b'
        , '1558b64e-ac43-4ae7-87dc-5416b857e7d1'
        , '0dd26a8d-6a53-4a2b-acca-dd48798d91d2'
        , 'D017EA85-3E63-412D-9CAB-5EC4935DBFC5'
        , 'f033c735-2cc3-4496-a851-ec33f68561b6'
    ]

    for session in sessions:
        print('***********************************************************************************')
        print('_______________________ {} ____________________'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        common = Common(data_provider)
        MSCToolBox(data_provider, output, common).main_calculation()
        common.commit_results_data()

