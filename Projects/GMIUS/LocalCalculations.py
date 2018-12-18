import os
from Projects.GMIUS.Helpers.Result_Uploader import ResultUploader
from Projects.GMIUS.Helpers.Entity_Uploader import EntityUploader
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GMIUS.Calculations import Calculations

if __name__ == '__main__':
    LoggerInitializer.init('gmius calculations')
    Config.init()
    project_name = 'gmius'
    # ru = ResultUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/Yogurt GMI KPI Template v0.2.xlsx')
    # ru = EntityUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/Yogurt GMI KPI Template v0.2.xlsx')
    sessions = [
                'ab70f0f1-7ea1-4e76-a21e-45b45809d301',
                '039907ec-6e6f-4d41-b1a1-595152e21225',
                '342c2485-28ee-46d2-8d6b-11f355f574d2',
                '70ec27f5-457c-4513-8927-697450c4cbe5',
                '3726b021-6bf6-40c2-aca3-2c8a28189cf1',
                'a01facf5-992d-462d-bfed-efc676bca6cb',
                '5ebee0e0-0bf6-433a-a702-76cd98a0ee0e',
                '4a1b84c6-8ce1-4f38-a305-036bafaffb78',
                'bb320b40-016f-40b0-8eea-225c81815724',
                '936360d6-9b90-40b4-9af9-4841312a9a1b',
                '52b8ca84-2756-4396-9058-9a3ad5c4cccb',
                '5aa81aa6-1bab-4730-8441-81ffb5513d11',
                '5a769904-58aa-42b3-bd8b-7f76fd8fd43a',
                '5f3e26ef-eafc-4729-b18d-51b8799a71be',
                '2fe10dac-8bda-40ba-91eb-ea86016a6c6b',
                '26175b23-f786-4564-a9a4-e8810ce231d9',
                  ]
    sessions = [
        'd0e02332-b182-4ec7-9776-f79ae47fc8a6',
        'ab70f0f1-7ea1-4e76-a21e-45b45809d301',
        '039907ec-6e6f-4d41-b1a1-595152e21225',
        '342c2485-28ee-46d2-8d6b-11f355f574d2',
        '70ec27f5-457c-4513-8927-697450c4cbe5',
        '3726b021-6bf6-40c2-aca3-2c8a28189cf1',
        'a01facf5-992d-462d-bfed-efc676bca6cb',
        '5ebee0e0-0bf6-433a-a702-76cd98a0ee0e',
        'ddce9cfd-63cc-444a-b37a-5aadda6fd695',
        '4a1b84c6-8ce1-4f38-a305-036bafaffb78',
        'bf8fa5b5-da02-4db8-87c4-3a4b4641fae7',
        'bb320b40-016f-40b0-8eea-225c81815724',
        '936360d6-9b90-40b4-9af9-4841312a9a1b',
        '52b8ca84-2756-4396-9058-9a3ad5c4cccb',
        '5aa81aa6-1bab-4730-8441-81ffb5513d11',
        'd219e1de-105f-4ff9-aa37-1f111273fdd1',
        '5a769904-58aa-42b3-bd8b-7f76fd8fd43a',
        '5f3e26ef-eafc-4729-b18d-51b8799a71be',
        'e24cc614-01e5-4418-b7f3-1bc4022c23f9',
        'a6cb804d-cd1b-4fd3-8419-ad93c22a5486',
        '844bb1aa-fbd8-4047-8408-0870b70b14cd',
        '06974bc7-3c7b-4e29-84f3-a58b54677f18',
        '508e23d5-f7c4-41a3-a5fd-9c0b123f82a3',
        '909f4198-d87a-45db-9edb-ca5ca2373fce',
        '5dce2225-2539-4616-80b5-995d6ab5b911',
        '002ba7b9-80a7-4ffa-947f-c8e6a885bda6',
        'f3478b52-9ed0-44a1-bd55-6515c10e6db0',
        '5a89a931-edd2-407c-a072-182af3625c95',
        '391e88c2-2e36-4ff8-96da-9386da40c30d',
        '452e7a5c-729f-4add-bce7-d7e899ad4166',
        '33a1d72f-4fe7-4bdb-9d75-04f74bc9b510',
        '6642b4df-b72e-405d-bc2b-3b3c43fe7f19',
        '344c03e8-c894-492f-8cc5-bb5e0d36b816',
        '20c3746e-858c-4d17-8f63-7c38c75715ec',
        '1d26c1fe-1d48-4a63-acf4-85a91fb0aa98',
        '78af0a3b-f5c9-4882-bb18-2cad53aa7de8',
        'b488817b-ff43-46ae-ade9-cd83c9ae3318',
        'd09e2478-1457-444e-b7dc-3541c3015a3e',
        '61e3f127-d5ba-459a-942a-629c62dbd45a',
        '1ac35ad4-0c82-4883-92f5-cc0dd3028e19',
        '6e6359f1-b0d5-4dd0-957d-a85261924d00',
        'cf9170e6-97ae-465b-a39f-3bf42ddee2e1',
        '54dc6402-a05a-4e46-aa8d-0ce24296514e',
        'd391e687-614c-4e4f-ab84-33e3ea854c0f',
        'd06994e0-afb2-47e1-8c2f-3350d1e894c2',
        'ab4c7c3f-36cb-4093-a51c-97a0da646fa5',
        '60c788a5-8122-482a-bacb-1fa59eab971a',
        '7e281219-5a2f-4f21-8c06-f32ba57d4e8c',
        '4a0e401b-7d08-4e36-951a-1fcf1a6ee3c2',
        '6ed54e66-0b0c-4f3e-a1be-348c4d9666c6',
        '0b3fa765-4e33-4bea-bb30-bfa89fb9a249',
        '7919ae90-1e8b-4fa6-8bbd-826ba7bf63ba',
        'cacb756a-403d-46ce-9146-dfa92008b971',
        'f8073334-feeb-4715-9aca-02a6f2ababe1',
        '47e47e2c-e675-4a0d-b68a-bd61f8fd74c2',
        'c70219ac-552e-46a8-9414-07c6b7c24d6e',
        '80195571-5057-426c-a91f-8595711c9775',
        '0d33dc92-1259-41c8-8aaa-1db2f23f2b8d',
        '59fe4d9a-ee83-480a-8764-4362712b991d',
        '53d2ec9c-4b45-4151-a0d0-bd03968d573b',
        'e32abffa-acd8-4f8d-8768-434ed510bf8d',
        'a9dc2182-3075-42ee-b352-ee4401da58ee',
        '52250035-df0d-4068-9f72-4f42b233a288',
        'e2776b01-f801-4e3f-9968-b5be73f4af1f',
        '82d7d0c5-a4fa-4b7f-b96f-18ea9a7b76d8',
        '14a0a187-fd7a-4c14-b1a2-4ff52a3e5fca',
        'b03b08ec-d004-4738-9130-a1f7efd8823a',
        '4a6ad7b0-deb3-468c-8b72-1ac64fd332a8',
        '9352dbe6-617b-4467-9502-ff5443602bf2',
        '6705fb30-2c10-4757-a694-f7c61d4c5f18',
        '7210ca26-b329-4dd2-8218-8477c5056cdb',
        'a9c1de09-ac67-4959-9f5d-0950973ea895',
        'b5e7200c-35a2-4bbd-b2e9-99f87e4769c8',
        '821279d3-f0d3-49bd-9215-1d0f15a8aedc',
        'd313fe6e-c5a2-4ae4-bb0b-204682175035',
        '5d840a51-110c-4962-8882-c9969f43cff5',
        '4519a833-4cf9-48ea-9295-5151aadc9cf7',
        '8cc20864-9d66-4193-beda-f1a0bc4fcae2',
        'a3f7cdb5-ed6a-4721-ab00-cfd2097cf491',
        '1449fe0f-3174-4a62-a4ab-fba275904e1f',
        '761d1be8-c949-47e0-8553-6b77b7f6aaf1',
        '3bf65a24-ecad-47f3-b10c-42b73470618c',
        '933f0732-1955-49a5-92e4-d4dd20124d13',
        'a5dc2e82-3efc-4e75-b50a-aa97ab9fcdbb',
        '74373d38-9856-4592-8d8d-9943c3ccdc1c',
        '91b77ceb-b977-4069-a327-cabf3a37a0ce',
        '967c75e0-06e5-4c6b-a916-5239b73dac8d',
        'b58b2b56-db77-47f3-90ce-1cd3071cc08b',
        'a04391b5-d645-4dff-8438-ce753f4d9081',
        '795e3c92-ef2e-40b1-bf41-6160c1df7d0e',
        'feb4cb05-0ec8-45ba-8661-cbf77d6cd844',
        '42273195-80e6-4bf7-b1ad-dff15439ae31',
        '2b727c4d-28c1-4e6a-b31b-ab8dbf0cd79e',
        'e059950d-2995-4fdb-a9ba-adcce4fb2379',
        'a233add0-84f6-476e-bf6d-bf3bdaba82ec',
        '02030fe8-3535-433a-86a5-2f7e26e9aa28',
        '80a1c162-4884-41cf-bd81-263f18dfa9aa',
        'f68f4c75-aca5-42aa-b7ac-b9441741478f',
        '1f0f07e5-8335-4f31-940a-a2431eecc21a',
        'b8cb27cb-d285-4270-bc47-cf27cf24c373',
        '30492697-837f-4813-ad2b-17407285a3af',
        '9f282286-c55a-4710-b19a-836f608e1d80',
        'e438d0ab-5347-4a40-9409-b4804ccd4b7f',
        'e8fac340-51c0-480c-aa20-37080e93ce21',
        '96c007a0-22ae-4cbc-8934-fc7a249ab1cb',
        'f272ca12-f8b4-4969-b7e8-100bdbec8563',
        '7058aec0-8b52-49be-bc8c-2106616164f7',
        'ac1762f2-842b-4d21-ada9-c461e2a267fa',
        '703904bb-cf4b-40ea-a651-84f9f2d1b0f8',
        '8295934f-a3fc-4749-a460-5bd7cfc324f5',
        '9120d01e-1aa7-45b6-acb0-74cad717bb70',
        '681b0f76-04cb-4660-a5de-64838c4d60da',
        '4b4710a5-6d43-48e5-8d02-0bba02b760d9',
        'e78880fd-1f5a-48da-a903-2c8bf0cb5292',
        '16337887-46a5-4a50-9ca0-8f477060f17b',
        '128f9537-cc21-4699-a565-471e9b44ec2a',
        '99f05cc6-9a2f-4cd4-82e5-a5e33a4f9c87',
        '84a882e9-a38b-484d-aebd-e0517484b540',
        '3dd480ce-fd76-458e-982f-59d6d0308b7b',
        '7623d005-7497-4191-91f0-d2ed343a80df',
        'ce6bd57c-30c7-4677-b85c-e3e038feef3b',
        'b88d6d15-57fe-4ff6-834e-534a3e6db5ad',
        '1bcdf875-f610-473b-bd60-74572959bc84',
        '18c1604c-8669-4390-a564-fecc00d1723b',
        'fdda0521-a4cd-4c2b-bcde-db6b44a4fd33',
        '8781d85a-32bd-4605-8ae6-a70af5424365',
        'b0cd0590-9b78-4d71-8515-99affb191e74',
        '87d65a51-66e7-4827-9b9f-bc22f37a1ce7',
        'a0fb392a-b134-41b5-be87-74c1f48475e2',
        '65f4c999-24a2-4c8d-80e6-d5d69e5c538c',
        'b1a9e30a-99d7-420a-ba87-0c9d6dc98cb4',
        '9a1875d1-60fb-407e-aaf7-97575ed0c38a',
        '01eeca26-9c67-4960-b5c9-c92cbf9dec69',
        '324f2a55-0f3b-4b85-9fe0-2fd87cf66ff7',
        '1dedd98a-d3c3-4a3f-9249-7b988ffd31e4',
        '3f193572-7445-4cc6-8f34-c7ff84e62c80',
        '83a134e6-bd41-4d9d-8cb9-0347af548dbe',
        '580771d0-b5b9-4698-9a15-24690787514f',
        'c1160c5f-318e-4e05-92e7-4a45ef15e144',
        '0772d5db-e4ae-4869-b0d8-a0a9b11df177',
        '33e336a1-00f1-4f0e-9844-3424d51a412b',
        '5b0d7fea-88ac-4cca-8731-da7565fc692a',
        'e4eaf146-4b4b-4190-908d-5d9d65e72fa5',
        '7b13b405-6ed6-4695-89cf-37c73942e74e',
        '89aaec46-f143-472e-890f-db5845745e7b',
        'c633d62d-f161-4c7c-a4e1-fcdafeeaf71d',
        'a2c285d3-c606-4a9b-bf72-c67d8a0bcf04',
        '76fbab6f-6684-44b2-a644-6d55a1953574',
        'f6564580-87d0-412f-b512-d7d3327b57b9',
        'e777f94e-bcd9-4be5-9d40-e0b85d47669a',
        'b874fee1-28a3-42bb-bcd6-157b40e5ef5e',
        'd6473bcb-f2e6-41b1-b5ab-cc35064b3c1b',
        '32e8efcd-dd00-46eb-bbc1-cae72e3fa77c',
        '3107e573-a53b-480d-9b58-d9414e87db76',
        'fe87134d-12eb-484b-a30e-4a46197c6d22',
        '3479f3d5-1f89-429b-9133-515736f9268e',
        'abef7877-1728-4644-a3f7-b5610510b30f',
        'bc790345-9651-4461-a2a8-3e5d8727fe29',
        '50f577b6-ca13-4e51-9401-1e891d538a79',
        'ab23ce42-0237-4268-96b5-36d261febfc4',
        '3b6f633e-d4c4-496b-99df-22c46b292b7d',
        '375c8541-273a-43a8-b345-92baf2b3f3bb',
        'd7c5ae8c-1595-4341-80bd-d8a746aa76a3',
        'df43e538-eba3-4380-afc3-2e8d12cffc34',
        '6c61a539-adcd-4bbc-90ea-a1c7f52b67af',
        'aa54510e-0a71-4abc-9e8e-43b149582739',
        '642a9ace-382b-4bda-97f6-c6cefab4c760',
        'b6c99b84-8bff-4109-8d81-fc893ff988df',
        '049a1b7d-b820-479e-81cd-e688dedf4ba8',
        '291b10e8-467a-45fd-bb8b-39ae0801ed48',
        'faf0171b-5749-4847-a2d5-6c08214b726e',
        'd98740c3-f544-4b1d-9924-d0878db0fda4',
        'f62b3538-ce6b-44ea-85a8-46732b71517e',
        'a984c869-cbbf-406f-949d-f73d16963236',
        '5f851314-bc38-4a54-a700-bb1e2e6aae00',
        '612bdf2a-2e0f-444a-8cb6-be12dea36230',
        '6e086cbc-c3de-4d3a-98dc-f1a4c2d029cb',
        '1480f40f-c34e-4e2d-a5b5-127bf8f0c799',
        '1e492ea4-52d4-42f7-b9ca-fd2680307218',
        '03f48522-ab98-4dc5-aae5-f4c3e628fd81',
        '7dfef107-a2d5-47a2-a427-448a80e566ae',
        '65c1853a-9d72-4647-8021-683ab1bd87f0',
        '4cd24ee9-897c-457e-9979-1a88c8b39586',
        '5d796ab1-8e01-4a30-aa4f-304fb798e6cd',
        '16b1f6a1-37ca-43fc-bcd5-911152b2689a',
        '2bc18291-0b50-4fe2-9499-128f6eadb618',
        'b7a60e22-35d4-49d6-be09-9f5acbec1291',
        '8c757d38-8db6-4fd4-a493-0295be19d9ab',
        '265f905d-4115-4da3-af30-d11ce00436ef',
        'bc310903-b360-4f27-a391-95d4cbb6c16e',
        'ba7be3d6-e460-4059-8d79-adde9a21a474',
        '965ea5e8-19c3-4028-9feb-cb1e028cdbe0',
        '543d4a17-b660-4408-9c82-9b5e8ed7617f',
        '4018f67b-49f0-4f76-b23c-ecd8240facf2',
        '1793a961-4726-46c5-82ed-23cd0d950952',
        '90589aa9-9f42-41f6-8895-f58f15c65e85',
        '15833d95-af8d-4971-9efe-8ba7fbf1e595',
        '8452c947-360e-4c2f-8e51-73563b218c42',
        '1a47dd4b-bd10-47f2-bd60-3f1a122b392c',
        '60ac5fbd-268d-4b23-a317-7ca124f5d0cd',
        '20bbdfd0-039c-4710-bbe5-de9ddcfcbde3',
        '228480d6-5649-4a27-8286-5695b34914f2',
        '0cb27e92-8e6e-421a-9dad-0b6acf1cf322',
        '472e6df7-4493-4745-b0b3-adf406357f75',
        '15342a6f-5a23-422e-b9a8-42f8735da93e',
        '9ad2559b-b974-4783-85fb-632e192ae45e',
        'b6a737ee-65d3-4292-86dd-cced1b1ee903',
        'ea4873ca-2d79-4bdf-be45-a74b998cb5d9',
        '926d8685-b675-499a-9448-503b92cfc1b2',
        'eeba68a4-f3bc-43aa-8c26-5521f482fb03',
        'a6cee8fe-9b78-4763-874f-eb935ef39902',
        '0658a21f-5b9a-43f1-ab55-5bdf32151a85',
        '128e01c6-53d8-4e5f-abed-986c10c32848',
        '64c9185a-97fb-47ac-87d9-5214ad22542b',
        '82d35871-eeb6-4b8f-b475-8ce6938e19ce',
        '1c224d37-9fa1-4e1a-bb29-244dedcbbf54',
        '117731e8-54c8-4534-8fee-6fa084366b4a',
        'd136e1d4-f6c6-4241-96ea-6ab0788bc09d',
        '1651fe3a-0417-4a28-ba76-d7e8385135b2',
        'e582b4ae-a71a-43d0-9db4-27f9ac75435a',
        'f3836f98-b4dc-4700-aed4-1b454f365258',
        '1055b569-3ab0-4446-8081-9673ed510c3f',
        'c63b474e-7f1d-471d-930d-9989c1f044c7',
        '40bd611f-10ff-4598-814f-fbe0eea2450b',
        '4f0afdce-7389-4c7b-8cb7-73d1cc444ece',
        '2ee56e0a-65b1-4231-9991-9426df85f876',
        '233aa677-c23a-446a-8b11-8e970f3e599e',
        '746a3233-370d-4496-95ba-58eef5a6c9a5',
        '4d39201e-96c1-4655-919e-ce90b3e01c26',
        '5dd2289f-bec8-4ac9-8324-b6eb26ca6367',
        'e0299487-2a52-41a6-b011-22b48b267cf3',
        '9168f8f7-1c29-4a27-854c-3b19a264b9c6',
        '80f02b8f-ffef-419d-b537-80e4a92366cd',
        '8edbab28-ef80-4204-90d0-f820b46a659f',
        '9f54361a-67be-401d-b344-d535bf3956c2',
        '01441561-1630-44f9-938d-9e3e746bf02e',
        '2ba595e4-584b-4ccd-ace9-2514586b01cf',
        '37930a2c-5d89-44a4-a388-f8f69ea4ec8d',
        '73ee1694-308f-4ac3-8d94-0207888d77e1',
        '27d868e7-0ce9-4c41-ae3e-141eda6f05a5',
        '9f42e5dd-c77e-4fc7-8569-31a77dc6fa16',
        '873a2a34-662c-4d7b-b7e8-fbe37a3b4402',
        '2d93444a-5830-48e8-9c4b-38fb7fe30039',
        '8e2e57b0-c695-4d4b-872d-cb36c9b86d73',
        'be4cafd0-53aa-4598-b4de-1696f3c930a7',
        '5fc424e8-a51b-4007-97ee-46a4a9002cbd',
        '3b21fb10-70aa-4217-8971-a5d7688c84dc',
        '707ca80a-3e28-435c-aaca-14055c71ede4',
        'c7747289-4b32-407f-b49f-7381b4f32cd4',
        '3d9b2873-8dc7-47b6-aff7-f5dd090d06c6',
        '4acac565-5b55-4235-ae73-f766398556b4',
        '44710524-a9ea-414a-b92b-a49515b27409',
        'b2089c6e-bb60-43a6-aa4c-a63efe147c06',
        '9080ce5b-2500-48eb-8a2a-fa5dc1403052',
        '97a2f052-8dc8-42f6-85a3-d2aef5811cc4',
        'fc189739-3c2d-4127-b0c9-b26c3b3deb02',
        '6a4225e8-2091-4939-b445-d17c14db1a60',
        '66d69050-fd38-4ac7-8a37-290785c67927',
        '1c480846-086a-4673-b717-68350eac541d',
        '2df357e6-780f-4e7c-a5f4-bf894a52d742',
        '75382865-d1ef-493f-9adb-5ffcccc3a131',
        '144db218-e80d-46fd-b7ed-9e7c13aec809',
        '0bf00b35-6059-4709-a60b-24072137416d',
        'b21be9f6-4622-41ec-ae4e-e3e88395f2db',
        '96e0b35c-e18d-4f5a-9a08-730e7b9bddd4',
        '483b45bb-1281-450e-ab08-bd8ba981e7be',
        '3197fd37-fc31-424e-9119-fbc6a48fc99b',
        'd464faa5-7507-4d9a-841a-45ea69fb8bf2',
        '3db0251a-90d3-4a6c-8831-2cf7b315c196',
        '83553d61-fed9-489d-9ac6-428e97557b3c',
        'f70303a3-f315-4f7f-92c8-fad65de52a27',
        '7f0b05be-7dc8-4ad5-aa9d-0ddf28117747',
        'be3257cb-3c2d-4b92-aa4b-3420522b83f4',
        '67a184a2-9345-4882-84b3-1989857bcc0c',
        'db453b7b-1387-4807-9102-e2b09abc2f8d',
        'd05ce40c-98b5-400a-9ab5-310bd37c058f',
        'cc66f9b3-bac0-44f8-bea7-acb1682aebab',
        'c9bb0c84-f995-4dea-99d6-a91f61e838c8',
        'adc47c25-5b8e-4e1c-8d9f-2ddf49af3c94',
        '0edb1044-b8b3-4018-a49e-344350533b18',
        'db77511b-251b-4c35-9f8e-66587357006c',
        'f4bc105e-454c-4580-b5e1-bfdd8c194f2f',
        'f984ea0c-4e19-4c6a-a4c8-00c3bde754a7',
        '1d544fda-2c4e-470c-85f0-e09ce61af86b',
        'f90dd4bd-380a-4e87-9654-7eb854cc2a45',
        '4f57e221-2d02-464b-bdc5-7252e6ec51f7',
        '6b6ca529-2fb4-4d75-a2df-ae3b5e4d2ba9',
        '52922fd2-5a97-43b3-b46e-3684e2a49a48',
        '93924ccb-166c-4b8d-acfc-c3bc1e9e3820',
        '8932230c-cb06-4b24-8244-7ce2aaf9152a',
        'd476fcc8-4075-4cf2-89da-250bd62d837c',
        '0f1bfe21-9bed-4a93-9cf4-29017b2c4f04',
        '18bfd6a4-bdbd-4bdb-83b4-400a7ef2192c',
        '76721a7c-3ea3-4f9c-8432-1e91e9c78253',
        'aea86c2d-c42b-4906-be50-6e00e72ad7ad',
        '7678269d-e3a3-41e3-b80b-6ca5a91a7f77',
        '18014c0a-113d-4aa9-b8c6-5b8fadba536c',
        '613a5399-fdd0-45dc-8e90-c5cbceaa78fa',
        '062e7496-d2d9-4164-a90e-5991d9802ff6',
        '9b42bded-8eaf-4d50-bafb-1ebc18151873',
        'e7ac7028-0db6-4ae8-a730-f4b6b851eb21',
        '2898e17d-3d47-432a-94f2-6aca5e00db90',
        '8e1a27e2-f452-4e93-80de-fbb9f86eee53',
        'fea127fc-487c-416f-b3bb-6abfa4d15476',
        '23605172-0c99-46f8-b30d-9ab3b76b8fec',
        '7d48103e-4d15-4491-8691-88fcbb55c06d',
        '02423830-d8d2-4e63-86af-e086d93246d8',
        '4257287f-9158-4286-a7e6-e0004c0eea1e',
        '4531c080-2883-43d9-a23c-98d789d57a2d',
        '29a354cb-fb38-4186-a175-bddaa9a20e2e',
        '2f4e2b44-2ca4-4d4a-a64c-d73698417d0d',
        'a8b7b2e9-5218-4696-a787-d83e8d070c80',
        '0c585d78-f96c-4299-8c7a-4464276e0111',
        '5b126a32-6909-498a-9aea-267bd4d8d7dc',
        '860212f7-25d6-4848-90f3-54c68b348837',
        'e0bde1b8-2e96-41c6-ba8c-adeddd992a12',
        '587b2582-b9d1-441e-b9e0-f4671b13abc2',
        '8ae33b7a-3b97-418b-a533-2f53d74b246f',
        '81fbe6a5-be55-4174-9e62-7ce9303a1769',
        '706970a7-cb28-4135-a5e2-b2117e37dda8',
        '9affa173-41e8-417b-840d-a1a8fa5a4690',
        '22d8f188-078c-4d98-bf61-588df173f9e7',
        '1c6b6ff9-b67f-4786-b690-a6696d2e0909',
        'e2397220-4f46-49d8-892e-0f065c2a68df',
        '5ee30b2e-dbf4-402c-9e1e-aa095cb547f7',
        '36e115f5-ed82-41c6-a2b5-a033b37496d5',
        'de99f3f4-b7d9-47d5-9215-777311984421',
        '6a15105b-2468-4258-96f9-8f76dc7e70e3',
        '69a9201d-942d-48e6-ae17-b4ca83695880',
        'a8759e61-1020-4cbb-bd3f-3fffa46e9029',
        '17240baa-547b-40c4-81b4-e9d2b43d7ef5',
        'b9253999-6328-40a4-92c3-9a75ff1804d5',
        'eda5b301-8a54-4ed7-81a1-df9c79a0e8c1',
        '2d5b205a-76fa-4c2a-97b5-c4d83fcd6b57',
        '5ca6588b-84ea-4400-bd99-ba6c983d6550',
        'f4b67c72-427c-4645-984f-d30993d91cfd',
        '2c5bc503-9dd8-448e-b7d0-112e53721e6d',
        '18479fa5-1208-48e9-b818-26c4e7d52f6a',
        '69c1be75-6390-43e3-be73-264de6ddafac',
        '811bc5c6-f2b5-46b5-af04-14eee8585570',
        '731e8145-da08-4a8e-a2ed-85b4218eb253',
        '5706dffd-4e98-4708-8dce-1b6b25a272e0',
        '644f28f9-7d98-4094-8754-cac71f63d0ac',
        '547f03f2-0045-4b09-91ba-f87246a93183',
        'fbca7c8d-e42d-40b5-b169-3143f19016b5',
        'd5cb2abe-b5c0-4cc1-b1e1-3ecf9a8f63ab',
        '2486d6bd-addf-46a9-af29-6fc2dec94f5b',
        'f884cbf7-3603-4840-a7d0-26ff21a6ac31',
        'bbd1f148-c82a-4b8f-a2f7-75c7de96963a',
        '064f2a01-4620-4214-9646-586ebc0a47e1',
        'a8b38d1c-8567-4c34-a52d-c60a26429465',
        '2598a748-74e4-48d0-b5f0-1f84d0c25627',
        'd350a117-4d32-45fd-bddd-e35f6ed8ea3c',
        'a896dd20-11ab-427a-a0b3-9164b5111352',
        'db09d6d0-22d4-454f-93ad-3245ff9ad787',
        'fc11430c-820a-4c8f-8c27-226079f30ef1',
        'c2de300c-a18e-4f25-b40a-fa22605e8429',
        '3533bbc6-36aa-4a39-8712-5ec1bc6f5ffc',
        '0ed5db7f-b63d-457f-99ac-fa51d86210ef',
        '6a19a650-fe9d-4977-99a2-329dd6bf0d00',
        'b3fc02e4-f5c3-4ecd-9045-4e3d10130437',
        '85eff935-782f-455e-bbce-9666396830d5',
        '357d0c76-9cec-4cbf-8e59-898b42fca14b',
        '5d6cd0fb-099d-407a-b026-5e003cddd787',
        'f85c6a21-d26d-49bf-86c5-ea4d2448b842',
        'effc6d8b-1a92-496a-8eb6-f2a2a10e17e9',
        '6e628cd6-9028-40ae-aff4-efc5534148da',
        '8f5918e5-f2ba-495b-b24c-9861dd43832f',
        'f59eb7e2-c7af-40c9-9e63-fae1049f2c3d',
        '9069bc9a-0185-41eb-8f64-6c00e56f342f',
        'f60395d7-f510-4726-a045-05a0566571f3',
        '058e33ff-c82b-4f00-b58d-169585d1f1f7',
        '103d03ae-9f44-41f2-a0a3-798b381e8ad5',
        '4534fcde-b16d-4b62-bac9-7bd452c166cc',
        'fc259431-d096-4ae6-bde7-690ca97b9fa9',
        'dffd6ef7-24fe-4bbf-8c12-2005cf705607',
        '9e6677f9-46ce-4d92-85ad-a474b311b49a',
        '377634c5-2a4a-4378-ac4f-7ef4ef88fad3',
        'b9294a6a-3450-4376-9404-43690d8f19af',
        'a29692e7-af3a-471b-b174-d7dc87bba63b',
        '28bb7e1c-8f18-46d4-bf89-1ef2a7360d19',
        'f5966559-a928-4e7d-8ef5-958a30f54cd4',
        '78f86a9a-af19-4040-a416-86ef116b761b',
        '10518d32-ec03-4ab4-a227-372303f6fd85',
        'fd8aeb95-f529-45da-b990-9e443f618a08',
        '78f9b9e7-620d-4aca-bb3c-6e10a2cb5794',
        'f999e2a0-4735-48cf-a8ad-95909e42cc99',
        '8d31a5f4-5663-4ec0-9990-bc918af31cc2',
        '6078948b-b5fa-4c9d-bbe5-bff1d91fee85',
        '618ab667-455f-4d28-bc96-f042bd413682',
        'a9335c0d-c6d2-4a5b-a2af-031961f718c3',
        'b249e8fe-bfdd-45e0-94eb-35dd0470dbd3',
        '511e964c-01a7-4cd7-bbff-86476b5423ec',
        '67dcbf28-2370-4f52-979a-430dc9f40485',
        '10e5cc01-2f59-4650-b1a4-ac2a29009107',
        'f3d56c4a-050c-4b6d-b617-0f50dabe19f2',
        '75bad6f3-3b5b-4bce-b1ac-88899ed4081c',
        '10c78f24-1442-4c6d-bdd7-4637c446a03e',
        '609d5147-13d5-4080-8481-4b9a87edde6f',
        '06ffcfba-b33f-46c9-90df-a64b1c1f09cc',
        '3b75c978-bc0c-4e5d-9248-2079a4d241f4',
        'ba9b59c0-53ba-497e-b5ac-a09d92ad96e4',
        'beaf834a-bfaa-47fb-a03f-f5e0e89fad24',
        'd98b22bc-5c78-402f-bd80-dcd20b20f4aa',
        '1509d2bf-62a3-49f6-95c0-bb5b8ffad8e8',
        'e9f714a6-deca-4805-b4e1-e498766de295',
        'bbe13585-0538-43be-a566-c1b686bcca1e',
        '88f2f611-bfc9-4adb-8557-c81c849943de',
        'cad4b758-4138-4ac8-a809-ea4d477d648e',
        '9bb06ec7-11f5-426e-b49e-5553adfa20c5',
        '3bb20352-a886-43a7-9193-d3b075be8e95',
        '646f89c8-7da8-42d2-94cf-9269f8574aaf',
        '313f0c86-095c-464f-bf5a-901781736544',
        '8cbe07c0-4a96-4e78-9284-033524d799d6',
        '0b16a419-8ea6-4f82-b087-6d1db71a0cec',
        '51f9c5ec-e2d6-4dc9-b145-ef2cb17ff42f',
        '3f9089ea-6314-49c9-a815-92dfad785240',
        '74fc33ad-a507-459a-bc5e-b852668f93f8',
        'ed3ac952-ebd7-4f6d-ad27-ebe55f2513c2',
        '88916cd3-5940-44d1-9564-5258534236da',
        '9e35cf1a-55c3-44fa-8298-35ca9bd8883a',
        '835c5ef6-17bf-477a-b352-31304b71f724',
        'fcee8444-2421-4097-af47-a51a35d21406',
        'd704c4d3-bfee-4e35-9355-b2d9a3eea89c',
        'e5ba29b6-18a9-49a3-948f-14a13b3b5a5c',
        '8f57ad39-196c-4f1e-a4b7-791664091444',
        'af2fec13-1467-46c5-804c-1d7187e1b142',
        'e81fc227-2628-4dac-a42d-71dca959467c',
        '2ba64880-f489-41c4-925b-3e025b5307c7',
        'c5434620-eeff-41e9-9e3c-fff69f1d7eeb',
        '65d6758e-4a6a-4881-b111-7674e5b7a5d6',
        '2b207418-74c4-45cf-aa09-068115d09f7a',
        'd213aa5c-74eb-456b-96e0-83f2f3e2d391',
        'db8f07a3-df0e-4ebe-801a-f3ac867e5769',
        '712e653c-f044-4e9d-ab01-5230dc5230d6',
        'bb6401af-6a8d-4431-8cce-aacac1ebbf18',
        '3230d72c-28a9-4b84-9b03-f3aeef68466a',
        'e94a4c83-9490-4151-96ec-cf9c0ee5c9f2',
        'df86ea6c-276c-4ca2-ba12-d0ddb8e9a147',
        '7a7b4585-4d3e-44a9-9e50-023a2afb2ac2',
        '11f54c0b-a88c-48fa-83c7-52c95ce3c8ff',
        '55530cce-e3a4-4b10-97db-d2ba6c9251a4',
        '16b608c2-a4a6-45ff-b7a9-952fb5773710',
        '205f6889-ed9d-4b5c-8b73-cf980174326d',
        '10f4ce11-28dd-48ac-a69a-4bbbe37b8707',
        'd587ce47-ca27-4e34-b74b-4ecacc95e2e5',
        '2c71b7ca-5102-4388-bae9-fcc66776cfb9',
        '9fe24ac7-920d-4546-a79e-44231e715780',
        'f769e601-75c1-49f0-b232-634f12e740dd',
        'ed6fa3d6-d615-43e5-892f-d5305e7ccc1d',
        'f938d5d1-3e2d-4a1a-96c8-ce4595476c28',
        '78c1e80c-b297-46b8-8aab-f45de4fcc079',
        '29a81eca-a310-47e3-8309-3c14527fc99a',
        'a5194f7b-b77d-4e1b-8d6e-c9bf1fbce2de',
        '87e38428-34f7-4c2c-ae30-0e2ffea17ae2',
        '7bcb09ed-12d7-4fd9-8f81-42e96489f750',
        '312a2e48-e647-4216-ae30-df5a00945def',
        '4d8daa8d-d34e-4bea-acf0-aadaf8865479',
        '16a0fa6e-ab50-48ff-aeec-15c1278ee29e',
        '21f45853-056e-4651-a3e7-f752a129ed65',
        '8fde4b03-2919-4596-ab6e-96898ef9b191',
        '0433787a-caca-4541-bf4d-b9c00dcd5b02',
        '0962b5f4-345c-4b7b-aa60-851b379c81eb',
        '4645d645-46b8-40da-8743-b60eea969f02',
        '266f35b2-09ca-461e-b0ed-96fefafbd3fc',
        '1f266884-44f9-4af9-b0fc-943ddf9a13d4',
        '1d7d1a96-3951-41d7-a7db-a6cdcb055065',
        '0955146f-96e5-4015-a349-33e0695849ed',
        '053f937f-6760-416c-9043-4397e96599a4',
        '36536ea2-a6c1-4c52-b9fc-8168fc0c385d',
        '2fe10dac-8bda-40ba-91eb-ea86016a6c6b',
        '10314089-10b2-416c-8db4-6c255f12492e',
        '3ee63d70-0696-4513-8307-957131460c3d',
        '50c99053-4504-4d05-af28-34a464706633',
        '26175b23-f786-4564-a9a4-e8810ce231d9',

    ]
    sessions = ['508e23d5-f7c4-41a3-a5fd-9c0b123f82a3']
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

