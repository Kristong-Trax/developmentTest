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
    # ru = ResultUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.5.xlsx')
    # eu = EntityUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.5.xlsx')
    # af = AtomicFarse(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.5.xlsx')
    # asdfas

    'missing'
    sessions = ['86e142d5-31a5-469b-bd89-104f88b041b3',
                '064f2a01-4620-4214-9646-586ebc0a47e1',
                '312a2e48-e647-4216-ae30-df5a00945def',
                'b8fcc7ac-da35-47e8-9ea5-383a2afbedb5',
                'a46fce27-f3b9-4d2a-ae03-fc0f03da6d30',
                '39564a00-5e62-4e1e-88dc-faf06f0b4bab',
                '62657be6-d37f-459c-8e97-177c8b564a82',
                'bb3317e0-49cc-4dcd-99d8-12772df0ee9c',
                '0caca320-7992-4798-93b3-0bf2ef071f4d',
                '12a65226-996a-41ea-a64a-900f0b36e3d5',
                '9951a242-3699-4346-83ee-969131c9e4bf',
                'fbca7c8d-e42d-40b5-b169-3143f19016b5',
                '08ded8d0-6068-4df8-9c05-4e151ea764a9',
                '4d39201e-96c1-4655-919e-ce90b3e01c26',
                '14cbc2c6-2a40-491f-bd5e-1f11d8f7a065',
                '37930a2c-5d89-44a4-a388-f8f69ea4ec8d',
                '9dda5ad2-ba22-493e-945f-b79386e10a39',
                '28d8bab5-affb-4812-9ed4-67c504d7356f',
                'ed997d7b-4ea0-44ee-b518-55e70c6029fc',
                'f8e1125c-8cca-4df0-a4c3-3ec256696c84',
                '128e01c6-53d8-4e5f-abed-986c10c32848',
                '5b44d5da-f21e-4e41-9397-45460fc77f40',
                '83e857ea-d233-4516-8e2b-a857be394b2e',
                'c7747289-4b32-407f-b49f-7381b4f32cd4',
                'b2070a87-6ebc-468c-a0e6-0f9c234d7a03',
                '9ec4eb70-a2f0-45ea-a890-c1c3d85d677e',
                '0fd121d8-6209-4837-af4c-f81c04b1d5ce',
                'cf93dc60-2b4b-4b35-bbba-ef424bc69555',
                'a5009945-eca4-4b64-b498-39137f785b9e',
                '0ed5db7f-b63d-457f-99ac-fa51d86210ef',
                'bb58a5e6-9913-489a-b640-32cd113c13f9',
                'bbd1f148-c82a-4b8f-a2f7-75c7de96963a',
                '7d0852f7-35bd-4337-8141-fcd77caab793',
                '5706dffd-4e98-4708-8dce-1b6b25a272e0',
                'd3cc1306-5b40-4303-b9dd-a3fe804832e6',
                'c712233b-0cd4-4b56-b989-f4d8b5f22cb6',
                '56e84270-d30b-46e1-a99f-b110c0c6335e',
                '6bce8b12-095a-43c2-bd97-34ff010f8acd',
                '2090174e-8534-48bc-a453-2016915459c6',
                '949f5c5d-d9f0-44d2-972a-9626e9a388ac',
                '61f23d03-5aac-4239-ab84-45205b5dc07d',
                'a7781a11-a8cc-4c32-ab22-caf607e6b5cb',
                '2dfd94d2-baca-4154-8c56-921e175b07fe',
                '32781e98-23a0-49cc-977c-e4d0944d6571',
                '1a2e98a7-b839-4684-9080-693f414fab1f',
                '263a3c66-7693-4d7b-957a-e84ad6067ba2',
                '5b178ee8-740e-4f36-bb79-8e61a4b4b4da',
                '5138656a-06bf-4fcf-af44-19a29f4806d7',
                'a5194f7b-b77d-4e1b-8d6e-c9bf1fbce2de',
                '02cdaaef-b08a-4282-ba6c-f708d019affe',
                'd282a3ea-f6c4-4183-a594-ab92131723e9',
                '96dc831d-7eee-4bb6-a107-d42460546085',
                '9919cb3c-3f9d-421c-ae49-40051db6bb57',
                '59d49bb2-4c3f-47f3-ada4-628ad5be3241',
                '6d43e81a-ffd6-4169-87b0-7e585e292ea7',
                '357d0c76-9cec-4cbf-8e59-898b42fca14b',
                'ea875c1a-adcb-42a0-9aad-85f9c20b642f',
                'f9bf2f8c-6ff9-489a-aace-08cab89f8646',
                '99f67d1a-db46-4e75-8b5f-c823cfc6c0bb',
                '9739619e-b151-4ab0-ac35-94c9d7da77f2',
                '746232ec-e18e-43a2-8983-1c04b750969b',
                '8eb24e39-62af-42db-80d5-f81cae340bfc',
                'c35abd74-eec7-48a4-bc1d-2273b705bb12',
                '51d01741-96fd-4b92-838b-7b74b0a1b07b',
                '490fe16b-5c3d-4b2f-a692-31ac51654701',
                '4f695fc6-4286-4852-80c6-0cf15f3e093d',
                '88f2f611-bfc9-4adb-8557-c81c849943de',
                '117731e8-54c8-4534-8fee-6fa084366b4a',
                '438fe17b-2f42-4d70-b4d4-6d45fce68436',
                '46d3385f-2def-464a-8579-e83daff12b82',
                'dec0aed1-8bfe-4b66-9ecb-4fad11eaa27a',
                '265977ea-8526-48a5-b24e-ed903f4b1207',
                '0bf00b35-6059-4709-a60b-24072137416d',
                '16a0fa6e-ab50-48ff-aeec-15c1278ee29e',
                'bab36894-39c0-4a5c-b7aa-c5cb498fb0e9',
                'bd2a4c9e-b223-4d80-8521-0b1e037bed88',
                '74c73830-7225-4e8f-a8b6-8d2f91c3b3bd',
                '965ea5e8-19c3-4028-9feb-cb1e028cdbe0',
                '852142e1-af2b-4b41-8701-a15f91bd0aa6',
                '208d1a5e-4b19-423b-b850-f085619d0f1d',
                'c459de75-b66e-48a5-9d9a-28f4a3345806',
                '644f28f9-7d98-4094-8754-cac71f63d0ac',
                '205f6889-ed9d-4b5c-8b73-cf980174326d',
                'a8b38d1c-8567-4c34-a52d-c60a26429465',
                'bd790a22-1807-44f9-b526-c4d9dc635058',
                '914ade25-c11c-4fff-92e5-cd6d3263a25c',
                '2c78d411-9c8c-400b-8a68-3edfc7a076c5',
                'bf9b7504-ceb8-4df4-9309-5ba18c5f68f7',
                'dc1e48d5-cd39-47e3-930c-1c49b47044f4',
                'ada5d070-311a-4eb2-a3e5-82dd25f04dac',
                # 'cd6d383d-c181-4f4a-b91e-f805f0fd4fb4',
                # 'faf0171b-5749-4847-a2d5-6c08214b726e',
                # '75bf493d-22df-46db-9871-35bd0b9dd406',
                # 'c7132ac7-eb55-4e94-9f95-70f8beaa7fd4',
                # 'd8ffa968-2b88-46ad-b4c7-cb756347aefb',
                # 'f221bd5a-53db-4fe7-968c-5b5aadeab8d9',
                # '06ccb59d-94c8-4103-80b5-4c3bd748dac0',
                # '39b05050-b830-41dc-a01c-0857a5d8fbc8',
                # '58db8913-e7ac-475e-a294-4e083764ab71',
                # '6cef1494-1ae5-4153-b512-44ed52a1ba54',
                # '3b072b3f-a395-4f94-8928-412b3371c357',
                # '1866639d-40ae-415a-b748-2da75c60773c',
                # '5d261680-6a8c-4eda-9c44-fde4d33a43d0',
                # '2cf47380-7de2-4d16-aa68-fb85e209fd20',
                # '3508bba0-7352-43b3-9e7b-cd93005f78dd',
                # 'fdde08dd-86cc-476d-af68-4aa6b9eb94f7',
                # '02423830-d8d2-4e63-86af-e086d93246d8',
                # '99e88f3b-5495-447f-92aa-655fc181284a',
                # 'a0af100e-adb0-46a8-b779-481f4663c52b',
                # '96bc9e20-cd5f-46f1-988a-06c984a1d6b3',
                # '9b50b0ff-8d85-4ace-8f37-4914c76c8ae3',
                # 'b37a3715-0113-4d7f-930e-96cec922bcbb',
                # 'a650886d-c19c-4705-a087-e4d973b1da8b',
                # '207225da-fcd6-4768-9242-bd81ced81a9d',
                # 'bd34e27a-9b94-4000-9fbc-6df0c9048483',
                # '74c60bb5-f04e-414f-be39-4ee07e7c75e0',
                # '9f4726da-df48-4f78-936a-fac4cc1653bd',
                # 'd2ab52c2-42ca-46ec-9ea0-b1ff8c1e0590',
                # '2cf5ee49-ba96-471d-ac9d-a2c802ca8710',
                # '68b06e0b-5588-47c0-b60e-5525aff88ece',
                # '0707be16-db73-44e1-9d78-64db00f5a5f7',
                # '18bfd6a4-bdbd-4bdb-83b4-400a7ef2192c',
                # '69a9201d-942d-48e6-ae17-b4ca83695880',
                # 'ba76d4e6-8d6b-41ae-b716-791acf9fba1d',
                # 'c4805fdb-d56e-463c-9fd8-e8459adb34ad',
                # '81d6c45f-46a0-43ae-9289-fd06c4e16dc2',
                # '08ebe49b-1d72-47c1-9f95-fe021a04c638',
                # '7d704edd-799f-43cf-b8f5-b7375778e571',
                # '0d457c56-593c-430b-8d63-b22ea3f555fd',
                # '5da9f8a1-1998-4545-a0c8-3a5f238582b4',
                # '649efc54-36e4-4448-bfbf-5324e526720e',
                # '38ec4a7e-b91b-4ea1-bdf2-087959ad7d38',
                # 'cbdc96a1-f35f-451d-bc6f-e8d3874fdd36',
                # '853618a6-bd56-49d9-bfb2-142cf1d14cf4',
                # 'fb148323-5757-4c52-ae23-b7f4b53f90e9',
                # '3107e573-a53b-480d-9b58-d9414e87db76',
                # 'ce8a9761-bdd3-4e7a-bf4a-af9d8a1cf43f',
                # 'a288c82f-ce44-494d-829e-7861733678cf',
                # '0afecbea-7e41-4df4-aa94-e26d392de292',
                # 'd587ce47-ca27-4e34-b74b-4ecacc95e2e5',
                # '7d48103e-4d15-4491-8691-88fcbb55c06d',
                # '2d970fde-ef22-4e46-9800-6f4075570d51',
                # '855b11b7-8691-4be7-99c3-4a4afd1fbeba',
                # '200feae7-5a71-466f-9400-4fdb9d1e412a',
                # '6c027ddb-fa12-40ae-81c2-ff67e59f0c48',
                # 'ad051f0d-7797-456d-8a92-1220b84a8093',
                # 'e3db2a00-d268-42b5-8114-4b0d492a5d6b',
                # 'd0278116-d109-4c90-9a65-f256564b09a8',
                # 'ccf2f6a3-b772-449f-bd94-1c918919a9f3',
                # '0388e279-8f05-47d1-a685-71da71bf8452',
                # 'e9ece002-8977-4b19-a42b-c5f06c82009b',
                # '9db4adb7-9218-4d95-bb3d-dfdf0bf9e23d',
                # 'f4b0b683-93ad-4030-8346-fe23ee870a5f',
                # '8c25d204-1be8-4c56-b0bc-8422442f024b',
                # 'afbf2b01-673a-4572-9be6-5bf0a60922bd',
                # 'f3836f98-b4dc-4700-aed4-1b454f365258',
                # 'cc13d23a-4aca-43be-995f-912fe345d1b3',
                # '62d0f1fb-fd0b-44f3-bcd2-e015fbe70c90',
                # '952ab0b0-590f-404c-a33e-c5b01a94ad55',
                # 'bf1049ac-52cf-4fcf-851f-4a69d865f10b',
                # '8e2e57b0-c695-4d4b-872d-cb36c9b86d73',
                # '50af27a8-9cc9-475e-86b1-8f447c36ace6',
                # '5c5877d6-3ebd-4e0f-94a3-fbeb4adf338c',
                # 'c63b474e-7f1d-471d-930d-9989c1f044c7',
                # '1651fe3a-0417-4a28-ba76-d7e8385135b2',
                # 'aa3620c9-5bf3-434d-962f-3545b21b46fc',
                # '87e38428-34f7-4c2c-ae30-0e2ffea17ae2',
                # '854c042c-f824-4bba-82b6-f30c04f40f9b',
                # '78c1e80c-b297-46b8-8aab-f45de4fcc079',
                # '6f37cbf3-de44-47b4-afcf-485ddd4be30d',
                # 'e5e62284-f4ef-469f-b36c-207282714339',
                # '39edefe4-160e-414c-9b8c-417832787473',
                # '7bc066ef-3d71-4bdf-942b-d31f770cfe2f',
                # '745b9d43-30ed-43fd-8b6e-7c6cab88dc62',
                # '706970a7-cb28-4135-a5e2-b2117e37dda8',
                # '493ab3ae-05f8-4e57-bd61-e2f19d085f12',
                # '82f71846-8c5f-406b-bf64-11c17590b366',
                # '2cd37654-6bae-4931-acb2-62aa28bffb2e',
                # '22828dc3-6ded-4839-9544-2b205aa67175',
                # '4d58d4cd-d6be-44c3-8e8f-635ddaefdf3c',
                # '6b090f81-0fde-4336-8858-d606913443a2',
                # '03b9fb23-49c4-46df-b008-56763fdef047',
                # '29a81eca-a310-47e3-8309-3c14527fc99a',
                # 'a309c6fc-fc54-41be-a586-4b06a1beed26',
                # '75e50726-5720-4baf-8f91-f3da92bf94ce',
                # '85017304-95ea-4c24-9f17-ad3db01084fe',
                # '7a74b128-83f4-4a79-972c-76893ff0a872',
                # '24e5a004-1842-428b-b9fc-fdc23bb4ed0d',
                # 'ed6fa3d6-d615-43e5-892f-d5305e7ccc1d',
                # '2824d60c-fd15-4851-935f-74d9512a1fc8',
                # '538d5fb7-4aaf-4bcc-bf30-e46e2e29936f',
                # 'c2de300c-a18e-4f25-b40a-fa22605e8429',
                # '55f0985c-67ba-4407-8cbb-1f39df2ff4f9',
                # 'f8c42204-ab9d-4764-aae0-9ac653463a4c',
                # '9fe24ac7-920d-4546-a79e-44231e715780',
                # '822a5335-bbdb-4bd6-91a9-f3fcfc48830a',
                # 'bbc411e8-963f-48ae-9c9b-28f033bdbbb0',
                # '351cf170-2959-4b22-8d56-116e16159a10',
                # '83951e03-891b-4d5c-b975-5da4c4c4ba01',
                # 'b786ae93-a413-4cc6-bf01-cf26b6100ab1',
                # 'a6a259af-d1c1-465c-b2f2-f64f2a4c1aba',
                # 'b6a737ee-65d3-4292-86dd-cced1b1ee903',
                # '4edbeb96-fd9f-4db0-88a6-edd85cabc0a3',
                # 'ba15c9b4-035e-4709-8267-9bf0d4f546d0',
                # 'b46b8a00-d8ed-4376-9bab-c900e7f89372',
                # '0f2640a1-ac4d-4af9-8727-f44b0892d06e',
                # 'eb8d5a8c-3dc6-4cfd-97cb-f4e7a9fff981',
                # '29f2be8a-8f30-43e3-9f6f-9cf51c5a085b',
                # 'd024456a-e48b-4cbf-b2c7-0c8598e3b8c4',
                # 'dd60db39-66f0-4f58-8d35-31bb1b09621a',
                # 'b8505ba5-2ed0-4cc9-b692-311e068ff336',
                # '41b5b99f-47d2-40a7-bf67-73a93a916d2a',
                # 'b4f96bba-5b9c-498c-9269-857f02de415d',
                # '3c2f0135-839d-44f2-ba55-e7216b999446',
                # '64c9185a-97fb-47ac-87d9-5214ad22542b',
                # '3d1a5edf-6bd6-4972-b18c-97a9e2c16c85',
                # '0cf105e8-4be5-48f4-ba68-134a968d40d4',
                # '73206e0e-a08b-485d-b317-ac8364f25a1e',
                # 'b53af44d-7a90-4d12-b7f9-a6f91ce41835',
                # 'f27131d0-de54-4799-ad6e-366aa9c2a627',
                # '4fd80d40-bd1a-4f4d-8f5e-f4cb6fd718f2',
                # '96e0b35c-e18d-4f5a-9a08-730e7b9bddd4',
                # '1ce7e9be-31ca-44cb-b8cb-c82c936444be',
                # '15a8c48b-1be0-4dd3-9e4f-b0ae14509207',
                # '2c555af2-9f7e-4e64-8197-afaf67a13c94',
                # '2e877460-fc11-466f-af5b-a9748d086615',
                # '82769d60-85ca-44e7-b692-e541dae70e73',
                # '909e75ae-0b4b-4ad5-9d68-edfaf693f0b0',
                # '52922fd2-5a97-43b3-b46e-3684e2a49a48',
                # '4450dd54-0dda-455d-b104-a243ee76c27f',
                # '02aa5dd4-0f71-4b75-92a4-06e88736f79e',
                # '3c2b2d5e-feb0-49da-8fb1-377b29c48629',
                # 'e4600660-55b3-42c0-9d88-3529cc38bfe6',
                # '94ae05dc-7f21-4430-8869-b05f6e98b712',
                # 'a29b517b-83fc-4bba-a791-4823ab5de127',
                # '0cf3a13d-0089-4d68-914c-7e2ca69ef252',
                # '38caa344-325a-493c-bbe9-df606b226b2c',
                # '29a354cb-fb38-4186-a175-bddaa9a20e2e',
                # '256cd677-cfd6-4ce9-a566-a1b7c873f1ef',
                # 'bbe13585-0538-43be-a566-c1b686bcca1e',
                # '1055b569-3ab0-4446-8081-9673ed510c3f',
                # '82d35871-eeb6-4b8f-b475-8ce6938e19ce',
                # '319a1c5b-56d7-4ab8-a3a6-ab5b8a22404b',
                # 'be4cafd0-53aa-4598-b4de-1696f3c930a7',
                # 'b666f5aa-8f89-4d97-a779-415b3853738a',
                # '546af823-1f3f-46af-a4c9-38c5c940e7d6',
                # '331cd4ed-6902-4427-b5e5-05e5b6825465',
                # 'a589012d-7404-4ae2-9fe1-08ffb2416484',
                # 'aae775b1-828c-4cb1-b8a3-40b92be0a4a9',
                # 'ff2bf608-2f63-4f4e-acfa-afa133fe4802',
                # 'b1f54d50-5ec4-47ec-b821-9af5db3d94ff',
                # 'bb8918d3-f201-4cfb-975a-b55275300992',
                # 'e582b4ae-a71a-43d0-9db4-27f9ac75435a',
                # 'af2c8de2-6bfc-4e9d-a59c-2b4656114019',
                # '13149b55-2344-497c-aa28-f4776f413113',
                # '8e6dc1c7-e6cc-48c0-9ffe-9fff5dd8bed8',
                # '5543b174-d787-4cd6-94a9-d6a8a6552766',
                # '8f507829-9d6a-45d2-9c58-2e849ecd2324',
                # '292cf8fb-dbab-4855-9895-fca5a9a0e5aa',
                # 'baa4dd84-8ef9-409f-8e1d-e04b1d927198',
                # '81afb3ee-7986-4008-af5a-dfd651d0ce4b',
                # 'f5554af5-2676-45c5-9e00-03101daec19d',
                # 'ea4873ca-2d79-4bdf-be45-a74b998cb5d9',
                # 'fe268fec-bb62-4d2f-a904-b4fd05a0f3ec',
                # 'e6f28133-29b7-4cef-b3a2-c83e4f64a00d',
                # '8fc91383-ef78-4d41-879d-b9923d8ddc68',
                # 'f984ea0c-4e19-4c6a-a4c8-00c3bde754a7',
                # '0d594c7a-58e6-48c7-93cb-2a1ac03e0f10',
                # '412660d2-33f5-45a6-9f66-bd0e49154c92',
                # 'db57d257-9fba-40d5-9bde-5c94ecd9eb71',
                # 'ec5b271c-58aa-4f36-abc6-7dc0e0226247',
                # '912048b0-96f0-4988-8f01-e3898e946579',
                # '2e17221f-3791-458a-8c57-420cc9e7f8f9',
                # 'cda7229f-8a7e-4957-9fca-f78560407947',
                # '4f4078de-733c-4217-84a3-9d3e191124b5',
                # '926d8685-b675-499a-9448-503b92cfc1b2',
                # 'eda5b301-8a54-4ed7-81a1-df9c79a0e8c1',
                # 'fce2d513-31e2-4887-a291-b30314f971cc',
                # 'f62873fd-b903-490a-8b3c-8257783d98db',
                # 'de013f14-c06f-4829-b805-c1479706f660',
                # '8a010e99-baa6-4219-8e8d-4ed7127cad5d',
                # '02dcd7bb-2e23-4735-b81c-822f4c20fd17',
                # '908f8f14-ff72-4d58-ad56-6a643587bf12',
                # '48bd936a-b2ee-4362-8b46-8f2406d43f65',
                # '643a79f8-74b8-4844-8917-da7f6f265d9f',
                # 'e459035e-ba18-4ce5-987f-87e8fa439b4c',
                # '347bf308-f5d1-4bd4-90a3-8c9c92a9ae40',
                # '2414a3ed-6c1a-4e9a-a447-28f93b81b359',
                # 'beaf834a-bfaa-47fb-a03f-f5e0e89fad24',
                # 'b11feda5-ee91-4a84-8626-0975280302c3',
                # '8603e1b3-0c45-46ee-9c28-75cb4c5529e4',
                # 'f6f4e95f-41c5-4a9d-842e-e10f13c6f8c3',
                # 'd704c4d3-bfee-4e35-9355-b2d9a3eea89c',
                # '184037ac-cbd1-49cd-91e6-60394a02718e',
                # '48027098-ad17-49e7-a1bf-a2b3d0a159e1',
                # 'f1d9d755-9654-4ddc-bfcc-4292ad2f1b0d',
                # 'f938d5d1-3e2d-4a1a-96c8-ce4595476c28',
                # '36e115f5-ed82-41c6-a2b5-a033b37496d5',
                # 'e2397220-4f46-49d8-892e-0f065c2a68df',
                # 'f49cb81b-0be7-4c2c-94b7-d2cb7a8a1938',
                # 'a9f97d58-4049-44d7-a5d2-e9cd588040eb',
                # 'b02f544c-5751-4ead-a903-4d00193486fd',
                # '6f76a983-a2e8-4a5a-a2dd-451589968a78',
                # '6dc78402-e2ba-4a5b-9d52-587809e78d88',
                # 'd1e5170f-ea05-4819-b072-612b497ee76e',
                # '3eb6c9ac-de5f-408d-a6e2-ec297397d4a1',
                # '1c6b6ff9-b67f-4786-b690-a6696d2e0909',
                # '13f45260-9f9b-44a8-8f4e-8654691bbce0',
                # 'd91fa309-631e-4e5b-b469-6cbf4f9bebce',
                # '4cb1ae40-a230-4483-b69c-446313f2a0ae',
                # '539531bb-24f1-408d-8bb5-5cb76dc379e2',
                # '6eb4ba59-164b-4362-9750-20926ed72311',
                # 'd2243ca8-fc84-4abe-9701-a66bebcb0388',
                # 'd36b7233-5b86-477b-b71c-a719dcc3d528',
                # '5cbba4bd-8df7-4c5a-bbb4-25e0a0c7ab5b',
                # 'ffa5201d-2793-4bc2-b088-bb6932a49381',
                # 'caab3836-8e31-44a2-9e13-f14dddfd94d8',
                # '0e1ec5f7-a289-4a7e-9795-ba7c356df699',
                # 'b3dba5a1-e7c3-4205-9eaf-1b217229011d',
                # 'a1207732-e71b-4923-9bd4-f297eee26bec',
                # '646f89c8-7da8-42d2-94cf-9269f8574aaf',
                # 'd412ba58-9ec4-4a59-b794-edc862b70e8a',
                # 'f91009e1-ad0c-4a07-a7f3-6a1e5f3701bb',
                # '8f8c18a7-2fc7-4928-ace8-a50dbe5254ff',
                # '513ad045-3af0-46ba-9755-9e8a5541d078',
                # 'e7612ccd-59c2-434e-9eea-c92bcdeb7e24',
                # 'a16b6553-5afc-48b9-81aa-da27944dbcc5',
                # 'efdb4e76-e62c-4a01-9c27-641ef6e49ddc',
                # 'b8d5313a-a852-4812-baec-a0ed85b7929d',
                # '70089f22-7d7a-4762-8f2b-ea9796239759',
                # '34f1c0f9-14c3-45da-b9d3-6932560dc00d',
                # '99d95c71-8831-404d-96b7-b7661ec75291',
                # '49156f05-e382-47fc-b7ee-2a87c8352201',
                # 'e1de0713-9f16-478a-a587-28dc6a2c116d',
                # 'a271c4cb-3931-437f-9af8-99739fee4770',
                # '968c4cf5-a48a-420f-a9a1-106c18f13ea7',
                # '7b94e3c6-dcbf-4380-9a61-e20c84109623',
                # '4f47b782-c2b2-43fa-b810-000e0cdf2657',
                # '98ee9285-a67a-4e14-aeca-3d7edc34ecc3',
                # '4a013dce-e096-495d-9282-9311163e5dca',
                # 'eeba68a4-f3bc-43aa-8c26-5521f482fb03',
                # 'ff0839aa-7ea4-4277-9ecd-e7b09d4587f0',
                # '5e922ba3-ec75-4736-aa49-2bbb944c8a44',
                # '337973e5-cb3b-4025-a725-d7bcfecd5536',
                # '10f4ce11-28dd-48ac-a69a-4bbbe37b8707',
                # 'e22c5f35-f6d1-48c2-877a-ce6254e876fe',
                # '80051428-8c7c-49b3-b057-b7f490f4a3df',
                # '695d0c27-a748-497a-9ef4-61fddf9d7ddd',
                # 'b5eda738-c5a0-4293-82e9-46944d575e7e',
                # '40feb530-b166-488b-8982-be411329f72d',
                # 'af2fec13-1467-46c5-804c-1d7187e1b142'
                ]
    # sessions = ['4b13b376-4486-4e5a-a00f-8fa22d47fc9d']
    # sessions = ['04771142-03b9-4e27-a074-45a8688a187b']
    sessions = ['d9748d6b-fc89-4bfc-a920-2779c51652c0']

    # sessions = [
    #     '36536ea2-a6c1-4c52-b9fc-8168fc0c385d',
    #     '2fe10dac-8bda-40ba-91eb-ea86016a6c6b',
    #     '10314089-10b2-416c-8db4-6c255f12492e',
    # ]
    # sessions = ['064f2a01-4620-4214-9646-586ebc0a47e1']

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

