# import pandas as pd
# from Trax.Utils.Conf.Keys import DbUsers
# from mock import patch
# 
# from KPIUtils.DB.Common import Common
from Projects.RINIELSENUS.KPIGenerator import MarsUsGenerator
from Projects.RINIELSENUS.Utils.ParseTemplates import ParseMarsUsTemplates
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log


__author__ = 'nethanel'


class MarsUsCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        if True:
            self.timer.start()
            try:
                MarsUsGenerator(self.data_provider, self.output).main_function()
            except:
                Log.error('Mars US kpis not calculated')
            self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('TREX')
    Config.init()
    # docker_user = DbUsers.Docker
    # dbusers_class_path = 'Trax.Utils.Conf.Keys'
    # dbusers_patcher = patch('{0}.DbUser'.format(dbusers_class_path))
    # dbusers_mock = dbusers_patcher.start()
    # dbusers_mock.return_value = docker_user
    project_name = 'rinielsenus'

    sessions = [
                '5687bd59-fd20-423e-8bea-d1b4a28da264',
                '0aa034cb-6057-4d62-8150-4b1207491e39',
                'ae04abb8-a0b2-49a0-a681-60563c5e6efe',
                '0fce09fd-0b33-4495-8e8b-e213aa30e94f',
                '11e562a0-928c-4794-9d9b-64cd6544c84d',
                '5C8E563C-66B6-405F-865F-22EDCD91C7A1'
                'bc90bfdb-e446-4de0-9911-22391648578b',
                'a4b927b2-42ac-4879-aec1-52bd2f716068'
    ]

    sessions = [
                '0784fd4e-1bde-4511-acb9-04d5cbd2aae9',
                '0cd3a0f5-2e5c-4163-8605-32eb689590b1',
                '13599d17-6bba-4926-a713-3051b411d6f4',
                # '1b814545-51b4-4ce8-960b-d53815937b5b',
                # '38992e47-d6ef-4ad5-987b-df27d9cfbbfb',
                # '62468ec9-d607-42f6-bb34-542410f1f556',
                ]

    sessions = [
                '03b2eb1d-fad9-430d-a04b-26e62ecd837c',
                '05894f40-b6fc-47ba-932d-e162b59322bb',
                '065fbf93-b335-45cb-8434-477913898594',
                '242d1600-e2c2-45f3-994c-eb57623a4deb'
                ]

    sessions = [
        '286f6319-793a-4c87-8292-18b75a77a36d',
        '7874fdbe-45be-40b0-97a7-7342b06371ac',
        'ed5b2ce1-1edf-4419-a4cb-70480632d046',
        '34a7540a-1528-430a-906c-d539228b754a',
        '0f349c72-33b9-4103-918f-e9706f3bb443',
        'fad57e54-de32-4aba-969e-70602f4be68e',
        '1b0a57d5-8fbb-4aa2-8d4e-51f01ca5ea24',
        '00ee60ac-1f8b-49c4-83e4-69a8fe500794',
        '6285d617-2b6a-44f0-8d7b-8bbbb93246d6',
        '73db7f22-3bda-495b-9dc1-8aecfafdaa65',
        '8b05e500-3607-4c63-97ec-db541a15a332',
        '4dd124a5-1070-4d7c-ab61-12164976be6d',
        '33d3ed1c-9885-43e2-ad8d-d5a17b135ffe',
        '39992742-027e-451e-8117-dbf1f153c640',
        'adf848d2-1f7c-452d-a942-422a41bdd476',
        '9af1e1ec-db23-4bfa-ae72-53b4fdfe3825',
        '2a9eb81f-d2e8-4be4-9ac5-89c5863d1ae6',
        '7cca8f0c-35e9-430c-8579-6ff590133c72',
        '1d12c1c3-ed95-4ceb-8168-98f5dc5da898',
        '0ca6d1fc-aec4-4f21-a876-49c93cc3a2cb',
        'e5598e23-5ab1-436c-ae76-ac27ea6de419',
        'f7adb544-1a15-432a-8d3c-37cea075a80c',
        '5852e668-2572-4326-a06a-cbbd7c1da99b',
        '3d36672b-6f26-458d-bfda-8dea75af5088',
        '1218bbd7-1837-476d-959a-6f53b4cecb45',
        '31be7499-991f-43ac-aa1c-f59bef30e106',
        '9f2656a7-2578-45f6-b31c-0e443d12a018',
        'def2feca-b3ee-435b-88f4-e147ecc31fd3',
        '268c9f2e-b68f-4a91-b60d-f7c509e689d6',
        '638565c3-c2f1-41a0-a45c-56cb16b69952',
        '47915b0e-37f9-48b8-a619-f38c85f21162',
        '9f382054-9237-422b-ae1c-3ff2aaa41dc5',
        '445fd6b4-fcc9-490f-8d19-32e7db80b445',
        '63a38dae-84da-4214-95b3-6cdd33c27869',
        '6aff2b3b-c03c-40c4-a150-e852515293eb',
        '9faac65a-5742-41b5-851a-e9961c440f72',
        '5d51148d-5407-419c-8d91-4f9463789154',
        '50e40ed5-7e18-4814-b574-c1d349e6b347',
        '8de76311-f697-406e-b4fd-ef43701a023c',
        '1f4b93e9-ffe9-41c8-b5e4-c5f45946d8da',
        '034efb73-af4e-49ca-9b66-54e5cc28849b',
        'a6b03f72-8331-4039-bfa3-e10f35b2994e',
        '64a37e68-0779-41c4-b349-ea182a99f9b2',
        '0b9585a0-fc83-4436-9433-f1d20bf67deb',
        '03e4b92c-dd3a-4c29-bb2d-d10977bc0a26',
        'ec4dedea-4d81-4d25-b2f4-323ff82fae17',
        '4a0210bd-c2ae-43bd-a445-14bafc045731',
        '3f5ae8f9-6ddb-4d55-be5c-aa58f03738dc',
        '9f4fcddc-1260-445f-ab71-c23f25df855b',
        'd88eba17-07fc-4e9b-a709-1f7bd031d8dc',
        '63af35df-13bb-4be5-bb0a-e580cc2d218f',
        'e84e2ebc-5467-4705-bce3-accb62ebd71a',
        '8f0501e8-0ff0-4b64-87df-8bb12da8a2db',
        '884f55c7-8e69-4e36-8fb7-4303a5fe9c9f',
        'd4391ffb-78a2-4f70-be3c-a0309f47b1ea',
        '264e869a-46d8-4ac7-9e50-3a72fa827b7f',
        '60c60c1c-49d0-4b1e-baed-e8c22fce823a',
        '0dc0d638-b94d-489b-be0a-912978ab2307',
        '55519496-7aa6-4457-90d7-c55bb3518220',
        '6ecda815-dce7-4814-aafb-d3d7ff942206',
        '625c47e2-ff4f-4014-ab5d-b14e92edd30a',
        '42bb2511-4286-45b1-8f63-96a63dd724f0',
        'e4e420b5-5689-41c2-aa91-645b8088b2f1',
        '9cf727d8-5912-4a0b-a822-dd2657046683',
        '55A1C12D-A090-4A43-A2FA-BEDABFD0602C',
        '4fd0f22f-d01f-416a-ae7d-49d0e086d6b0',
        '785d14ba-7a56-45a1-9ce0-09330ddf4663',
        'ab6a64d1-895e-44da-ad42-a31532b026ff',
        'ba90a93a-d91a-4849-978c-e3f004844542',
        '8139b681-5fbe-4db7-835d-81fdab81e0ad',
        '7fce2e46-a1d1-439b-b969-b146e0e021f6',
        'b44a8339-149b-4562-b79e-f4bd7134eba6',
        'a179d477-3f92-47b6-80c2-42e753684f57',
        '95d105a3-2e56-4a1d-92c4-18d791d29139',
        '2a3cfd62-e6d6-4e13-8d7b-10051e8a5dcb',
        'defda8f5-555a-4451-a5ea-5700666ed0af',
        '10fca537-cb2c-4180-91f3-b20f8c76b21a',
        '193e1b13-9b3f-4395-946d-27d2035a5b5b',
        '531c205c-91e6-42b4-a22a-845b8ac5887d',
        '98de34ef-8450-4b36-ae42-d16377700c64',
        'baa76bd4-7102-48d9-9394-1ba5d22f613d',
        'ef01eb8e-4734-4eb1-9578-bc496612be0b',
        '0341ad40-cc8a-4330-9005-bb2e442c52a9',
        '56d1ee8e-78a8-4b66-a5ab-f7699c03b62d',
        'a7b76738-3855-43ef-9d74-4b050bc85386',
        '44c8c8f3-ff48-47a4-9902-535c1a33eb0b',
        'dab51ce6-2e79-49c6-a847-c2d156f162a7',
        '5154d829-ded3-46c6-a3fc-6c50e83ca301',
        'ae0fee0a-e0f3-41ea-92ee-96d05f7da3ed',
        '151df471-7b06-4d0e-b703-b77fe2d64175',
        '25b70d93-4a3d-464d-bfb4-b6d1ca513c1f',
        '356ba7c1-8cd9-4bea-b4eb-3c15492e40fd',
        'a6b6932e-3cdc-4d5a-b0bd-8550cc6ddccc',
        '4d5ffa46-1140-489d-b84e-02c65fa625e9',
        'd79fb7d0-87b2-4186-a576-db847b26c3e7',
        'b4a83b3b-dfbe-4373-aece-c3df9e263cd2',
        'fef87bea-4ab8-4665-90bc-185ac74ebfc1',
        '7cae271f-d71c-40df-bed2-4fe5d6615049',
        'e6251ddd-947d-4004-ab2f-2da69b9696a6',
        '5a80ee0a-5494-43ca-af03-3f5dda9381aa',
        '7d83782e-b7ca-4f42-b20a-8497ed283b18',
        '0b9b5190-837b-44de-91ab-434dd6345014',
        '2e17bc43-15e5-41e2-a8aa-bade38370535',
        'c9af02d5-ad6d-4f4c-946d-29ca604b282d',
        'e1a85e66-d719-4712-9af8-56ec4b51ba17',
        'b436bee0-0cbc-4c18-998d-7ce5d4291ec4',
        '7875100c-53f2-4709-beae-17f8d437b38d',
        '8f81b0ac-b01b-444c-85c9-53b92891b146',
        'a092113d-fa9c-4378-8a44-1cf3761f0702',
        '3b7fd281-2473-4923-abfa-1ac596b7a0c9',
        '8292cd07-a11b-47f4-87ff-5b599e74a06f',
        'a9dd6322-3459-4830-9982-da02e4c03b51',
        '3a0b16fa-1c50-4cb6-a847-c7b893790b56',
        'B40D2056-1A8C-4653-A64B-11FD0675613A',
        'b9c316e2-fa02-4971-aa30-b316d7375e31',
        'e8d50c6c-a7f8-401a-a4d4-149415365cf7',
        '4996c65e-3e0c-464a-ba42-cea5ab5c4a64',
        '4a147dbe-e17b-46e2-97b7-b0c9569bebb7',
        '81c888e5-e612-4073-9308-15a58096c793',
        '7623e29e-570d-479a-93f4-5d989c5269be',
        '969d428e-6cf6-4d58-a41f-b3ac1082b902',
        'ca5055d2-3764-41e6-8e37-32703f076d85',
        'ede337b9-9125-444f-8174-399802e32bad',
        '6ba7c71d-fa41-43a7-8780-fd8004e7df52',
        'a371155f-122f-42c6-a504-dadc27cf8e75',
        '7e92ea3a-690e-4fd6-b08a-35213712898c',
        'fc54e697-4027-4789-afcc-0bc40717aee4',
        '8b3c2a3d-0d79-4216-aa82-95baec3f0717',
        'ca13d508-3932-4302-bbd5-ec04b1873cbe',
        '1cb9808e-56da-4cc9-b7c5-cc00abe256e9',
        '1855459d-41b6-4d69-b1e9-8570ba567433',
        'd88262a5-f2aa-4cf4-a780-5ce38342b460',
        'd94091e4-f9d9-4328-9973-6792148746f7',
        '3bb3e06c-c9ca-413f-8f7b-d2c3d7200a43',
        '8bf044f5-779c-4866-992b-f7d226d5ab86',
        '3b475a59-27e4-4bbf-a5a3-88553161cbfe',
        '6425f9d4-ef0f-406f-94dd-8d1d29337c76',
        'be538b2f-93e3-431a-8a2c-5fc71636a876',
        '77f625b4-f05b-4995-b37c-a43e2abbd4aa',
        'c768c2fb-d616-4bd6-81d1-b283039b8133',
        '11a50a42-a411-41e4-911b-7cf64d908eda',
        '51297971-95d0-4f60-839c-39ce730051e3',
        '8736f2a2-5567-4981-88de-921f129fd5df',
        'd9f99fb2-8d03-4602-bf91-32ee7d8bc798',
        '5a3be797-be7c-4533-9a2c-bfbde5e425d9',
        'ccc819c8-2563-4eeb-8431-69a9a26baf08',
        '97e3a273-0ff9-4c62-b67e-70c2a78ca7d7',
        '1ee02ca0-70c9-4f85-8499-d9e7c555fe03',
        'b3970bc2-de98-4dc5-9289-75cf9255e8d0',
        'e8450e96-3317-4d68-a4e7-d07ff1275c58',
        '7fc91390-36b7-4095-aa44-b9c523ab9679',
        'fa136deb-a491-427d-b42d-e0252f1038e0',
        'd98742ea-eb56-43a6-b2b4-8560560a8266',
        'a131cbb9-e3b0-4d54-8f38-7bd9e7693304',
        'ba29475a-b417-4726-b891-5dedcb384423',
        'f828199d-46c5-4c99-badd-aa81b7820094',
        'fa63cf49-dd01-46be-8841-44cefa95847c',
        '72cb9789-fe91-4fc9-9173-f2c34d6d2e39',
        '99d0aa5b-4778-43fb-9cd4-a18d755d26ee',
        '2ce863fc-abb5-453c-b335-e2ec6d4793cf',
        'cca66be2-b9db-42e5-ab02-70950ffa534d',
        '0f37bb88-0e4b-41d0-b428-74115e9cb147',
        '047b82bc-0ec2-4db4-a6ee-eb9eb7c0120f',
        '3f4db9d8-a5a2-40c8-ad54-d14f2654021c',
        '9255845a-c8f0-474a-a997-cbd5818467a4',
        'a9ebf135-cfb2-481b-9e11-ee3ae0836a10',
        '3ad2cf4a-a0b4-453b-8b13-a9a1a7f7add6',
        '1ed26b25-553e-402c-a13b-deee68e03e02',
        'e0fc2d3f-9a68-4e33-827d-f781a63b87c5',
        '3fd722dd-f077-4094-866c-2ffe7b731fa3',
        '0515b8d2-3dd0-4970-951c-044d2a058dfc',
        '27e6653b-0ffe-436f-8f1d-a86dac548ab9',
        '6a8ebfe7-97f9-43c3-a892-c10aaea15cee',
        '5af3ed5d-a105-4127-b6b6-ecbb22be665a',
        '77c4d446-7089-4924-b3d3-38251c1b1775',
        'ce1e00d5-b3be-4cc0-9992-b5d920220c79',

    ]

    sessions = [
        '286f6319-793a-4c87-8292-18b75a77a36d',
        # '7874fdbe-45be-40b0-97a7-7342b06371ac'

    ]
    # sessions = ['0007c2c6-af03-454b-aff3-23f9c4018f27']

    sessions = [
        'd4391ffb-78a2-4f70-be3c-a0309f47b1ea',
        'b3970bc2-de98-4dc5-9289-75cf9255e8d0',
        # '63a38dae-84da-4214-95b3-6cdd33c27869'
    ]

    # sessions = pd.read_csv('/home/Ilan/Documents/projects/marus/0612_batch_300.csv')['session_uid'].tolist()[:25]

    for session in sessions:
        print('*******************************************************************')
        print('--------------{}-------------'.format(session))
        Log.info('starting session : {}'.format(session))
        data_provider = KEngineDataProvider(project_name)
        # session = Common(data_provider).get_session_id(session)
        data_provider.load_session_data(session)
        output = Output()
        MarsUsCalculations(data_provider, output).run_project_calculations()

