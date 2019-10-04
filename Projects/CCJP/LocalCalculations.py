# import pandas as pd
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.CCJP.Calculations import Calculations
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.CCJP.SceneKpis.SceneCalculations import SceneCalculations
#
#
# def save_scene_item_facts_to_data_provider(data_provider, output):
#     scene_item_facts_obj = output.get_facts()
#     if scene_item_facts_obj:
#         scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
#     else:
#         scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
#     scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
#     data_provider.set_scene_item_facts(scene_item_facts)
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('CCJP calculations')
#     Config.init()
#     project_name = 'ccjp'
#     data_provider = KEngineDataProvider(project_name)
#     # leave empty for all scenes
#     session_list = {
#                         # "160EFF82-1864-4CEF-89C6-77CACA9F6E64": []
#                         # , "3E586224-27EB-4B73-B3BE-022E74F5B640": []
#                         # , "46314FF2-A429-4AE4-9093-9360BC9DD98E": []
#                         # , "961D59BA-E48D-494B-8ABC-5DA57DB53C85": []
#                         # , "0F53E59E-5D54-4C47-BAA2-3520523CD618": []
#                         # , "B7A3D56B-C985-47D4-B0E6-D274BBB5050D": []
#                         # , "1A1BD8B0-A234-47ED-B8C0-B6033BEF0F5B": []
#                         # , "7C2EADA1-74DF-4B4B-9ACD-137044F43E2F": []
#                         # , "0B618FCF-6196-4672-8E78-4D0CE6AEB6E6": []
#                         # , "A1AD959F-3663-4D31-8206-76CC0410B1BB": []
#                         # , "7A620130-209A-4FE9-A39C-A5B8F2051A27": []
#                         # , "9A3399BF-FDF6-443E-82C6-936E7731CBFA": []
#                         # , "50BD698F-F5A0-4834-B5C7-8177A8E555CA": []
#                         # , "7A0469A0-7BF4-481D-8ED9-A1CBE5AF6619": []
#                         # , "2C224013-F669-4242-B453-33856EEB2E5C": []
#                         # "D129517C-8F28-4193-873E-31DEB5CE8891": [],
#                         # "B96A1B54-2B7C-4AAF-B18B-47868C53A771": [],
#                         # "F609EE84-A672-453B-8E7D-6E8F5D4E5104": [],
#                         # "62ED21F0-F28B-4DE4-BA22-5329A1E29D5E": [],
#                         # "40495AD6-B8D8-4918-B1AD-496D85B65683": [],
#                         # "0F0F403B-CD79-4A1E-94F4-18CA60507BF0": [],
#                         # "BB8A4111-CD81-4B9F-950D-500C540C4A27": [],
#                         # "4B9A2142-6E74-4145-89E8-818EC841C7E8": [],
#                         # "CBEB064B-AFEE-4B41-870F-75456184D55B": [],
#                         # "7D2F8C9A-DDEA-492F-8C3C-CCD79619D914": [],
#                         # "230B4AF4-12DD-42A1-9038-2A1249FB9F79": [],
#                         # "5FF62A34-BA2F-4469-BDB2-763947225651": [],
#                         # "FEEC33E0-CA40-4C90-9977-FBEBCC1F4605": [],
#                         # "10AAD318-4ADE-43E6-9CA5-62C67CB3F94E": [],
#                         # "2F64C529-FF44-4D64-84A0-E496F3F98EFF": [],
#                         # "0E011635-B487-46EE-BD60-20562F39B008": [],
#                         # "3B8AE8B4-9969-45D2-8B97-00A9D4585762": [],
#                         # "5CAC4163-ECA4-48EE-9265-664938A4C5DB": [],
#                         # "78824824-1162-49A3-85AD-FBD0A3FE014E": [],
#                         # "A94BB4A5-4AF2-47CF-912D-C8BCA5DA89FE": [],
#                         # "ADF86715-9E45-47EF-B3A4-47CA18AC5713": []
#
#         "312F8D5E-A3FA-41BD-AB6F-11C40C765BFB": [],
#         "4F2B73F6-5BB2-4544-96AD-7A8F368ACA6F": [],
#         "F5914138-4E38-4220-9CED-C2CB9C360451": [],
#         "0B62F517-C685-420C-876E-A47C55ED78F9": [],
#         "431BEBF5-D09D-4134-B007-FD1F97E4243F": [],
#         "A06743E4-5505-4DC0-B3FB-7C3C2B43EA12": [],
#         "A90925BD-9542-4359-B3C3-C88CBB530258": [],
#         "ECA83C73-EB32-466D-A3B3-800321B0124A": [],
#         "631D6D0F-7F7F-4914-BE61-64B8AF2D0580": [],
#         "9EA2F456-8A51-4D1C-8F54-F4759F9160EE": [],
#         "83770452-3E1E-4D55-97CA-C72ADAD9DC7F": [],
#         "8ADCCB70-D9ED-436B-98B1-F4DC02DEA881": [],
#         "10DCC7CF-ADCC-4022-A3A2-CA8117A77718": [],
#         "9B8129CE-BEAE-4289-8695-FB9AAB292BEF": [],
#         "0FFDF229-B23F-4189-B047-7125FD414E24": [],
#         "5530FAE3-D6D6-46E5-8BC7-7DE4C7CE8E97": [],
#         "C0C0436F-A104-468A-96D7-878129BCD3ED": [],
#         "4D7EF413-5F72-4302-9F51-36E1B33ACE80": [],
#         "9C646051-390E-4F3E-9E6E-4842E44619D7": [],
#         "FBA9E01A-13AB-4342-A74E-274A3193BD49": [],
#         "410D8B69-9939-4E5E-9909-39E85AE79DFC": [],
#         "1F30CAA2-8AE0-474D-A211-674C13B2DE0D": []
#                     }
#     num=0
#     for session in session_list:
#         num=num+1
#
#         # scenes = session_list[session]
#         # if len(scenes) == 0:
#         #     data_provider = KEngineDataProvider(project_name)
#         #     data_provider.load_session_data(session)
#         #     scif = data_provider['scene_item_facts']
#         #     scenes = scif['scene_id'].unique().tolist()
#         # for scene in scenes:
#         #     print('scene={}'.format(scene))
#         #     data_provider = KEngineDataProvider(project_name)
#         #     data_provider.load_scene_data(session, scene)
#         #     output = VanillaOutput()
#         #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
#         #     save_scene_item_facts_to_data_provider(data_provider, output)
#         #     SceneCalculations(data_provider).calculate_kpis()
#         print("Session_uid:{}, num:{} ".format(session, num))
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
