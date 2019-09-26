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
#     session_list = {'4D54603F-9A36-46A1-830D-1789F3ED763B': [],
#                     'D66518E4-0D7E-4647-B865-327A269ED5DA': [],
#                     '76A9CA13-1D23-4BB8-BC6F-FA3C9D3B1C93': [],
#                     '945ED0B9-7346-4E6F-B56F-4E2F2B75F18C': [],
#                     '5673f455-2947-4168-aa8c-031b375a48f6': [],
#                     'ECF0042D-3AC2-4793-AE69-9BD7DE41FA70': [],
#                     '1D02DBEE-3062-4F80-A258-97B62F95746F': [],
#                     '09C8ADE3-4957-4172-B467-18EB80342C66': [],
#                     'EE3451D3-D8F9-4233-B6DB-E25B153853CD': [],
#                     'B20632EB-E1EB-4504-8E0C-52F450CE9931': [],
#                     '3307DEEF-E6E1-4335-B52F-89C6AAE3E485': [],
#                     '00976d6a-7fea-49b2-8c1b-9ab7fea1a070': [],
#                     'D86100CA-D401-43A5-A277-FA7E7A2C2203': []
#                    }
#     for session in session_list:
#         scenes = session_list[session]
#         if len(scenes) == 0:
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_session_data(session)
#             scif = data_provider['scene_item_facts']
#             scenes = scif['scene_id'].unique().tolist()
#         for scene in scenes:
#             print('scene')
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene)
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
