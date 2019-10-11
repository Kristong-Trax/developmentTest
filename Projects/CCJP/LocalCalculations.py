import pandas as pd
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.CCJP.Calculations import Calculations
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.CCJP.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)

# if __name__ == '__main__':
#     LoggerInitializer.init('CCJP calculations')
#     Config.init()
#     project_name = 'ccjp'
#     data_provider = KEngineDataProvider(project_name)
#     # leave empty for all scenes
#     session_list = {"1F30CAA2-8AE0-474D-A211-674C13B2DE0D": []}
#     num = 0
#     for session in session_list:
#         num = num+1
#         scenes = session_list[session]
#         if len(scenes) == 0:
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_session_data(session)
#             scif = data_provider['scene_item_facts']
#             scenes = scif['scene_id'].unique().tolist()
#         for scene in scenes:
#             print('scene={}'.format(scene))
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene)
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
#         print("Session_uid:{}, num:{} ".format(session, num))
#
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
