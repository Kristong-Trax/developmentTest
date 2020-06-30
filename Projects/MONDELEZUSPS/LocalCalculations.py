
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MONDELEZUSPS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('mondelezusps calculations')
    Config.init()
    project_name = 'mondelezusps'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['540b2e3d-988c-43ef-9897-7ac5850d654f']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

# import pandas as pd
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.MONDELEZUSPS.Calculations import Calculations
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.MONDELEZUSPS.SceneKpis.SceneCalculations import SceneCalculations
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
#     LoggerInitializer.init('mondelezusps calculations')
#     Config.init()
#     project_name = 'mondelezusps'
#     data_provider = KEngineDataProvider(project_name)
#     session_list = {
#         'de3619ed-2557-4c6e-b97b-1e05be9f98c8': [],
#         'bc647fe2-b377-40a3-9abd-2153d60e1b0c':[],
#         '540b2e3d-988c-43ef-9897-7ac5850d654f':[],
#         '6555cf89-66b0-46e6-bf72-ea7c1e140bf4':[],
#         'b4ce4e36-2df5-47a9-9951-53133c9bdebc':[]
#
#                     }  # leave empty for all scenes
#     # session_list = ['a5216695-4828-48e2-971b-29cd2af42189']
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

