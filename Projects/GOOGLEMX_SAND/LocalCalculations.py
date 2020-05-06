from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
import pandas as pd
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GOOGLEMX_SAND.Calculations import Calculations
from Projects.GOOGLEMX_SAND.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


# if __name__ == '__main__':
#     LoggerInitializer.init('GOOGLEMX calculations')
#     Config.init()
#     project_name = 'googlemx-sand'
#
#     # all sessions still in new status
#     sessions = [
#         ('c52a5d6f-c434-4aba-bf61-374623ebfb16', [311827, 311843]),
#         ('a9fd1037-88f6-4c6b-b31e-c72b7eaec8f4', [308047, 308081]),
#         ('be64b68e-f52f-4a29-8ec2-ec0d8a2096f7', [310364, 310371, 310399, 310406])
#     ]
#
#     for session, scenes in sessions:
#         # if len(scenes) == 0:
#         #     data_provider = KEngineDataProvider(project_name)
#         #     data_provider.load_session_data(session)
#         #     scif = data_provider['scene_item_facts']
#         #     scenes = scif['scene_id'].unique().tolist()
#         # for scene in scenes:
#         #     print('scene: {}'.format(scene))
#         #     data_provider = KEngineDataProvider(project_name)
#         #     data_provider.load_scene_data(session, scene)
#         #     output = VanillaOutput()
#         #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
#         #     save_scene_item_facts_to_data_provider(data_provider, output)
#         #     SceneCalculations(data_provider).calculate_kpis()
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
