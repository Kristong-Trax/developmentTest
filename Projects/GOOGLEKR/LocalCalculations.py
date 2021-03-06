from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
import pandas as pd
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GOOGLEKR.Calculations import Calculations
from Projects.GOOGLEKR.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


# if __name__ == '__main__':
#     LoggerInitializer.init('googlekr calculations')
#     Config.init()
#     project_name = 'googlekr'
#     sessions = [
#         '68ac2242-d6d5-4a08-ba5c-ba5418c69852',
#         'dd7452bb-134c-4b5a-a695-be6355695b48',
#         '33203324-796f-42f5-af71-9acb0e48b2f4',
#         'ba74b587-901d-437d-ab34-2ffc33c49aaa',
#         '9b33b4d0-cb67-4e9b-82d9-4d4c3c505491'
#     ]
#
#     for session in sessions:
#         # data_provider = KEngineDataProvider(project_name)
#         # data_provider.load_session_data(session)
#         # scif = data_provider['scene_item_facts']
#         # scenes = scif['scene_id'].unique().tolist()
#
#         # scenes = [348549, 348552]
#         #
#         # for scene in scenes:
#         #     print('scene')
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
