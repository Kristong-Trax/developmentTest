import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBZA_SAND.Calculations import CCBZA_SANDCalculations, CCBZA_SANDSceneCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations

#
# if __name__ == '__main__':
#     LoggerInitializer.init('ccbza-sand calculations')
#     Config.init()
#     project_name = 'ccbza-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     CCBZA_SANDCalculations(data_provider, output).run_project_calculations()

def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)

if __name__ == '__main__':
    LoggerInitializer.init('ccbza-sand calculations')
    Config.init()
    project_name = 'ccbza-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = 'AD29338A-C2D9-4486-BD94-7B1E32224A11'
    session = 'E6BBF9D5-114E-4176-A35E-B84ABD0C11B5'
#     data_provider.load_session_data(session)
#     output = Output()
#     CCBZA_SANDCalculations(data_provider, output).run_project_calculations()
#     scenes = [2]
    scenes = [4, 5, 8, 9, 12]
    for scene in scenes:
        data_provider.load_scene_data(session, scene)
        output = VanillaOutput()
        SceneVanillaCalculations(data_provider, output).run_project_calculations()
        save_scene_item_facts_to_data_provider(data_provider, output)
        CCBZA_SANDSceneCalculations(data_provider).run_scene_calculations()