
import pandas as pd
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.HBCDE_SAND.Calculations import Calculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('hbcde-sand calculations')
    Config.init()
    project_name = 'hbcde-sand'
    data_provider = KEngineDataProvider(project_name)
    session_list = {'1090438D-009E-4842-8581-E4366F5430CA': []}
    for session in session_list:
        # scenes = session_list[session]
        # if len(scenes) == 0:
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_session_data(session)
        #     scif = data_provider['scene_item_facts']
        #     scenes = scif['scene_id'].unique().tolist()
        # for scene in scenes:
        #     print('scene')
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
