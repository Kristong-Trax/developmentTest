
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCANDINA_AR.Calculations import Calculations
from Projects.CCANDINA_AR.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
import pandas as pd

def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)

if __name__ == '__main__':




    LoggerInitializer.init('ccandina-ar calculations')
    Config.init()
    project_name = 'ccandinaar'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
        'fc427d3c-9fba-47e9-94ab-6550ef2ab1e5',
        #             'ebec5cdf-c448-41ec-9569-b22e8d75dd09'
                    ]
    # for session in session_list:
    #     data_provider.load_session_data(session)
    #     output = Output()
    #     # Calculations(data_provider, output).run_project_calculations()
    #     scif = data_provider['scene_item_facts']
    #     scenes = scif['scene_id'].unique().tolist()
    #
    #     for scene in scenes:
    #         print(scene)
    #         data_provider = KEngineDataProvider(project_name)
    #         data_provider.load_scene_data(session, scene)
    #         output = VanillaOutput()
    #         SceneVanillaCalculations(data_provider, output).run_project_calculations()
    #         save_scene_item_facts_to_data_provider(data_provider, output)
    #         SceneCalculations(data_provider).calculate_kpis()

    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

