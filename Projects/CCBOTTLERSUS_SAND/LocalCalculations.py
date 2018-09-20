import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.CCBOTTLERSUS_SAND.Calculations import CCBOTTLERSUS_SANDCalculations
from Projects.CCBOTTLERSUS_SAND.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus-sand'
    session = 'e7067c2d-1712-4dfd-8700-b1390f20cdd8'
    scenes = [817627
              # 524253,
              #   524260,
              #   524274,
              #   524306,
              #   524317,
              #   524330,
              #   524338,
              #   524343,
              #   524354,
              #   524398,
              #   524420,
              #   524445,
              #   524494,
            ]
    for scene in scenes:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_scene_data(session, scene)
        output = VanillaOutput()
        SceneVanillaCalculations(data_provider, output).run_project_calculations()
        save_scene_item_facts_to_data_provider(data_provider, output)
        SceneCalculations(data_provider).calculate_kpis()
    sessions = [
        "bc8a1244-a53d-4b2f-9591-fcf57f9d3054"
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUS_SANDCalculations(data_provider, output).run_project_calculations()
