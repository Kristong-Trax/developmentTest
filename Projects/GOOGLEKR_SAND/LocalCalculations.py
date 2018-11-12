from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
import pandas as pd
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GOOGLEKR_SAND.Calculations import GOOGLEKR_SANDCalculations
from Projects.GOOGLEKR_SAND.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('googlekr-sand calculations')
    Config.init()
    project_name = 'googlekr-sand'
    sessions = ["8e69ef91-1275-42ba-82a3-12a775683fb6"]
    sessions = [
                '3985c4f3-8e86-4cd0-8455-9202306e7d3e',
                'A7330F2D-620A-4821-A62C-329CFB45D867',
                'A4CDE6E6-9145-484C-9894-3B78B08085CA',
                '78949CC4-5E67-47CF-A043-ED832497C1C5',
                '924d3ce8-10f1-4083-9aa6-39b023eca812',
                '9595AA6E-CB9C-49DE-B043-0920DCF9EEE0',
                'f3a1a34e-043d-4c8b-99a5-3f9b7f9e0847',
                '7c73be34-20b9-4d6c-b48f-ec4a04745660',
                '5E35A64E-6B49-4106-AC14-BFBFC6BFF4DB',
                'ECFEEC98-4C2C-46F6-851F-97DBE14A3254',
                'C23CF630-5953-48A3-AE82-FF1E5BE0C8E0',
                '698CC197-6902-42AF-8015-AF6FD0DAF8D1'
                ]
    sessions = ['97FA6E6D-F1F1-47E4-8697-96DB1BDCB338']
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        scif = data_provider['scene_item_facts']
        scenes = scif['scene_id'].unique().tolist()
        # scenes = [887]
        for scene in scenes:
            print('scene')
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene)
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()
        output = Output()
        GOOGLEKR_SANDCalculations(data_provider, output).run_project_calculations()

