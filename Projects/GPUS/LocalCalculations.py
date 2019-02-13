import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.GPUS.Calculations import Calculations
from Projects.GPUS.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('gpus calculations')
    Config.init()
    project_name = 'gpus'

    sessions = [
                # 'ff3f79ef-6d5e-4ea8-9a87-87ea60cbf629',
                # 'd78f0285-b4e2-4a75-831f-f7b4a81b6eef',
                # '040aac07-013b-4d6b-a408-a6348faf317d'
                ]

    sessions = [

        # session_uid
        '08465b37-6157-49f2-9ffd-852c8a00553b',

        # '5c4499dc-6f4c-40ef-bf3d-b02c07901e23',
        # '0ef472a4-ffd8-47f3-8f6f-f8dd45c5c828',
        # '3a5d3658-80c3-4975-8181-1c5d17736179',
        # 'f1ab67aa-c862-4985-8538-8f535ca722a5',
        # '2b19564d-5f69-4716-a6da-b22c92a9fe68',
        # 'd6b86d95-25b2-4dbd-a684-1fe0ce9b4c3a',
        # '7c8dcf9b-a8f7-48a7-aedd-ff7fe736d237',
        # '541e9f98-5f0b-4294-9c43-e3d7865d5199',
        # 'e88ec29b-19bd-417c-af66-75d333b789a6',

    ]

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)

        scif = data_provider['scene_item_facts']
        scenes = scif['scene_id'].unique().tolist()

        # for scene in scenes:
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()

        output = Output()
        Calculations(data_provider, output).run_project_calculations()

