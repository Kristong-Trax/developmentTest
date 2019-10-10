import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput

from Projects.RINIELSENUS.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('Tyson calculations')
    Config.init()
    project_name = 'rinielsenus'

    sessions = [
        '23ad27c5-888c-4f84-a4ee-a8f959b563ff',
        '5d85be01-83b1-49bf-81bc-19ebe720a61d',
        'ec3fdc9f-235f-4b3f-92ee-f44bf51fc7e4',
        '868a8683-f99a-4906-b175-590a082e9c7c'



    ]

    for session in sessions:
        print( '\n~~~~~~~~~~~~~~~~~~~~~~~ {} ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)

        scif = data_provider['scene_item_facts']
        scenes = scif['scene_uid'].unique().tolist()

        scenes = ['9ff5527d-b3ad-4ba6-b8e9-0c5b31e47819',
                  '0bae2465-81a8-41cd-a20c-66eb92917759',
                  '8c2573ae-456e-4825-ac83-0ecb828b87dd',
                  'd076a8ac-525e-4a14-9f35-7481ab859b9c'
                  ]

        for scene in scenes:
            print('\n-------------------------------- {} ----------------------------'.format(scene))
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene_uid=scene)
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()

        #output = Output()
        #Calculations(data_provider, output).run_project_calculations()

