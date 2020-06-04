from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
import pandas as pd
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GOOGLEBR_SAND.Calculations import Calculations
from Projects.GOOGLEBR_SAND.SceneKpis.SceneCalculations import SceneCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('googlebr-sand calculations')
    Config.init()
    project_name = 'googlebr-sand'
    sessions = [('2b0dd747-01db-4997-bee0-37a829eb3688', [222929, 222912, 222931, 222941]),
                ('574cec0f-7536-4aca-9613-fc31d1ae59b4', [223006, 223013]),
                ('60e63e0d-6f77-4261-8232-e7e0d2f8e067', [223075, 223044]),
                ('bb72269a-39fa-48cd-84b0-4b034d204891', [223026, 223028]),
                ('bf90b409-aea0-485a-b5a2-fb8b91df2d64', [222910, 222918]),
                ('c401bf62-e37e-4b89-ab32-d982c7641ed1', [223065, 223077]),
                ('daefa2fd-e553-4b65-95be-52d6a8d5d5a0', [222997, 223004])]
    for session, scenes in sessions:
        # data_provider = KEngineDataProvider(project_name)
        # data_provider.load_session_data(session)
        # scif = data_provider['scene_item_facts']
        # scenes = scif['scene_id'].unique().tolist()

        # scenes = [392]

        for scene in scenes:
            print('scene')
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene)
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
