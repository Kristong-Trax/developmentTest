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


if __name__ == '__main__':
    LoggerInitializer.init('GOOGLEMX calculations')
    Config.init()
    project_name = 'googlemx-sand'

    # all sessions still in new status
    sessions = [
        ('f8612b45-d30c-44ab-927d-6c69a7ffc561', [309692, 309708]),
        ('fb53ffc9-108d-4078-b62f-de3fcf85d7ce', [315570, 315592]),
        ('fbf0cc96-ca77-40dc-a650-d9f60c463760', [308474, 308487]),
        ('fc7a4ac1-9196-4b08-a12b-f7310e234195', [308659, 308690]),
        ('fe6b9adc-06a7-4175-8965-3da4d13f0501', [308842, 308856]),
        ('fef13067-48ea-4595-90bf-ba83513c7774', [316576, 316607]),
        ('fff926b6-60c5-40d5-b8ce-2540e10a8664', [309563, 309570, 309639, 309645]),
    ]

    for session, scenes in sessions:
        # if len(scenes) == 0:
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_session_data(session)
        #     scif = data_provider['scene_item_facts']
        #     scenes = scif['scene_id'].unique().tolist()
        # for scene in scenes:
        #     print('scene: {}'.format(scene))
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
