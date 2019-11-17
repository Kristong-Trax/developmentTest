
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSUAE_SAND.Calculations import MarsuaeSandCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd
from Projects.MARSUAE_SAND.SceneKpis.SceneCalculations import MARSUAE_SANDSceneCalculations
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('marsuae-sand calculations')
    Config.init()
    project_name = 'marsuae-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = 'baa68060-8464-4841-82ad-f28f28047b06'    #SSS A
    # session = 'D7156E31-F359-4358-A193-0B41E4C8A65F' # sss a
    # session = '7CB61001-022D-4077-B275-AB9A9E0C2AFF'    #Hypers - no ass results for some reason
    # session_list = ['D53E1FAA-BC36-457B-96C8-79EE7753B929', '7CB61001-022D-4077-B275-AB9A9E0C2AFF'] # scenes Hypers - prices in db
    # session = '7CB61001-022D-4077-B275-AB9A9E0C2AFF' # scenes Hypers - prices in db
    # session = '8C545C66-D892-4173-8354-0B6BF97B189D' # sss a all templates
    # session = '84e3fd3e-ae17-438d-8f52-5db0623f32a0' # convenience b
    # session = '8E742C0B-5AAC-4CE6-8C5D-1E31246E54B1' # discounter
    session_list = ["730A6FDB-3ED5-474A-88AD-BB89243A4C50"]
    # session_list = ['F65C2806-9427-4885-AACA-F7DD4AC7E2A4']
    # session_list = ["d83743b9-1f36-4586-b0de-cd93d6bc84a2"]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        MarsuaeSandCalculations(data_provider, output).run_project_calculations()
        # scenes = data_provider.scenes_info.scene_fk.tolist()
        # scenes = [35003]
        # for scene in scenes:
        #     print scene
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     MARSUAE_SANDSceneCalculations(data_provider).calculate_kpis()