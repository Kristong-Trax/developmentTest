
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSUAE.Calculations import MarsuaeCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd
from Projects.MARSUAE.SceneKpis.SceneCalculations import MARSUAESceneCalculations
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
    LoggerInitializer.init('marsuae calculations')
    Config.init()
    project_name = 'marsuae'
    data_provider = KEngineDataProvider(project_name)
    # session = 'baa68060-8464-4841-82ad-f28f28047b06'    #SSS A
    # session = 'D7156E31-F359-4358-A193-0B41E4C8A65F' # sss a
    # session = '7CB61001-022D-4077-B275-AB9A9E0C2AFF'    #Hypers - no ass results for some reason
    # session_list = ['D53E1FAA-BC36-457B-96C8-79EE7753B929', '7CB61001-022D-4077-B275-AB9A9E0C2AFF'] # scenes Hypers - prices in db
    # session = '7CB61001-022D-4077-B275-AB9A9E0C2AFF' # scenes Hypers - prices in db
    # session = '8C545C66-D892-4173-8354-0B6BF97B189D' # sss a all templates
    # session = '84e3fd3e-ae17-438d-8f52-5db0623f32a0' # convenience b
    # session = '8E742C0B-5AAC-4CE6-8C5D-1E31246E54B1' # discounter
    # session_list = ["730A6FDB-3ED5-474A-88AD-BB89243A4C50", "422C31C6-1118-4271-9852-A72CB03955FE",
    #                 "1B46DE0A-6440-4639-BE0F-7B44F2162E6B", "C0394F77-8A3C-4AAA-A5EB-9821D085F24F",
    #                 "e4e5e230-4b51-4908-a959-51a46fa5f54f", "fb6f25a7-2d03-49e9-8216-25bb9dede3e2",
    #                 "494773A6-6E17-418C-B965-F0847B6701AB", "82AA237B-040C-434F-BF57-9421F481168B",
    #                 "228ae67a-c47d-468b-9964-05e913d7c847", "5C98034A-3F49-4606-AB5C-409CE210A6BC"]
    session_list = ['C8074CC7-A00E-48CA-B366-B3B18930E399']
    # session_list = ["d83743b9-1f36-4586-b0de-cd93d6bc84a2"]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        MarsuaeCalculations(data_provider, output).run_project_calculations()
        # scenes = data_provider.scenes_info.scene_fk.tolist()
        # scenes = [35003]
        # for scene in scenes:
        #     print scene
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     MARSUAESceneCalculations(data_provider).calculate_kpis()