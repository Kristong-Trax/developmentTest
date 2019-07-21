

from Projects.DIAGEOUK.SceneKpis.SceneCalculations import DIAGEOUKSceneToolBox
from Projects.DIAGEOUK.Calculations import DIAGEOUKCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
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
    LoggerInitializer.init('diageoie calculations')
    Config.init()
    project_name = 'diageouk'
    data_provider = KEngineDataProvider(project_name)
    session = '7B55D2B3-3728-4534-A7F7-4A5520748085'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOUKCalculations(data_provider, output).run_project_calculations()
    # scenes = data_provider.scenes_info.scene_fk.tolist()
    # for scene in scenes:
    #     data_provider.load_scene_data(session, scene)
    #     output = VanillaOutput()
    #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
    #     save_scene_item_facts_to_data_provider(data_provider, output)
    #     DIAGEOUKSceneToolBox(data_provider).calculate_kpis()


