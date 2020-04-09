import pandas as pd
from collections import OrderedDict

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Projects.CCJP_SAND.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.CCJP_SAND.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('CCJP_SAND.Local calculations')
    Config.init()

    # session level kpi calc
    # project_name = 'CCJP_SAND.
    # data_provider = KEngineDataProvider(project_name)
    # # leave empty for all scenes
    # session = 'E731D801-244D-4956-A209-DC83A8AB9053'
    # data_provider.load_session_data(session)
    # output = Output()
    # Calculations(data_provider, output).run_project_calculations()

    # For scene leve kpis
    project_name = 'ccjp-sand'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        ('007b9fa1-6b33-4cb0-8ed0-d986e6af028f', ['769a58da-b684-4788-9ab9-ec7362d075a1']),
    ])

    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene_uid=e_scene)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()