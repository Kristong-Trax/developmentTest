import pandas as pd
from collections import OrderedDict

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.PNGJP_SAND.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id',
                                     Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('pngjp-sand Scene Calculations')
    Config.init()
    project_name = 'pngjp-sand'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        # ('', [''])
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
