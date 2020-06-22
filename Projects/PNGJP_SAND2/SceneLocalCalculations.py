import pandas as pd
from collections import OrderedDict

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.PNGJP_SAND2.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
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
    LoggerInitializer.init('pngjp-sand2 Scene Calculations')
    Config.init()
    project_name = 'pngjp-sand2'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        # ('0FFF1A54-5759-4EA1-B260-4D604F7D7334', ['2B373278-8CF6-479F-9829-5B95A4D7930E'])
        # ('86A09858-6B90-454B-9547-8C33CD12688C', ['64FF34E4-82E9-4860-8759-B65B4C55BFB6']),
        # ('86A09858-6B90-454B-9547-8C33CD12688C', ['16DFB0C6-E86D-4B23-80DC-A3BFB6328CA1']),
        # ('86A09858-6B90-454B-9547-8C33CD12688C', ['B52A863F-83BE-4B50-9FF4-FB89A84E6BDD']),
        # ('86A09858-6B90-454B-9547-8C33CD12688C', ['2206986B-8AE9-4618-8663-7EFFEAA4EBBD']),
        # ('86A09858-6B90-454B-9547-8C33CD12688C', ['DAC40AC4-1B80-4FCF-A40C-F90D022B3EE4']),
        # ('86A09858-6B90-454B-9547-8C33CD12688C', ['3C9DE671-9116-4E79-9AE2-010FEF7694C7'])
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
