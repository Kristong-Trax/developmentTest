import pandas as pd
from collections import OrderedDict
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.LIONJP.SceneKpis.SceneCalculations import SceneCalculations
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
    LoggerInitializer.init('lionjp Scene Calculations')
    Config.init()
    project_name = 'lionjp'

    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        # ('67557C68-79E0-4636-BDF8-E4E1C39FA062', ['9079CDE7-1FEC-4C25-94C5-5C8467BEDDAB',
        #                                           '85F78B63-494D-4613-9B6E-4224902AF4DF',
        #                                           '78CD9801-FF37-40EF-B331-193BDCF6B082',
        #                                           'E0B35137-1FAF-49AC-8083-E99B0520A724',
        #                                           '9FF61127-2550-45F9-970F-B916BE789286',
        #                                           '3085E08E-F46A-4315-878E-D786A5D18F4B',
        #                                           '05F4C745-33E1-441E-B53A-C13C17F08D73'])
        # ('0C8C1649-A253-4B1B-8E23-A85E73ADC0D5', ["94DE2F60-29FC-485F-9751-7B260A9A4D12",
        #                                           "F5F8B892-4745-4F40-88D7-3F2D03FB0DE7"])
        ('0C8C1649-A253-4B1B-8E23-A85E73ADC0D5', ['F5F8B892-4745-4F40-88D7-3F2D03FB0DE7'])
    ])

    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider1 = KEngineDataProvider(project_name)
            data_provider1.load_scene_data(session, scene_uid=e_scene)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            output1 = VanillaOutput()
            SceneVanillaCalculations(data_provider1, output1).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider1, output1)
            SceneCalculations(data_provider1).calculate_kpis()
