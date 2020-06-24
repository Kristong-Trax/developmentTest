import pandas as pd
from collections import OrderedDict

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.PSAPAC_SAND3.SceneKpis.SceneCalculations import SceneCalculations
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
    LoggerInitializer.init('psapac-sand3 Scene Calculations')
    Config.init()
    project_name = 'psapac-sand3'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        # ('FCEF707B-E6F9-4341-B6CA-77938320E2FD', ['D8C75D44-ECC9-4F64-B783-2E1286E76D1E']),
        # ('941B1EFF-D760-4CBA-BDDC-6C687222ABFD', ['27D6C1DA-529A-4122-83E5-61D55745F9FB',
        #                                           'BC173A23-1332-4AAB-A37D-0FA500B4FCC0',
        #                                           ]),
        # ('B91B9EEC-30EF-42D0-A6A0-959744194A24', ['F31B96D0-6C73-460C-BE31-DA04A64D41B7',
        #                                           ]),
        # ('4DEA13CF-FFD8-4CB6-89AE-A97E964A0B5B', [
        #     # '0A772895-1439-4871-9983-48E227E58C56',
        #     #                                       '8481E773-8D91-41E2-97F6-D297C8593311',
        #                                           '007812DD-5195-4DB5-85A9-A6315454C798',
        #                                           '1D6F0339-988D-4A9B-8935-13FCD93762D2',
        #                                           '9782126A-AC0B-47C1-A362-8A08CFAC4529'
        #                                           ]),

        # ('0BD1A441-BEFD-4688-9350-64D6412B2ABB', ['5C3E76DA-0896-4098-8382-558F182F56B1']),
        # ('BE989AAD-40D1-44AC-8B7B-B94D95150C07', ['D32EB976-2309-46B0-B8B9-92C0CC4AE949'])

        # ('0BD1A441-BEFD-4688-9350-64D6412B2ABB', [])
        # 'BE989AAD-40D1-44AC-8B7B-B94D95150C07'
        # '941B1EFF-D760-4CBA-BDDC-6C687222ABFD'
        # '57F0C987-2B6D-4CBD-A0F3-39A461388F6F'
        # '6A099B1C-CE97-44DC-A6E4-5D498D822992')

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
