import pandas as pd
from collections import OrderedDict

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Projects.CCJP.Calculations import Calculations
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.CCJP.SceneKpis.SceneCalculations import SceneCalculations
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
    LoggerInitializer.init('CCJP Local calculations')
    Config.init()

    # session level kpi calc
    # project_name = 'ccjp'
    # data_provider = KEngineDataProvider(project_name)
    # # leave empty for all scenes
    # session = 'E731D801-244D-4956-A209-DC83A8AB9053'
    # data_provider.load_session_data(session)
    # output = Output()
    # Calculations(data_provider, output).run_project_calculations()

    # For scene leve kpis
    project_name = 'JRIJP_LOCAL'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        ('0C1CE6BC-6A1C-4BC7-882F-F98DBD5D8FC5', ['A860F4F1-8666-4779-88F2-42E4DD5D4D99']),
        ('FB9CA65B-4AE1-429B-8ABA-FDA440EF4407', ['7096DB40-D7A9-4B4E-B6B8-DBFDF276F560',
                                                  'B1FB3273-81B9-4243-92B8-92778D82BEF5']),
        ('6EE4B140-F8C4-43D9-88A1-7E8A15150730', ['8B7CD90A-CC26-4147-808A-F905F8257FD3',
                                                  '7CA2A00E-1773-4F3F-9E0B-C54CF3A12DBF']),
        ('FA2ED7DE-84C0-4842-B9D3-CC3292EC1293', ['BA2D0EA7-7507-4091-ADBC-06CCB6738CA7',
                                                  'F692BE92-AF27-4272-B4FA-4CFFB914EC55']),
        ('F3CD965D-1A3B-4E51-9266-08995030B5D8', ['024D9DD8-0A83-4AF9-ADCC-2B473EB6C5A3',
                                                  '024D9DD8-0A83-4AF9-ADCC-2B473EB6C5A3'])

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