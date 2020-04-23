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
        ('FF240BF7-8FDE-4C20-A0EE-0F2382651045', ['2181A72D-8EA2-4AED-A6FD-27D30AC52E37']),
        ('004B6908-191C-46AC-BDAD-184CEFD8D37A', [
                                                  '0F93BD58-F7A1-4ED2-A982-542457362B7D',   # '2020-03-18', 'Pain Main Shelf'
                                                  '1C7F0538-C61A-457D-AF66-196ABC966520',   # '2020-03-18', 'Wellness Main Shelf'
                                                  'B564227A-C5D4-4224-BD9A-4BAE464CB0D2',   # '2020-03-18', 'Pain Main Shelf'
                                                  '10E5FEB2-9611-446A-8768-0BCD7283CDC4'
                                                ]),  # '2020-03-18', 'Pain Main Shelf'
        ('0AE386E9-8BE4-4DEC-9D99-CB96999FB92A', [
                                                  '78B6AFC3-4676-4C7E-9620-FBAF4A5E1785',   # '2020-03-18', 'Wellness Main Shelf'
                                                  'CB5BE93D-7BB6-4763-AB6C-7D004E06025D',   # '2020-03-18', 'Pain Main Shelf'
                                                  '9833BE9A-1CEA-4AD1-8A06-6139F4B7DE4B',   # '2020-03-18', 'Pain Main Shelf'
                                                  'CF86831A-DAD4-47FA-B461-B387CED6FBA3'
                                                ]),  # '2020-03-18', 'Pain Main Shelf'
        ('1B0750F4-EBC1-4692-97C3-9C0291F3F485', [
                                                  '1FE9A8EB-60F0-4558-8CCA-8347303F58E6',   # '2020-03-18', 'Wellness Main Shelf'
                                                  '797E8F23-F7AA-4EA4-94B7-406A0E3016E1',   # '2020-03-18', 'Pain Main Shelf'
                                                  'CFF314D7-5346-4FF7-A69C-1FAF05EE1481',   # '2020-03-18', 'Pain Main Shelf'
                                                  '63364D4F-F8C7-4525-838A-24FF4C3526A9'
                                                ])  # '2020-03-18', 'Pain Main Shelf'
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
