import pandas as pd
from collections import OrderedDict
from Projects.GSKAU.KPIGenerator import Generator
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.GSKAU.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

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
    LoggerInitializer.init('gskau calculations')
    Config.init()
    project_name = 'gskau'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        ('9C40E0FC-A4B1-47C9-BE13-BBC496145CB7', [
            '3C29C4A0-D7DF-4F85-BCB5-447AFE938C1C',
            '11621ED5-7968-4D11-9BF5-CB8565743719',
            'C3ED0FE6-0E71-4D62-A9B6-1EC61D43D957',
            'F14299DF-7BB6-4789-980E-F866BA29AD47'

        ]),
    ])

    # for session, scenes in session_scene_map.iteritems():
    #     for e_scene in scenes:
    #         print "\n"
    #         data_provider = KEngineDataProvider(project_name)
    #         data_provider.load_scene_data(session, scene_uid=e_scene)
    #         Log.info("**********************************")
    #         Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
    #         Log.info("**********************************")
    #         output = VanillaOutput()
    #         SceneVanillaCalculations(data_provider, output).run_project_calculations()
    #         save_scene_item_facts_to_data_provider(data_provider, output)
    #         SceneCalculations(data_provider).calculate_kpis()

    sessions = [
        '9C40E0FC-A4B1-47C9-BE13-BBC496145CB7'
    ]
    # RUN FOR Session level KPIs
    data_provider = KEngineDataProvider(project_name)

    for sess in sessions:
        Log.info("[Session level] Running for session: {}".format(sess))
        data_provider.load_session_data(sess)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
