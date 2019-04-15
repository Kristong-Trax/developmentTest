import pandas as pd
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.PNGCN_PROD.KPISceneGenerator import SceneGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data



class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.data_provider = data_provider
        self.scene_generator = SceneGenerator(self._data_provider)
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_kpis(self):
        # self.timer.start()
        self.scene_generator.PngcnSceneKpis()
        # self.timer.stop('KPIGenerator.run_project_calculations')

def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)

# if __name__ == '__main__':
#     LoggerInitializer.init('pngcn calculations')
#     Config.init()
#     project_name = 'pngcn-prod'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'ebebc629-6b82-4be8-a872-0caa248ea248'
#
#     scenes = [16588190]
#     for scene in scenes:
#         data_provider.load_scene_data(session, scene)
#         output = VanillaOutput()
#         SceneVanillaCalculations(data_provider, output).run_project_calculations()
#         save_scene_item_facts_to_data_provider(data_provider, output)
#         SceneCalculations(data_provider).calculate_kpis()
