import pandas as pd
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
# from Projects.CCIT.KPIGenerator import Generator
from Projects.PNGCN_SAND.KPISceneGenerator import SceneGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.PNGCN_SAND.Calculations import CCBZA_SANDCalculations, CCBZA_SANDSceneCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations



class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGenerator(self._data_provider)
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_scene_kpis(self):
        # self.timer.start()
        self.scene_generator.scene_share_of_display()
        # self.timer.stop('KPIGenerator.run_project_calculations')

def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('pngcn-sand calculations')
    Config.init()
    project_name = 'pngcn-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '78A1F5E7-A28A-4ED2-9D69-F233DB081099'

    scenes = [5410946]
    # 5410927,5410946,5410962,5410975,5410992,5411003,5411020,5411045,5411064,5411082,5411106,5411115,
    #           5411184,5411220,5411263]
    for scene in scenes:
        data_provider.load_scene_data(session, scene)
        output = VanillaOutput()
        SceneVanillaCalculations(data_provider, output).run_project_calculations()
        save_scene_item_facts_to_data_provider(data_provider, output)
        SceneCalculations(data_provider).calculate_scene_kpis()
