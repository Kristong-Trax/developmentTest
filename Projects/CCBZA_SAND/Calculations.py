
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCBZA_SAND.KPIGenerator import CCBZA_SANDGenerator

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass


__author__ = 'natalyak'


class CCBZA_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBZA_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

class CCBZA_SANDSceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(CCBZA_SANDSceneCalculations, self).__init__(data_provider)
        self.scene_generator = CCBZA_SANDGenerator(self._data_provider)

    def run_scene_calculations(self):
        # self.timer.start()
        self.scene_generator.main_scene_function()
        # self.timer.stop('KPIGenerator.run_project_calculations')

    @staticmethod
    def save_scene_item_facts_to_data_provider(data_provider, output):
        scene_item_facts_obj = output.get_facts()
        if scene_item_facts_obj:
            scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
        else:
            scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
        scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
        data_provider.set_scene_item_facts(scene_item_facts)

if __name__ == '__main__':
    LoggerInitializer.init('ccbza-sand calculations')
    Config.init()
    project_name = 'ccbza-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'AD29338A-C2D9-4486-BD94-7B1E32224A11'
        # 'E6BBF9D5-114E-4176-A35E-B84ABD0C11B5'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        CCBZA_SANDCalculations(data_provider, output).run_project_calculations()
