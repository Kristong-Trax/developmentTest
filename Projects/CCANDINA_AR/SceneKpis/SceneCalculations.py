from Projects.CCANDINA_AR.KPISceneGenerator import SceneGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass

__author__ = 'Nicolas'


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGenerator(self._data_provider, [])

    def calculate_kpis(self):
        self.scene_generator.main_function()

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.CCANDINA_AR.Calculations import Calculations
#
#
# def save_scene_item_facts_to_data_provider(data_provider, output):
#     scene_item_facts_obj = output.get_facts()
#     if scene_item_facts_obj:
#         scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
#     else:
#         scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
#     scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
#     data_provider.set_scene_item_facts(scene_item_facts)
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('ccandinaar calculations')
#     Config.init()
#     project_name = 'ccandinaar'
#     data_provider = KEngineDataProvider(project_name)
#     sessions_data = {
#         '713cde0c-ea84-4c77-a039-efe6f637d6cc': [27986,27990,27992,27993,27995],
#     }
#     for session in sessions_data:
#         for scene in sessions_data[session]:
#             print('Calculating scene id: ' + str(scene))
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene)
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
