from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.CCUS.KPIGenerator import CCUSGenerator


__author__ = 'ortal_shivi_Jasmine'


class CCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.CCUS.SceneKpis.SceneCalculations import SceneCalculations
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
# if __name__ == '__main__':
#     LoggerInitializer.init('ccus calculations')
#     Config.init()
#     project_name = 'ccus-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session_and_scenes = {
#         # "00004868-bde7-4495-9d09-3036d1f26b5b": [863470, 863543, 863545],
#         # "00006BEC-3629-4ED3-BCB9-0242A3C90472":	[1300604, 1300619, 1300647,	1300665, 1300683, 1300695, 1300704,	1300708, 1300720, 1300737, 1300754, 1300765, 1300783, 1300804, 1300848, 1300853, 1300877, 1300882, 1300920, 1300970, 1301017, 1301034],
#         # "0000BD22-F95F-4A26-A9B0-70420C671AA3":	[914219, 914221, 914222, 914230, 914232, 914234, 914235],
#         "8395fc95-465b-47c2-ad65-6d10de13cd75":	{'scene1':[10474779, '88a6cdff-4215-4efd-a7a5-23da566ab62f'],
#                                                 'scene2':[10420017, '723eb38f-e241-4718-a8e1-0ff8d8cc1a1f']},
#     }
#     for session in session_and_scenes.keys():
#         for scene in session_and_scenes[session]:
#             data_provider.load_scene_data(session, session_and_scenes[session][scene][0],session_and_scenes[session][scene][1])
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
#
#         data_provider.load_session_data(session)
#         output = Output()
#         CCUSCalculations(data_provider, output).run_project_calculations()
