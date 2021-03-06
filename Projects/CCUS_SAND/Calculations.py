from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.CCUS_SAND.KPIGenerator import CCUS_SANDGenerator
# from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock
# from Projects.CCUS_SAND.SceneKpis.SceneCalculations import CCUS_SANDSceneCalculations

__author__ = 'ortal_shivi'


class CCUS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUS_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccus-sand calculations')
#     Config.init()
#     project_name = 'ccus-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session_and_scenes = {
#         "00004868-bde7-4495-9d09-3036d1f26b5b": [863425, 863470, 863543, 863545],
#         "00006BEC-3629-4ED3-BCB9-0242A3C90472":	[1300604, 1300619, 1300647,	1300665, 1300683, 1300695, 1300704,	1300708, 1300720, 1300737, 1300754, 1300765, 1300783, 1300804, 1300848, 1300853, 1300877, 1300882, 1300920, 1300970, 1301017, 1301034],
#         "0000BD22-F95F-4A26-A9B0-70420C671AA3":	[914219, 914221, 914222, 914230, 914232, 914234, 914235],
#     }
#     for session in session_and_scenes.keys():
#         for scene in session_and_scenes[session]:
#             data_provider.load_scene_data(session, scene)
#             CCUS_SANDSceneCalculations(data_provider).calculate_kpis()
#         # data_provider.load_session_data(session)
#         # output = Output()
#         # CCUS_SANDCalculations(data_provider, output).run_project_calculations()
