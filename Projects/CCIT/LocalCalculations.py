
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.CCIT.Calculations import Calculations
# from Projects.CCIT.SceneKpis.SceneCalculations import SceneCalculations


# if __name__ == '__main__':
#     LoggerInitializer.init('ccit calculations')
#     Config.init()
#     project_name = 'ccit'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0CEEC13D-BDF7-4922-82D0-AA2880269A03'
# #     data_provider.load_session_data(session)
# #     output = Output()
# #     Calculations(data_provider, output).run_project_calculations()
#     scenes = [2, 3]
#     for scene in scenes:
#         data_provider.load_scene_data(session, scene)
#         SceneCalculations(data_provider).calculate_kpis()
