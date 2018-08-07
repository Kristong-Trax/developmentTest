
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.CCBZA_SAND.Calculations import CCBZA_SANDCalculations


# if __name__ == '__main__':
#     LoggerInitializer.init('ccbza-sand calculations')
#     Config.init()
#     project_name = 'ccbza-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()


# if __name__ == '__main__':
#     LoggerInitializer.init('ccbza-sand calculations')
#     Config.init()
#     project_name = 'ccbza-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'AD29338A-C2D9-4486-BD94-7B1E32224A11'
# #     data_provider.load_session_data(session)
# #     output = Output()
# #     CCBZA_SANDCalculations(data_provider, output).run_project_calculations()
#     scenes = [2, 3]
#     for scene in scenes:
#         data_provider.load_scene_data(session, scene)
#         CCBZA_SANDCalculations(data_provider).run_scene_calculations()