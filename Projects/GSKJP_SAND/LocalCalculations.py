#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.GSKJP_SAND.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gskjp-sand calculations')
#     Config.init()
#     project_name = 'gskjp-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b953d4c7-c5c3-4727-b3d9-f17d29b58602'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
