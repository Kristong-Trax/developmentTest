#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.HEINEKENTW_SAND.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('heinekentw-sand calculations')
#     Config.init()
#     project_name = 'heinekentw-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'BADAA71D-C8F9-4042-B1DE-E0D7642A965B'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
