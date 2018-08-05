#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.HEINEKENTW.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('heinekentw calculations')
#     Config.init()
#     project_name = 'heinekentw'
#     data_provider = KEngineDataProvider(project_name)
#     session = '1A9A4F96-64CE-4589-9F2F-0D1617DE6A23'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
