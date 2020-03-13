#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.GSKAU.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gskau calculations')
#     Config.init()
#     project_name = 'gskau'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'C1E5E425-4681-4512-864B-05C0ECEE3843'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
