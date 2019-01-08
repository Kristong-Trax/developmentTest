#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.NESTLEBR.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('nestlebr calculations')
#     Config.init()
#     project_name = 'nestlebr'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'be9c32e1-97d9-4a00-80e9-2f012b5d60de'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
