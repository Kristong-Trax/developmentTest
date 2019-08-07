#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.NESTLEIL.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('nestleil calculations')
#     Config.init()
#     project_name = 'nestleil'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'fe2fed63-7043-49cb-a862-d3c017b44f31'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
