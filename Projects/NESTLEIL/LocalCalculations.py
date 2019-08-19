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
#     session = 'ad083ee9-71b6-4b62-8168-8e4015635799'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
