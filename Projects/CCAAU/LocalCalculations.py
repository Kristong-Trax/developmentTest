#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.CCAAU.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('ccaau calculations')
#     Config.init()
#     project_name = 'ccaau'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'FEB549E1-DFB7-4B8E-95BF-587C329F2468'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
