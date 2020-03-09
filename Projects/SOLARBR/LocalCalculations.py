#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.SOLARBR.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('solarbr calculations')
#     Config.init()
#     project_name = 'solarbr'
#     data_provider = KEngineDataProvider(project_name)
#     # session =  '8879bfe9-5c5e-441d-8984-21fdcc1e4bad'
#
#
#     sessions = ['ed15c8b9-9bd6-49a1-a7d6-a3cfe624a299']
#
#
#     for session in sessions:
#         print(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
