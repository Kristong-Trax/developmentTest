# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.NESTLEBR.Calculations import Calculations
#
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('nestlebr calculations')
#     Config.init()
#     project_name = 'nestlebr'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['cd600ac6-cee5-496a-b2d7-c323b83caee9',
#                 'fc0510d9-4a9b-4869-81cf-d3fc28c640f4']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
