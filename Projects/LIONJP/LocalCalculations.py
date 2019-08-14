#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.LIONJP.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('lionjp calculations')
#     Config.init()
#     project_name = 'lionjp'
#     data_provider = KEngineDataProvider(project_name)
#
#     sessions = [
#         '5D2B2887-C7A8-4349-BD08-1C52BA722329',
#
#     ]
#     for session in sessions:
#         print "Running for {}".format(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
