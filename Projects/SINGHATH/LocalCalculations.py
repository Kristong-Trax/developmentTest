#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.SINGHATH.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     Config.init()
#     project_name = 'singhath'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         'dc692d9f-1a94-4efb-af47-178b354e3183',
#     ]
#     for session in sessions:
#         print "Running session >>", session
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#         print "*******************************"
