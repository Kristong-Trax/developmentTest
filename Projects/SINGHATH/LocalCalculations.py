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
#         '0b2bc3c7-9e9d-4de2-8ef9-4b31bcfc03ed',
#         'b1c87d1b-3661-4dcf-b18d-fe8c827a1fa2',
#     ]
#     for session in sessions:
#         print "Running session >>", session
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#         print "*******************************"
