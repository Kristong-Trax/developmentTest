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
#     sessions = ["20A34BF0-0E1E-4ED4-A18A-B5EF47AC6361",
#                 "851762DC-A2D9-41B8-84BE-8E32F680AC9B"]
#
#     for session in sessions:
#         print "Running for {}".format(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
