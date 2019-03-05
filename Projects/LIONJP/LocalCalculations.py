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
#         '1C74055B-E079-4FD9-8DC6-72B10B6723EC',
#         '9C3B6415-5732-4C59-A588-28EF1A1D5DD7',
#         '8B6C334E-649D-453B-A508-3D2CDC483FE4',
#
#     ]
#     for session in sessions:
#         print "Running for {}".format(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
