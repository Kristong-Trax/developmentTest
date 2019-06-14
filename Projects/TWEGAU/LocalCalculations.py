#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.TWEGAU.Calculations import Calculations
#
# if __name__ == '__main__':
#     LoggerInitializer.init('twegau calculations')
#     Config.init()
#     project_name = 'twegau'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         '37CB709B-E44F-4250-AC7F-39A540C38AC7',
#         'CE422F5F-24C3-4160-93F2-F06E6D55B6BF',
#         '0E18536A-352D-4EA6-A3A8-E33A2C8BAE35',
#         '25E48E96-7C20-4137-B21D-0A9E905AA11A',
#         'D7FE3ED4-F996-4C35-9AEE-0509FA0E2403'
#     ]
#
#     for session in sessions:
#         print "Running session >>", session
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#         print "*******************************"
