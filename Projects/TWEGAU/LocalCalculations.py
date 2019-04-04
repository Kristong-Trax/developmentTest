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
#         '1D599FA2-F976-40C5-8F16-4083379DD6CA',
#         '005AC4A4-7031-43DF-A545-90702E99056E',
#         'CD88A798-0F99-4E7C-B077-5F0238AD9754',
#         'CC724EC9-AFAD-492B-8309-34CDAFF59291',
#         'CB790711-CC7B-40DE-87E4-83039A27A6C3',
#         '5366EF45-19C8-4681-A125-AE79FAA175F0',
#         '25E48E96-7C20-4137-B21D-0A9E905AA11A'
#     ]
#
#     for session in sessions:
#         print "Running session >>", session
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#         print "*******************************"
