#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.JRIJP.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('jrijp calculations')
#     Config.init()
#     project_name = 'jrijp'
#     data_provider = KEngineDataProvider(project_name)
#
#     sessions = [
#         # 'EBA5C272-6360-461A-A14C-9383008AEB25',  # no scenes
#         # '90e21abd-f9b0-4fed-b13d-439721f83f7e',  # no scenes
#         # '74b4993a-f13f-11e8-9ba7-1253817b9c00',  # no scene item facts
#         "DB8BED93-422D-44DB-AB6A-E04390E22F12",
#         "E8A57D54-4B31-4197-BCEC-8448E2595A71",
#         "DAACC387-4209-4FDD-8E57-A4A19B3C7999",
#     ]
#     for session in sessions:
#         print "Running for {}".format(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
