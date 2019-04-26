#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Projects.SINGHATH_SAND.Calculations import Calculations
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#
# if __name__ == '__main__':
#     # LoggerInitializer.init('singha sand calculations')
#     Config.init()
#     project_name = 'singhath-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         '0b2bc3c7-9e9d-4de2-8ef9-4b31bcfc03ed',
#         '167fd775-f44d-42df-a84b-addecc733165',
#         '80b5eb67-2517-4158-bde9-a1ec28f2c19a',
#         '8739c6f4-7e37-4d12-a91a-9357de3e08a7',
#         'b1c87d1b-3661-4dcf-b18d-fe8c827a1fa2',
#     ]
#     for session in sessions:
#         print "Running session >>", session
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#         print "*******************************"
