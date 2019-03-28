#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.DIAGEOUS_SAND.Calculations import DIAGEOUS_SANDCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageous calculations')
#     Config.init()
#     project_name = 'diageous-sand'
#     sessions = [
#         "A27A5DEB-E598-45A3-ADAE-F56A2F0FA320",
#     ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         DIAGEOUS_SANDCalculations(data_provider, output).run_project_calculations()
