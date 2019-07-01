#
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.DIAGEOUS.Calculations import DIAGEOUSCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageous calculations')
#     Config.init()
#     project_name = 'diageous'
#     sessions = [
#         # '5C8B3DEE-0F6D-4D30-9756-6BBAB250D4BE',
#         '2C4D043B-3C74-4DBE-8DFD-2240F1A38A7D'
#     ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         DIAGEOUSCalculations(data_provider, output).run_project_calculations()
