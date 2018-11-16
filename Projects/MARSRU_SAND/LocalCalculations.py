# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.MARSRU_SAND.Calculations import ProjectCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('MARSRU calculations')
#     Config.init()
#     project_name = 'marsru-sand'
#     session_uids = [
#         '265666b4-07ca-4529-88d2-bd686c44786a',
#         'f2c37663-24e5-4942-bea0-eabb03f9704b',
#         'cf6ec220-ec7c-4dfa-8538-5d28c749ff47',
#         '1734f51f-4ab4-477c-98dc-37ee72b825b4',
#         '09a432be-6793-4e65-8368-332eae1d51ee',
#
#         '74c19ca8-9a73-4115-9ab8-d5b7caeacf80',
#         '479ec010-5432-4ecb-bcf6-2204d8c609ec',
#         '2af45d57-4dcb-4dea-a07c-dfc5e96c4a93',
#         '7570a6b0-cf01-426c-9f0a-47a94805616d',
#         '8fed9c54-366f-4c77-b991-1a60c6864759',
#     ]
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         ProjectCalculations(data_provider, output).run_project_calculations()
#
