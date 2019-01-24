# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.MARSRU_SAND.Calculations import MARSRU_SANDCalculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('MARSRU calculations')
#     Config.init()
#     project_name = 'marsru2-sand'
#     session_uids = [
#         'fedf7bf0-eab8-48f0-bd57-268b74eedc3c',
#         'f864e7b6-1e2d-4254-8a41-2bbbc7bdb1ae',
#         'fe73636c-c26a-41bf-9367-9414088e8904',
#         '3572b477-ed1c-47e7-871c-acf5dc8f963c',
#         '9a748454-96fe-4dda-a95a-e6b96ca00c31',
#         'c20fb9c7-b0ff-4525-85d9-1194424bda8f',
#         '4222fcc4-db17-4300-a9ac-c105b9ea7107',
#         '1f779b63-0c2f-4ebb-89a5-e7df4376a560',
#         # 'ffca80a8-917a-4091-8216-cd8e1393d235',
#         # 'ff69dea6-b0df-4a81-bed7-94c2011c8c18'
#
#         # '8a3d85bf-b31c-4951-9c1a-afdc79b03ba2',
#         # '955d47a6-5c8d-4f83-9646-2f9b4f0c46b9',
#         #
#         # '265666b4-07ca-4529-88d2-bd686c44786a',
#         # 'f2c37663-24e5-4942-bea0-eabb03f9704b',
#         # 'cf6ec220-ec7c-4dfa-8538-5d28c749ff47',
#         # '1734f51f-4ab4-477c-98dc-37ee72b825b4',
#         # '09a432be-6793-4e65-8368-332eae1d51ee',
#         #
#         # '74c19ca8-9a73-4115-9ab8-d5b7caeacf80',
#         # '479ec010-5432-4ecb-bcf6-2204d8c609ec',
#         # '2af45d57-4dcb-4dea-a07c-dfc5e96c4a93',
#         # '7570a6b0-cf01-426c-9f0a-47a94805616d',
#         # '8fed9c54-366f-4c77-b991-1a60c6864759',
#     ]
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         MARSRU_SANDCalculations(data_provider, output).run_project_calculations()
#
